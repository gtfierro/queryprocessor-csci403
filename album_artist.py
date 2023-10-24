import pandas as pd
import seaborn as sns
from queryprocessor.relation import Relation, Schema, Attribute
from queryprocessor.operator import (
    QueryPlan,
    SelectOp,
    ProjectOp,
    JoinOp,
    RenameOp,
    HashJoinOp,
    OrderByOp,
    OrderedSelectOp,
    ScanOp,
    IndexScanOp,
)
from queryprocessor.benchmark import benchmark
from queryprocessor.parse import parse_sql
import time

album_schema = Schema(
    (
        Attribute("artist_id", int),
        Attribute("title", str),
        Attribute("year", int),
        Attribute("genre", str),
    )
)

album_relation = Relation("album", album_schema)
album_relation.create_index("year")
album_relation.create_index("artist_id")


artist_schema = Schema(
    (
        Attribute("name", str),
        Attribute("id", int),
    )
)
artist_relation = Relation("artist", artist_schema)
artist_relation.create_index("id")

# emulate HDD
album_relation = album_relation.to_ssd()

# query is SELECT title, name, year FROM album JOIn artist ON artist_id = id WHERE year = 1970
query = "SELECT title, name, year FROM album JOIN artist ON artist_id = id WHERE year = 1970"
parsed = parse_sql(query)
with open("album_artist.dot", "w") as f:
    f.write(parsed.to_dot())

# simple plan for SELECT title, name, year FROM album JOIN artist ON artist_id = id WHERE year = 1970
nested_loop_join_plan = QueryPlan(
    plan=SelectOp(
        "year == 1970",
        ProjectOp(
            ["title", "name", "year"],
            JoinOp("artist_id == id", ScanOp(album_relation), ScanOp(artist_relation)),
        ),
    )
)

# use hash join
hash_join_plan = QueryPlan(
    plan=SelectOp(
        "year == 1970",
        ProjectOp(
            ["title", "name", "year"],
            HashJoinOp(
                ("artist_id", "id"), ScanOp(album_relation), ScanOp(artist_relation)
            ),
        ),
    )
)

# select year on album before join
select_first = QueryPlan(
    plan=ProjectOp(
        ["title", "name"],
        JoinOp(
            "artist_id == id",
            SelectOp("year == 1970", ScanOp(album_relation)),
            ScanOp(artist_relation),
        ),
    )
)

# index scan on year
index_scan_plan = QueryPlan(
    plan=SelectOp(
        "year == 1970",
        ProjectOp(
            ["title", "name", "year"],
            JoinOp(
                "artist_id == id",
                IndexScanOp(album_relation, "year", 1970),
                ScanOp(artist_relation),
            ),
        ),
    )
)

# index scan with hashjoin
index_scan_plan2 = QueryPlan(
    plan=SelectOp(
        "year == 1970",
        ProjectOp(
            ["title", "name", "year"],
            HashJoinOp(
                ("artist_id", "id"),
                IndexScanOp(album_relation, "year", 1970),
                ScanOp(artist_relation),
            ),
        ),
    )
)


plans = {
    "nested_loop_join": nested_loop_join_plan,
    "hash_join": hash_join_plan,
    "select_first": select_first,
    "index_scan_naive_join": index_scan_plan,
    "index_scan_hash_join": index_scan_plan2,
}

results = benchmark([album_relation, artist_relation], plans, max_k=4)
print(results.to_string(index=False))
sns_plot = sns.lineplot(
    x="size", y="time", hue="plan", data=results, markers=True, dashes=False, marker="o"
)
sns_plot.set(yscale="log")
sns_plot.set(xscale="log")
sns_plot.set(xlabel="Number of tuples", ylabel="Time (s)", title="Album-Artist Join (HDD)")
sns_plot.figure.savefig("album_artist.png")
