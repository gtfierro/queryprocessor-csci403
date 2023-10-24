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
        Attribute("artist", str),
        Attribute("title", str),
        Attribute("year", int),
        Attribute("genre", str),
    )
)

album_relation = Relation("album", album_schema)
# index the album relation on the year attribute
album_relation.create_index("year")

# emulate HDD
album_relation = album_relation.to_hdd()

# query is SELECT title, year, genre FROM album WHERE year = 1970
query = "SELECT title, year, genre FROM album WHERE year = 1970"
parsed = parse_sql(query)
with open("album.dot", "w") as f:
    f.write(parsed.to_dot())


# simple plan for SELECT title, year, genre FROM album WHERE year = 1970
simple_plan = QueryPlan(
    plan=ProjectOp(
        ["title", "year", "genre"], SelectOp("year == 1970", ScanOp(album_relation))
    )
)

# sort album by year in ascending order before selecting
order_plan = QueryPlan(
    plan=ProjectOp(
        ["title", "year", "genre"],
        SelectOp("year == 1970", OrderByOp("year", ScanOp(album_relation))),
    )
)

# sort album by year in ascending order before selecting, using a order-aware select operator
order_plan2 = QueryPlan(
    plan=ProjectOp(
        ["title", "year", "genre"],
        OrderedSelectOp("year == 1970", OrderByOp("year", ScanOp(album_relation))),
    )
)

# use index scan
order_plan3 = QueryPlan(
    plan=ProjectOp(
        ["title", "year", "genre"],
        OrderByOp("year", IndexScanOp(album_relation, "year", 1970)),
    )
)

plans = {
    "simple": simple_plan,
    "ordered_scan": order_plan,
    "ordered_select": order_plan2,
    "index_scan": order_plan3,
}

results = benchmark([album_relation], plans, max_k=6)
print(results.to_string(index=False))
# save the plot to a png file
sns_plot = sns.lineplot(
    x="size", y="time", hue="plan", data=results, markers=True, dashes=False, marker="o"
)
sns_plot.set(yscale="log")
sns_plot.set(xscale="log")
sns_plot.set(xlabel="Number of tuples", ylabel="Time (s)", title="Album Query (HDD)")
sns_plot.figure.savefig("album.png")
