import pandas as pd
from queryprocessor.relation import Relation
from queryprocessor.operator import QueryPlan
import time
from typing import Dict, List


def benchmark(
    relations: List[Relation], plans: Dict[str, QueryPlan], max_k=5
) -> pd.DataFrame:
    """
    Benchmark the execution of a query plan against a relation.
    Log the seconds+milliseconds to run the query as the size of the relation increases
    """

    relation_sizes = map(lambda k: 10**k, range(1, max_k + 1))
    timings = []

    for size in relation_sizes:
        print(f"Running benchmark for relation size {size}")
        for relation in relations:
            relation.clear()
            relation.generate_tuples(size)

        for name, plan in plans.items():
            # skip nested loop join for large relations
            if name == "nested_loop_join" and size >= 10000:
                continue
            print(f"\tRunning benchmark for query {name}")
            start = time.time()
            rows = list(plan.execute())
            end = time.time()
            timings.append(
                {"rows": len(rows), "size": size, "time": end - start, "plan": name}
            )

    return pd.DataFrame(timings)
