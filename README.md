# Relational Query Processor

This repository provides a *basic* implementation of relational algebra to enable exploration
of different aspects of a relational query processing engine.

The implementation is distributed over the following files:

- `queryprocessor/relation.py`: implements the structure of relations in a database:
    - Attribute: Defines individual attributes with names and data types.
    - Schema: Describes a set of attributes, akin to a table structure.
    - Relation: Represents a table with methods to manage tuples, simulate storage types (memory, SSD, HDD), and create/search indexes.
- `queryprocessor/algebra.py`: represents a query tree consisting of relational algebra operators. This only represents the structure of the tree -- not the implementation of the individual operators.
- `queryprocessor/operator.py`: implements several relational algebra operators to illustrate differentiation in performance and complexity.
    Contains a `QueryPlan` object which implements a basic "execute" function to pull tuples out of the query plan.
- `queryprocessor/benchmark.py`: emits a Pandas DataFrame containing benchmarking results for running a set of query plans on a set of relations

The `operator.py` file implements the following relational algebra operators:
- Scan
    - `ScanOp`: linear scan over relation
    - `IndexScanOp`: scans tuples which have a given value in an indexed column
- Select
    - `SelectOp`: naive selection; uses a Python expression as the predicate so use `year == 1970` instead of `year = 1970`
    - `OrderedSelectOp`: takes advantage of a sorted relation to skip rows on an equality selection
- Project
    - `ProjectOp`: simple projection operator
- Join
    - `JoinOp`: nested loop join
    - `HashJoinOp`: hash join with build and probe phases
- Rename
    - `RenameOp`: simple rename operator
- OrderByOp
    - `OrderByOp`: simple ordering operator; materializes the full underlying relation to perform the sort


The following features are not included in this repository:
- SQL parser
- query planner
