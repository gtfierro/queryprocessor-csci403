"""
Provides simple implementations of the relational algebra operators. 
"""
import time
from queryprocessor.algebra import Scan, Select, Project, Join, Rename, AlgebraNode
from queryprocessor.parse import parse_sql
from queryprocessor.relation import Relation, Schema, Attribute
from dataclasses import dataclass
from typing import Optional, Any, Tuple


class QueryPlan(object):
    """
    Takes an algebra node and generates a query plan from the node.
    """

    def __init__(
        self, plan: "Operator"
    ):
        self.plan = plan

    def execute(self):
        """Execute the query plan"""
        for row in self.plan:
            yield row


class Operator:
    """base node for all operators in a relational query plan"""

    def __init__(self):
        self.child: Optional[Operator] = None

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return self.__repr__()

    def __rshift__(self, other):
        """Connect this operator to another operator"""
        self.child = other
        return other

    def to_dot(self):
        """Generate a DOT representation of the query plan rooted at this operator"""
        return f"digraph G {{\n{self._dot()}\n}}"

    def _dot(self):
        """Generate a DOT representation of the query plan rooted at this operator"""
        raise NotImplementedError


class ScanOp(Operator):
    """Scan a table"""

    def __init__(self, table: Relation):
        super(ScanOp, self).__init__()
        self.table = table

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        for row in self.table.tuples:
            # yield dictionary of attribute name to value
            yield {
                attr.name: value
                for attr, value in zip(self.table.schema.attributes, row)
            }

    def __repr__(self):
        return f"Scan({self.table.name})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n'


class IndexScanOp(Operator):
    """Scan a table using an index for all rows with a given value"""

    def __init__(self, table: Relation, index_name: str, value: Any = None):
        super(IndexScanOp, self).__init__()
        self.table = table
        self.index_name = index_name
        self.value = value

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        for row in self.table.find_by_index(self.index_name, self.value):  # type: ignore
            # yield dictionary of attribute name to value
            yield {
                attr.name: value
                for attr, value in zip(self.table.schema.attributes, row)
            }

    def __repr__(self):
        return f"IndexScan({self.table.name}, {self.index_name})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n'


class SelectOp(Operator):
    """Select rows from a table"""

    def __init__(self, predicate, child):
        super(SelectOp, self).__init__()
        self.predicate = predicate
        self.child = child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        for row in self.child:  # type: ignore
            if eval(self.predicate, globals(), row):
                yield row

    def __repr__(self):
        return f"Select({self.predicate})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n' + self.child._dot()


class OrderedSelectOp(Operator):
    """Select rows from a table. Find the first row that satisfies the predicate,
    then yield all rows until the predicate is no longer satisfied."""

    def __init__(self, predicate, child):
        super(OrderedSelectOp, self).__init__()
        self.predicate = predicate
        self.child = child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        found = False
        for row in self.child:  # type: ignore
            if eval(self.predicate, globals(), row):
                found = True
                yield row
            elif found:
                break

    def __repr__(self):
        return f"Select({self.predicate})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n' + self.child._dot()


class ProjectOp(Operator):
    """Project columns from a table"""

    def __init__(self, columns, child):
        super(ProjectOp, self).__init__()
        self.columns = columns
        self.child = child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        for row in self.child:  # type: ignore
            yield {column: row[column] for column in self.columns}

    def __repr__(self):
        return f"Project({self.columns})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n' + self.child._dot()


class JoinOp(Operator):
    """Join two tables"""

    def __init__(self, predicate, left_child, right_child):
        super(JoinOp, self).__init__()
        self.predicate = predicate
        self.left_child = left_child
        self.right_child = right_child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        # nested loop join
        for left_row in self.left_child:
            for right_row in self.right_child:
                if eval(self.predicate, globals(), {**left_row, **right_row}):
                    yield {**left_row, **right_row}

    def __repr__(self):
        return f"Join({self.predicate})"

    def _dot(self):
        return (
            f'  "{str(self)}" [shape=box]\n'
            + self.left_child._dot()
            + self.right_child._dot()
        )


class HashJoinOp(Operator):
    """Hash join two tables"""

    def __init__(self, predicate: Tuple[str, str], left_child, right_child):
        super(HashJoinOp, self).__init__()
        self.predicate = predicate
        self.left_child = left_child
        self.right_child = right_child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        # build hash table
        hash_table = {}
        for row in self.left_child:
            key = row[self.predicate[0]]
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(row)

        # probe hash table
        for row in self.right_child:
            key = row[self.predicate[1]]
            if key in hash_table:
                for left_row in hash_table[key]:
                    yield {**left_row, **row}

    def __repr__(self):
        return f"HashJoin({self.predicate})"

    def _dot(self):
        return (
            f'  "{str(self)}" [shape=box]\n'
            + self.left_child._dot()
            + self.right_child._dot()
        )


class RenameOp(Operator):
    """Rename a table and its attributes"""

    def __init__(self, table_name, columns, child):
        super(RenameOp, self).__init__()
        self.table_name = table_name
        self.columns = columns
        self.child = child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        for row in self.child:
            # rename the attributes
            row = row.rename(self.columns)
            yield row

    def __repr__(self):
        return f"Rename({self.table_name}, {self.columns})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n' + self.child._dot()


class OrderByOp(Operator):
    """Order a table by a column"""

    def __init__(self, column, child):
        super(OrderByOp, self).__init__()
        self.column = column
        self.child = child

    def __iter__(self):
        """Iterate over the rows produced by this operator"""
        rows = list(self.child)
        rows.sort(key=lambda row: row[self.column])
        for row in rows:
            yield row

    def __repr__(self):
        return f"OrderBy({self.column})"

    def _dot(self):
        return f'  "{str(self)}" [shape=box]\n' + self.child._dot()


if __name__ == "__main__":
    students = Relation(
        "students",
        Schema((Attribute("id", int), Attribute("name", str), Attribute("age", int))),
    )
    students.generate_tuples(10)
    ast = Select("age > 10", Scan(students))
    plan = QueryPlan(ast)
    print(plan.plan.to_dot())
    plan.execute()
