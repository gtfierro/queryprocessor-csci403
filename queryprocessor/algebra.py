"""
Define the relational algebra DAG for a query
"""

from queryprocessor.relation import Relation


class AlgebraNode(object):
    """
    Base class for all relational algebra nodes
    """

    def __init__(self, children=None):
        if children is None:
            children = []
        self.children = children

    def __repr__(self):
        return self.__class__.__name__

    def dump_ast(self, indent=0):
        """
        Dump the AST to stdout
        """
        print(" " * indent + str(self))
        for child in self.children:
            child.dump_ast(indent + 2)

    def to_dot(self):
        """
        Convert the AST to DOT format, using a helper method
        """
        return "digraph G {\n" + self._to_dot() + "}\n"

    def _to_dot(self):
        """
        Convert the AST to DOT format
        """
        dot = ""
        for child in self.children:
            dot += "  " + f'"{str(self)}"' + " -> " + f'"{str(child)}"' + "\n"
            dot += child._to_dot()
        return dot


class Scan(AlgebraNode):
    """
    Scan a table
    """

    def __init__(self, table: Relation):
        super(Scan, self).__init__()
        self.table = table

    def __repr__(self):
        return "Scan({})".format(self.table)


class Select(AlgebraNode):
    """
    Select rows from a table
    """

    def __init__(self, predicate, child):
        super(Select, self).__init__([child])
        self.predicate = predicate

    def __repr__(self):
        return "Select({})".format(self.predicate)


class Project(AlgebraNode):
    """
    Project columns from a table
    """

    def __init__(self, columns, child):
        super(Project, self).__init__([child])
        self.columns = columns

    def __repr__(self):
        return "Project({})".format(self.columns)


class Join(AlgebraNode):
    """
    Join two tables
    """

    def __init__(self, predicate, left_child, right_child):
        super(Join, self).__init__([left_child, right_child])
        self.predicate = predicate

    def __repr__(self):
        return "Join({})".format(self.predicate)


class Rename(AlgebraNode):
    """
    Rename a table and its attributes
    """

    def __init__(self, table_name, columns, child):
        super(Rename, self).__init__([child])
        self.table_name = table_name
        self.columns = columns

    def __repr__(self):
        return "Rename({})".format(self.table_name)


class GroupBy(AlgebraNode):
    """
    Group rows by a set of columns
    """

    def __init__(self, columns, child):
        super(GroupBy, self).__init__([child])
        self.columns = columns

    def __repr__(self):
        return "GroupBy({})".format(self.columns)


class Aggregate(AlgebraNode):
    """
    Aggregate over a set of columns
    """

    def __init__(self, columns, child):
        super(Aggregate, self).__init__([child])
        self.columns = columns

    def __repr__(self):
        return "Aggregate({})".format(self.columns)


class OrderBy(AlgebraNode):
    """
    Order rows by a set of columns
    """

    def __init__(self, columns, child):
        super(OrderBy, self).__init__([child])
        self.columns = columns

    def __repr__(self):
        return "OrderBy({})".format(self.columns)


class Limit(AlgebraNode):
    """
    Limit the number of rows
    """

    def __init__(self, limit, child):
        super(Limit, self).__init__([child])
        self.limit = limit

    def __repr__(self):
        return "Limit({})".format(self.limit)


class Union(AlgebraNode):
    """
    Union two tables
    """

    def __init__(self, left_child, right_child):
        super(Union, self).__init__([left_child, right_child])

    def __repr__(self):
        return "Union"


class Difference(AlgebraNode):
    """
    Difference two tables
    """

    def __init__(self, left_child, right_child):
        super(Difference, self).__init__([left_child, right_child])

    def __repr__(self):
        return "Difference"


class Intersect(AlgebraNode):
    """
    Intersect two tables
    """

    def __init__(self, left_child, right_child):
        super(Intersect, self).__init__([left_child, right_child])

    def __repr__(self):
        return "Intersect"
