"""
Defines a simple representation of relations and relational algebra for the purposes
of teaching relational algebra and query optimization to undergraduates.
"""
import time
from faker import Faker
from dataclasses import dataclass, field
from typing import Tuple, List, Any, Optional
import random
import bintrees


@dataclass(frozen=True)
class Attribute:
    """Represents an attribute in a relational schema."""
    name: str
    domain: type


@dataclass(frozen=True)
class Schema:
    """Represents a relational schema."""
    attributes: Tuple[Attribute, ...]


@dataclass
class Relation:
    """Represents a relation as a set of tuples."""
    name: str
    schema: Schema
    tuples: List[Tuple] = field(default_factory=list)
    indexes: dict = field(default_factory=dict)  # This will hold our B-Tree indexes
    # sleeptime is used to simulate the time it takes to read a tuple from disk
    # 0.0 = in-memory, 0.01 = SSD, 0.1 = HDD (rough estimates)
    sleeptime: float = 0.0

    def clear(self):
        """Clear the relation of all tuples."""
        self.tuples = []

    def create_index(self, attribute_name: str):
        """Create an index on the specified attribute."""
        attr_position = self._get_attribute_position(attribute_name)
        if attr_position is None:
            raise ValueError(f"No such attribute: {attribute_name}")

        index = bintrees.RBTree()
        for t in sorted(self.tuples, key=lambda t: t[attr_position]):
            key = t[attr_position]
            if key not in index:
                index[key] = []
            index[key].append(t)

        self.indexes[attribute_name] = index

    # iterate over tuples in the relation
    def __iter__(self):
        for t in self.tuples:
            time.sleep(self.sleeptime)
            yield t

    def iter_index(self, attribute_name: str):
        """Iterate over tuples in the relation using an index."""
        index = self.indexes.get(attribute_name)
        if not index:
            raise ValueError(f"No index on attribute: {attribute_name}")
        for key, tuples in index.items():
            time.sleep(self.sleeptime)
            yield from tuples

    def to_hdd(self) -> "Relation":
        """Return a new relation that is stored on disk."""
        return Relation(
            self.name, self.schema, self.tuples, self.indexes, sleeptime=0.1
        )

    def to_ssd(self) -> "Relation":
        """Return a new relation that is stored on SSD."""
        return Relation(
            self.name, self.schema, self.tuples, self.indexes, sleeptime=0.01
        )

    def find_by_index(self, attribute_name: str, value: Any) -> Optional[List[Tuple]]:
        """Find tuples by indexed attribute."""
        index = self.indexes.get(attribute_name)
        if not index:
            raise ValueError(f"No index on attribute: {attribute_name}")
        return index.get(value, [])

    def _get_attribute_position(self, attribute_name: str) -> Optional[int]:
        """Get the position of an attribute in the schema."""
        for i, attr in enumerate(self.schema.attributes):
            if attr.name == attribute_name:
                return i
        return None

    def generate_tuples(self, n: int) -> None:
        """Generate n tuples with random values for each attribute."""
        fake = Faker()

        for _ in range(n):
            tuple_values = []
            for attr in self.schema.attributes:
                if attr.domain is float:
                    tuple_values.append(
                        random.uniform(0.0, 100.0)
                    )  # generates float between 0 and 100
                elif attr.domain is bool:
                    tuple_values.append(random.choice([True, False]))
                elif attr.domain is str and attr.name == "name":
                    tuple_values.append(fake.name())
                elif attr.domain is str and attr.name == "title":
                    tuple_values.append(fake.sentence(nb_words=3))
                elif attr.domain is int and attr.name == "year":
                    tuple_values.append(random.randint(1950, 2020))
                elif attr.domain is int:
                    tuple_values.append(
                        random.randint(0, 100)
                    )  # generates int between 0 and 100
                elif attr.domain is str:
                    tuple_values.append(
                        fake.text(max_nb_chars=20)
                    )  # generates a random text
                # You can add more domain types or adjust attributes based on their names here.

            self.tuples.append(tuple(tuple_values))

        # update indexes
        for attr_name in self.indexes:
            self.create_index(attr_name)


if __name__ == "__main__":
    # define a schema for a 'album' table with (artist, title, year, genre) attributes
    album_schema = Schema(
        (
            Attribute("artist", str),
            Attribute("title", str),
            Attribute("year", int),
            Attribute("genre", str),
        )
    )
    album_relation = Relation("album", album_schema)
    album_relation.generate_tuples(10)
