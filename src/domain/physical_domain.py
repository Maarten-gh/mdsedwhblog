from dataclasses import dataclass, field


@dataclass
class Column:
    name: str
    datatype: str
    nullable: bool
    length: int | None = None
    scale: int | None = None

    @property
    def fulltype(self) -> str:
        fulltype = ""

        if (self.length != None and self.scale != None):
            fulltype = f"{self.datatype}({self.length}, {self.scale})"
        elif (self.length != None):
            fulltype = f"{self.datatype}({self.length})"
        else:
            fulltype = self.datatype

        fulltype += " " + ("NULL" if self.nullable else "NOT NULL")

        return fulltype


@dataclass
class PrimaryKeyConstraint:
    name: str
    column_names: list[str] = field(default_factory=list)


@dataclass
class ForeignKeyConstraint:
    name: str
    foreign_schema_name: str
    foreign_table_name: str
    column_names: list[str] = field(default_factory=list)
    foreign_column_names: list[str] = field(default_factory=list)


@dataclass
class Table:
    name: str
    primary_key_constraint: PrimaryKeyConstraint
    columns: list[Column] = field(default_factory=list)
    foreign_key_constraints: list[ForeignKeyConstraint] = field(default_factory=list)


@dataclass
class Schema:
    name: str
    tables: list[Table] = field(default_factory=list)


@dataclass
class PhysicalModel:
    schemas: list[Schema] = field(default_factory=list)