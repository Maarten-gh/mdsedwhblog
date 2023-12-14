from dataclasses import dataclass, field

from .physical_domain import PhysicalModel, Schema, Table, Column


@dataclass
class ColumnMapping:
    target_column: Column
    source_column: Column | None
    expression: str | None = None
    

@dataclass
class TableLoadStep:
    description: str
    column_mappings: list[ColumnMapping] = field(default_factory=list)

@dataclass
class TableMapping:
    source_table: Table
    target_table: Table

    load_steps: list[TableLoadStep] = field(default_factory=list)


@dataclass
class SchemaMapping:
    source_schema: Schema
    target_schema: Schema

    table_mappings: list[TableMapping] = field(default_factory=list)


@dataclass
class PhysicalModelMapping:
    source_model: PhysicalModel
    target_model: PhysicalModel

    schema_mappings: list[SchemaMapping] = field(default_factory=list)
