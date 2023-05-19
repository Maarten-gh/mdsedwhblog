from dataclasses import dataclass, field

from .physical_domain import PhysicalModel, Schema, Table, Column


@dataclass
class ColumnMapping:
    source_column: Column
    target_column: Column


@dataclass
class TableMapping:
    source_table: Table
    target_table: Table

    column_mappings: list[ColumnMapping] = field(default_factory=list)


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
