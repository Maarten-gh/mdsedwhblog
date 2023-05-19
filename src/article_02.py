from domain.physical_domain import PhysicalModel, Schema, Table, Column, PrimaryKeyConstraint
from domain.physical_mapping_domain import PhysicalModelMapping, SchemaMapping, TableMapping, ColumnMapping


def transform_pyhical_source_model_to_physical_staging_model_and_mapping(source_model: PhysicalModel) -> tuple[PhysicalModel, PhysicalModelMapping]:
    staging_schemas_and_mappings = [
        transform_source_schema_to_staging_schema_and_mapping(s) for s in source_model.schemas]
    staging_model = PhysicalModel(
        schemas=[ssam[0] for ssam in staging_schemas_and_mappings]
    )
    mapping = PhysicalModelMapping(
        source_model=source_model,
        target_model=staging_model,
        schema_mappings=[ssam[1] for ssam in staging_schemas_and_mappings],
    )
    return (
        staging_model,
        mapping,
    )


def transform_source_schema_to_staging_schema_and_mapping(source_schema: Schema) -> tuple[Schema, SchemaMapping]:
    staging_tables_and_mappings = [
        transform_source_table_to_staging_table_and_mapping(t) for t in source_schema.tables]
    staging_schema = Schema(
        name=f"{source_schema.name}_stg",
        tables=[stam[0] for stam in staging_tables_and_mappings],
    )
    mapping = SchemaMapping(
        source_schema=source_schema,
        target_schema=staging_schema,
        table_mappings=[stam[1] for stam in staging_tables_and_mappings],
    )
    return (
        staging_schema,
        mapping,
    )


def transform_source_table_to_staging_table_and_mapping(source_table: Table) -> tuple[Table, TableMapping]:
    staging_columns_and_mappings = [
        transform_source_column_to_staging_column_and_mapping(c, source_table) for c in source_table.columns]
    staging_table = Table(
        name=source_table.name,
        primary_key_constraint=PrimaryKeyConstraint(
            name=source_table.primary_key_constraint.name,
            column_names=["stg_runId"] +
            source_table.primary_key_constraint.column_names,
        ),
        columns=create_technical_staging_columns(
        ) + [scam[0] for scam in staging_columns_and_mappings],
        foreign_key_constraints=[],
    )
    mapping = TableMapping(
        source_table=source_table,
        target_table=staging_table,
        column_mappings=[scam[1] for scam in staging_columns_and_mappings],
    )
    return (
        staging_table,
        mapping,
    )


def create_technical_staging_columns() -> list[Column]:
    return [
        Column(
            name="stg_timestamp_utc",
            datatype="datetime2",
            nullable=False,
        ),
        Column(
            name="stg_runId",
            datatype="uniqueidentifier",
            nullable=False,
        ),
    ]


def transform_source_column_to_staging_column_and_mapping(source_column: Column, source_table: Table) -> tuple[Column, ColumnMapping]:
    staging_column = Column(
        name=source_column.name,
        datatype=source_column.datatype,
        nullable=False if source_column.name in source_table.primary_key_constraint.column_names else True,
        length=source_column.length,
        scale=source_column.scale,
    )
    mapping = ColumnMapping(
        source_column=source_column,
        target_column=staging_column,
    )
    return (
        staging_column,
        mapping,
    )


def generate_staging_etl_code(physical_model_mapping: PhysicalModelMapping) -> str:
    """Generates ETL code based on database model."""
    from article_01 import generate_code

    with open("article-02/templates/source-to-staging-etl.sql.jinja", "r") as template_file:
        return generate_code(template_file.read(), physical_model_mapping)


def generate_hda_etl_code(physical_model_mapping: PhysicalModelMapping) -> str:
    """Generates ETL code based on database model."""
    from article_01 import generate_code

    with open("article-02/templates/staging-to-hda-etl.sql.jinja", "r") as template_file:
        return generate_code(template_file.read(), physical_model_mapping)


def transform_staging_physical_model_mapping_to_hda_physical_model_mapping(physical_model_mapping: PhysicalModelMapping) -> PhysicalModelMapping:
    hda_schema_mappings = [transform_staging_schema_mapping_to_hda_schema_mapping(schema_mapping) for schema_mapping in physical_model_mapping.schema_mappings]
    hda_physical_model = PhysicalModel(
        schemas=[hsm.target_schema for hsm in hda_schema_mappings]
    )
    mapping = PhysicalModelMapping(
        source_model=physical_model_mapping.target_model,
        target_model=hda_physical_model,
        schema_mappings=hda_schema_mappings,
    )
    return mapping


def transform_staging_schema_mapping_to_hda_schema_mapping(schema_mapping: SchemaMapping) -> SchemaMapping:
    hda_table_mappings = [transform_staging_table_mapping_to_hda_table_mapping(table_mapping) for table_mapping in schema_mapping.table_mappings]
    hda_schema = Schema(
        name=f"{schema_mapping.source_schema.name}_hda",
        tables=[htm.target_table for htm in hda_table_mappings],
    )
    mapping = SchemaMapping(
        source_schema=schema_mapping.target_schema,
        target_schema=hda_schema,
        table_mappings=hda_table_mappings,
    )
    return mapping


def transform_staging_table_mapping_to_hda_table_mapping(table_mapping: TableMapping) -> TableMapping:
    hda_column_mappings = [transform_staging_column_mapping_to_hda_column_mapping(column_mapping) for column_mapping in table_mapping.column_mappings]
    hda_table = Table(
        name=table_mapping.target_table.name,
        primary_key_constraint=PrimaryKeyConstraint(
            name=f"pk_{table_mapping.target_table.name}",
            column_names=["hda_timestamp_utc"] + table_mapping.source_table.primary_key_constraint.column_names,
        ),
        columns=[hcm.target_column for hcm in hda_column_mappings],
        foreign_key_constraints=[],
    )
    mapping = TableMapping(
        source_table=table_mapping.target_table,
        target_table=hda_table,
        column_mappings=hda_column_mappings,
    )
    return mapping


def transform_staging_column_mapping_to_hda_column_mapping(column_mapping: ColumnMapping) -> ColumnMapping:
    hda_column = Column(
        name=column_mapping.target_column.name,
        datatype=column_mapping.target_column.datatype,
        nullable=column_mapping.target_column.nullable,
        length=column_mapping.target_column.length,
        scale=column_mapping.target_column.scale,
    )
    mapping = ColumnMapping(
        source_column=column_mapping.target_column,
        target_column=hda_column,
    )
    return mapping


if __name__ == "__main__":
    # The orders model generated in article 1 is used as basis for the code
    # for article 2.
    from article_01 import orders_database_model as orders_source_model, generate_dml_code

    source_dml_code = generate_dml_code(orders_source_model)

    orders_staging_model, orders_source_to_staging_mapping = \
        transform_pyhical_source_model_to_physical_staging_model_and_mapping(
            orders_source_model
        )

    staging_dml_code = generate_dml_code(orders_staging_model)

    staging_etl_code = generate_staging_etl_code(orders_source_to_staging_mapping)

    orders_staging_to_hda_mapping = \
        transform_staging_physical_model_mapping_to_hda_physical_model_mapping(
            orders_source_to_staging_mapping
        )

    hda_dml_code = generate_dml_code(orders_staging_to_hda_mapping.target_model)

    hda_etl_code = generate_hda_etl_code(orders_staging_to_hda_mapping)

    with open("article-02/code/article_02.sql", "w") as output_file:
        from datetime import datetime
        print(f"/*", file=output_file)
        print(f" * The code in this file is automatically generated @ {datetime.now()}.", file=output_file)
        print(f" * Manual changes to this file will be removed when this file is regenerated.", file=output_file)
        print(f" */", file=output_file)
        print(source_dml_code, file=output_file)
        print(staging_dml_code, file=output_file)
        print(staging_etl_code, file=output_file)
        print(hda_dml_code, file=output_file)
        print(hda_etl_code, file=output_file)
