from jinja2 import Environment, FileSystemLoader
import os

from domain.physical_domain import (
    PhysicalModel,
    Schema,
    Table,
    Column,
    PrimaryKeyConstraint,
)
from domain.physical_mapping_domain import (
    ColumnMapping,
    PhysicalModelMapping,
    SchemaMapping,
    TableLoadStep,
    TableMapping,
)


def transform_source_model_to_hda_model_mapping(
        source_model: PhysicalModel
) -> PhysicalModelMapping:
    hda_schema_mappings = [transform_source_schema_tho_hda_schema_mapping(s) for s in source_model.schemas]
    hda_model = PhysicalModel(
        schemas=[m.target_schema for m in hda_schema_mappings]
    )
    mapping = PhysicalModelMapping(
        source_model=source_model,
        target_model=hda_model,
        schema_mappings=hda_schema_mappings,
    )
    return mapping


def transform_source_schema_tho_hda_schema_mapping(
        source_schema: Schema
) -> SchemaMapping:
    hda_table_mappings = [transfrom_source_table_to_hda_table_mapping(t) for t in source_schema.tables]
    hda_schema = Schema(
        name=f"{source_schema.name}_hda",
        tables=[m.target_table for m in hda_table_mappings],
    )
    mapping = SchemaMapping(
        source_schema=source_schema,
        target_schema=hda_schema,
        table_mappings=hda_table_mappings,
    )
    return mapping


def transfrom_source_table_to_hda_table_mapping(
        source_table: Table
) -> TableMapping:
    column_mappings = \
        [transform_source_column_to_hda_column_mapping(
            c, source_table.primary_key_constraint.column_names
            ) for c in source_table.columns
        ]
    insert_changes_step = \
        transfrom_source_table_to_hda_step_insert_changes(
            column_mappings)
    insert_deletes_step = \
        transfrom_source_table_to_hda_step_insert_deletes(
            column_mappings,
            source_table.primary_key_constraint)
    hda_table = Table(
        name=source_table.name,
        primary_key_constraint=create_hda_primary_key_constraint(
            source_table.primary_key_constraint),
        columns=[m.target_column for m in insert_changes_step.column_mappings],
        foreign_key_constraints=[],
    )
    mapping = TableMapping(
        source_table=source_table,
        target_table=hda_table,
        load_steps=[
            insert_changes_step,
            insert_deletes_step
        ],
    )
    return mapping


def transfrom_source_table_to_hda_step_insert_changes(
    column_mappings: list[ColumnMapping]
) -> TableLoadStep:
    technical_hda_column_mappings = \
        create_technical_hda_column_mappings("0")
    hda_column_mappings = \
        technical_hda_column_mappings + \
        column_mappings
    
    return TableLoadStep(
        description="Load HDA table with changes from source table",
        column_mappings=hda_column_mappings
    )


def transfrom_source_table_to_hda_step_insert_deletes(
    column_mappings: list[ColumnMapping],
    source_primary_key: PrimaryKeyConstraint
) -> TableLoadStep:
    technical_hda_column_mappings = \
        create_technical_hda_column_mappings("1")
    hda_column_mappings = \
        technical_hda_column_mappings + \
        [m for m in column_mappings if m.source_column.name in source_primary_key.column_names]
    
    return TableLoadStep(
        description="Add records to the HDA table for deleted source records",
        column_mappings=hda_column_mappings,
    )


def transform_source_column_to_hda_column_mapping(
        source_column: Column,
        not_null_column_names: list[str]
) -> ColumnMapping:
    hda_column = Column(
        name=source_column.name,
        datatype=source_column.datatype,
        length=source_column.length,
        scale=source_column.scale,
        nullable=source_column.name not in not_null_column_names
    )
    mapping = ColumnMapping(
        target_column=hda_column,
        source_column=source_column,
    )
    return mapping


def create_technical_hda_column_mappings(hda_voided_value: str) -> list[ColumnMapping]:
    return [
        ColumnMapping(
            source_column=None,
            target_column=Column(
                name="hda_validFrom_utc",
                datatype="datetime2",
                nullable=False,
            ),
            expression='@timestamp_utc'
        ),
        ColumnMapping(
            source_column=None,
            target_column=Column(
                name="hda_voided",
                datatype="bit",
                nullable=False,
            ),
            expression=hda_voided_value
        )
    ]


def create_hda_primary_key_constraint(primary_key_constraint: PrimaryKeyConstraint) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(
        name=primary_key_constraint.name,
        column_names=primary_key_constraint.column_names + ["hda_validFrom_utc"],
    )


def generate_dml_code(physical_model_mapping: PhysicalModelMapping) -> str:
    return generate_code("dml.sql.jinja", physical_model_mapping)


def generate_hda_pit_code(physical_model_mapping: PhysicalModelMapping) -> str:
    """Generates ETL code based on database model."""
    return generate_code("hda_pit.sql.jinja", physical_model_mapping)


def generate_hda_etl_code(physical_model_mapping: PhysicalModelMapping) -> str:
    """Generates ETL code based on database model."""
    return generate_code("source-to-hda-etl.sql.jinja", physical_model_mapping)
       

def generate_lineage_code(physical_model_mapping: PhysicalModelMapping) -> str:
    return generate_code("mapping.md.jinja", physical_model_mapping)


def generate_code(template_name: str, model: object) -> str:
    """Generates code based on a model dictionary and a template string."""
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../article-02/templates/")
    loader = FileSystemLoader(template_dir)
    environment = Environment(loader=loader)
    template = environment.get_template(f"{template_name}")

    return template.render(**model.__dict__).strip() + '\n'


if __name__ == "__main__":
    # The orders model generated in article 1 is used as basis for the code
    # for article 2.
    from article_01 import orders_database_model as orders_source_model

    orders_source_to_hda_mapping = \
        transform_source_model_to_hda_model_mapping(
            orders_source_model
        )

    src_dml_code = generate_dml_code(orders_source_to_hda_mapping.source_model)

    hda_dml_code = generate_dml_code(orders_source_to_hda_mapping.target_model)

    hda_pit_code = generate_hda_pit_code(orders_source_to_hda_mapping.target_model)

    hda_etl_code = generate_hda_etl_code(orders_source_to_hda_mapping)

    hda_lineage_code = generate_lineage_code(orders_source_to_hda_mapping)

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../article-02/code/article_02.sql"), "w") as output_file:
        from datetime import datetime
        print(f"/*", file=output_file)
        print(f" * The code in this file is automatically generated @ {datetime.now()}.", file=output_file)
        print(f" * Manual changes to this file will be removed when this file is regenerated.", file=output_file)
        print(f" */", file=output_file)
        print(src_dml_code, file=output_file)
        print(hda_dml_code, file=output_file)
        print(hda_pit_code, file=output_file)
        print(hda_etl_code, file=output_file)

        print("""
INSERT INTO [orders].[Product] (
  [id]
, [description]
) VALUES (
  '11111111-1111-1111-1111-111111111111', 'Red hat'
), (
  '22222222-2222-2222-2222-222222222222', 'Green glasses'
), (
  '33333333-3333-3333-3333-333333333333', 'Blue jeans'
)
;

EXEC [orders_hda].[usp_load_Product] @timestamp_utc = '2023-12-01';

SELECT * FROM [orders].[Product];
SELECT * FROM [orders_hda].[Product];
SELECT * FROM [orders_hda].[ufn_pit_Product]('2023-12-02');

UPDATE
  [orders].[Product]
SET
  [description] = 'Yellow glasses'
WHERE
  [id] = '22222222-2222-2222-2222-222222222222'
;
DELETE FROM
  [orders].[Product]
WHERE
  [id] = '33333333-3333-3333-3333-333333333333'
;

EXEC [orders_hda].[usp_load_Product] @timestamp_utc = '2023-12-05';

SELECT * FROM [orders].[Product];
SELECT * FROM [orders_hda].[Product];
SELECT * FROM [orders_hda].[ufn_pit_Product]('2023-12-06');

UPDATE
  [orders].[Product]
SET
  [description] = 'Chartreuse glasses'
WHERE
  [id] = '22222222-2222-2222-2222-222222222222'
;
INSERT INTO [orders].[Product] (
  [id]
, [description]
) VALUES (
  '33333333-3333-3333-3333-333333333333', 'Blue jeans'
);

EXEC [orders_hda].[usp_load_Product] @timestamp_utc = '2023-12-09';

SELECT * FROM [orders].[Product];
SELECT * FROM [orders_hda].[Product];
SELECT * FROM [orders_hda].[ufn_pit_Product]('2023-12-10');
              """, file=output_file)

        for source_schema in orders_source_to_hda_mapping.source_model.schemas:
            for source_table in [t for t in source_schema.tables if t.foreign_key_constraints]:
                print (f"IF EXISTS(SELECT 1 FROM [sys].[tables] WHERE [name] = '{source_table.name}' AND [schema_id] = SCHEMA_ID('{source_schema.name}'))\nBEGIN", file=output_file)
                for fk in source_table.foreign_key_constraints:
                    print(f"  ALTER TABLE [{source_schema.name}].[{source_table.name}] DROP CONSTRAINT [{fk.name}];", file=output_file)
                print (f"END;\nGO", file=output_file)
            for source_table in source_schema.tables:
                print(f"DROP TABLE IF EXISTS [{source_schema.name}].[{source_table.name}];\nGO", file=output_file)
            print(f"DROP SCHEMA IF EXISTS [{source_schema.name}];\nGO", file=output_file)

        for target_schema in orders_source_to_hda_mapping.target_model.schemas:
            for target_table in target_schema.tables:
                print(f"DROP PROCEDURE IF EXISTS [{target_schema.name}].[usp_load_{target_table.name}];\nGO", file=output_file)
                print(f"DROP FUNCTION IF EXISTS [{target_schema.name}].[ufn_pit_{target_table.name}];\nGO", file=output_file)
                print(f"DROP TABLE IF EXISTS [{target_schema.name}].[{target_table.name}];\nGO", file=output_file)
            print(f"DROP SCHEMA IF EXISTS [{target_schema.name}];\nGO", file=output_file)


    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../article-02/code/article_02.md"), "w") as output_file:
        print("# Lineage", file=output_file)
        print(file=output_file)
        print(f"The code in this file is automatically generated @ {datetime.now()}.", file=output_file)
        print(f"Manual changes to this file will be removed when this file is regenerated.", file=output_file)
        print(file=output_file)
        print("# HDA lineage", file=output_file)
        print(file=output_file)
        print(hda_lineage_code, file=output_file)
        print(file=output_file)