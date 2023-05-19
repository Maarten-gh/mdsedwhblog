from domain.physical_domain import PhysicalModel, Schema, Table, Column, ForeignKeyConstraint, PrimaryKeyConstraint
from domain.logical_domain import LogicalModel, Domain, Entity, Property, Relation

orders_domain_model = LogicalModel(
    domains=[
        Domain(
            name="orders",
            entities=[
                Entity(
                    name="Customer",
                    properties=[
                        Property(
                            name="address",
                            datatype="string",
                        ),
                    ],
                ),
                Entity(
                    name="Product",
                    properties=[
                        Property(
                            name="description",
                            datatype="string",
                        ),
                    ],
                ),
                Entity(
                    name="Order",
                    properties=[
                        Property(
                            name="orderTimestamp_utc",
                            datatype="timestamp",
                        ),
                        Property(
                            name="amount",
                            datatype="int",
                        ),
                    ],
                    relations=[
                        Relation(
                            role="orderedBy",
                            domain_name="orders",
                            entity_name="Customer",
                        ),
                        Relation(
                            role="orderFor",
                            domain_name="orders",
                            entity_name="Product",
                        ),
                    ],
                ),
            ],
        ),
    ],
)


def logical_model_to_physical_model(logical_model: LogicalModel) -> PhysicalModel:
    return PhysicalModel(
        schemas=[domain_to_schema(d) for d in logical_model.domains]
    )


def domain_to_schema(domain: Domain) -> Schema:
    return Schema(
        name=domain.name,
        tables=[entity_to_table(e) for e in domain.entities],
    )


def entity_to_table(entity: Entity) -> Table:
    id_column = Column(
        name="id",
        datatype="uniqueidentifier",
        nullable=False,
    )

    primary_key_constraint = PrimaryKeyConstraint(
        name=f"pk_{entity.name}",
        column_names=["id"],
    )

    return Table(
        name=entity.name,
        columns=[id_column] + [property_to_column(p) for p in entity.properties] + [
            relation_to_column(r) for r in entity.relations],
        primary_key_constraint=primary_key_constraint,
        foreign_key_constraints=[
            relation_to_foreign_key_constraint(r) for r in entity.relations],
    )


def property_to_column(property: Property) -> Column:
    return Column(
        name=property.name,
        nullable=True,
        **property_datatype_to_column_datatype(property),
    )


def relation_to_column(relation: Relation) -> Column:
    return Column(
        name=f"{relation.role}_{relation.entity_name}_id",
        datatype="uniqueidentifier",
        nullable=False,
    )


def relation_to_foreign_key_constraint(relation: Relation) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        name=f"fk_{relation.role}_{relation.entity_name}",
        column_names=[f"{relation.role}_{relation.entity_name}_id",],
        foreign_schema_name=relation.domain_name,
        foreign_table_name=relation.entity_name,
        foreign_column_names=["id"],
    )


def property_datatype_to_column_datatype(property: Property) -> dict[str, any]:
    """
    Returns a column datatype, length and scale based on the data type of the
    provided property. If the type cannot be resolved, the datatype of the
    property is used and length and scale are both `None`.
    """
    result = dict(
        datatype=property.datatype,
        length=None,
        scale=None,
    )

    if "string" == property.datatype:
        result["datatype"] = 'nvarchar'
        result["length"] = 255
    elif "timestamp" == property.datatype:
        result["datatype"] = 'datetime2'

    return result


orders_database_model = logical_model_to_physical_model(orders_domain_model)


dml_template_text = """
{# Define funcionality to quote and join lists of names #}
{% macro q(names) -%}
[{{ names|join('].[') }}]
{%- endmacro %}

{# Define a macro to put commas ',' before items in a loop,
   except before the first item #}
{% macro c(loop) -%}
{{ '  ' if loop.index0 == 0 else ', ' }}
{%- endmacro %}

{# Loop trough all schemas and create SQL DML statements -#}
{% for schema in schemas %}
CREATE SCHEMA {{ q([schema.name]) }};
GO

{# Loop trough all tables in the schema and output a CREATE TABLE statement -#}
{% for table in schema.tables %}
CREATE TABLE {{ q([schema.name, table.name]) }} (
{% for column in table.columns -%}
{{ c(loop) }}{{ q([column.name]) }} {{ column.fulltype }}
{% endfor -%}
, CONSTRAINT {{ q([table.primary_key_constraint.name]) }}
    PRIMARY KEY (
    {% for column_name in table.primary_key_constraint.column_names -%}
    {{ c(loop) }}{{ q([column_name]) }}
    {% endfor -%}
    )
)
;
GO
{% endfor -%}
 
{# Loop trough tables with foreign keys and output FOREIGN KEY constraints -#}
{% for table in schema.tables|selectattr('foreign_key_constraints') %}
ALTER TABLE {{ q([schema.name, table.name]) }}
ADD
{% for fkey in table.foreign_key_constraints -%}
{{ c(loop) }}CONSTRAINT {{ q([fkey.name]) }}
    FOREIGN KEY 
      ({{ q(fkey.column_names) }})
    REFERENCES {{ q([fkey.foreign_schema_name, fkey.foreign_table_name]) }} 
      ({{ q(fkey.foreign_column_names) }})
{% endfor -%}
;
GO
{% endfor %}
 
{% endfor %}
"""


def generate_code(template_text: str, model: object) -> str:
    """Generates code based on a model dictionary and a template string."""
    from jinja2 import Environment, BaseLoader
    template = Environment(loader=BaseLoader).from_string(template_text)

    return template.render(**model.__dict__).strip() + '\n'


def generate_dml_code(physical_model: PhysicalModel) -> str:
    """Generates DDL code based on database model."""
    return generate_code(dml_template_text, physical_model)


dml_code = generate_dml_code(orders_database_model)

print(dml_code)
