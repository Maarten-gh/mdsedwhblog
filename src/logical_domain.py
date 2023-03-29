from dataclasses import dataclass, field


@dataclass
class Relation:
    role: str
    domain_name: str
    entity_name: str


@dataclass
class Property:
    name: str
    datatype: str


@dataclass
class Entity:
    name: str
    properties: list[Property] = field(default_factory=list)
    relations: list[Relation] = field(default_factory=list)


@dataclass
class Domain:
    name: str
    entities: list[Entity] = field(default_factory=list)


@dataclass
class LogicalModel:
    domains: list[Domain] = field(default_factory=list)