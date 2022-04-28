from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException


@dataclass
class DorisQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class DorisIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DorisRelation(BaseRelation):
    quote_policy: DorisQuotePolicy = DorisQuotePolicy()
    include_policy: DorisIncludePolicy = DorisIncludePolicy()
    quote_character: str = "`"

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException(f"Cannot set database {self.database} in Doris!")

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a Doris relation with schema and database set to include, but only one can be set"
            )
        return super().render()
