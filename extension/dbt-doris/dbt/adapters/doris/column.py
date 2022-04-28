from dataclasses import dataclass

from dbt.adapters.base.column import Column


@dataclass
class DorisColumn(Column):
    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    def __repr__(self) -> str:
        return f"<DorisColumn {self.name} ({self.data_type})>"
