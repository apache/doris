#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dbt.adapters.sql import SQLAdapter

from enum import Enum
import re
import time
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Optional,
    Tuple,
)

import agate
import dbt.exceptions
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.meta import available
from dbt.adapters.doris.column import DorisColumn
from dbt.adapters.doris.connections import DorisConnectionManager
from dbt.adapters.doris.relation import DorisRelation
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME, LIST_SCHEMAS_MACRO_NAME
from dbt_common.clients.agate_helper import table_from_rows
from dbt.adapters.doris.doris_column_item import DorisColumnItem


class Engine(str, Enum):
    olap = "olap"
    mysql = "mysql"
    elasticsearch = "elasticsearch"
    hive = "hive"
    iceberg = "iceberg"


class PartitionType(str, Enum):
    list = "LIST"
    range = "RANGE"


class DorisConfig(AdapterConfig):
    engine: Engine
    duplicate_key: Tuple[str]
    partition_by: Tuple[str]
    partition_type: PartitionType
    partition_by_init: List[str]
    distributed_by: Tuple[str]
    buckets: int
    properties: Dict[str, str]


class DorisAdapter(SQLAdapter):
    ConnectionManager = DorisConnectionManager
    Relation = DorisRelation
    AdapterSpecificConfigs = DorisConfig
    Column = DorisColumn

    def valid_incremental_strategies(self):
        """Return the dbt built-in strategies implemented by dbt-doris."""
        return ["append", "merge", "delete+insert", "insert_overwrite"]

    @staticmethod
    def _parse_doris_version(version_string):
        if not version_string:
            return None

        match = re.search(
            r"(?:doris\s+version\s+)?doris-"
            r"(\d+)\.(\d+)(?:\.(\d+))?",
            str(version_string),
            flags=re.IGNORECASE,
        )
        if match is None:
            return None

        return tuple(int(part or 0) for part in match.groups())

    def _doris_version(self):
        _, variables = self.execute(
            "show variables like 'version_comment'",
            auto_begin=False,
            fetch=True,
        )
        if len(variables.rows) > 0:
            version = self._parse_doris_version(variables.rows[0][1])
            if version is not None and version != (0, 0, 0):
                return version

        # Source/custom builds may omit a usable version from version_comment.
        _, frontends = self.execute(
            "show frontends",
            auto_begin=False,
            fetch=True,
        )
        version_index = list(frontends.column_names).index("Version")
        connected_index = (
            list(frontends.column_names).index("CurrentConnected")
            if "CurrentConnected" in frontends.column_names
            else None
        )
        rows = list(frontends.rows)
        if connected_index is not None:
            connected_rows = [
                row
                for row in rows
                if str(row[connected_index]).lower() in ("yes", "true")
            ]
            if connected_rows:
                rows = connected_rows

        for row in rows:
            version = self._parse_doris_version(row[version_index])
            if version is not None and version != (0, 0, 0):
                return version
        return None

    @available
    def supports_transactional_delete_insert(self):
        """Whether Doris supports DELETE and INSERT SELECT in one transaction."""
        version = self._doris_version()
        return version is not None and version >= (3, 0, 0)

    def _latest_schema_change_job(self, relation: BaseRelation):
        schema = self.quote(relation.schema)
        table_name = relation.identifier.replace("'", "''")
        _, table = self.execute(
            "show alter table column from {} "
            "where TableName = '{}' "
            "order by JobId desc limit 1".format(schema, table_name),
            auto_begin=False,
            fetch=True,
        )
        if len(table.rows) == 0:
            return None

        row = table.rows[0]
        return {
            "job_id": str(row[0]),
            "state": str(row[9]).upper(),
            "message": str(row[10] or ""),
        }

    @available
    def get_latest_schema_change_job_id(self, relation: BaseRelation):
        """Return the newest column-alter job id for a Doris table."""
        job = self._latest_schema_change_job(relation)
        return None if job is None else job["job_id"]

    @available
    def wait_for_schema_change(
        self,
        relation: BaseRelation,
        previous_job_id=None,
        timeout_seconds: int = 300,
    ):
        """Wait for the column-alter job started by the preceding DDL."""
        deadline = time.monotonic() + timeout_seconds

        while True:
            job = self._latest_schema_change_job(relation)
            if job is None or job["job_id"] == previous_job_id:
                return
            if job["state"] == "FINISHED":
                return
            if job["state"] == "CANCELLED":
                raise dbt.exceptions.DbtRuntimeError(
                    "Doris schema change job {} for {} was cancelled: {}".format(
                        job["job_id"],
                        relation,
                        job["message"],
                    )
                )
            if time.monotonic() >= deadline:
                raise dbt.exceptions.DbtRuntimeError(
                    "Timed out after {} seconds waiting for Doris schema "
                    "change job {} on {} (state: {})".format(
                        timeout_seconds,
                        job["job_id"],
                        relation,
                        job["state"],
                    )
                )
            time.sleep(0.2)

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def quote(cls, identifier):
        return "`{}`".format(identifier)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        return super().get_relation(None, schema, identifier)

    def drop_schema(self, relation: BaseRelation):
        relations = self.list_relations(
            database=relation.database,
            schema=relation.schema
        )
        for relation in relations:
            self.drop_relation(relation)
        super().drop_schema(relation)

    def list_relations_without_caching(self, schema_relation: DorisRelation) -> List[DorisRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _database, name, schema, type_info = row
            rel_type = RelationType.View if "view" in type_info else RelationType.Table
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=rel_type,
            )
            relations.append(relation)

        return relations

    def _catalog_filter_table(
            self, table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return table.where(self._catalog_filter_schemas(used_schemas))

    @staticmethod
    def _catalog_filter_schemas(
            used_schemas: FrozenSet[Tuple[str, str]]
    ):
        schemas = frozenset((None, s.lower()) for d, s in used_schemas)

        def predicate(row: agate.Row) -> bool:
            table_database = row.get("table_database")
            table_schema = row.get("table_schema")
            if table_schema is None:
                return False
            return (table_database, table_schema.lower()) in schemas

        return predicate

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.HasNulls(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "boolean"

    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        if quote_config is None or quote_config:
            return self.quote(column)
        return column

    # Methods used in adapter tests
    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"{add_to} + interval {number} {interval}"

    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_column_constraints = []
        for v in raw_columns.values():
            cols_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
            data_type = v.get('data_type')
            comment = v.get('description')

            column = DorisColumnItem(cols_name, data_type, comment, "")
            rendered_column_constraints.append(column)

        return rendered_column_constraints
