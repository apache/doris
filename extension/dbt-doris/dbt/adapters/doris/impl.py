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
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple
)

import agate
import dbt.exceptions
from dbt.adapters.base.relation import InformationSchema, BaseRelation
from dbt.adapters.doris.column import DorisColumn
from dbt.adapters.doris.connections import DorisConnectionManager
from dbt.adapters.doris.relation import DorisRelation
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME, LIST_SCHEMAS_MACRO_NAME
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.doris.doris_column_item import DorisColumnItem
from dbt_common.exceptions import CompilationError



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

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    def quote(self, identifier):
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

    def _get_one_catalog(
            self,
            information_schema: InformationSchema,
            schemas: Set[str],
            manifest: Manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            raise CompilationError(
                f"Expected only one schema in Doris _get_one_catalog, found " f"{schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, manifest)

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
