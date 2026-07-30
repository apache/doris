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

"""Unit coverage for Doris incremental strategy contracts and SQL."""

import mysql.connector
import pytest
from dbt.adapters.doris.column import DorisColumn
from dbt.adapters.doris.connections import DorisConnectionManager
from dbt.adapters.doris.impl import DorisAdapter
from dbt.adapters.doris.relation import DorisRelation
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import DbtRuntimeError

from .macro_harness import (
    CapturedCompilerError,
    FakeAdapter,
    FakeColumn,
    FakeConfig,
    FakeRelation,
    MacroRunner,
)

INCREMENTAL_MACROS = (
    "materializations/incremental/incremental.sql",
    "materializations/incremental/help.sql",
    "materializations/incremental/strategies.sql",
)
RELATION_MACROS = ("adapters/relation.sql",)


def statement_count(sql):
    return len([part for part in sql.split(";") if part.strip()])


def incremental_runner(config=None):
    return MacroRunner(
        *INCREMENTAL_MACROS,
        context={
            "adapter": FakeAdapter(),
            "config": FakeConfig(config),
            "model": {
                "unique_id": "model.project.model",
                "name": "model",
            },
        },
    )


def validate(config):
    return incremental_runner(config).render(
        "dbt_doris_validate_get_incremental_strategy",
        FakeConfig(config),
    )


def strategy_args(**updates):
    values = {
        "target_relation": FakeRelation(identifier="target"),
        "temp_relation": FakeRelation(identifier="target__dbt_tmp"),
        "unique_key": ["id"],
        "dest_columns": [FakeColumn("id"), FakeColumn("value")],
        "incremental_predicates": None,
        "source_sql": "select 1 as id, 'new' as value",
        "temp_relation_exists": False,
        "overwrite_partitions": None,
    }
    values.update(updates)
    return values


class TestIncrementalValidation:
    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({"unique_key": ["id"]}, "default"),
            ({}, "default"),
            ({"incremental_strategy": "append"}, "append"),
            ({"incremental_strategy": "insert_overwrite"}, "insert_overwrite"),
        ],
    )
    def test_public_strategy_name(self, config, expected):
        assert validate(config) == expected

    @pytest.mark.parametrize("strategy", ["merge", "delete+insert"])
    def test_keyed_strategies_require_a_key(self, strategy):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate({"incremental_strategy": strategy})
        assert "requires a 'unique_key'" in str(excinfo.value)

    @pytest.mark.parametrize("strategy", ["merge", "delete+insert"])
    def test_keyed_strategies_accept_composite_keys(self, strategy):
        assert (
            validate(
                {
                    "incremental_strategy": strategy,
                    "unique_key": ["tenant_id", "id"],
                }
            )
            == strategy
        )

    def test_delete_insert_alias_is_rejected(self):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "delete_insert",
                    "unique_key": "id",
                }
            )
        assert "Use 'delete+insert'" in str(excinfo.value)

    def test_unimplemented_grants_fail_before_execution(self):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "append",
                    "grants": {"select": ["analyst"]},
                }
            )
        assert "not implemented" in str(excinfo.value)

    def test_bare_sequence_config_is_rejected(self):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "merge",
                    "unique_key": "id",
                    "sequence_col": "updated_at",
                }
            )
        assert "function_column.sequence_col" in str(excinfo.value)

    def test_merge_rejects_merge_on_read_property(self):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "merge",
                    "unique_key": "id",
                    "properties": {
                        "enable_unique_key_merge_on_write": "false",
                    },
                }
            )
        assert "delete+insert" in str(excinfo.value)

    @pytest.mark.parametrize(
        "property_name",
        [
            "function_column.sequence_col",
            "FUNCTION_COLUMN.SEQUENCE_TYPE",
        ],
    )
    def test_delete_insert_rejects_sequence_property(self, property_name):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "delete+insert",
                    "unique_key": "id",
                    "properties": {property_name: "sequence_id"},
                }
            )
        assert "strategy='merge'" in str(excinfo.value)

    @pytest.mark.parametrize(
        "strategy",
        ["append", "merge", "delete+insert", "insert_overwrite"],
    )
    def test_predicates_are_rejected_by_builtins(self, strategy):
        config = {
            "incremental_strategy": strategy,
            "incremental_predicates": ["DBT_INTERNAL_DEST.id > 0"],
        }
        if strategy in ["merge", "delete+insert"]:
            config["unique_key"] = "id"
        with pytest.raises(CapturedCompilerError):
            validate(config)

    def test_partition_config_is_validated(self):
        with pytest.raises(CapturedCompilerError):
            validate(
                {
                    "incremental_strategy": "append",
                    "overwrite_partitions": ["p1"],
                }
            )
        assert (
            validate(
                {
                    "incremental_strategy": "insert_overwrite",
                    "partition_by": ["event_date"],
                    "overwrite_partitions": "*",
                }
            )
            == "insert_overwrite"
        )

    def test_partial_merge_is_rejected(self):
        with pytest.raises(CapturedCompilerError) as excinfo:
            validate(
                {
                    "incremental_strategy": "merge",
                    "unique_key": "id",
                    "merge_update_columns": ["value"],
                }
            )
        assert "native MERGE INTO" in str(excinfo.value)

    def test_source_must_return_every_unique_key(self):
        runner = incremental_runner()
        with pytest.raises(CapturedCompilerError):
            runner.render(
                "doris__validate_source_unique_key_columns",
                [FakeColumn("value")],
                ["tenant_id", "id"],
            )

    def test_unique_key_type_change_requires_full_refresh(self):
        runner = incremental_runner()
        with pytest.raises(CapturedCompilerError) as excinfo:
            runner.render(
                "doris__validate_unique_key_schema_changes",
                {
                    "new_target_types": [
                        {"column_name": "id", "new_type": "bigint"}
                    ]
                },
                ["id"],
            )
        assert "--full-refresh" in str(excinfo.value)


class TestIncrementalStrategySql:
    @pytest.mark.parametrize(
        "macro",
        [
            "doris__get_incremental_append_sql",
            "doris__get_incremental_merge_sql",
        ],
    )
    def test_direct_insert_inlines_source_once(self, macro):
        runner = incremental_runner()
        sql = runner.sql(macro, strategy_args())
        assert statement_count(sql) == 1
        assert sql.count("select 1 as id") == 1
        assert "__dbt_tmp" not in sql
        assert "insert into `dbt_test`.`target` (`id`, `value`)" in sql

    @pytest.mark.parametrize(
        ("partitions", "expected"),
        [
            (None, "insert overwrite table `dbt_test`.`target` (`id`, `value`)"),
            ("*", "partition(*) (`id`, `value`)"),
            (["p1", "p2"], "partition(`p1`, `p2`) (`id`, `value`)"),
        ],
    )
    def test_native_insert_overwrite(self, partitions, expected):
        sql = incremental_runner().sql(
            "doris__get_incremental_insert_overwrite_sql",
            strategy_args(overwrite_partitions=partitions),
        )
        assert statement_count(sql) == 1
        assert expected in sql
        assert "__dbt_tmp" not in sql

    def test_public_delete_insert_is_one_safe_statement(self):
        runner = incremental_runner()
        sql = runner.sql(
            "doris__get_incremental_delete_insert_sql",
            strategy_args(temp_relation_exists=True),
        )
        assert statement_count(sql) == 1
        assert runner.statements == []
        assert "`dbt_test`.`target__dbt_tmp`" in sql
        assert "DBT_INTERNAL_DUPLICATE_KEYS" in sql

    @pytest.mark.parametrize(
        "macro",
        [
            "doris__get_incremental_append_sql",
            "doris__get_incremental_merge_sql",
            "doris__get_incremental_delete_insert_sql",
            "doris__get_incremental_insert_overwrite_sql",
        ],
    )
    def test_standard_five_key_contract_reads_temp_relation(self, macro):
        arg_dict = strategy_args()
        for key in (
            "source_sql",
            "temp_relation_exists",
            "overwrite_partitions",
        ):
            arg_dict.pop(key)
        sql = incremental_runner().sql(macro, arg_dict)
        assert "`dbt_test`.`target__dbt_tmp`" in sql
        assert "from ( )" not in sql

    @pytest.mark.parametrize(
        ("unique_key", "expected"),
        [
            (None, "select DBT_INTERNAL_SOURCE.`id`"),
            (["id"], "DBT_INTERNAL_DUPLICATE_KEYS"),
        ],
    )
    def test_default_routes_by_unique_key(self, unique_key, expected):
        sql = incremental_runner().sql(
            "doris__get_incremental_default_sql",
            strategy_args(unique_key=unique_key),
        )
        assert expected in sql

    def test_initial_unique_ctas_validates_the_prepared_contract_sql_once(self):
        prepared_sql = (
            "select cast(raw_id as int) as id, value "
            "from raw_events /* PREPARED_CONTRACT_SOURCE */"
        )
        runner = MacroRunner(
            *INCREMENTAL_MACROS,
            "materializations/table/create_table_as.sql",
            context={
                "adapter": FakeAdapter(),
                "config": FakeConfig({"unique_key": ["id"]}),
                "model": {
                    "unique_id": "model.project.model",
                    "name": "model",
                },
                "doris__unique_key": lambda: "unique key(`id`)",
                "doris__table_comment": lambda: "",
                "doris__partition_by": lambda: "",
                "doris__distributed_by": lambda: "",
                "doris__properties": lambda *args: "",
            },
        )

        sql = runner.sql(
            "doris__get_incremental_create_table_as_sql",
            "merge",
            FakeRelation(identifier="target"),
            prepared_sql,
            [FakeColumn("id"), FakeColumn("value")],
        )

        assert sql.count("PREPARED_CONTRACT_SOURCE") == 1
        assert (
            "from ( select cast(raw_id as int) as id, value "
            "from raw_events /* PREPARED_CONTRACT_SOURCE */ "
            ") DBT_INTERNAL_RAW_SOURCE"
        ) in sql


def test_valid_incremental_strategy_allowlist():
    adapter = object.__new__(DorisAdapter)
    assert adapter.valid_incremental_strategies() == [
        "append",
        "merge",
        "delete+insert",
        "insert_overwrite",
    ]


@pytest.mark.parametrize(
    ("raw_type", "expected"),
    [
        ("VARCHAR(20)", "varchar(20)"),
        ("DECIMAL(18, 4)", "decimal(18,4)"),
        ("CHAR(7)", "CHAR(7)"),
        ("DATETIMEV2(6)", "DATETIMEV2(6)"),
        ("ARRAY<VARCHAR(20)>", "ARRAY<VARCHAR(20)>"),
    ],
)
def test_doris_column_preserves_parameterized_types(raw_type, expected):
    column = DorisColumn.from_description("value", raw_type)
    assert column.data_type == expected


def test_doris_column_widens_with_valid_varchar_syntax():
    target = DorisColumn.from_description("value", "varchar(10)")
    source = DorisColumn.from_description("value", "varchar(40)")

    assert target.can_expand_to(source)
    assert DorisColumn.string_type(source.string_size()) == "varchar(40)"


def test_view_query_extraction_does_not_split_identifier_containing_as():
    show_create_sql = (
        "CREATE VIEW `events`\n"
        "(ASSET_ID, VALUE)\n"
        " AS select 1 AS `ASSET_ID`, 'current' AS `VALUE`;"
    )

    query = MacroRunner(*RELATION_MACROS).render(
        "doris__view_query_from_show_create",
        show_create_sql,
    )

    assert query == "select 1 AS `ASSET_ID`, 'current' AS `VALUE`"


@pytest.mark.parametrize(
    ("version_string", "expected"),
    [
        ("doris-4.1.2-rc01-abc", (4, 1, 2)),
        ("Doris version doris-3.0.7-release", (3, 0, 7)),
        ("doris-2.1.10", (2, 1, 10)),
        ("5.7.99", None),
        ("doris version doris-0.0.0-dev", (0, 0, 0)),
    ],
)
def test_parse_doris_version(version_string, expected):
    assert DorisAdapter._parse_doris_version(version_string) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ((2, 1, 10), False),
        ((3, 0, 0), True),
        ((4, 1, 2), True),
        ((0, 0, 0), False),
        (None, False),
    ],
)
def test_transactional_delete_insert_version_gate(monkeypatch, version, expected):
    adapter = object.__new__(DorisAdapter)
    monkeypatch.setattr(adapter, "_doris_version", lambda: version)
    assert adapter.supports_transactional_delete_insert() is expected


def test_version_falls_back_from_zero_comment_to_frontend(monkeypatch):
    adapter = object.__new__(DorisAdapter)

    class Table:
        def __init__(self, rows, column_names=()):
            self.rows = rows
            self.column_names = column_names

    results = iter(
        [
            (
                None,
                Table(
                    [
                        (
                            "version_comment",
                            "doris version doris-0.0.0-dev",
                        )
                    ]
                ),
            ),
            (
                None,
                Table(
                    [("doris-4.1.2-rc01-abc", "Yes")],
                    ("Version", "CurrentConnected"),
                ),
            ),
        ]
    )
    monkeypatch.setattr(adapter, "execute", lambda *args, **kwargs: next(results))

    assert adapter._doris_version() == (4, 1, 2)


def test_unknown_zero_version_is_not_treated_as_transaction_capable(monkeypatch):
    adapter = object.__new__(DorisAdapter)

    class Table:
        def __init__(self, rows, column_names=()):
            self.rows = rows
            self.column_names = column_names

    results = iter(
        [
            (
                None,
                Table(
                    [
                        (
                            "version_comment",
                            "doris version doris-0.0.0-dev",
                        )
                    ]
                ),
            ),
            (
                None,
                Table(
                    [("doris-0.0.0-dev", "Yes")],
                    ("Version", "CurrentConnected"),
                ),
            ),
        ]
    )
    monkeypatch.setattr(adapter, "execute", lambda *args, **kwargs: next(results))

    assert adapter.supports_transactional_delete_insert() is False


def test_multi_statement_drain_propagates_later_error(monkeypatch):
    manager = object.__new__(DorisConnectionManager)

    class Cursor:
        with_rows = False

        @staticmethod
        def nextset():
            raise mysql.connector.DatabaseError("later statement failed")

    monkeypatch.setattr(
        SQLConnectionManager,
        "add_query",
        lambda *args, **kwargs: (object(), Cursor()),
    )
    monkeypatch.setattr(manager, "get_if_exists", lambda: None)

    with pytest.raises(DbtRuntimeError) as excinfo:
        manager.add_query("set ok = true; set invalid = true")
    assert "later statement failed" in str(excinfo.value)


def test_database_error_rolls_back_and_closes_transaction_flag(monkeypatch):
    manager = object.__new__(DorisConnectionManager)

    class Handle:
        rollback_calls = 0

        def rollback(self):
            self.rollback_calls += 1

    class Connection:
        handle = Handle()
        state = "open"
        transaction_open = True

    connection = Connection()
    monkeypatch.setattr(manager, "get_if_exists", lambda: connection)

    with pytest.raises(DbtRuntimeError) as excinfo:
        with manager.exception_handler("insert into target select * from stage"):
            raise mysql.connector.DatabaseError("insert failed")

    assert "insert failed" in str(excinfo.value)
    assert connection.handle.rollback_calls == 1
    assert connection.transaction_open is False
    assert connection.state == "open"


def test_rollback_failure_marks_connection_failed(monkeypatch):
    manager = object.__new__(DorisConnectionManager)

    class Handle:
        @staticmethod
        def rollback():
            raise RuntimeError("connection lost during rollback")

    class Connection:
        handle = Handle()
        state = "open"
        transaction_open = True

    connection = Connection()
    monkeypatch.setattr(manager, "get_if_exists", lambda: connection)

    with pytest.raises(DbtRuntimeError) as excinfo:
        with manager.exception_handler("delete from target"):
            raise mysql.connector.DatabaseError("delete failed")

    assert "delete failed" in str(excinfo.value)
    assert connection.transaction_open is False
    assert connection.state == "fail"


def test_schema_change_waits_for_finished_job(monkeypatch):
    adapter = object.__new__(DorisAdapter)
    relation = DorisRelation.create(schema="analytics", identifier="events")
    jobs = iter(
        [
            {"job_id": "2", "state": "RUNNING", "message": ""},
            {"job_id": "2", "state": "FINISHED", "message": ""},
        ]
    )
    monkeypatch.setattr(adapter, "_latest_schema_change_job", lambda _: next(jobs))
    sleeps = []
    monkeypatch.setattr(
        "dbt.adapters.doris.impl.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    adapter.wait_for_schema_change(relation, previous_job_id="1")
    assert sleeps == [0.2]


def test_latest_schema_change_job_orders_by_job_id(monkeypatch):
    adapter = object.__new__(DorisAdapter)
    relation = DorisRelation.create(schema="analytics", identifier="events")
    captured = {}

    class Result:
        rows = []

    def execute(sql, auto_begin, fetch):
        captured["sql"] = sql
        return None, Result()

    monkeypatch.setattr(adapter, "execute", execute)

    assert adapter._latest_schema_change_job(relation) is None
    assert "order by JobId desc limit 1" in captured["sql"]


def test_schema_change_cancel_is_reported(monkeypatch):
    adapter = object.__new__(DorisAdapter)
    relation = DorisRelation.create(schema="analytics", identifier="events")
    monkeypatch.setattr(
        adapter,
        "_latest_schema_change_job",
        lambda _: {
            "job_id": "2",
            "state": "CANCELLED",
            "message": "invalid type conversion",
        },
    )
    with pytest.raises(DbtRuntimeError) as excinfo:
        adapter.wait_for_schema_change(relation, previous_job_id="1")
    assert "invalid type conversion" in str(excinfo.value)
