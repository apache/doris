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

"""
Tests for Doris incremental materialization:
- append writes directly without a staging relation
- merge uses a Merge-on-Write Unique Key table without a staging relation
- delete+insert freezes the batch in a physical staging relation
- insert_overwrite performs real whole-table and partition overwrites
- full refresh replaces the target and preserves its Doris table configuration
"""

import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.util import relation_from_name, run_dbt


def _run_and_capture_sql(model_name, args=None, expect_pass=True):
    """Run dbt and return SQLQuery events for one model.

    Inspecting the catalog after a run only proves that a staging relation was
    cleaned up. SQLQuery events prove whether dbt physically created and read one
    during the run.
    """
    statements = []

    def capture_sql(event):
        if (
            event.info.name == "SQLQuery"
            and event.data.node_info.node_name == model_name
        ):
            statements.append(" ".join(event.data.sql.lower().split()))

    results = run_dbt(
        args or ["run"],
        expect_pass=expect_pass,
        callbacks=[capture_sql],
    )
    return results, list(statements)


def _assert_no_physical_dbt_staging(statements):
    physical_staging_statements = [
        statement
        for statement in statements
        if "__dbt_tmp" in statement
        and "create table" in statement
    ]
    assert physical_staging_statements == []


def _dbt_helper_relations(project, relation):
    return project.run_sql(
        "select table_name from information_schema.tables "
        f"where table_schema = '{relation.schema}' "
        f"and table_name like '{relation.identifier}__dbt_%' "
        "order by table_name",
        fetch="all",
    )


# -- Append strategy: works with duplicate key tables --

INCREMENTAL_APPEND_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 4 as id, 'dave' as name
union all
select 5 as id, 'eve' as name
{% else %}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
union all
select 3 as id, 'charlie' as name
{% endif %}
"""


# -- Merge strategy: Doris Unique Key upsert --

INCREMENTAL_MERGE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id'],
    distributed_by=['id'],
    sql_header='set enable_nereids_planner = true',
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'true'
    }
) }}

{% if is_incremental() %}
select 1 as id, 'alice_updated' as name, 150 as score
union all
select 4 as id, 'dave' as name, 400 as score
{% else %}
select 1 as id, 'alice' as name, 100 as score
union all
select 2 as id, 'bob' as name, 200 as score
union all
select 3 as id, 'charlie' as name, 300 as score
{% endif %}
"""


INCREMENTAL_DUPLICATE_KEY_MERGE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'true'
    }
) }}

{% if is_incremental() %}
select 1 as id, 'conflicting_first' as name
union all
select 1 as id, 'conflicting_second' as name
{% else %}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
{% endif %}
"""


INCREMENTAL_COMPOSITE_MERGE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['tenant_id', 'id'],
    distributed_by=['tenant_id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 1 as tenant_id, 1 as id, 'updated' as value
union all
select 2 as tenant_id, 2 as id, 'new' as value
{% else %}
select 1 as tenant_id, 1 as id, 'old' as value
union all
select 1 as tenant_id, 2 as id, 'keep' as value
union all
select 2 as tenant_id, 1 as id, 'other_tenant' as value
{% endif %}
"""


# -- Delete and insert: physical staging freezes one batch for both DMLs --

INCREMENTAL_DELETE_INSERT_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id'],
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'false'
    }
) }}

{% if is_incremental() %}
select 1 as id, 'alice_replaced' as name, 175 as score
union all
select 4 as id, 'dave' as name, 400 as score
{% else %}
select 1 as id, 'alice' as name, 100 as score
union all
select 2 as id, 'bob' as name, 200 as score
union all
select 3 as id, 'charlie' as name, 300 as score
{% endif %}
"""


INCREMENTAL_DELETE_INSERT_MOW_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'true'
    }
) }}

{% if is_incremental() %}
select 1 as id, 'alice_replaced' as name, 175 as score
union all
select 4 as id, 'dave' as name, 400 as score
{% else %}
select 1 as id, 'alice' as name, 100 as score
union all
select 2 as id, 'bob' as name, 200 as score
union all
select 3 as id, 'charlie' as name, 300 as score
{% endif %}
"""


INCREMENTAL_DELETE_INSERT_SEQUENCE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'true'
    }
) }}

{% if is_incremental() %}
select 1 as id, 50 as sequence_id, 'lower_sequence' as value
{% else %}
select 1 as id, 100 as sequence_id, 'original' as value
{% endif %}
"""


INCREMENTAL_DUPLICATE_KEY_DELETE_INSERT_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'false'
    }
) }}

{% if is_incremental() %}
select 1 as id, 'conflicting_first' as name
union all
select 1 as id, 'conflicting_second' as name
{% else %}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
{% endif %}
"""


INCREMENTAL_COMPOSITE_DELETE_INSERT_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['tenant_id', 'id'],
    distributed_by=['tenant_id'],
    properties={
        'replication_num': '1',
        'enable_unique_key_merge_on_write': 'false'
    }
) }}

{% if is_incremental() %}
select 1 as tenant_id, 1 as id, 'replaced' as value
union all
select 2 as tenant_id, 2 as id, 'new' as value
{% else %}
select 1 as tenant_id, 1 as id, 'old' as value
union all
select 1 as tenant_id, 2 as id, 'keep' as value
union all
select 2 as tenant_id, 1 as id, 'other_tenant' as value
{% endif %}
"""


# -- Insert overwrite: replace the complete target with the current batch --

INCREMENTAL_OVERWRITE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 1 as id, 'alice_replaced' as name
union all
select 4 as id, 'dave' as name
{% else %}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
union all
select 3 as id, 'charlie' as name
{% endif %}
"""


# -- Static partition overwrite: replace p1 while retaining p2 --

INCREMENTAL_STATIC_PARTITION_OVERWRITE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    overwrite_partitions=['p1'],
    duplicate_key=['part_id'],
    partition_by=['part_id'],
    partition_type='RANGE',
    partition_by_init=[
        'PARTITION p1 VALUES LESS THAN ("2")',
        'PARTITION p2 VALUES LESS THAN ("3")'
    ],
    distributed_by=['part_id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 1 as part_id, 'static_new_p1' as value
{% else %}
select 1 as part_id, 'static_old_p1' as value
union all
select 2 as part_id, 'static_unchanged_p2' as value
{% endif %}
"""


# -- Dynamic partition overwrite: replace only partitions present in this batch --

INCREMENTAL_DYNAMIC_PARTITION_OVERWRITE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    overwrite_partitions='*',
    duplicate_key=['part_id'],
    partition_by=['part_id'],
    partition_type='RANGE',
    partition_by_init=[
        'PARTITION p1 VALUES LESS THAN ("2")',
        'PARTITION p2 VALUES LESS THAN ("3")'
    ],
    distributed_by=['part_id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 1 as part_id, 'dynamic_new_p1' as value
{% else %}
select 1 as part_id, 'dynamic_old_p1' as value
union all
select 2 as part_id, 'dynamic_unchanged_p2' as value
{% endif %}
"""


# -- Full refresh --

INCREMENTAL_FULL_REFRESH_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={
        'replication_num': '1',
        'disable_auto_compaction': 'true'
    }
) }}

select 1 as id, 'only_row' as name
"""


INCREMENTAL_VIEW_TO_TABLE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    duplicate_key=['ASSET_ID'],
    distributed_by=['ASSET_ID'],
    properties={'replication_num': '1'}
) }}

select 7 as `ASSET_ID`, 'new_table' as value
"""


INCREMENTAL_VARCHAR_WIDEN_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

{% if is_incremental() %}
select 2 as id, cast('expanded' as varchar(40)) as name
{% else %}
select 1 as id, cast('a' as varchar(5)) as name
{% endif %}
"""


INCREMENTAL_RECOVERY_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select *
from dbt_incremental_intentional_missing_relation
"""


class TestDorisIncrementalAppend:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_append.sql": INCREMENTAL_APPEND_SQL}

    def test_incremental_append(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_append")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        results, statements = _run_and_capture_sql("incremental_append")
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )
        assert rows == [
            (1, "alice"),
            (2, "bob"),
            (3, "charlie"),
            (4, "dave"),
            (5, "eve"),
        ]

        direct_inserts = [
            statement
            for statement in statements
            if "insert into" in statement and "incremental_append" in statement
        ]
        assert len(direct_inserts) == 1
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalMerge:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_merge.sql": INCREMENTAL_MERGE_SQL}

    def test_merge_upserts_without_staging(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_merge")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        results, statements = _run_and_capture_sql("incremental_merge")
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name, score from {relation} order by id",
            fetch="all",
        )
        assert rows == [
            (1, "alice_updated", 150),
            (2, "bob", 200),
            (3, "charlie", 300),
            (4, "dave", 400),
        ]

        create_table = project.run_sql(
            f"show create table {relation}",
            fetch="one",
        )[1].lower()
        assert "unique key" in create_table
        assert '"enable_unique_key_merge_on_write" = "true"' in create_table

        direct_inserts = [
            statement
            for statement in statements
            if "insert into" in statement and "incremental_merge" in statement
        ]
        assert len(direct_inserts) == 1
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalMergeRejectsDuplicateKeys:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_duplicate_key_merge.sql": (
                INCREMENTAL_DUPLICATE_KEY_MERGE_SQL
            ),
        }

    def test_duplicate_source_keys_fail_without_changing_target(self, project):
        assert len(run_dbt(["run"])) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_duplicate_key_merge",
        )
        rows_before = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )

        failure, statements = _run_and_capture_sql(
            "incremental_duplicate_key_merge",
            expect_pass=False,
        )
        assert len(failure.results) == 1
        assert any(
            "dbt_internal_duplicate_keys" in statement
            for statement in statements
        )
        _assert_no_physical_dbt_staging(statements)

        rows_after = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )
        assert rows_after == rows_before == [
            (1, "alice"),
            (2, "bob"),
        ]


class TestDorisIncrementalCompositeMerge:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_composite_merge.sql": INCREMENTAL_COMPOSITE_MERGE_SQL}

    def test_merge_uses_all_unique_key_columns(self, project):
        assert len(run_dbt(["run"])) == 1
        results, statements = _run_and_capture_sql("incremental_composite_merge")
        assert len(results) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_composite_merge",
        )
        rows = project.run_sql(
            f"select tenant_id, id, value from {relation} "
            "order by tenant_id, id",
            fetch="all",
        )
        assert rows == [
            (1, 1, "updated"),
            (1, 2, "keep"),
            (2, 1, "other_tenant"),
            (2, 2, "new"),
        ]
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalDeleteInsert:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delete_insert.sql": INCREMENTAL_DELETE_INSERT_SQL,
        }

    def test_delete_insert_reuses_and_cleans_staging(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_delete_insert")
        results, statements = _run_and_capture_sql("incremental_delete_insert")
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name, score from {relation} order by id",
            fetch="all",
        )
        assert rows == [
            (1, "alice_replaced", 175),
            (2, "bob", 200),
            (3, "charlie", 300),
            (4, "dave", 400),
        ]

        staging_name = "incremental_delete_insert__dbt_tmp"
        assert any(
            "create table" in statement and staging_name in statement
            for statement in statements
        )
        assert any(
            staging_name in statement
            and (
                "delete from" in statement
                or "__doris_delete_sign__" in statement
            )
            for statement in statements
        )
        assert any(
            "insert into" in statement
            and "incremental_delete_insert" in statement
            and staging_name in statement
            and "__doris_delete_sign__" not in statement
            for statement in statements
        )
        assert any(
            "drop table if exists" in statement and staging_name in statement
            for statement in statements
        )

        begin_statements = [
            index
            for index, statement in enumerate(statements)
            if statement.rstrip(";").endswith("begin")
        ]
        delete_statements = [
            index
            for index, statement in enumerate(statements)
            if "delete from" in statement
            and "incremental_delete_insert" in statement
        ]
        insert_statements = [
            index
            for index, statement in enumerate(statements)
            if "insert into" in statement
            and "incremental_delete_insert" in statement
            and staging_name in statement
        ]
        commit_statements = [
            index
            for index, statement in enumerate(statements)
            if statement.rstrip(";").endswith("commit")
        ]
        assert len(begin_statements) == 1
        assert len(delete_statements) == 1
        assert len(insert_statements) == 1
        assert len(commit_statements) == 1
        assert (
            begin_statements[0]
            < delete_statements[0]
            < insert_statements[0]
            < commit_statements[0]
        )
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalDeleteInsertOnMow:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delete_insert_mow.sql": (
                INCREMENTAL_DELETE_INSERT_MOW_SQL
            ),
        }

    def test_delete_insert_uses_single_mow_upsert(self, project):
        assert len(run_dbt(["run"])) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_delete_insert_mow",
        )
        results, statements = _run_and_capture_sql(
            "incremental_delete_insert_mow"
        )
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name, score from {relation} order by id",
            fetch="all",
        )
        assert rows == [
            (1, "alice_replaced", 175),
            (2, "bob", 200),
            (3, "charlie", 300),
            (4, "dave", 400),
        ]

        create_table = project.run_sql(
            f"show create table {relation}",
            fetch="one",
        )[1].lower()
        assert "unique key" in create_table
        assert '"enable_unique_key_merge_on_write" = "true"' in create_table

        direct_inserts = [
            statement
            for statement in statements
            if "insert into" in statement
            and "incremental_delete_insert_mow" in statement
        ]
        assert len(direct_inserts) == 1
        assert "dbt_internal_duplicate_keys" in direct_inserts[0]

        _assert_no_physical_dbt_staging(statements)
        assert not any(
            statement.rstrip(";").endswith("begin")
            for statement in statements
        )
        assert not any("delete from" in statement for statement in statements)
        assert not any(
            statement.rstrip(";").endswith("commit")
            for statement in statements
        )
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalDeleteInsertRejectsSequence:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delete_insert_sequence.sql": (
                INCREMENTAL_DELETE_INSERT_SEQUENCE_SQL
            ),
        }

    def test_sequence_target_fails_before_staging_or_write(self, project):
        relation = relation_from_name(
            project.adapter,
            "incremental_delete_insert_sequence",
        )
        project.run_sql(
            f"create table {relation} ("
            "`id` int, `sequence_id` int, `value` varchar(20)"
            ") unique key(`id`) "
            "distributed by hash(`id`) buckets auto "
            "properties("
            '"replication_num" = "1", '
            '"enable_unique_key_merge_on_write" = "true", '
            '"function_column.sequence_col" = "sequence_id"'
            ")"
        )
        project.run_sql(
            f"insert into {relation} values (1, 100, 'original')"
        )

        failure, statements = _run_and_capture_sql(
            "incremental_delete_insert_sequence",
            expect_pass=False,
        )
        assert len(failure.results) == 1
        assert "sequence column" in failure.results[0].message.lower()
        _assert_no_physical_dbt_staging(statements)

        rows = project.run_sql(
            f"select id, sequence_id, value from {relation}",
            fetch="all",
        )
        assert rows == [(1, 100, "original")]
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalDeleteInsertRejectsDuplicateKeys:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_duplicate_key_delete_insert.sql": (
                INCREMENTAL_DUPLICATE_KEY_DELETE_INSERT_SQL
            ),
        }

    def test_duplicate_source_keys_fail_without_changing_target(self, project):
        assert len(run_dbt(["run"])) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_duplicate_key_delete_insert",
        )
        rows_before = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )

        failure, statements = _run_and_capture_sql(
            "incremental_duplicate_key_delete_insert",
            expect_pass=False,
        )
        assert len(failure.results) == 1
        assert any(
            "dbt_internal_duplicate_keys" in statement
            for statement in statements
        )
        assert not any(
            statement.rstrip(";").endswith("begin")
            for statement in statements
        )

        rows_after = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )
        assert rows_after == rows_before == [
            (1, "alice"),
            (2, "bob"),
        ]


class TestDorisIncrementalCompositeDeleteInsert:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_composite_delete_insert.sql": (
                INCREMENTAL_COMPOSITE_DELETE_INSERT_SQL
            ),
        }

    def test_delete_insert_matches_the_complete_composite_key(self, project):
        assert len(run_dbt(["run"])) == 1
        results, statements = _run_and_capture_sql(
            "incremental_composite_delete_insert"
        )
        assert len(results) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_composite_delete_insert",
        )
        rows = project.run_sql(
            f"select tenant_id, id, value from {relation} "
            "order by tenant_id, id",
            fetch="all",
        )
        assert rows == [
            (1, 1, "replaced"),
            (1, 2, "keep"),
            (2, 1, "other_tenant"),
            (2, 2, "new"),
        ]

        delete_sql = next(
            statement for statement in statements if "delete from" in statement
        )
        assert "dbt_internal_dest.`tenant_id`" in delete_sql
        assert "dbt_internal_dest.`id`" in delete_sql
        assert delete_sql.count("<=>") == 2
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalInsertOverwrite:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_overwrite.sql": INCREMENTAL_OVERWRITE_SQL}

    def test_whole_table_insert_overwrite_removes_old_rows(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_overwrite")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        results, statements = _run_and_capture_sql("incremental_overwrite")
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )
        assert rows == [
            (1, "alice_replaced"),
            (4, "dave"),
        ]

        overwrite_statements = [
            statement
            for statement in statements
            if "insert overwrite" in statement
            and "incremental_overwrite" in statement
        ]
        assert len(overwrite_statements) == 1
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalStaticPartitionOverwrite:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_static_partition_overwrite.sql": (
                INCREMENTAL_STATIC_PARTITION_OVERWRITE_SQL
            ),
        }

    def test_static_partition_overwrite_replaces_only_named_partition(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_static_partition_overwrite",
        )
        results, statements = _run_and_capture_sql(
            "incremental_static_partition_overwrite"
        )
        assert len(results) == 1

        rows = project.run_sql(
            f"select part_id, value from {relation} order by part_id",
            fetch="all",
        )
        assert rows == [
            (1, "static_new_p1"),
            (2, "static_unchanged_p2"),
        ]

        overwrite_statements = [
            statement
            for statement in statements
            if "insert overwrite" in statement
            and "incremental_static_partition_overwrite" in statement
        ]
        assert len(overwrite_statements) == 1
        assert "partition(`p1`)" in overwrite_statements[0].replace(" ", "")
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalDynamicPartitionOverwrite:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_dynamic_partition_overwrite.sql": (
                INCREMENTAL_DYNAMIC_PARTITION_OVERWRITE_SQL
            ),
        }

    def test_dynamic_partition_overwrite_preserves_unseen_partitions(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(
            project.adapter,
            "incremental_dynamic_partition_overwrite",
        )
        results, statements = _run_and_capture_sql(
            "incremental_dynamic_partition_overwrite"
        )
        assert len(results) == 1

        rows = project.run_sql(
            f"select part_id, value from {relation} order by part_id",
            fetch="all",
        )
        assert rows == [
            (1, "dynamic_new_p1"),
            (2, "dynamic_unchanged_p2"),
        ]

        overwrite_statements = [
            statement
            for statement in statements
            if "insert overwrite" in statement
            and "incremental_dynamic_partition_overwrite" in statement
        ]
        assert len(overwrite_statements) == 1
        assert "partition(*)" in overwrite_statements[0].replace(" ", "")
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalVarcharWidening:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_varchar_widen.sql": INCREMENTAL_VARCHAR_WIDEN_SQL,
        }

    def test_ignore_widens_string_without_physical_staging(self, project):
        relation = relation_from_name(
            project.adapter,
            "incremental_varchar_widen",
        )
        project.run_sql(
            f"create table {relation} ("
            "`id` int, `name` varchar(5)"
            ") duplicate key(`id`) "
            "distributed by hash(`id`) buckets auto "
            'properties("replication_num" = "1")'
        )
        project.run_sql(f"insert into {relation} values (1, 'a')")

        results, statements = _run_and_capture_sql("incremental_varchar_widen")
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, name from {relation} order by id",
            fetch="all",
        )
        assert rows == [(1, "a"), (2, "expanded")]

        column_type = project.run_sql(
            "select column_type from information_schema.columns "
            f"where table_schema = '{relation.schema}' "
            f"and table_name = '{relation.identifier}' "
            "and column_name = 'name'",
            fetch="one",
        )[0]
        widened_size = int(
            column_type.lower().removeprefix("varchar(").removesuffix(")")
        )
        assert widened_size >= 40
        assert any(
            "create or replace view" in statement
            and "__dbt_tmp" in statement
            for statement in statements
        )
        _assert_no_physical_dbt_staging(statements)
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalFullRefresh:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_fr.sql": INCREMENTAL_FULL_REFRESH_SQL}

    def test_full_refresh(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_fr")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 1

        results = run_dbt(["run"])
        assert len(results) == 1
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 2

        results, statements = _run_and_capture_sql(
            "incremental_fr",
            ["run", "--full-refresh"],
        )
        assert len(results) == 1

        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 1

        create_table = project.run_sql(
            f"show create table {relation}",
            fetch="one",
        )[1].lower()
        assert "duplicate key" in create_table
        assert "distributed by hash(`id`)" in create_table
        assert '"disable_auto_compaction" = "true"' in create_table

        # Full refresh intentionally builds an intermediate table before the
        # atomic REPLACE WITH TABLE. The no-staging rule applies to ordinary
        # incremental DML, not to safe full-refresh replacement.
        assert any(
            "create table" in statement
            and "incremental_fr__dbt_tmp" in statement
            for statement in statements
        )
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalViewToTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_view_to_table.sql": INCREMENTAL_VIEW_TO_TABLE_SQL,
        }

    def test_view_with_as_identifier_is_replaced_by_table(self, project):
        relation = relation_from_name(
            project.adapter,
            "incremental_view_to_table",
        )
        project.run_sql(
            f"create view {relation} (`ASSET_ID`, `value`) as "
            "select 99 as `ASSET_ID`, 'old_view' as `value`"
        )

        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            f"select `ASSET_ID`, `value` from {relation}",
            fetch="all",
        )
        assert rows == [(7, "new_table")]
        table_type = project.run_sql(
            "select table_type from information_schema.tables "
            f"where table_schema = '{relation.schema}' "
            f"and table_name = '{relation.identifier}'",
            fetch="one",
        )[0]
        assert table_type == "BASE TABLE"
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalBackupRecovery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_recovery.sql": INCREMENTAL_RECOVERY_SQL}

    def test_restores_view_backup_before_a_failed_retry(self, project):
        relation = relation_from_name(project.adapter, "incremental_recovery")
        backup_name = f"{relation.identifier}__dbt_backup"
        project.run_sql(
            f"create view `{relation.schema}`.`{backup_name}` "
            "as select 99 as id, 'old_definition' as value"
        )

        failure = run_dbt(["run"], expect_pass=False)
        assert len(failure.results) == 1

        rows = project.run_sql(
            f"select id, value from {relation}",
            fetch="all",
        )
        assert rows == [(99, "old_definition")]
        assert _dbt_helper_relations(project, relation) == []


class TestDorisIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    """Run dbt's 1.12 schema-change contract against logical source views."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+properties": {
                    "replication_num": "1",
                }
            }
        }
