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
Tests for Doris table materialization with various key types,
distributed by, properties, and table re-creation (exchange).
"""

import pytest
from dbt.tests.util import run_dbt, relation_from_name


# -- Model SQL definitions --

TABLE_BASIC_SQL = """
{{ config(
    materialized='table',
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select 1 as id, 'alice' as name, 100 as score
union all
select 2 as id, 'bob' as name, 200 as score
union all
select 3 as id, 'charlie' as name, 300 as score
"""

TABLE_DUPLICATE_KEY_SQL = """
{{ config(
    materialized='table',
    duplicate_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
"""

TABLE_BUCKETS_AUTO_SQL = """
{{ config(
    materialized='table',
    duplicate_key=['id'],
    distributed_by=['id'],
    buckets=3,
    properties={'replication_num': '1'}
) }}

select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
"""

TABLE_WITH_COMMENT_SQL = """
{{ config(
    materialized='table',
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select 1 as id, 'test' as name
"""

TABLE_WITH_COMMENT_SCHEMA_YML = """
version: 2
models:
  - name: table_with_comment
    description: "This is a test table with comments"
    columns:
      - name: id
        description: "The primary key"
      - name: name
        description: "The user name"
"""


class TestDorisTableBasic:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_basic.sql": TABLE_BASIC_SQL}

    def test_table_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "table_basic")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        result = project.run_sql(f"select sum(score) from {relation}", fetch="one")
        assert result[0] == 600


class TestDorisTableDuplicateKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_dup_key.sql": TABLE_DUPLICATE_KEY_SQL}

    def test_duplicate_key(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "table_dup_key")
        result = project.run_sql(
            f"show create table {relation}", fetch="one"
        )
        assert "DUPLICATE KEY" in result[1]


class TestDorisTableBuckets:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_buckets.sql": TABLE_BUCKETS_AUTO_SQL}

    def test_table_buckets(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "table_buckets")
        result = project.run_sql(
            f"show create table {relation}", fetch="one"
        )
        assert "BUCKETS 3" in result[1]
        assert "DUPLICATE KEY" in result[1]


class TestDorisTableRerun:
    """Test that re-running table materialization replaces the table via exchange."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_basic.sql": TABLE_BASIC_SQL}

    def test_table_rerun(self, project):
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "table_basic")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        # Second run (should replace via exchange)
        results = run_dbt(["run"])
        assert len(results) == 1

        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3


class TestDorisTableWithComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_with_comment.sql": TABLE_WITH_COMMENT_SQL,
            "schema.yml": TABLE_WITH_COMMENT_SCHEMA_YML,
        }

    def test_table_comment(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "table_with_comment")
        result = project.run_sql(
            f"show create table {relation}", fetch="one"
        )
        assert "This is a test table with comments" in result[1]
