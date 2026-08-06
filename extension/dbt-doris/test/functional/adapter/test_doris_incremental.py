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
- append strategy (duplicate key table)
- insert_overwrite strategy (unique key table)
- full refresh mode
"""

import pytest
from dbt.tests.util import run_dbt, relation_from_name


# -- Append strategy: works with duplicate key tables --

INCREMENTAL_APPEND_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
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


# -- Insert overwrite strategy: works with unique key tables --

INCREMENTAL_UNIQUE_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
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


# -- Full refresh --

INCREMENTAL_FULL_REFRESH_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select 1 as id, 'only_row' as name
"""


class TestDorisIncrementalAppend:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_append.sql": INCREMENTAL_APPEND_SQL}

    def test_incremental_append(self, project):
        # First run: creates table with 3 rows
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_append")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        # Second run: appends 2 more rows
        results = run_dbt(["run"])
        assert len(results) == 1

        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 5


class TestDorisIncrementalUniqueKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_unique.sql": INCREMENTAL_UNIQUE_SQL}

    def test_incremental_unique_key(self, project):
        # First run: creates unique key table with 3 rows
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_unique")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        # Second run: upserts 2 rows (1 update + 1 new)
        results = run_dbt(["run"])
        assert len(results) == 1

        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 4

        # Verify id=1 was updated
        result = project.run_sql(
            f"select name, score from {relation} where id = 1", fetch="one"
        )
        assert result[0] == "alice_updated"
        assert result[1] == 150

        # Verify id=2 remains unchanged
        result = project.run_sql(
            f"select name from {relation} where id = 2", fetch="one"
        )
        assert result[0] == "bob"


class TestDorisIncrementalFullRefresh:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_fr.sql": INCREMENTAL_FULL_REFRESH_SQL}

    def test_full_refresh(self, project):
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "incremental_fr")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 1

        # Second run: normal incremental (appends same row)
        results = run_dbt(["run"])
        assert len(results) == 1
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 2

        # Full refresh: should reset to 1 row
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1

        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 1
