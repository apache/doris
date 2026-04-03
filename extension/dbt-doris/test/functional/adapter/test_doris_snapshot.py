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
Tests for Doris snapshot (SCD Type 2) using check strategy.
Doris snapshots require a unique key table for the upsert-based merge.
"""

import pytest
from dbt.tests.util import run_dbt, relation_from_name


SOURCE_TABLE_SQL = """
{{ config(
    materialized='table',
    unique_key=['id'],
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select 1 as id, 'alice' as name, 100 as score
union all
select 2 as id, 'bob' as name, 200 as score
"""

SNAPSHOT_SQL = """
{% snapshot snap_users %}

{{
    config(
        target_database=target.schema,
        target_schema=target.schema,
        unique_key='id',
        strategy='check',
        check_cols=['name', 'score'],
    )
}}

select * from {{ ref('snap_source') }}

{% endsnapshot %}
"""


class TestDorisSnapshot:
    @pytest.fixture(scope="class")
    def models(self):
        return {"snap_source.sql": SOURCE_TABLE_SQL}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap_users.sql": SNAPSHOT_SQL}

    def test_snapshot(self, project):
        # Create the source table
        results = run_dbt(["run"])
        assert len(results) == 1

        # First snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "snap_users")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 2

        # All rows should have dbt_valid_to = NULL (current)
        result = project.run_sql(
            f"select count(*) from {relation} where dbt_valid_to is null",
            fetch="one",
        )
        assert result[0] == 2
