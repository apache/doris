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
Tests for Doris seed functionality: CSV loading with type inference
and custom column types.
"""

import pytest
from dbt.tests.util import run_dbt, relation_from_name


SEED_CSV = """id,name,score,active
1,alice,100,true
2,bob,200,false
3,charlie,300,true
"""

SEED_WITH_TYPES_YML = """
seeds:
  - name: seed_typed
    config:
      column_types:
        id: int
        name: varchar(100)
        score: bigint
        active: boolean
"""

SEED_TYPED_CSV = """id,name,score,active
1,alice,100,true
2,bob,200,false
"""

MODEL_FROM_SEED_SQL = """
{{ config(
    materialized='table',
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select * from {{ ref('seed_basic') }}
"""


class TestDorisSeedBasic:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_basic.csv": SEED_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+properties": {"replication_num": "1"}}}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_seed_basic(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "seed_basic")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3


class TestDorisSeedWithTypes:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_typed.csv": SEED_TYPED_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {
            "+properties": {"replication_num": "1"},
            "test": {"seed_typed": {"column_types": {
                "id": "int",
                "name": "varchar(100)",
                "score": "bigint",
                "active": "boolean",
            }}},
        }}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_seed_with_types(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "seed_typed")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 2


class TestDorisSeedAndModel:
    """Test that a model can reference a seed."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_basic.csv": SEED_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+properties": {"replication_num": "1"}}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_from_seed.sql": MODEL_FROM_SEED_SQL}

    def test_seed_then_model(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "model_from_seed")
        result = project.run_sql(f"select count(*) from {relation}", fetch="one")
        assert result[0] == 3

        result = project.run_sql(f"select sum(score) from {relation}", fetch="one")
        assert result[0] == 600
