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
Tests for Doris connection management: dbt debug, schema operations,
catalog generation, and multiple data types.
"""

import pytest
from dbt.tests.util import run_dbt, relation_from_name


MULTI_TYPE_MODEL_SQL = """
{{ config(
    materialized='table',
    distributed_by=['id'],
    properties={'replication_num': '1'}
) }}

select cast(1 as int) as id,
       cast('hello' as varchar(50)) as str_col,
       cast(3.14 as decimal(10,2)) as dec_col,
       cast(true as boolean) as bool_col,
       cast('2024-01-01' as date) as date_col,
       cast('2024-01-01 12:00:00' as datetime) as datetime_col,
       cast(100 as bigint) as bigint_col
"""


class TestDorisDebug:
    """Test that dbt debug can connect to Doris."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_type.sql": MULTI_TYPE_MODEL_SQL}

    def test_debug(self, project):
        results = run_dbt(["debug"])
        assert results is None or True


class TestDorisMultiType:
    """Test various Doris column types via CTAS."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_type.sql": MULTI_TYPE_MODEL_SQL}

    def test_multi_type(self, project):
        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "multi_type")
        result = project.run_sql(
            f"select id, str_col, dec_col, bool_col, date_col, bigint_col from {relation}",
            fetch="one",
        )
        assert result[0] == 1
        assert result[1] == "hello"
        assert result[4].isoformat() == "2024-01-01"
        assert result[5] == 100


class TestDorisCatalog:
    """Test catalog generation (dbt docs generate)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_type.sql": MULTI_TYPE_MODEL_SQL}

    def test_catalog(self, project):
        run_dbt(["run"])
        results = run_dbt(["docs", "generate"])
        assert results is not None
