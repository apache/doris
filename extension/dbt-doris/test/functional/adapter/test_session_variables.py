#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Functional test for session_variables. Requires a running Doris (set DORIS_TEST_* env)."""

import os
import pytest

from dbt.tests.util import run_dbt


class TestSessionVariables:
    """Verify session_variables from profile are applied on connection."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "doris",
            "threads": 1,
            "host": os.getenv("DORIS_TEST_HOST", "127.0.0.1"),
            "user": os.getenv("DORIS_TEST_USER", "root"),
            "password": os.getenv("DORIS_TEST_PASSWORD", ""),
            "port": int(os.getenv("DORIS_TEST_PORT", "9030")),
            "schema": os.getenv("DORIS_TEST_SCHEMA", "dbt"),
            "session_variables": {
                "time_zone": "Asia/Shanghai",
            },
        }

    def test_session_variables_applied(self, project):
        run_dbt(["debug"])
        result = project.run_sql("SHOW VARIABLES LIKE 'time_zone'", fetch="all")
        assert result is not None
        assert len(result) >= 1
        row = result[0]
        assert len(row) >= 2
        assert row[0].lower() == "time_zone"
        assert "Shanghai" in str(row[1])
