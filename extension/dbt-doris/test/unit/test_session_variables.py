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

"""Simple unit tests for session_variables (no Doris required)."""

import pytest
from unittest.mock import MagicMock

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.doris.connections import DorisConnectionManager, DorisCredentials


class TestDorisCredentialsSessionVariables:
    """DorisCredentials session_variables field."""

    def test_without_session_variables(self):
        cred = DorisCredentials(
            host="127.0.0.1",
            port=9030,
            username="root",
            password="",
            schema="dbt",
        )
        assert cred.session_variables is None

    def test_with_session_variables(self):
        cred = DorisCredentials(
            host="127.0.0.1",
            port=9030,
            username="root",
            password="",
            schema="dbt",
            session_variables={"time_zone": "Asia/Shanghai", "exec_mem_limit": 8589934592},
        )
        assert cred.session_variables["time_zone"] == "Asia/Shanghai"
        assert cred.session_variables["exec_mem_limit"] == 8589934592

    def test_empty_session_variables(self):
        cred = DorisCredentials(
            host="127.0.0.1",
            port=9030,
            username="root",
            password="",
            schema="dbt",
            session_variables={},
        )
        assert cred.session_variables == {}


class TestSetSessionVariables:
    """_set_session_variables builds correct SET SQL (mock connection)."""

    def test_string_and_int(self):
        connection = MagicMock(spec=Connection)
        cursor = MagicMock()
        connection.handle.cursor.return_value = cursor

        DorisConnectionManager._set_session_variables(
            connection,
            {"time_zone": "Asia/Shanghai", "exec_mem_limit": 8589934592},
        )

        assert cursor.execute.call_count == 2
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert "SET time_zone = 'Asia/Shanghai'" in calls
        assert "SET exec_mem_limit = 8589934592" in calls
        cursor.close.assert_called_once()

    def test_string_with_quote_escaped(self):
        connection = MagicMock(spec=Connection)
        cursor = MagicMock()
        connection.handle.cursor.return_value = cursor

        DorisConnectionManager._set_session_variables(connection, {"x": "a'b"})

        cursor.execute.assert_called_once_with("SET x = 'a''b'")
        cursor.close.assert_called_once()

    def test_bool_values(self):
        connection = MagicMock(spec=Connection)
        cursor = MagicMock()
        connection.handle.cursor.return_value = cursor

        DorisConnectionManager._set_session_variables(
            connection,
            {"enable_profile": True, "enable_debug": False},
        )

        assert cursor.execute.call_count == 2
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert "SET enable_profile = TRUE" in calls
        assert "SET enable_debug = FALSE" in calls
