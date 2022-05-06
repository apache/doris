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

from contextlib import contextmanager
from dataclasses import dataclass
from typing import ContextManager, Optional, Union

import MySQLdb
import MySQLdb.cursors
from dbt import exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.events import AdapterLogger

logger = AdapterLogger("doris")


@dataclass
class DorisAdapterCredentials(Credentials):
    host: str = "127.0.0.1"
    port: int = 9030
    username: str = "root"
    password: str = ""
    database: Optional[str] = None
    schema: Optional[str] = None

    @property
    def type(self):
        return "doris"

    def _connection_keys(self):
        return "host", "port", "user", "schema"

    @property
    def unique_field(self) -> str:
        return self.host

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise exceptions.RuntimeException(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Doris, database must be omitted or have the same value as"
                f" schema."
            )


class DorisAdapterConnectionManager(SQLConnectionManager):
    TYPE = "doris"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open")
            return connection
        credentials = connection.credentials
        kwargs = {
            "host": credentials.host,
            "port": credentials.port,
            "user": credentials.username,
            "password": credentials.password,
        }
        try:
            connection.handle = MySQLdb.connect(**kwargs)
            connection.state = ConnectionState.OPEN
        except MySQLdb.Error as e:
            logger.debug(f"Error connecting to database: {e}")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise exceptions.FailedToConnectException(str(e))
        return connection

    def cancel(self, connection: Connection):
        connection.handle.close()

    @classmethod
    def get_response(cls, cursor: MySQLdb.cursors.Cursor) -> Union[AdapterResponse, str]:
        code = "Unknown cursor state/status"
        rows = cursor.rowcount
        return AdapterResponse(
            code=code,
            _message=f"{rows} rows affected",
            rows_affected=rows,
        )

    @contextmanager  # type: ignore
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        except MySQLdb.DatabaseError as e:
            logger.debug(f"Doris database error: {e}, sql: {sql}")
            raise exceptions.DatabaseException(str(e)) from e
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            if isinstance(e, exceptions.RuntimeException):
                raise e
            raise exceptions.RuntimeException(str(e)) from e

    def begin(self):
        """
        https://doris.apache.org/zh-CN/sql-reference/sql-statements/Data%20Manipulation/BEGIN.html#description
        Doris only support transaction in insert, just ignore it
        """
        pass

    def commit(self):
        pass
