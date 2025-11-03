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

import mysql.connector

from dbt import exceptions
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("doris")


@dataclass
class DorisCredentials(Credentials):
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
        return self.schema

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Doris, database must be omitted or have the same value as"
                f" schema."
            )


class DorisConnectionManager(SQLConnectionManager):
    TYPE = "doris"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open")
            return connection
        credentials = cls.get_credentials(connection.credentials)
        kwargs = {
            "host": credentials.host,
            "port": credentials.port,
            "user": credentials.username,
            "password": credentials.password,
            "buffered": True,
            "charset": "utf8",
            "get_warnings": True,
        }

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'
        except mysql.connector.Error:

            try:
                logger.debug("Failed connection without supplying the `database`. "
                             "Trying again with `database` included.")
                connection.handle = mysql.connector.connect(**kwargs)
                connection.state = 'open'
            except mysql.connector.Error as e:

                logger.debug("Got an error when attempting to open a mysql "
                             "connection: '{}'"
                             .format(e))

                connection.handle = None
                connection.state = 'fail'

                raise exceptions.DbtRuntimeError(str(e))
        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def cancel(self, connection: Connection):
        connection.handle.close()

    @classmethod
    def get_response(cls, cursor) -> Union[AdapterResponse, str]:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount
        return AdapterResponse(
            code=code,
            _message=f"{num_rows} rows affected",
            rows_affected=num_rows,
        )

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except mysql.connector.DatabaseError as e:
            logger.debug(f"Doris database error: {e}, sql: {sql}")
            raise exceptions.DbtRuntimeError(str(e)) from e
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            if isinstance(e, exceptions.DbtRuntimeError):
                raise e
            raise exceptions.DbtRuntimeError(str(e)) from e

    @classmethod
    def begin(self):
        """
        https://doris.apache.org/docs/data-operate/import/import-scenes/load-atomicity/
        Doris's inserting always transaction, ignore it
        """
        pass

    @classmethod
    def commit(self):
        pass
