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
from mysql.connector.constants import FieldType

from dbt import exceptions
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection
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
        return "host", "port", "username", "schema"

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
            "database": credentials.schema,
            "buffered": True,
            "charset": "utf8",
            "get_warnings": True,
        }

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'
        except mysql.connector.Error as e:
            # If the database does not exist yet, connect without it.
            # dbt will create the database/schema via create_schema().
            if e.errno == 1049:  # Unknown database
                logger.debug(
                    f"Database '{credentials.schema}' does not exist, "
                    "connecting without database."
                )
                kwargs.pop("database", None)
                try:
                    connection.handle = mysql.connector.connect(**kwargs)
                    connection.state = 'open'
                except mysql.connector.Error as e2:
                    logger.debug(
                        "Got an error when attempting to open a Doris "
                        "connection: '{}'".format(e2)
                    )
                    connection.handle = None
                    connection.state = 'fail'
                    raise exceptions.DbtRuntimeError(str(e2))
            else:
                logger.debug("Got an error when attempting to open a Doris "
                             "connection: '{}'"
                             .format(e))

                connection.handle = None
                connection.state = 'fail'

                raise exceptions.DbtRuntimeError(str(e))
        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

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
        except mysql.connector.Error as e:
            # A small number of adapter operations (currently transactional
            # delete+insert on Doris 3.0+) issue a literal BEGIN. Roll back
            # immediately on a failed statement so a pooled connection never
            # retains an open server-side transaction. Outside an explicit
            # transaction mysql-connector's rollback is a harmless no-op.
            connection = self.get_if_exists()
            if connection is not None and connection.handle is not None:
                try:
                    connection.handle.rollback()
                except Exception:
                    # The server-side transaction state is now unknown. Mark
                    # the connection unusable so the pool cannot issue later
                    # model SQL on it.
                    connection.state = "fail"
                    logger.debug(
                        "Failed to roll back Doris connection after error; "
                        "marking the connection failed"
                    )
                connection.transaction_open = False
            logger.debug(f"Doris database error: {e}, sql: {sql}")
            raise exceptions.DbtRuntimeError(str(e)) from e
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            if isinstance(e, exceptions.DbtRuntimeError):
                raise e
            raise exceptions.DbtRuntimeError(str(e)) from e

    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        """Map mysql-connector type codes to Doris type names."""
        mapping = {
            FieldType.TINY: "TINYINT",
            FieldType.SHORT: "SMALLINT",
            FieldType.LONG: "INT",
            FieldType.FLOAT: "FLOAT",
            FieldType.DOUBLE: "DOUBLE",
            FieldType.NULL: "NULL",
            FieldType.TIMESTAMP: "DATETIME",
            FieldType.LONGLONG: "BIGINT",
            FieldType.INT24: "INT",
            FieldType.DATE: "DATE",
            FieldType.TIME: "TIME",
            FieldType.DATETIME: "DATETIME",
            FieldType.YEAR: "INT",
            FieldType.NEWDATE: "DATE",
            FieldType.VARCHAR: "VARCHAR",
            FieldType.BIT: "BOOLEAN",
            FieldType.JSON: "JSON",
            FieldType.NEWDECIMAL: "DECIMAL",
            FieldType.DECIMAL: "DECIMAL",
            FieldType.ENUM: "VARCHAR",
            FieldType.SET: "VARCHAR",
            FieldType.TINY_BLOB: "STRING",
            FieldType.MEDIUM_BLOB: "STRING",
            FieldType.LONG_BLOB: "STRING",
            FieldType.BLOB: "STRING",
            FieldType.VAR_STRING: "VARCHAR",
            FieldType.STRING: "STRING",
            FieldType.GEOMETRY: "STRING",
        }
        return mapping.get(type_code, "STRING")

    def begin(self):
        """
        Doris BEGIN limitation: once BEGIN is issued, only INSERT/UPDATE/DELETE/
        COMMIT/ROLLBACK are allowed — SELECT and DDL will error with:
        "This is in a transaction, only insert, update, delete, commit, rollback
        is acceptable."

        We must NOT send literal BEGIN SQL. We only maintain dbt-core's
        transaction_open flag so the framework tracks state correctly.
        """
        connection = self.get_thread_connection()
        if connection.transaction_open is True:
            raise exceptions.DbtRuntimeError(
                "Tried to begin a new transaction on connection '{}', but "
                "it already had one open!".format(connection.name)
            )
        connection.transaction_open = True
        return connection

    def commit(self):
        """
        Do not send literal COMMIT SQL — bare COMMIT without BEGIN is a no-op
        in Doris, but we avoid it for clarity. Just reset the framework flag.
        """
        connection = self.get_thread_connection()
        connection.transaction_open = False
        return connection

    def add_begin_query(self):
        """Override to prevent literal 'BEGIN' SQL from being sent to Doris."""
        pass

    def add_commit_query(self):
        """Override to prevent literal 'COMMIT' SQL from being sent to Doris."""
        pass

    def add_query(
        self,
        sql,
        auto_begin=True,
        bindings=None,
        abridge_sql_log=False,
        retryable_exceptions=(),
        retry_limit=1,
    ):
        """Run a query, then drain any extra result sets it produced.

        mysql-connector executes semicolon-separated statements in a single
        `execute()` call but only surfaces the first result. The remaining sets
        stay queued on the connection, and the next statement fails with
        `2014 (HY000) Commands out of sync; you can't run this command now`.
        The originating statement itself succeeds, so the error surfaces on
        whatever dbt does next -- typically dropping a temp relation, which then
        leaks.

        Macros are expected to emit one statement per dbt statement, so this is a
        safety net rather than the primary fix. Draining only runs for statements
        that produced no fetchable rows, which leaves ordinary SELECT results
        intact for dbt to consume.
        """
        connection, cursor = super().add_query(
            sql,
            auto_begin=auto_begin,
            bindings=bindings,
            abridge_sql_log=abridge_sql_log,
            retryable_exceptions=retryable_exceptions,
            retry_limit=retry_limit,
        )

        if cursor is not None and not cursor.with_rows:
            drained = 0
            with self.exception_handler(sql):
                while cursor.nextset():
                    drained += 1
            if drained:
                logger.debug(
                    f"Drained {drained} extra result set(s); a macro emitted "
                    f"multiple statements in one dbt statement: {sql}"
                )

        return connection, cursor
