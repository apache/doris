#!/usr/bin/env python3
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

"""Exercise native scans against the dedicated, test-command-enabled fixture."""

import argparse
import concurrent.futures
import os
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pymongo
import pymysql

CATALOG = "mongodb_native_lifecycle"
NAMESPACE = CATALOG + ".doris_mongodb_native."
APPLICATION = "doris-native-lifecycle"


def connect():
    connection = pymysql.connect(
        host=os.environ["DORIS_FE_HOST"],
        port=int(os.environ["DORIS_FE_PORT"]),
        user=os.environ["DORIS_FE_USER"],
        password=os.environ.get("DORIS_FE_PASSWORD", ""),
        autocommit=True,
        read_timeout=30,
    )
    with connection.cursor() as cursor:
        # The 2,081-row fixture must cross both source and Doris batch boundaries.
        cursor.execute("SET batch_size=1024")
    return connection


def sql(statement, connection=None):
    if connection is None:
        with connect() as owned_connection:
            return sql(statement, owned_connection)
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_catalog(uri):
    parsed = urlsplit(uri)
    options = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() != "appname"]
    options.append(("appName", APPLICATION))
    properties = {
        "type": "mongodb",
        "mongodb.uri": urlunsplit(parsed._replace(query=urlencode(options))),
        "mongodb.databases": "doris_mongodb_native",
        "mongodb.socket_timeout_ms": "1000",
        "mongodb.pool_max_clients": "2",
    }
    sql("DROP CATALOG IF EXISTS " + CATALOG)
    sql(
        "CREATE CATALOG "
        + CATALOG
        + " PROPERTIES ("
        + ",".join(quote(key) + "=" + quote(value) for key, value in properties.items())
        + ")"
    )


def count():
    result = sql("SELECT count(*),sum(seq) FROM " + NAMESPACE + "large_records")
    assert result == ((2081, 2164240),), result


def expect_error(statement, fragments=("mongodb",)):
    try:
        sql(statement)
    except pymysql.MySQLError as error:
        assert any(part in str(error).lower() for part in fragments), str(error)
        return
    raise AssertionError("The injected failure did not fail the query")


def cursors_closed(source):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        opened = source.admin.command("serverStatus")["metrics"]["cursor"]["open"][
            "total"
        ]
        if opened == 0:
            return
        time.sleep(0.1)
    raise AssertionError("MongoDB cursors remain open: " + str(opened))


def failpoint(source, data):
    return source.admin.command(
        {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"appName": APPLICATION, **data},
        }
    )["count"]


def off(source):
    source.admin.command({"configureFailPoint": "failCommand", "mode": "off"})


def run(source, uri):
    assert source.doris_mongodb_native.large_records.count_documents({}) == 2081
    create_catalog(uri)
    count()
    cursors_closed(source)
    for _ in range(10):
        sql("SELECT seq FROM " + NAMESPACE + "large_records LIMIT 1")
    cursors_closed(source)
    expect_error("SELECT seq FROM " + NAMESPACE + "historical_records")
    cursors_closed(source)
    count()
    print("PASS: EOF, LIMIT and decoding-error cleanup", flush=True)

    failpoint(source, {"failCommands": ["getMore"], "errorCode": 2})
    try:
        expect_error("SELECT sum(seq) FROM " + NAMESPACE + "large_records")
    finally:
        off(source)
    cursors_closed(source)
    count()
    print("PASS: getMore error and recovery", flush=True)

    # A longer socket timeout makes the deadline case fail only if query_timeout is honored.
    for hint, socket_timeout, fragments in [
        ("", "1000", ("mongodb",)),
        ("/*+ SET_VAR(query_timeout=1) */", "5000", ("timeout", "mongodb")),
    ]:
        sql(
            "ALTER CATALOG "
            + CATALOG
            + " SET PROPERTIES ('mongodb.socket_timeout_ms'="
            + quote(socket_timeout)
            + ")"
        )
        failpoint(
            source,
            {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 3000},
        )
        try:
            expect_error(
                "SELECT " + hint + " sum(seq) FROM " + NAMESPACE + "large_records",
                fragments,
            )
        finally:
            off(source)
        cursors_closed(source)
        count()
    print("PASS: socket/query timeouts and recovery", flush=True)

    # The socket timeout still exceeds the failpoint delay, leaving time to observe and cancel it.
    entered = failpoint(
        source,
        {"failCommands": ["getMore"], "blockConnection": True, "blockTimeMS": 3000},
    )
    with connect() as connection:
        connection_id = sql("SELECT connection_id()", connection)[0][0]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                sql, "SELECT sum(seq) FROM " + NAMESPACE + "large_records", connection
            )
            try:
                # Wait for an actual blocked getMore; sleeping can cancel before the scan opens.
                source.admin.command(
                    {
                        "waitForFailPoint": "failCommand",
                        "timesEntered": entered + 1,
                        "maxTimeMS": 10000,
                    }
                )
                sql("KILL QUERY " + str(connection_id))
                try:
                    future.result(timeout=10)
                except pymysql.MySQLError as error:
                    assert "cancel" in str(error).lower(), str(error)
                else:
                    raise AssertionError("The cancelled query succeeded")
            finally:
                off(source)
    cursors_closed(source)
    count()
    print("PASS: cancellation during getMore and recovery", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: count(), range(20)))
    cursors_closed(source)
    print("PASS: concurrent readers and cleanup", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-failpoints",
        action="store_true",
        required=True,
        help="use only with an otherwise idle, disposable MongoDB fixture",
    )
    parser.parse_args()
    uri = os.environ["MONGODB_URI"]
    with pymongo.MongoClient(
        uri, appname="lifecycle-controller", retryReads=False
    ) as source:
        try:
            run(source, uri)
        finally:
            off(source)


if __name__ == "__main__":
    main()
