// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_pythonudf_timestamp_ns") {
    def pythonVersions = sql "SHOW PYTHON VERSIONS"
    assertTrue(!pythonVersions.isEmpty(), "No Python runtime is available on all backends")
    def runtimeVersion = pythonVersions[0][0]

    sql "DROP TABLE IF EXISTS test_pythonudf_timestamp_ns"
    sql """
        CREATE TABLE test_pythonudf_timestamp_ns (
            id INT,
            ts TIMESTAMP_NS,
            items ARRAY<TIMESTAMP_NS>,
            by_name MAP<STRING, TIMESTAMP_NS>,
            record STRUCT<ts:TIMESTAMP_NS>
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_pythonudf_timestamp_ns VALUES
            (1, '1677-09-21 00:12:43.145224192',
                ['1677-09-21 00:12:43.145224192'],
                {'value':'1677-09-21 00:12:43.145224192'},
                {'1677-09-21 00:12:43.145224192'}),
            (2, '1969-12-31 23:59:59.999999999',
                ['1969-12-31 23:59:59.999999999'],
                {'value':'1969-12-31 23:59:59.999999999'},
                {'1969-12-31 23:59:59.999999999'}),
            (3, '1970-01-01 00:00:00.000000000',
                ['1970-01-01 00:00:00.000000000'],
                {'value':'1970-01-01 00:00:00.000000000'},
                {'1970-01-01 00:00:00.000000000'}),
            (4, '2024-02-29 12:34:56.123456789',
                ['2024-02-29 12:34:56.123456789'],
                {'value':'2024-02-29 12:34:56.123456789'},
                {'2024-02-29 12:34:56.123456789'}),
            (5, '2024-02-29 12:34:56.000000000',
                ['2024-02-29 12:34:56.000000000'],
                {'value':'2024-02-29 12:34:56.000000000'},
                {'2024-02-29 12:34:56.000000000'}),
            (6, '2262-04-11 23:47:16.854775807',
                ['2262-04-11 23:47:16.854775807'],
                {'value':'2262-04-11 23:47:16.854775807'},
                {'2262-04-11 23:47:16.854775807'}),
            (7, NULL, [NULL], {'value':NULL}, {NULL})
    """

    sql "DROP FUNCTION IF EXISTS python_udf_timestamp_ns(TIMESTAMP_NS)"
    sql """
        CREATE FUNCTION python_udf_timestamp_ns(TIMESTAMP_NS)
        RETURNS TIMESTAMP_NS
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtimeVersion}",
            "always_nullable" = "true"
        )
        AS \$\$
def evaluate(value):
    return value
\$\$;
    """
    sql "DROP FUNCTION IF EXISTS python_udf_timestamp_ns_array(ARRAY<TIMESTAMP_NS>)"
    sql """
        CREATE FUNCTION python_udf_timestamp_ns_array(ARRAY<TIMESTAMP_NS>)
        RETURNS ARRAY<TIMESTAMP_NS>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtimeVersion}",
            "always_nullable" = "true"
        )
        AS \$\$
def evaluate(value):
    return value
\$\$;
    """
    sql "DROP FUNCTION IF EXISTS python_udf_timestamp_ns_map(MAP<STRING, TIMESTAMP_NS>)"
    sql """
        CREATE FUNCTION python_udf_timestamp_ns_map(MAP<STRING, TIMESTAMP_NS>)
        RETURNS MAP<STRING, TIMESTAMP_NS>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtimeVersion}",
            "always_nullable" = "true"
        )
        AS \$\$
def evaluate(value):
    return value
\$\$;
    """
    sql "DROP FUNCTION IF EXISTS python_udf_timestamp_ns_struct(STRUCT<ts:TIMESTAMP_NS>)"
    sql """
        CREATE FUNCTION python_udf_timestamp_ns_struct(STRUCT<ts:TIMESTAMP_NS>)
        RETURNS STRUCT<ts:TIMESTAMP_NS>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtimeVersion}",
            "always_nullable" = "true"
        )
        AS \$\$
def evaluate(value):
    if value is None:
        return None
    return (value['ts'],)
\$\$;
    """

    order_qt_timestamp_ns_scalar """
        SELECT id, python_udf_timestamp_ns(ts)
        FROM test_pythonudf_timestamp_ns ORDER BY id
    """
    order_qt_timestamp_ns_array """
        SELECT id, python_udf_timestamp_ns_array(items)
        FROM test_pythonudf_timestamp_ns ORDER BY id
    """
    order_qt_timestamp_ns_map """
        SELECT id, python_udf_timestamp_ns_map(by_name)
        FROM test_pythonudf_timestamp_ns ORDER BY id
    """
    order_qt_timestamp_ns_struct """
        SELECT id, python_udf_timestamp_ns_struct(record)
        FROM test_pythonudf_timestamp_ns ORDER BY id
    """
}
