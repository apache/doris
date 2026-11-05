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

suite("test_python_arrow_time_types") {
    def runtimeVersion = getPythonUdfRuntimeVersion()
    def originalTimezone = sql("SELECT @@time_zone")[0][0]
    assertTrue(sql("SHOW VARIABLES LIKE 'enable_python_udf_datetime_without_timezone'").isEmpty())
    sql "DROP TABLE IF EXISTS python_arrow_time_values"
    sql """CREATE TABLE python_arrow_time_values (id INT, ts DATETIME(6))
           DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO python_arrow_time_values VALUES
           (1, '1969-12-31 23:59:59.999999'),
           (2, '1969-12-31 23:59:59.123456'),
           (3, '2023-04-20 00:00:00.123456'),
           (4, '2026-10-10 10:00:00.123456'), (5, NULL)"""
    try {
        [0, 3, 6].each { scale ->
            def name = "python_arrow_datetime_constant_${scale}"
            def micros = scale == 0 ? 0 : (scale == 3 ? 123000 : 123456)
            sql "DROP FUNCTION IF EXISTS ${name}()"
            sql """CREATE FUNCTION ${name}() RETURNS DATETIME(${scale})
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                           "runtime_version"="${runtimeVersion}") AS \$\$
from datetime import datetime

def evaluate():
    return datetime(2026, 10, 10, 10, 0, 0, ${micros})
\$\$"""
        }
        ["scalar", "list", "series"].each { mode ->
            def name = "python_arrow_datetime_${mode}"
            def annotation = mode == "list" ? ": list" : (mode == "series" ? ": pd.Series" : "")
            def body = mode == "scalar"
                    ? "return None if value is None else value.replace(tzinfo=None)"
                    : (mode == "list"
                        ? "return [None if item is None else item.replace(tzinfo=None) for item in value]"
                        : "return value.dt.tz_localize(None)")
            sql "DROP FUNCTION IF EXISTS ${name}(DATETIME(6))"
            sql """CREATE FUNCTION ${name}(DATETIME(6)) RETURNS DATETIME(6)
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                           "runtime_version"="${runtimeVersion}") AS \$\$
import pandas as pd

def evaluate(value${annotation}):
    ${body}
\$\$"""
        }
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_array(ARRAY<DATETIME(6)>)"
        sql """CREATE FUNCTION python_arrow_datetime_array(ARRAY<DATETIME(6)>)
            RETURNS ARRAY<DATETIME(6)>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(values):
    return [None if value is None else value.replace(tzinfo=None) for value in values]
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_inspect(DATETIME(6))"
        sql """CREATE FUNCTION python_arrow_datetime_inspect(DATETIME(6)) RETURNS STRING
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return None if value is None else str(value.utcoffset())
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_format(DATETIME(6))"
        sql """CREATE FUNCTION python_arrow_datetime_format(DATETIME(6)) RETURNS STRING
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f')
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_max(DATETIME(6))"
        sql """CREATE AGGREGATE FUNCTION python_arrow_datetime_max(DATETIME(6))
            RETURNS DATETIME(6)
            PROPERTIES("type"="PYTHON_UDF", "symbol"="Maximum",
                       "runtime_version"="${runtimeVersion}") AS \$\$
class Maximum:
    def __init__(self):
        self.value = None
    def accumulate(self, value):
        if value is not None:
            value = value.replace(tzinfo=None)
            if self.value is None or value > self.value:
                self.value = value
    @property
    def aggregate_state(self):
        return self.value
    def merge(self, value):
        self.accumulate(value)
    def finish(self):
        return self.value
    def reset(self):
        self.value = None
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_rows(DATETIME(6))"
        sql """CREATE TABLES FUNCTION python_arrow_datetime_rows(DATETIME(6))
            RETURNS ARRAY<STRUCT<ts:DATETIME(6), timezone:STRING>>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    yield (None if value is None else value.replace(tzinfo=None),
           None if value is None else str(value.utcoffset()))
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_datetime_strings(DATETIME(6))"
        sql """CREATE TABLES FUNCTION python_arrow_datetime_strings(DATETIME(6))
            RETURNS ARRAY<STRING>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    yield (None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f'),)
\$\$"""
        ["UTC", "+06:00", "+05:45", "-03:30", "Asia/Shanghai"].eachWithIndex { zone, index ->
            sql "SET time_zone = '${zone}'"
            quickTest("constants_match_native_${index}", """SELECT
                python_arrow_datetime_constant_0(), python_arrow_datetime_constant_3(),
                python_arrow_datetime_constant_6()""", """SELECT
                CAST('2026-10-10 10:00:00' AS DATETIME(0)),
                CAST('2026-10-10 10:00:00.123' AS DATETIME(3)),
                CAST('2026-10-10 10:00:00.123456' AS DATETIME(6))""")
            quickTest("round_trip_matches_native_${index}", """SELECT id,
                python_arrow_datetime_scalar(ts), python_arrow_datetime_list(ts),
                python_arrow_datetime_series(ts) FROM python_arrow_time_values ORDER BY id""",
                "SELECT id, ts, ts, ts FROM python_arrow_time_values ORDER BY id")
            quickTest("format_matches_native_${index}", """SELECT id,
                python_arrow_datetime_format(ts) FROM python_arrow_time_values ORDER BY id""",
                """SELECT id, date_format(ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM python_arrow_time_values ORDER BY id""")
            quickTest("maximum_matches_native_${index}",
                "SELECT python_arrow_datetime_max(ts) FROM python_arrow_time_values",
                "SELECT max(ts) FROM python_arrow_time_values")
            quickTest("groups_match_native_${index}", """SELECT id, python_arrow_datetime_max(ts)
                FROM python_arrow_time_values GROUP BY id ORDER BY id""",
                "SELECT id, max(ts) FROM python_arrow_time_values GROUP BY id ORDER BY id")
            quickTest("window_matches_native_${index}", """SELECT id,
                python_arrow_datetime_max(ts) OVER (
                    ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                FROM python_arrow_time_values ORDER BY id""", """SELECT id,
                max(ts) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                FROM python_arrow_time_values ORDER BY id""")
            quickTest("table_format_matches_native_${index}", """SELECT id, result.value
                FROM python_arrow_time_values
                LATERAL VIEW python_arrow_datetime_strings(ts) result AS value ORDER BY id""",
                """SELECT id, date_format(ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM python_arrow_time_values ORDER BY id""")
            quickTest("constants_${index}", """SELECT python_arrow_datetime_constant_0(),
                python_arrow_datetime_constant_3(), python_arrow_datetime_constant_6()""")
            quickTest("values_${index}", """SELECT id, ts, python_arrow_datetime_scalar(ts),
                python_arrow_datetime_list(ts), python_arrow_datetime_series(ts),
                python_arrow_datetime_inspect(ts), python_arrow_datetime_array(array(ts, NULL))
                FROM python_arrow_time_values ORDER BY id""")
            quickTest("groups_${index}", """SELECT id, python_arrow_datetime_max(ts), max(ts)
                FROM python_arrow_time_values GROUP BY id ORDER BY id""")
            quickTest("rows_${index}", """SELECT id, ts, result.value
                FROM python_arrow_time_values
                LATERAL VIEW python_arrow_datetime_rows(ts) result AS value ORDER BY id""")
        }
    } finally {
        sql "SET time_zone = '${originalTimezone}'"
    }
}
