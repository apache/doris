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
        sql "DROP TABLE IF EXISTS python_arrow_ns_values"
        sql """CREATE TABLE python_arrow_ns_values (id INT, ts TIMESTAMP_NS)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO python_arrow_ns_values VALUES
            (0, '1677-09-21 00:12:43.145224192'),
            (1, '1677-09-21 00:12:43.145224193'),
            (2, '1969-12-31 23:59:59.999999999'),
            (3, '1970-01-01 00:00:00.000000000'),
            (4, '2026-10-10 10:00:00.123456789'),
            (5, '2262-04-11 23:47:16.854775807'), (6, NULL), (7, NULL), (8, NULL)"""
        def nsTypes = [value: "TIMESTAMP_NS", array: "ARRAY<TIMESTAMP_NS>",
                       map: "MAP<STRING, TIMESTAMP_NS>", record: "STRUCT<ts:TIMESTAMP_NS>"]
        def nsExpressions = [value: "ts",
            array: """CASE WHEN id = 7 THEN CAST(NULL AS ARRAY<TIMESTAMP_NS>)
                WHEN id = 8 THEN CAST(array() AS ARRAY<TIMESTAMP_NS>) ELSE array(ts, NULL) END""",
            map: """CASE WHEN id = 7 THEN CAST(NULL AS MAP<STRING, TIMESTAMP_NS>)
                WHEN id = 8 THEN CAST(map() AS MAP<STRING, TIMESTAMP_NS>)
                ELSE map('value', ts, 'null', CAST(NULL AS TIMESTAMP_NS)) END""",
            record: """CASE WHEN id = 7 THEN CAST(NULL AS STRUCT<ts:TIMESTAMP_NS>)
                ELSE named_struct('ts', ts) END"""]
        ["scalar", "list", "series"].each { mode ->
            def annotation = mode == "list" ? ": list" : (mode == "series" ? ": pd.Series" : "")
            nsTypes.each { kind, type ->
                def name = "python_arrow_ns_${mode}_${kind}"
                sql "DROP FUNCTION IF EXISTS ${name}(${type})"
                sql """CREATE FUNCTION ${name}(${type}) RETURNS ${type}
                    PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                               "runtime_version"="${runtimeVersion}") AS \$\$
import pandas as pd

def evaluate(value${annotation}):
    return value
\$\$"""
                def expression = nsExpressions[kind]
                quickTest("timestamp_ns_${mode}_${kind}", """SELECT id,
                    ${name}(${expression})
                    FROM python_arrow_ns_values ORDER BY id""",
                    "SELECT id, ${expression} FROM python_arrow_ns_values ORDER BY id")
            }
        }
        sql "DROP FUNCTION IF EXISTS python_arrow_ns_series_hour(TIMESTAMP_NS)"
        sql """CREATE FUNCTION python_arrow_ns_series_hour(TIMESTAMP_NS) RETURNS INT
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
import pandas as pd

def evaluate(value: pd.Series):
    return value.dt.hour
\$\$"""
        quickTest("timestamp_ns_series_datetime_operations", """SELECT id,
            python_arrow_ns_series_hour(ts) FROM python_arrow_ns_values WHERE id > 0 ORDER BY id""",
            "SELECT id, hour(ts) FROM python_arrow_ns_values WHERE id > 0 ORDER BY id")
        sql "DROP FUNCTION IF EXISTS python_arrow_timestamptz_format(TIMESTAMPTZ(6))"
        sql """CREATE FUNCTION python_arrow_timestamptz_format(TIMESTAMPTZ(6)) RETURNS STRING
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f')
\$\$"""
        sql "SET time_zone = '+06:00'"
        quickTest("timestamptz_session_timezone", """SELECT python_arrow_timestamptz_format(
            CAST('2026-10-10 10:00:00.123456+06:00' AS TIMESTAMPTZ(6)))""",
            "SELECT '2026-10-10 10:00:00.123456'")
        ["scalar", "list", "series"].each { mode ->
            def annotation = mode == "list" ? ": list" : (mode == "series" ? ": pd.Series" : "")
            sql "DROP FUNCTION IF EXISTS python_arrow_timestamptz_${mode}(TIMESTAMPTZ(6))"
            sql """CREATE FUNCTION python_arrow_timestamptz_${mode}(TIMESTAMPTZ(6))
                RETURNS TIMESTAMPTZ(6)
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                           "runtime_version"="${runtimeVersion}") AS \$\$
import pandas as pd

def evaluate(value${annotation}):
    return value
\$\$"""
        }
        sql "DROP FUNCTION IF EXISTS python_arrow_timestamptz_array(ARRAY<TIMESTAMPTZ(6)>)"
        sql """CREATE FUNCTION python_arrow_timestamptz_array(ARRAY<TIMESTAMPTZ(6)>)
            RETURNS ARRAY<TIMESTAMPTZ(6)>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return value
\$\$"""
        ["max", "hour"].each { result ->
            def returnType = result == "max" ? "TIMESTAMPTZ(6)" : "INT"
            def finish = result == "max" ? "self.value" : "None if self.value is None else self.value.hour"
            sql "DROP FUNCTION IF EXISTS python_arrow_timestamptz_${result}(TIMESTAMPTZ(6))"
            sql """CREATE AGGREGATE FUNCTION python_arrow_timestamptz_${result}(TIMESTAMPTZ(6))
                RETURNS ${returnType}
                PROPERTIES("type"="PYTHON_UDF", "symbol"="Maximum",
                           "runtime_version"="${runtimeVersion}") AS \$\$
class Maximum:
    def __init__(self):
        self.value = None
    def accumulate(self, value):
        if value is not None and (self.value is None or value > self.value):
            self.value = value
    @property
    def aggregate_state(self):
        return self.value
    def merge(self, value):
        self.accumulate(value)
    def finish(self):
        return ${finish}
    def reset(self):
        self.value = None
\$\$"""
        }
        sql "DROP FUNCTION IF EXISTS python_arrow_timestamptz_rows(TIMESTAMPTZ(6))"
        sql """CREATE TABLES FUNCTION python_arrow_timestamptz_rows(TIMESTAMPTZ(6))
            RETURNS ARRAY<STRUCT<ts:TIMESTAMPTZ(6), local_time:STRING>>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    yield (value, None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f'))
\$\$"""
        def zonedInput = """SELECT id,
            CAST(concat(CAST(ts AS STRING), '+06:00') AS TIMESTAMPTZ(6)) AS zoned_ts
            FROM python_arrow_time_values
            UNION ALL SELECT 6, CAST('2026-01-10 10:00:00.123456+06:00' AS TIMESTAMPTZ(6))
            UNION ALL SELECT 7, CAST('2026-07-10 10:00:00.123456+06:00' AS TIMESTAMPTZ(6))"""
        ["UTC", "+06:00", "+05:45", "-03:30", "America/New_York",
         "UTC+08:00", "GMT+08:00", "UTC+05:45", "GMT-03:30", "UTC+00:00"].eachWithIndex { zone, index ->
            sql "SET time_zone = '${zone}'"
            quickTest("timestamptz_format_${index}", """SELECT id,
                python_arrow_timestamptz_format(zoned_ts) FROM (${zonedInput}) input ORDER BY id""",
                """SELECT id, date_format(zoned_ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM (${zonedInput}) input ORDER BY id""")
            quickTest("timestamptz_round_trip_${index}", """SELECT id,
                python_arrow_timestamptz_scalar(zoned_ts) <=> zoned_ts,
                python_arrow_timestamptz_list(zoned_ts) <=> zoned_ts,
                python_arrow_timestamptz_series(zoned_ts) <=> zoned_ts,
                python_arrow_timestamptz_array(array(zoned_ts, NULL)) <=> array(zoned_ts, NULL)
                FROM (${zonedInput}) input ORDER BY id""",
                "SELECT id, true, true, true, true FROM (${zonedInput}) input ORDER BY id")
            quickTest("timestamptz_aggregate_${index}", """SELECT
                python_arrow_timestamptz_max(zoned_ts), python_arrow_timestamptz_hour(zoned_ts)
                FROM (${zonedInput}) input""", """SELECT max(zoned_ts), hour(max(zoned_ts))
                FROM (${zonedInput}) input""")
            quickTest("timestamptz_groups_${index}", """SELECT id % 3,
                python_arrow_timestamptz_max(zoned_ts), python_arrow_timestamptz_hour(zoned_ts)
                FROM (${zonedInput}) input GROUP BY id % 3 ORDER BY 1""", """SELECT id % 3,
                max(zoned_ts), hour(max(zoned_ts))
                FROM (${zonedInput}) input GROUP BY id % 3 ORDER BY 1""")
            quickTest("timestamptz_window_${index}", """SELECT id,
                python_arrow_timestamptz_hour(zoned_ts) OVER (
                    ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                FROM (${zonedInput}) input ORDER BY id""", """SELECT id,
                hour(max(zoned_ts) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW))
                FROM (${zonedInput}) input ORDER BY id""")
            quickTest("timestamptz_table_${index}", """SELECT id, result.value.ts, result.value.local_time
                FROM (${zonedInput}) input
                LATERAL VIEW python_arrow_timestamptz_rows(zoned_ts) result AS value ORDER BY id""",
                """SELECT id, zoned_ts, date_format(zoned_ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM (${zonedInput}) input ORDER BY id""")
        }
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
        ["scalar", "list", "series"].each { mode ->
            def name = "python_arrow_time_${mode}"
            def annotation = mode == "list" ? ": list" : (mode == "series" ? ": pd.Series" : "")
            sql "DROP FUNCTION IF EXISTS ${name}(TIME(6))"
            sql """CREATE FUNCTION ${name}(TIME(6)) RETURNS TIME(6)
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                           "runtime_version"="${runtimeVersion}") AS \$\$
import pandas as pd

def evaluate(value${annotation}):
    return value
\$\$"""
        }
        [0, 3].each { scale ->
            def name = "python_arrow_time_precision_${scale}"
            sql "DROP FUNCTION IF EXISTS ${name}(TIME(6))"
            sql """CREATE FUNCTION ${name}(TIME(6)) RETURNS TIME(${scale})
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                           "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return value
\$\$"""
        }
        sql "DROP FUNCTION IF EXISTS python_arrow_time_array(ARRAY<TIME(6)>)"
        sql """CREATE FUNCTION python_arrow_time_array(ARRAY<TIME(6)>) RETURNS ARRAY<TIME(6)>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return value
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_time_inspect(TIME(6))"
        sql """CREATE FUNCTION python_arrow_time_inspect(TIME(6)) RETURNS STRING
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return f"{type(value).__name__}:{value}"
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_time_max(TIME(6))"
        sql """CREATE AGGREGATE FUNCTION python_arrow_time_max(TIME(6)) RETURNS TIME(6)
            PROPERTIES("type"="PYTHON_UDF", "symbol"="Maximum",
                       "runtime_version"="${runtimeVersion}") AS \$\$
class Maximum:
    def __init__(self):
        self.value = None
    def accumulate(self, value):
        if value is not None and (self.value is None or value > self.value):
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
        sql "DROP FUNCTION IF EXISTS python_arrow_time_rows(TIME(6))"
        sql """CREATE TABLES FUNCTION python_arrow_time_rows(TIME(6)) RETURNS ARRAY<TIME(6)>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    yield (value,)
\$\$"""
        sql "DROP FUNCTION IF EXISTS python_arrow_time_value(INT)"
        sql """CREATE FUNCTION python_arrow_time_value(INT) RETURNS TIME(6)
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(index):
    return [float('nan'), float('inf'), float('-inf'),
            839.0 * 3600 * 1000000, -839.0 * 3600 * 1000000, 0.5, -0.5,
            float(839 * 3600 * 1000000 - 1), -float(839 * 3600 * 1000000 - 1)][index]
\$\$"""
        def timeRows = ["00:00:00.000000", "00:00:01.234500", "-00:00:01.234500",
                        "100:02:03.123456", "-100:02:03.123456", "838:59:59.000000",
                        "-838:59:59.000000", "00:00:00.000001", "-00:00:00.000001", null,
                        "838:59:59.999999", "-838:59:59.999999"]
        def timeInput = timeRows.withIndex().collect { value, index ->
            def literal = value == null ? "NULL" : "'${value}'"
            "SELECT ${index} AS id, ${literal} AS text_value, CAST(${literal} AS TIME(6)) AS time_value"
        }.join(" UNION ALL ")
        quickTest("time_round_trip_matches_native", """SELECT id,
            python_arrow_time_scalar(time_value) <=> time_value,
            python_arrow_time_list(time_value) <=> time_value,
            python_arrow_time_series(time_value) <=> time_value,
            python_arrow_time_array(array(time_value, NULL)) <=> array(time_value, NULL)
            FROM (${timeInput}) input ORDER BY id""", """SELECT id,
            true, true, true, true FROM (${timeInput}) input ORDER BY id""")
        [0, 3].each { scale ->
            def expectedValues = scale == 0
                ? ["00:00:00", "00:00:01", "-00:00:01", "100:02:03", "-100:02:03",
                   "838:59:59", "-838:59:59", "00:00:00", "00:00:00", null]
                : ["00:00:00.000", "00:00:01.235", "-00:00:01.235", "100:02:03.123", "-100:02:03.123",
                   "838:59:59.000", "-838:59:59.000", "00:00:00.000", "00:00:00.000", null]
            def expectedRows = expectedValues.withIndex().collect { value, index ->
                def literal = value == null ? "NULL" : "'${value}'"
                "SELECT ${index} AS id, CAST(${literal} AS STRING) AS result"
            }.join(" UNION ALL ")
            quickTest("time_precision_${scale}", """SELECT id,
                CAST(python_arrow_time_precision_${scale}(time_value) AS STRING)
                FROM (${timeInput}) input WHERE id < 10 ORDER BY id""",
                "SELECT * FROM (${expectedRows}) expected ORDER BY id")
            [7, 8].each { index ->
                test {
                    sql "SELECT python_arrow_time_precision_${scale}(python_arrow_time_value(${index}))"
                    exception "Doris TIME range after rounding"
                }
            }
        }
        quickTest("time_input_is_float_microseconds", """SELECT
            python_arrow_time_inspect(CAST('00:00:01' AS TIME(6)))""", "SELECT 'float:1000000.0'")
        quickTest("time_boundary_microseconds", """SELECT
            python_arrow_time_inspect(python_arrow_time_value(7)),
            python_arrow_time_inspect(python_arrow_time_value(8))""",
            "SELECT 'float:3020399999999.0', 'float:-3020399999999.0'")
        quickTest("time_maximum_matches_native", """SELECT python_arrow_time_max(time_value) <=> max(time_value)
            FROM (${timeInput}) input""", "SELECT true")
        quickTest("time_groups_match_native", """SELECT id % 3,
            python_arrow_time_max(time_value) <=> max(time_value)
            FROM (${timeInput}) input GROUP BY id % 3 ORDER BY 1""", """SELECT id % 3,
            true FROM (${timeInput}) input GROUP BY id % 3 ORDER BY 1""")
        quickTest("time_window_matches_native", """SELECT id,
            python_arrow_time_max(time_value) OVER (
                ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            <=> max(time_value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            FROM (${timeInput}) input ORDER BY id""",
            "SELECT id, true FROM (${timeInput}) input ORDER BY id")
        quickTest("time_table_matches_native", """SELECT id, result.value <=> time_value
            FROM (${timeInput}) input
            LATERAL VIEW python_arrow_time_rows(time_value) result AS value ORDER BY id""",
            "SELECT id, true FROM (${timeInput}) input ORDER BY id")
        (0..6).each { index ->
            test {
                sql "SELECT python_arrow_time_value(${index})"
                exception (index < 5 ? "Doris TIME range" : "whole microseconds")
            }
        }
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
