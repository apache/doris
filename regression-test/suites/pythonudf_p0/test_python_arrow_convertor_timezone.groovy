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

suite("test_python_arrow_convertor_timezone") {
    def runtimeVersion = getPythonUdfRuntimeVersion()
    def originalTimezone = sql("select @@time_zone")[0][0]
    def scalar = "python_arrow_convertor_scalar"
    def tableFunction = "python_arrow_convertor_rows"
    def aggregate = "python_arrow_convertor_max"
    try {
        sql "DROP TABLE IF EXISTS python_arrow_convertor_values"
        sql """CREATE TABLE python_arrow_convertor_values (id INT, ts DATETIME(6))
               DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO python_arrow_convertor_values VALUES
               (1, '1969-12-31 23:59:59.999999'),
               (2, '2023-04-20 00:00:00.123456'), (3, NULL)"""
        [scalar, tableFunction, aggregate].each { name ->
            sql "DROP FUNCTION IF EXISTS ${name}(DATETIME(6))"
        }
        sql """CREATE FUNCTION ${scalar}(DATETIME(6)) RETURNS STRING
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    return None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f')
\$\$"""
        sql """CREATE TABLES FUNCTION ${tableFunction}(DATETIME(6))
            RETURNS ARRAY<STRUCT<value:STRING>>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                       "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(value):
    yield (None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f'),)
\$\$"""
        sql """CREATE AGGREGATE FUNCTION ${aggregate}(DATETIME(6)) RETURNS DATETIME(6)
            PROPERTIES("type"="PYTHON_UDF", "symbol"="Maximum",
                       "runtime_version"="${runtimeVersion}") AS \$\$
class Maximum:
    def __init__(self):
        self.value = None
    @property
    def aggregate_state(self):
        return self.value
    def accumulate(self, value):
        if value is not None and (self.value is None or value > self.value):
            self.value = value
    def merge(self, value):
        self.accumulate(value)
    def finish(self):
        return self.value
\$\$"""
        // The Python protocol uses its declared default offset regardless of the session zone.
        // Check values observed inside Python as well as returned values to detect offset cancellation.
        ["UTC", "+05:45", "-03:30", "Asia/Shanghai"].each { zone ->
            sql "SET time_zone = '${zone}'"
            assertEquals([[true], [true], [true]], sql("""
                SELECT ${scalar}(ts) <=> date_format(ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM python_arrow_convertor_values ORDER BY id"""))
            assertEquals([[true], [true], [true]], sql("""
                SELECT result.value <=> date_format(ts, '%Y-%m-%d %H:%i:%s.%f')
                FROM python_arrow_convertor_values
                LATERAL VIEW ${tableFunction}(ts) result AS value ORDER BY id"""))
            assertEquals([[true]], sql("""
                SELECT ${aggregate}(ts) <=> max(ts) FROM python_arrow_convertor_values"""))
            assertEquals([[true], [true]], sql("""
                SELECT ${aggregate}(ts) <=> max(ts) FROM python_arrow_convertor_values
                GROUP BY id % 2 ORDER BY id % 2"""))
        }
    } finally {
        sql "SET time_zone = '${originalTimezone}'"
        [scalar, tableFunction, aggregate].each { name ->
            try_sql("DROP FUNCTION IF EXISTS ${name}(DATETIME(6))")
        }
    }
}
