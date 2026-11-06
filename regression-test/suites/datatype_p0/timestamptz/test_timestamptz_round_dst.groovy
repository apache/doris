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

suite("test_timestamptz_round_dst") {
    def cases = [
        [1, '2024-03-10 04:30:00.123456-04:00', '2024-03-09 00:00:00-05:00'],
        [2, '2024-07-15 12:00:00.123456-04:00', '2024-01-01 00:00:00-05:00'],
        [3, '2024-01-15 12:00:00.123456-05:00', '2024-07-01 00:00:00-04:00'],
        [4, '2024-11-03 01:35:00.123456-04:00', '2024-11-02 00:00:00-04:00'],
        [5, '2024-11-03 01:35:00.123456-05:00', '2024-11-02 00:00:00-04:00'],
        [6, '2024-03-10 01:45:00.123456-05:00', '2024-03-09 00:30:00.123456-05:00']
    ]
    def units = [year: 1, quarter: 1, month: 1, day: 2, hour: 2, minute: 10, second: 7]
    def originalZone = sql("SELECT @@time_zone")[0][0]
    def originalBeFold = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    def originalSkipFold = sql("SELECT @@debug_skip_fold_constant")[0][0]
    try {
        sql "SET time_zone = 'America/New_York'"
        sql "DROP TABLE IF EXISTS test_timestamptz_round_dst"
        sql """
            CREATE TABLE test_timestamptz_round_dst (
                id INT, ts TIMESTAMPTZ(6), origin TIMESTAMPTZ(6)
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        sql "INSERT INTO test_timestamptz_round_dst VALUES " + cases.collect { row ->
            "(${row[0]}, '${row[1]}', '${row[2]}')"
        }.join(',')
        sql "SET enable_fold_constant_by_be = false"
        for (def zoneCase : [ny: 'America/New_York', utc: '+00:00', fixed: '+08:00']) {
            String zone = zoneCase.value
            sql "SET time_zone = '${zone}'"
            for (def unit : units) {
                def projections = { date, origin ->
                    ['floor', 'ceil'].collect { op ->
                        "CAST(${unit.key}_${op}(${date}, ${unit.value}, ${origin}) AS VARCHAR(64))"
                    }.join(', ')
                }
                def literalSql = cases.collect { row ->
                    def date = "CAST('${row[1]}' AS TIMESTAMPTZ(6))"
                    def origin = "CAST('${row[2]}' AS TIMESTAMPTZ(6))"
                    "SELECT ${row[0]} AS id, ${projections(date, origin)}"
                }.join(' UNION ALL ')
                sql "SET debug_skip_fold_constant = false"
                "order_qt_${zoneCase.key}_${unit.key}_fe"(literalSql)
                sql "SET debug_skip_fold_constant = true"
                "order_qt_${zoneCase.key}_${unit.key}_be"(literalSql)
                "order_qt_${zoneCase.key}_${unit.key}_columns"(
                        "SELECT id, ${projections('ts', 'origin')} FROM test_timestamptz_round_dst ORDER BY id")
            }
        }
        sql "SET time_zone = 'America/New_York'"
        for (boolean skipFold : [false, true]) {
            sql "SET debug_skip_fold_constant = ${skipFold}"
            for (def offsetCase : [early: '-04:00', late: '-05:00']) {
                String offset = offsetCase.value
                "qt_default_origin_${skipFold}_${offsetCase.key}"("""
                    SELECT hour_floor(CAST('2024-11-03 01:35:00${offset}' AS TIMESTAMPTZ(0))),
                           minute_floor(CAST('2024-11-03 01:35:00${offset}' AS TIMESTAMPTZ(0)), 10),
                           minute_ceil(CAST('2024-11-03 01:35:00${offset}' AS TIMESTAMPTZ(0)), 10),
                           second_floor(CAST('2024-11-03 01:35:00${offset}' AS TIMESTAMPTZ(0)), 7),
                           second_ceil(CAST('2024-11-03 01:35:00${offset}' AS TIMESTAMPTZ(0)), 7)
                """)
            }
        }
    } finally {
        sql "SET time_zone = '${originalZone}'"
        sql "SET enable_fold_constant_by_be = ${originalBeFold}"
        sql "SET debug_skip_fold_constant = ${originalSkipFold}"
    }
}
