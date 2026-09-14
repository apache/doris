
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

suite("test_time_field_from_unixtime_boundary") {
    sql "DROP TABLE IF EXISTS test_time_field_from_unixtime_boundary"
    sql """CREATE TABLE test_time_field_from_unixtime_boundary (
        id INT, ts BIGINT, fractional_ts DECIMAL(18, 6)
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1")"""

    def zones = ["UTC": 0, "Asia/Shanghai": 28800, "-08:00": -28800, "+14:00": 50400]
    zones.each { zone, offset ->
        sql "SET time_zone='${zone}'"
        sql "TRUNCATE TABLE test_time_field_from_unixtime_boundary"
        def lastSecond = 253402300799L - offset
        sql """INSERT INTO test_time_field_from_unixtime_boundary VALUES
            (1, 253402243199, 253402243199.123456),
            (2, 253402243200, 253402243200.123456),
            (3, ${lastSecond}, ${lastSecond}.999999),
            (4, NULL, NULL)"""
        def query = """SELECT id,
            HOUR(CAST(FROM_UNIXTIME(ts) AS DATETIMEV2)),
            MINUTE(CAST(FROM_UNIXTIME(ts) AS DATETIMEV2)),
            SECOND(CAST(FROM_UNIXTIME(ts) AS DATETIMEV2)),
            MICROSECOND(CAST(FROM_UNIXTIME(fractional_ts) AS DATETIMEV2(6)))
            FROM test_time_field_from_unixtime_boundary ORDER BY id"""
        sql "SET disable_nereids_expression_rules=''"
        def original = query.replace("SELECT", "SELECT /*+ SET_VAR("
                + "disable_nereids_expression_rules='SIMPLIFY_DATETIME_FUNCTION') */")
        explain {
            sql(original)
            notContains "hour_from_unixtime"
        }
        explain {
            sql(query)
            contains "hour_from_unixtime"
        }
        // Compare the rewrite with the original expression using the framework's two-SQL check.
        check_sqls_result_equal(original, query)
        testFoldConst("SELECT HOUR_FROM_UNIXTIME(${lastSecond}), "
                + "MINUTE_FROM_UNIXTIME(${lastSecond}), SECOND_FROM_UNIXTIME(${lastSecond}), "
                + "MICROSECOND_FROM_UNIXTIME(CAST(${lastSecond}.999999 AS DECIMAL(18, 6)))")
        // A column input prevents constant folding from hiding BE boundary checks.
        sql "INSERT INTO test_time_field_from_unixtime_boundary VALUES (5, ${lastSecond + 1}, NULL)"
        test {
            sql """SELECT HOUR(CAST(FROM_UNIXTIME(ts) AS DATETIMEV2))
                FROM test_time_field_from_unixtime_boundary WHERE id = 5"""
            exception "The input value of hour_from_unixtime is out of range in the session time zone"
        }
    }
}
