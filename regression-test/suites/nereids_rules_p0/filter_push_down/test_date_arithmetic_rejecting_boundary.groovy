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

suite("test_date_arithmetic_rejecting_boundary") {
    sql "DROP TABLE IF EXISTS test_date_boundary_input"
    sql "DROP TABLE IF EXISTS test_date_boundary_right"
    sql """CREATE TABLE test_date_boundary_input (id INT, dt DATETIME(6), arr ARRAY<INT>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """CREATE TABLE test_date_boundary_right (dt DATETIME(6))
        DUPLICATE KEY(dt) DISTRIBUTED BY HASH(dt) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO test_date_boundary_input VALUES
        (1,'9999-12-31 23:59:59.000001',[]), (2,'2020-01-01 00:00:00.000001',[1])"""
    sql "INSERT INTO test_date_boundary_right VALUES ('9999-12-31 23:59:59.000001')"

    // The cast narrows precision so inverse date comparison rewriting cannot hide early evaluation.
    // No optimizer rule is disabled: EXCEPT must reject the overflowing row before arithmetic.
    order_qt_except """SELECT d FROM (
        SELECT CAST(WEEKS_ADD(dt,5) AS DATETIME(0)) AS d FROM (
            SELECT dt FROM test_date_boundary_input
            EXCEPT SELECT dt FROM test_date_boundary_right
        ) difference
    ) s WHERE d='2020-02-05'"""

    sql "TRUNCATE TABLE test_date_boundary_right"
    sql "INSERT INTO test_date_boundary_right VALUES ('2020-01-01 00:00:00.000001')"
    order_qt_intersect """SELECT d FROM (
        SELECT CAST(WEEKS_ADD(dt,5) AS DATETIME(0)) AS d FROM (
            SELECT dt FROM test_date_boundary_input
            INTERSECT SELECT dt FROM test_date_boundary_right
        ) common_rows
    ) s WHERE d='2020-02-05'"""

    // Non-outer explode drops the overflowing row because its array is empty.
    order_qt_generate """SELECT d FROM (
        SELECT CAST(WEEKS_ADD(dt,5) AS DATETIME(0)) AS d
        FROM test_date_boundary_input LATERAL VIEW explode(arr) lv AS item
    ) s WHERE d='2020-02-05'"""

    // Identity date arithmetic and ordinary aliases remain pushable at these boundaries.
    order_qt_zero_offset """SELECT d FROM (
        SELECT CAST(WEEKS_ADD(dt,0) AS DATETIME(0)) AS d
        FROM test_date_boundary_input LATERAL VIEW explode(arr) lv AS item
    ) s WHERE d='2020-01-01'"""
    test {
        sql "SELECT WEEKS_ADD(dt,5) FROM test_date_boundary_input"
        exception "out of range"
    }
}
