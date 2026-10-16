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

suite("test_uniq_theta") {
    sql "DROP TABLE IF EXISTS test_uniq_theta"
    sql """
        CREATE TABLE test_uniq_theta (
            id int,
            grp int,
            v_int int,
            v_bigint bigint,
            v_str varchar(64),
            v_date date,
            v_dec decimal(10, 2),
            v_null int
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // empty table -> 0
    qt_empty "SELECT uniq_theta(v_int) FROM test_uniq_theta"

    sql """
        INSERT INTO test_uniq_theta VALUES
        (1, 1, 10, 1000, 'aaa', '2024-01-01', 1.10, 7),
        (2, 1, 20, 2000, 'bbb', '2024-01-02', 2.20, null),
        (3, 1, 30, 3000, 'ccc', '2024-01-03', 3.30, 7),
        (4, 2, 10, 1000, 'aaa', '2024-01-01', 1.10, null),
        (5, 2, 40, 4000, 'ddd', '2024-01-04', 4.40, 8),
        (6, 2, 50, 5000, 'eee', '2024-01-05', 5.50, 9)
    """

    // exact at small cardinality across types
    qt_int "SELECT uniq_theta(v_int) FROM test_uniq_theta"
    qt_bigint "SELECT uniq_theta(v_bigint) FROM test_uniq_theta"
    qt_str "SELECT uniq_theta(v_str) FROM test_uniq_theta"
    qt_date "SELECT uniq_theta(v_date) FROM test_uniq_theta"
    qt_dec "SELECT uniq_theta(v_dec) FROM test_uniq_theta"

    // consistent with count(distinct) on small data
    qt_vs_count_distinct """
        SELECT uniq_theta(v_int), count(distinct v_int) FROM test_uniq_theta
    """
    // consistent with approx_count_distinct on small data
    qt_vs_ndv """
        SELECT uniq_theta(v_str), approx_count_distinct(v_str) FROM test_uniq_theta
    """

    // nulls excluded; column has 3 distinct non-null values (7,8,9)
    qt_nulls "SELECT uniq_theta(v_null) FROM test_uniq_theta"

    // group by
    qt_group_by """
        SELECT grp, uniq_theta(v_int) FROM test_uniq_theta GROUP BY grp ORDER BY grp
    """

    // all-null column -> 0
    sql "DROP TABLE IF EXISTS test_uniq_theta_allnull"
    sql """
        CREATE TABLE test_uniq_theta_allnull (id int, v int)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO test_uniq_theta_allnull VALUES (1, null), (2, null)"
    qt_all_null "SELECT uniq_theta(v) FROM test_uniq_theta_allnull"
}
