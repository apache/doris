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

suite("test_nereids_having") {

    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"

    sql "DROP TABLE IF EXISTS t1"

    sql """
        CREATE TABLE t1 (
            pk INT,
            a1 INT,
            a2 INT
        ) DUPLICATE KEY (pk) DISTRIBUTED BY HASH (pk)
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        INSERT INTO t1 VALUES
            (1, 1, 1),
            (1, 1, 2),
            (1, 1, 3),
            (2, 2, 2),
            (2, 2, 4),
            (2, 2, 6),
            (3, 3, 3),
            (3, 3, 6),
            (3, 3, 9)
    """

    sql "SET enable_fallback_to_original_planner=false"

    order_qt_select "SELECT a1 as value FROM t1 GROUP BY a1 HAVING a1 > 0";
    order_qt_select "SELECT a1 as value FROM t1 GROUP BY a1 HAVING value > 0";
    order_qt_select "SELECT SUM(a2) FROM t1 GROUP BY a1 HAVING a1 > 0";
    order_qt_select "SELECT a1 FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
    order_qt_select "SELECT a1, SUM(a2) FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
    order_qt_select "SELECT a1, SUM(a2) as value FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
    order_qt_select "SELECT a1, SUM(a2) as value FROM t1 GROUP BY a1 HAVING value > 0";
    order_qt_select "SELECT a1, SUM(a2) FROM t1 GROUP BY a1 HAVING MIN(pk) > 0";
    order_qt_select "SELECT a1, SUM(a1 + a2) FROM t1 GROUP BY a1 HAVING SUM(a1 + a2) > 0";
    order_qt_select "SELECT a1, SUM(a1 + a2) FROM t1 GROUP BY a1 HAVING SUM(a1 + a2 + 3) > 0";
    order_qt_select "SELECT a1 FROM t1 GROUP BY a1 HAVING COUNT(*) > 0";
}
