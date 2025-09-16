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

suite("test_join_local_shuffle", "query,p0") {
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_1;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_2;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_3;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_4;"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
        CREATE TABLE test_join_local_shuffle_1 (
            `c1` int(11) NULL COMMENT "",
            `c2` int(11) NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`c1`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`c1`) BUCKETS 16
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
          );
        """
    sql """
        CREATE TABLE test_join_local_shuffle_2 (
            `c1` int(11) NULL COMMENT "",
            `c2` int(11) NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`c1`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`c1`) BUCKETS 16
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
          );
        """

    sql """
        CREATE TABLE test_join_local_shuffle_3 (
            `c1` int(11) NULL COMMENT "",
            `c2` int(11) NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`c1`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`c1`) BUCKETS 16
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
          );
            """
    sql """
            CREATE TABLE test_join_local_shuffle_4 (
                `c1` int(11) NULL COMMENT "",
                `c2` int(11) NULL COMMENT ""
              ) ENGINE=OLAP
              DUPLICATE KEY(`c1`)
              COMMENT "OLAP"
              DISTRIBUTED BY HASH(`c1`) BUCKETS 16
              PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
              );
                """

    sql "insert into test_join_local_shuffle_1 values(0, 1);"
    sql "insert into test_join_local_shuffle_2 values(2, 0);"
    sql "insert into test_join_local_shuffle_3 values(2, 0);"
    sql "insert into test_join_local_shuffle_4 values(0, 1);"
    qt_sql " select  /*+SET_VAR(disable_join_reorder=true,enable_local_shuffle=true) */ * from (select c1, max(c2) from (select b.c1 c1, b.c2 c2 from test_join_local_shuffle_3 a join [shuffle] test_join_local_shuffle_1 b on a.c2 = b.c1 join [broadcast] test_join_local_shuffle_4 c on b.c1 = c.c1) t1 group by c1) t join [shuffle] test_join_local_shuffle_2 on t.c1 = test_join_local_shuffle_2.c2; "

    sql "DROP TABLE IF EXISTS test_join_local_shuffle_1;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_2;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_3;"
    sql "DROP TABLE IF EXISTS test_join_local_shuffle_4;"
}
