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

suite("query_on_specific_partition") {
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS t_p;
    """

    sql """
        CREATE TABLE t_p (
            id BIGINT,
            val BIGINT,
            str VARCHAR(114)
        ) DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ('5'),
            PARTITION `p2` VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "replication_num"="1"
        );
    """

    sql """ALTER TABLE t_p ADD TEMPORARY PARTITION tp1 VALUES [("15"), ("20"));"""

    sql "INSERT INTO t_p VALUES(1, 1,'1')"
    sql "INSERT INTO t_p VALUES(7, 1,'3')"
    sql "INSERT INTO t_p TEMPORARY PARTITION(tp1) values(16,1234, 't');"

    sql "SET enable_fallback_to_original_planner=false"

    qt_sql "SELECT * FROM t_p PARTITION p1"

    qt_sql "SELECT * FROM t_p PARTITION p2"

    order_qt_sql "SELECT * FROM t_p PARTITIONS (p2, p1)"

    order_qt_sql "SELECT * FROM t_p PARTITIONS (p2, p1) WHERE id > 1"

    qt_sql """select * from t_p temporary partition(tp1);"""

    qt_sql """select * from t_p temporary partitions(tp1);"""

    qt_sql """select * from t_p temporary partition tp1;"""
}
