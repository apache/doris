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
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS t_p;
    """

    sql """
        CREATE TABLE t_p (
            id BIGINT,
            var VARIANT
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

    sql """INSERT INTO t_p VALUES(1, '{"a" : 1}')"""
    sql """INSERT INTO t_p VALUES(7, '{"a" : 2}')"""
    sql """INSERT INTO t_p TEMPORARY PARTITION(tp1) values(16, '{"a" : 3}');"""

    sql "SET enable_fallback_to_original_planner=false"

    qt_sql "SELECT * FROM t_p PARTITION p1"

    qt_sql "SELECT * FROM t_p PARTITION p2"

    order_qt_sql "SELECT * FROM t_p PARTITIONS (p2, p1)"

    order_qt_sql "SELECT * FROM t_p PARTITIONS (p2, p1) WHERE id > 1 and cast(var['a'] as int) > 0"

    qt_sql """select * from t_p temporary partition(tp1);"""

    qt_sql """select * from t_p temporary partitions(tp1);"""

    qt_sql """select * from t_p temporary partition tp1;"""

    sql """
        CREATE TABLE IF NOT EXISTS test_iot (
                `test_int` int NOT NULL,
                `test_var` variant NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`test_int`)
        PARTITION BY LIST (`test_int`)
        (
                PARTITION p1 VALUES IN ("1","2","3"),
                        PARTITION p2 VALUES IN ("4","5","6")
        )
        DISTRIBUTED BY HASH(`test_int`) BUCKETS 3
        PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO test_iot VALUES(1,'{"a" : 1}'),(4, '{"a" : 2}');
    """

    qt_sql """
        SELECT * FROM test_iot PARTITION p1;
    """

// temporary partition test
    sql """
            DROP TABLE IF EXISTS ut_p;
        """

    sql """
        CREATE TABLE ut_p (
            id BIGINT,
            var VARIANT 
        ) unique KEY(`id`)
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

    sql """ALTER TABLE ut_p ADD TEMPORARY PARTITION tp1 VALUES [("5"), ("7"));"""

    sql """INSERT INTO ut_p TEMPORARY PARTITION(tp1) values(6, '{"x" : "123"}');"""
    sql """INSERT INTO ut_p values(6, '{"x" : 456}');"""
    sql """INSERT INTO ut_p values(3, '{"x" : 789}');"""

    sql "set enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_sql """select * from ut_p temporary partitions(tp1);"""

    explain {
        sql "select * from ut_p temporary partitions(tp1);"
        contains "partitions=1/2 (tp1)"
    }

    explain {
        sql """select * from ut_p temporary partitions(tp1) where cast(var['a'] as int) > 0"""
        contains "partitions=1/2 (tp1)"
    }

    explain {
        sql "select * from ut_p temporary partitions(tp1) where cast(var['a'] as int)  > 0"
        contains "partitions=1/2 (tp1)"
    }

    explain {
        sql "select * from ut_p partitions(p2) where cast(var['a'] as int)  > 0"
        contains "partitions=1/2 (p2)"
    }

    explain {
        sql "select * from ut_p temporary partitions(tp1) where id = 8"
        contains "VEMPTYSET"
    }    

    explain {
        sql "select * from ut_p where id = 3 and cast(var['a'] as int) = 789"
        contains "partitions=1/2 (p1)"
        contains "tablets=1/3"
    }    
}
