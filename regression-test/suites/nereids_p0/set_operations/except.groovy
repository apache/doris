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

suite("test_except", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists t1;"
    sql "drop table if exists t2;"
    sql "drop table if exists t3;"
    sql "set ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql '''
    CREATE TABLE t1 (
        a      int NOT NULL,
        b       VARCHAR(25) NOT NULL,
    )ENGINE=OLAP
    unique KEY(`a`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''
    sql '''
    CREATE TABLE t2 (
        a      int NOT NULL,
        b       VARCHAR(25) NOT NULL,
    )ENGINE=OLAP
    unique KEY(`a`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''
    sql '''
    CREATE TABLE t3 (
        a      int NOT NULL,
        b       VARCHAR(25) NOT NULL,
    )ENGINE=OLAP
    unique KEY(`a`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''

    sql """
    insert into t1 values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');
    """
    sql """
    insert into t2 values(1, 'k'), (3, 'l'), (5, 'm'), (7, 'n'), (9, 'o');
    """
    sql """
    insert into t3 values(10, 'j'), (1, 'k'), (4, 'l'), (5, 'm'), (7, 'n'), (8, 'o');
    """

    // (a except b) except c
    order_qt_except_left_deep """
        select * from (select * from t1 except select * from t2) t except select * from t3;
    """

    qt_except_left_deep_shape """
        explain shape plan select * from (select * from t1 except select * from t2) t except select * from t3;
    """

    // a except (b except c)
    order_qt_except_right_deep """
        select * from t1 except select * from (select * from t2 except select * from t3) t;
    """

    qt_except_right_deep_shape """
        explain shape plan select * from t1 except select * from (select * from t2 except select * from t3) t;
    """

    // (a except b) except (c except d)
    order_qt_except_left_deep_right_deep """
        select * from (select * from t1 except select * from t2) t except select * from (select * from t3 except select * from t2) t;
    """

    qt_except_left_deep_right_deep_shape """
        explain shape plan select * from (select * from t1 except select * from t2) t except select * from (select * from t3 except select * from t2) t;
    """

    // (a union b) except c
    order_qt_except_left_deep """
        select * from (select * from t1 union select * from t2) t except select * from t3;
    """

    qt_except_left_deep_shape """
        explain shape plan select * from (select * from t1 union select * from t2) t except select * from t3;
    """

    // a except (b union c)
    order_qt_except_right_deep """
        select * from t1 except select * from (select * from t2 union select * from t3) t;
    """

    qt_except_right_deep_shape """
        explain shape plan select * from t1 except select * from (select * from t2 union select * from t3) t;
    """

    // (a union b) except (c union d)
    order_qt_except_left_deep_right_deep """
        select * from (select * from t1 union select * from t2) t except select * from (select * from t3 union select * from t2) t;
    """

    qt_except_left_deep_right_deep_shape """
        explain shape plan select * from (select * from t1 union select * from t2) t except select * from (select * from t3 union select * from t2) t;
    """
}
