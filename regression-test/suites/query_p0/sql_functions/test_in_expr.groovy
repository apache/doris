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

suite("test_in_expr", "query") { // "arrow_flight_sql", groovy not support print arrow array type, throw IndexOutOfBoundsException.
    def nullTableName = "in_expr_test_null"
    def notNullTableName = "in_expr_test_not_null"

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${nullTableName} (
              `cid` int(11) NULL,
              `number` int(11) NULL,
              `addr` varchar(256) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`cid`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`cid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ insert into ${nullTableName} values(100,1,'a'),(101,2,'b'),(102,3,'c'),(103,4,'d'),(104,null,'e'),(105,6, null) """


    sql """DROP TABLE IF EXISTS ${notNullTableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${notNullTableName} (
              `cid` int(11) not NULL,
              `number` int(11) not NULL,
              `addr` varchar(256) not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`cid`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`cid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """

    sql """ insert into ${notNullTableName} values(100,1,'a'),(101,2,'b'),(102,3,'c'),(103,4,'d') """

    // 1 in expr
    // 1.1 nullable
    // 1.1.1 string + set_not_null
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d')"

    // 1.1.2 string + null_in_set
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d', null)"

    qt_select "select * from ${nullTableName} where addr not in ('d', null)"

    qt_select "select * from ${nullTableName} where not(addr not in ('d', null))"

    qt_select "select * from ${nullTableName} where addr in ('d', null)"

    qt_select "select * from ${nullTableName} where not(addr in ('d', null))"

    // 1.1.3 non-string
    qt_select "select t1.addr from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.number in (3)"

    // 1.2 not null
    // 1.2.1 string + set_not_null
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d')"

    // 1.1.2 string + null_in_set
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr in ('d', null)"

    // 1.1.3 non-string
    qt_select "select t1.addr from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.number in (3)"




    // 2 not in expr
    // 2.1 nullable
    // 2.1.1 string + set_not_null
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d') order by t1.number"

    // 2.1.2 string + null_in_set
    qt_select "select t1.number from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d', null) "

    // 2.1.3 non-string
    qt_select "select t1.addr from ${nullTableName} t1 left join ${nullTableName} t2 on t1.cid=t2.cid where t2.number not in (3) order by t1.addr "

    // 2.2 not null
    // 2.2.1 string + set_not_null
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d') order by t1.number "

    // 2.1.2 string + null_in_set
    qt_select "select t1.number from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.addr not in ('d', null)"

    // 2.1.3 non-string
    qt_select "select t1.addr from ${notNullTableName} t1 left join ${notNullTableName} t2 on t1.cid=t2.cid where t2.number not in (3) order by t1.addr "

    sql """DROP TABLE IF EXISTS ${nullTableName}"""
    sql """DROP TABLE IF EXISTS ${notNullTableName}"""

    // from https://github.com/apache/doris/issues/19374
    sql """DROP TABLE IF EXISTS t11"""
    sql """
            CREATE TABLE t11(c0 CHAR(109) NOT NULL) DISTRIBUTED BY HASH (c0) BUCKETS 13 PROPERTIES ("replication_num" = "1");
        """
    sql """
        insert into t11 values ('1'), ('1'), ('2'), ('2'), ('3'), ('4');
        """
    
    qt_select "select t11.c0 from t11 group by t11.c0 having not ('1' in (t11.c0)) order by t11.c0;"
    qt_select "select t11.c0 from t11 group by t11.c0 having ('1' not in (t11.c0)) order by t11.c0;"

    qt_select "SELECT  (abs(1)=1) IN (null) FROM t11;"

    qt_select """
    SELECT CASE (TIMESTAMP '1970-12-04 03:51:34' NOT BETWEEN TIMESTAMP '1970-11-11 16:41:26' AND CURRENT_TIMESTAMP())  WHEN (0.7532032132148743 BETWEEN 0.7817240953445435 AND CAST('.' AS FLOAT) ) THEN (- (- 2093562249))  WHEN CAST((true IN (false)) AS BOOLEAN)  THEN (+ 1351956476) END  FROM t11 WHERE (NULL IN (CASE CAST('-658171195' AS BOOLEAN)   WHEN ((TIMESTAMP '1970-02-25 22:11:59') IS NOT NULL) THEN NULL  WHEN ((true) IS NULL) THEN NULL END )) GROUP BY t11.c0 ORDER BY 1;
    """

    sql " drop table if exists `array_in_test` "
    sql """
    CREATE TABLE `array_in_test` (
        `id` int(11) NULL COMMENT "",
        `c_array` ARRAY<int(11)> NULL COMMENT ""
    )
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """ INSERT INTO `array_in_test` VALUES (1, [1,2,3,4,5]); """

    qt_select """ select c_array, c_array in (null) from array_in_test; """

    sql " drop table if exists `json_in_test` "
    sql """
        CREATE TABLE json_in_test (
            id INT,
            j JSON
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO json_in_test VALUES(26, '{"k1":"v1", "k2": 200}'); """

    test {
        sql """ select j, j in (null) from json_in_test; """
        exception "errCode"
    }

    sql " drop table if exists `bitmap_in_test` "
    sql """
        create table bitmap_in_test (
            datekey int,
            hour int,
            device_id bitmap BITMAP_UNION
        )
        aggregate key (datekey, hour)
        distributed by hash(datekey, hour) buckets 1
        properties(
          "replication_num" = "1"
        );
    """

    sql """ insert into bitmap_in_test values (20200622, 1, to_bitmap(243));"""
    test {
        sql """ select device_id, device_id in (to_bitmap(1)) from bitmap_in_test; """
        exception "errCode"
    }
    test {
        sql """ select device_id, device_id in (to_bitmap(1),to_bitmap(2),to_bitmap(243)) from bitmap_in_test; """
        exception "errCode"
    }

    sql " drop table if exists `hll_in_test` "
    sql """
        create table hll_in_test (
            id int,
            pv hll hll_union
        )
        Aggregate KEY (id)
        distributed by hash(id) buckets 10
        PROPERTIES(
            "replication_num" = "1"
        )
    """

    sql """ insert into hll_in_test values(1, hll_hash(1)) """
    test {
        sql """ select id, pv in (hll_hash(1)) from hll_in_test; """
        exception "errCode"
    }
    test {
        sql """ select id, pv in (hll_hash(1), hll_hash(2)) from hll_in_test; """
        exception "errCode"
    }

}
