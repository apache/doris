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

suite("test_nereids_set_operation") {

    sql "SET enable_nereids_planner=true"

    sql "DROP TABLE IF EXISTS setOperationTable"
    sql "DROP TABLE IF EXISTS setOperationTableNotNullable"

    sql """
        CREATE TABLE `setOperationTable` (
        `k1` bigint(20) NULL,
        `k2` bigint(20) NULL,
        `k3` bigint(20) NULL,
        `k4` bigint(20) not null,
        `k5` varchar(10),
        `k6` varchar(10)
        ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        CREATE TABLE `setOperationTableNotNullable` (
         `k1` bigint(20) NOT NULL,
         `k2` bigint(20) NOT NULL,
         `k3` bigint(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """ drop table if exists test_table;"""
    sql """
        CREATE TABLE `test_table`
        (
            `day` date
        ) ENGINE = OLAP DUPLICATE KEY(`day`)
        DISTRIBUTED BY HASH(`day`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into test_table values('2020-05-25');"""

    sql """
        INSERT INTO setOperationTable VALUES
            (1, 1, 1, 3, 'a', 'b'),
            (1, 1, 2, 3, 'a', 'c'),
            (1, 1, 3, 4, 'a' , 'd'),
            (1, 0, null, 4, 'b' , 'b'),
            (2, 2, 2, 5, 'b', 'c'),
            (2, 2, 4, 5, 'b' , 'd'),
            (2, 2, 6, 4, 'c', 'b'),
            (2, 2, null, 4, 'c', 'c'),
            (3, 3, 3, 3, 'c', 'd'),
            (3, 3, 6, 3, 'd', 'b'),
            (3, 3, 9, 4, 'd', 'c'),
            (3, 0, null, 5, 'd', 'd')
    """

    sql """
        insert into setOperationTableNotNullable values
        (1, 0, 0),
        (1, 1, 3), 
        (1, 1, 2), 
        (1, 1, 1), 
        (2, 2, 0), 
        (2, 2, 6), 
        (2, 2, 4), 
        (2, 2, 2), 
        (3, 0, 0), 
        (3, 3, 9), 
        (3, 3, 6), 
        (3, 3, 3);
    """

    sql "SET enable_fallback_to_original_planner=false"

    // union
    order_qt_select1 "select k1+1, k2 from setOperationTable union select k1, k3 from setOperationTableNotNullable;";
    order_qt_select2 "select k1+1, k3 from setOperationTableNotNullable union select k1+1, k2 from setOperationTable;";

    order_qt_select3 "select k5, k6, k1 from setOperationTable union select k1, k2, k3 from setOperationTableNotNullable";
    order_qt_select4 "select k1, k2, k3 from setOperationTableNotNullable union select k5, k6, k1 from setOperationTable";

    order_qt_select5 "select k1+1, k2 from setOperationTable union all select k1, k3 from setOperationTableNotNullable;";
    order_qt_select6 "select k1+1, k3 from setOperationTableNotNullable union all select k1+1, k2 from setOperationTable;";

    order_qt_select7 "select k5, k6, k1 from setOperationTable union all select k1, k2, k3 from setOperationTableNotNullable";
    order_qt_select8 "select k1, k2, k3 from setOperationTableNotNullable union all select k5, k6, k1 from setOperationTable";


    // except
    order_qt_select9 "select k1+1, k2 from setOperationTable except select k1, k3 from setOperationTableNotNullable;";
    order_qt_select10 "select k1+1, k3 from setOperationTableNotNullable except select k1+1, k2 from setOperationTable;";

    order_qt_select11 "select k5, k6, k1 from setOperationTable except select k1, k2, k3 from setOperationTableNotNullable";
    order_qt_select12 "select k1, k2, k3 from setOperationTableNotNullable except select k5, k6, k1 from setOperationTable";

    //intersect
    order_qt_select13 "select k1+1, k2 from setOperationTable intersect select k1, k3 from setOperationTableNotNullable;";
    order_qt_select14 "select k1+1, k3 from setOperationTableNotNullable intersect select k1+1, k2 from setOperationTable;";

    order_qt_select15 "select k5, k6, k1 from setOperationTable intersect select k1, k2, k3 from setOperationTableNotNullable";
    order_qt_select16 "select k1, k2, k3 from setOperationTableNotNullable intersect select k5, k6, k1 from setOperationTable";

    // mix
    order_qt_select17 """
            select k1, k3 from setOperationTableNotNullable union all 
            select k1, k5 from setOperationTable except
            select k2, k1 from setOperationTableNotNullable
            """

    order_qt_select18 """
            select k1, k3 from setOperationTableNotNullable union all 
            (select k1, k5 from setOperationTable union
            select k2, k1 from setOperationTableNotNullable)
    """

    order_qt_select19 """
            (select k1, k3 from setOperationTableNotNullable union all 
            select k1, k5 from setOperationTable) union
            select k2, k1 from setOperationTableNotNullable
    """

    order_qt_select20 """
            select * from (select k1, k2 from setOperationTableNotNullable union all select k1, k5 from setOperationTable) t;
    """

    order_qt_select21 """            select * from (select k1, k2 from setOperationTableNotNullable union select k1, k5 from setOperationTable) t;
    """

    order_qt_select24 """select * from (select 1 a, 2 b 
		    union all select 3, 4 
		    union all select 10, 20) t where a<b order by a, b"""

    order_qt_select25 """
            select k1, sum(k2) from setOperationTableNotNullable group by k1
            union distinct (select 2,3)
    """

    order_qt_select26 """
            (select 2,3)
            union distinct
            select k1, sum(k2) from setOperationTableNotNullable group by k1
            union distinct (select 2,3)
    """

    order_qt_select27 """
            (select 1, 'a', NULL, 10.0)
            union all (select 2, 'b', NULL, 20.0)
	        union all (select 1, 'a', NULL, 10.0)
	"""

    order_qt_select28 """
            (select 10, 10.0, 'hello', 'world') union all
            (select k1, k2, k3, k4 from setOperationTable where k1=1) union all
	        (select 20, 20.0, 'wangjuoo4', 'beautiful') union all
	        (select k2, k3, k1, k3 from setOperationTableNotNullable where k2>0)
	        """

    order_qt_select29 """
        select * from (
            (select 10, 10.0, 'hello', 'world') union all
            (select k1, k2, k3, k4 from setOperationTable where k1=1) union all
	        (select 20, 20.0, 'wangjuoo4', 'beautiful') union all
	        (select k2, k3, k1, k3 from setOperationTableNotNullable where k2>0)) t
	    """

    // test_union_basic
    qt_union30 """select 1 c1, 2  union select 1.01, 2.0 union (select 0.0001, 0.0000001) order by c1"""
    qt_union31 """select 1 c1, 2 union (select "hell0", "") order by c1"""
    qt_union32 """select 1 c1, 2  union select 1.0, 2.0 union (select 1.00000000, 2.00000) order by c1"""
    qt_union33 """select 1 c1, 2  union all select 1.0, 2.0 union (select 1.00000000, 2.00000) order by c1"""
    qt_union34 """select 1 c1, 2  union all select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by c1"""
    qt_union35 """select 1 c1, 2  union select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by c1"""
    qt_union36 """select 1 c1, 2  union distinct select 1.0, 2.0 union distinct (select 1.00000000, 2.00000) order by c1"""
    qt_union38 """select "2016-07-01" c1 union (select "2016-07-02") order by c1"""

    // test_union_bug
    // PALO-3617
    qt_union36 """select * from (select 1 as a, 2 as b union select 3, 3) c where a = 1"""
    
    // cast类型
    def res5 = sql"""(select k1, k2 from setOperationTable) union (select k2, cast(k1 as int) from setOperationTable)
       order by k1, k2"""
    def res6 = sql"""(select k1, k2 from setOperationTable) union (select k2, cast(k1 as int) from setOperationTable order by k2)
       order by k1, k2"""
    check2_doris(res5, res6)
    def res7 = sql"""(select k1, k2 from setOperationTable) union (select k2, cast(k3 as int) from setOperationTable) order by k1, k2"""

    def res8 = sql"""(select k1, k2 from setOperationTable) union (select k2, cast(k3 as int) from setOperationTable order by k2) order
        by k1, k2"""
    check2_doris(res7, res8)
    // 不同类型不同个数
    test {
        sql """select k1, k2 from setOperationTable union select k1, k3, k4  from setOperationTable order by k1, k2"""
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    qt_union39 """(select  k1 from setOperationTable order by k1) union all (select k1 from setOperationTableNotNullable order by k1) order by k1;"""

    order_qt_union40 """
        SELECT k1 FROM setOperationTable WHERE k2 = 2 
        INTERSECT 
        SELECT k1 FROM setOperationTable WHERE k1 = 1 
        UNION 
        SELECT k1 FROM setOperationTable WHERE k3 = 2
    """

    order_qt_union41 """
    SELECT k1 FROM setOperationTable WHERE k2 = 1
    EXCEPT
    SELECT k1 FROM setOperationTable WHERE k3 = 2
    UNION
    (SELECT k1 FROM setOperationTable WHERE k3 = 2
    INTERSECT
    SELECT k1 FROM setOperationTable WHERE k2 > 0)
    """

    order_qt_union42 """
    SELECT k1 FROM setOperationTable WHERE k2 = 1
    EXCEPT
    SELECT k1 FROM setOperationTable WHERE k3 = 2
    UNION ALL
    (SELECT k1 FROM setOperationTable WHERE k3 = 2
    INTERSECT
    SELECT k1 FROM setOperationTable WHERE k2 > 0)
    """

    order_qt_select43 """
        SELECT * FROM (select k1, k3 from setOperationTableNotNullable order by k3 union all 
            select k1, k5 from setOperationTable) t;
    """

    order_qt_select44 """
    select k1, k3 from setOperationTableNotNullable order by k3 union all 
            select k1, k5 from setOperationTable
    """

    order_qt_select45 """
    (select k1, k3 from setOperationTableNotNullable order by k3) union all 
            (select k1, k5 from setOperationTable)
    """

    order_qt_select46 """
    (with cte AS (select k1, k3 from setOperationTableNotNullable) select * from cte order by k3) union all 
            (select k1, k5 from setOperationTable)
    """

    order_qt_union43 """select '2020-05-25' day from test_table union all select day from test_table;"""
    
    qt_union44 """
        select * from
            (select day from test_table
            union all
            select DATE_FORMAT(day, '%Y-%m-%d %H') dt_h from test_table
            ) a
        order by 1
    """

    // test union distinct column prune
    qt_union45 """
        select count(*) from (select 1, 2 union select 1,1 ) a;
    """
}
