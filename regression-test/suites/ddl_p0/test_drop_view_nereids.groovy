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

suite("test_drop_view_nereids") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    sql "SET enable_unicode_name_support=true;"
    sql """DROP TABLE IF EXISTS count_distinct_drop_nereids"""
    sql """
        CREATE TABLE IF NOT EXISTS count_distinct_drop_nereids
        (
            RQ DATE NOT NULL  COMMENT "日期",
            v1_drop VARCHAR(100) NOT NULL  COMMENT "字段1",
            v2 VARCHAR(100) NOT NULL  COMMENT "字段2",
            v3 VARCHAR(100) REPLACE_IF_NOT_NULL  COMMENT "字段3"
        )
        AGGREGATE KEY(RQ,v1_drop,v2)
        PARTITION BY RANGE(RQ)
        (
            PARTITION p20220908 VALUES LESS THAN ('2022-09-09')
        )
        DISTRIBUTED BY HASH(v1_drop,v2) BUCKETS 3
        PROPERTIES(
        "replication_num" = "1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "3"
        );
    """

    sql """
    CREATE VIEW IF NOT EXISTS test_count_distinct_drop_nereids
    (
        RQ comment "日期",
        v1_drop comment "v1_drop",
        v2 comment "v2",
        v3 comment "v3"
    )
    AS
    select aa.RQ as RQ, aa.v1_drop as v1_drop,aa.v2 as v2 , bb.v3 as v3  from
    (
        select RQ, count(distinct v1_drop) as v1_drop , count(distinct  v2 ) as v2
        from count_distinct_drop_nereids
        group by RQ
    ) aa
    LEFT JOIN
    (
        select RQ, max(v3) as v3
        from count_distinct_drop_nereids
        group by RQ
    ) bb
    on aa.RQ = bb.RQ;
    """

    sql """select * from test_count_distinct_drop_nereids"""
    sql """DROP VIEW IF EXISTS test_count_distinct_drop_nereids"""
    sql """DROP TABLE IF EXISTS count_distinct_drop_nereids"""

    sql """DROP TABLE IF EXISTS test_view_drop_t1"""
    sql """
    CREATE TABLE `test_view_drop_t1` (
        k1 int,
        k2 date,
        v1_drop int
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`,`k2`)
        COMMENT '测试'
        PARTITION BY RANGE(k2) (
        PARTITION p1 VALUES [('2023-07-01'), ('2023-07-10')),
        PARTITION p2 VALUES [('2023-07-11'), ('2023-07-20'))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );"""
    sql """DROP TABLE IF EXISTS test_view_drop_t2"""
    sql """
    CREATE TABLE `test_view_drop_t2` (
        k1 int,
        k2 date,
        v1_drop int
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`,`k2`)
        COMMENT '测试'
        PARTITION BY RANGE(k2) (
        PARTITION p1 VALUES [('2023-07-01'), ('2023-07-05')),
        PARTITION p2 VALUES [('2023-07-05'), ('2023-07-15'))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); """
    sql """
        CREATE VIEW IF NOT EXISTS my_view_drop_nereids AS
        SELECT test_view_drop_t1.* FROM test_view_drop_t1 PARTITION(p1) JOIN test_view_drop_t2 PARTITION(p2) ON test_view_drop_t1.k1 = test_view_drop_t2.k1; """
    sql """SELECT * FROM my_view_drop_nereids"""
    sql """DROP VIEW IF EXISTS my_view_drop_nereids"""
    sql """DROP TABLE IF EXISTS test_view_drop_t1"""
    sql """DROP TABLE IF EXISTS test_view_drop_t2"""


    sql """DROP TABLE IF EXISTS view_baseall_drop_nereids"""
    sql """DROP VIEW IF EXISTS test_view7_drop_nereids"""
    sql """DROP VIEW IF EXISTS test_view8_drop"""
    sql """
        CREATE TABLE `view_baseall_drop_nereids` (
            `k1` int(11) NULL,
            `k3` array<int> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """
    sql """insert into view_baseall_drop_nereids values(1,[1,2,3]);"""
    sql """insert into view_baseall_drop_nereids values(2,[10,-2,8]);"""
    sql """insert into view_baseall_drop_nereids values(3,[-1,20,0]);"""

    qt_test_view_1 """ select * from view_baseall_drop_nereids order by k1; """
    qt_test_view_2 """ select *, array_map(x->x>0,k3) from view_baseall_drop_nereids order by k1; """
    qt_test_view_3 """ select *, array_filter(x->x>0,k3),array_filter(`k3`, array_map(x -> x > 0, `k3`)) from view_baseall_drop_nereids order by k1; """


    sql """
    create view IF NOT EXISTS test_view7_drop_nereids (k1,k2,k3,k4) as
            select *, array_filter(x->x>0,k3),array_filter(`k3`, array_map(x -> x > 0, `k3`)) from view_baseall_drop_nereids order by k1;
    """
    qt_test_view_4 """ select * from test_view7_drop_nereids order by k1; """

    sql """
    create view IF NOT EXISTS test_view8_drop_nereids (k1,k2,k3) as
            select *, array_map(x->x>0,k3) from view_baseall_drop_nereids order by k1;
    """
    qt_test_view_5 """ select * from test_view8_drop_nereids order by k1; """

    sql """DROP TABLE IF EXISTS view_column_name_test_drop_nereids"""
    sql """
     CREATE TABLE IF NOT EXISTS view_column_name_test_drop_nereids
    (
        `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
        `type` TINYINT NOT NULL COMMENT "[-128, 127]",
        `error_code` INT COMMENT "[-2147483648, 2147483647]",
        `error_msg` VARCHAR(300) COMMENT "[1-65533]",
        `op_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]",
        `op_time` DATETIME COMMENT "['0000-01-01 00:00:00', '9999-12-31 23:59:59']",
        `target` float COMMENT "4 字节",
        `source` double COMMENT "8 字节",
        `lost_cost` decimal(12,2) COMMENT "",
        `remark` string COMMENT "1m size",
        `op_userid` LARGEINT COMMENT "[-2^127 + 1 ~ 2^127 - 1]",
        `plate` SMALLINT COMMENT "[-32768, 32767]",
        `iscompleted` boolean COMMENT "true 或者 false"
    )
    DISTRIBUTED BY HASH(`type`) BUCKETS 1
    PROPERTIES ('replication_num' = '1');
    """

    sql """
    DROP VIEW IF EXISTS v1_drop
    """
    sql """
    CREATE VIEW v1_drop AS 
    SELECT
      error_code, 
      1, 
      'string', 
      now(), 
      dayofyear(op_time), 
      cast (source AS BIGINT), 
      min(`timestamp`) OVER (
        ORDER BY 
          op_time DESC ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 FOLLOWING
      ), 
      1 > 2,
      2 + 3,
      1 IN (1, 2, 3, 4), 
      remark LIKE '%like', 
      CASE WHEN remark = 's' THEN 1 ELSE 2 END,
      TRUE | FALSE 
    FROM 
      view_column_name_test_drop_nereids
    """

    // test with as
    sql """
          DROP TABLE IF EXISTS mal_test_view_drop
         """

    sql """
         create table mal_test_view_drop(pk int, a int, b int) distributed by hash(pk) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into mal_test_view_drop values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
        ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);
      """
    sql "DROP VIEW  if exists test_view_with_as_drop"
    sql """CREATE VIEW if not exists test_view_with_as_drop AS (
            with t1 as (select * from mal_test_view_drop),  t2 as (select * from mal_test_view_drop),  
            t3 as (select * from mal_test_view_drop) SELECT * FROM t1);"""
    qt_test_with_as "select * from test_view_with_as_drop order by pk, a, b"

    // test union
    sql "DROP VIEW  if exists test_view_union_drop"
    sql """CREATE VIEW test_view_union_drop(c1,c2,c3) AS 
            (select * from mal_test_view_drop Union all SELECT * FROM mal_test_view_drop);"""
    qt_test_union "select c1,c2,c3 from test_view_union_drop order by c1,c2,c3"

    // test count(*)
    sql "drop view if exists test_view_count_star_drop;"
    sql "CREATE VIEW test_view_count_star_drop(c1) AS (select count(*) from mal_test_view_drop having count(*) > 0);"
    qt_test_count_star "select c1 from test_view_count_star_drop order by c1"

    // test expression
    sql "drop view if exists test_view_expression_drop;"
    sql """CREATE VIEW test_view_expression_drop(c1,c2,c3) AS (select a+1,abs(a)+2+1 as c2, cast(b as varchar(10)) as c1 from mal_test_view_drop);"""
    qt_test_expression "select * from test_view_expression_drop order by c1,c2,c3"

    // test alias
    sql "drop view if exists test_view_alias_drop;"
    sql """CREATE VIEW test_view_alias_drop(c1,c2,c3) AS (
        select c8 as c9, c2 as c3, c1 as c4 from (select a+1 c8,abs(a)+2+1 as c2, cast(b as varchar(10)) as c1 from mal_test_view_drop) t);"""
    qt_test_alias "select * from test_view_alias_drop order by c1,c2,c3;"

    // test * except
    sql "drop view if exists test_view_star_except_drop;"
    sql """
        create view test_view_star_except_drop as select * except(pk) from mal_test_view_drop;
        """
    qt_test_star_except "select * from test_view_star_except_drop order by a, b;"

    // test create view from view
    sql "drop view if exists test_view_from_view_drop"
    sql "create view test_view_from_view_drop(c1,c2,c3) as select * from test_view_with_as_drop"
    qt_test_create_view_from_view "select * from test_view_from_view_drop order by c1,c2,c3"

    // test mv prefix in name
    sql "drop view if exists test_mv_prefix_in_view_define_drop;"
    sql "create view test_mv_prefix_in_view_define_drop(`mva_hello`, c2) as select a,b from mal_test_view_drop;"

    // test backquote in name
    sql "drop view if exists test_backquote_in_view_define_drop;"
    sql "create view test_backquote_in_view_define_drop(`abc`, c2) as select a,b from mal_test_view_drop;"
    qt_test_backquote_in_view_define_drop "select * from test_backquote_in_view_define_drop order by abc, c2;"

    sql "drop view if exists test_backquote_in_table_alias_drop;"
    sql "create view test_backquote_in_table_alias_drop(c1, c2) as  select * from (select a,b from mal_test_view_drop) `ab``c`;"
    qt_test_backquote_in_table_alias_drop "select * from test_backquote_in_table_alias_drop order by c1, c2;"

    // test invalid column name
    sql """set enable_unicode_name_support = true;"""
    sql "drop view if exists test_mv_prefix_in_view_define_drop;"
    sql "create view test_mv_prefix_in_view_define_drop as select a as 'mv_hello',b from mal_test_view_drop;"

    // test invalid column name
    sql "drop view if exists test_invalid_column_name_in_table_drop;"
    sql "create view test_invalid_column_name_in_table_drop as select a ,b from mal_test_view_drop;"
    order_qt_test_invalid_column_name_in_table_drop "select * from test_invalid_column_name_in_table_drop"

    sql "alter view test_invalid_column_name_in_table_drop as select a as 'mv_hello',b from mal_test_view_drop;"

    sql """set enable_unicode_name_support = false;"""

    sql "drop table if exists create_view_table1_drop"
    sql """CREATE TABLE create_view_table1_drop (
            id INT,
                    value1 INT,
            value2 VARCHAR(50)
    ) distributed by hash(id) buckets 10 properties("replication_num"="1");"""
    sql """INSERT INTO create_view_table1_drop (id, value1, value2) VALUES
    (1, 10, 'A'),
    (2, 20, 'B'),
    (3, 30, 'C'),
    (4, 40, 'D');"""
    sql "drop table if exists create_view_table2_drop"
    sql """CREATE TABLE create_view_table2_drop (
            id INT,
            table1_id INT,
            value3 INT,
            value4 VARCHAR(50)
    ) distributed by hash(id) buckets 10 properties("replication_num"="1");"""
    sql """INSERT INTO create_view_table2_drop (id, table1_id, value3, value4) VALUES
    (1, 1, 100, 'X'),
    (2, 2, 200, 'Y'),
    (3, 2, 300, 'Z'),
    (4, 3, 400, 'W');"""

    sql "drop view if exists test_view_generate_drop"
    sql """create view test_view_generate_drop as select * from create_view_table1_drop lateral view EXPLODE(ARRAY(30,60)) t1 as age"""
    qt_test_generate "select * from test_view_generate_drop order by 1,2,3,4;"

    sql "drop view if exists test_view_generate_drop_with_column_drop"
    sql """create view test_view_generate_drop_with_column_drop as select * from create_view_table1_drop lateral view EXPLODE_numbers(id) t1 as age"""
    qt_test_generate_with_column "select * from test_view_generate_drop_with_column_drop order by 1,2,3,4"

    sql "drop view if exists test_view_col_alias_drop"
    sql """create view test_view_col_alias_drop as select id as `c1`, value1 as c2 from create_view_table1_drop;"""
    qt_test_col_alias "select * from test_view_col_alias_drop order by 1,2"

    sql "drop view if exists test_view_col_alias_drop_specific_name_drop"
    sql """create view test_view_col_alias_drop_specific_name_drop(col1,col2) as select id as `c1`, value1 as c2 from create_view_table1_drop;"""
    qt_test_col_alias_with_specific_name "select * from test_view_col_alias_drop_specific_name_drop order by 1,2"

    sql "drop view if exists test_view_table_alias_drop"
    sql """create view test_view_table_alias_drop as select * from (
            select id as `c1`, value1 as c2 from create_view_table1_drop limit 10) as t ;"""
    qt_test_table_alias "select * from test_view_table_alias_drop order by 1,2"

    sql "drop view if exists test_view_join_table_alias_drop"
    sql """create view test_view_join_table_alias_drop as select * from (
            select t1.id as `c1`, value1 as c2 from create_view_table1_drop t1 inner join create_view_table2_drop t2 on t1.id=t2.id limit 10) as t ;"""
    qt_test_join_table_alias "select * from test_view_join_table_alias_drop order by 1,2"

    sql "drop function if exists alias_function_create_view_test_drop(INT)"
    sql "CREATE ALIAS FUNCTION alias_function_create_view_test_drop(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"
    sql "drop view if exists test_view_alias_drop_udf_drop;"
    sql "CREATE VIEW if not exists test_view_alias_drop_udf_drop AS (select alias_function_create_view_test_drop(id) as c1,abs(id) from create_view_table1_drop);"
    qt_test_alias_udf "select * from test_view_alias_drop_udf_drop order by 1,2"

    String db = context.config.getDbNameByFile(context.file)
    log.info("db:${db}")
    sql "drop view if exists test_view_alias_drop_udf_drop_with_db_drop;"
    sql "CREATE VIEW if not exists test_view_alias_drop_udf_drop_with_db_drop AS (select ${db}.alias_function_create_view_test_drop(id) as c1,abs(id) from create_view_table1_drop);"
    qt_test_alias_with_db_udf "select * from test_view_alias_drop_udf_drop_with_db_drop order by 1,2"

    def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("jarPath:${jarPath}")

    sql "drop function if exists java_udf_create_view_test_drop(date, date)"
    sql """ CREATE FUNCTION java_udf_create_view_test_drop(date, date) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest1",
            "type"="JAVA_UDF"
        ); """

    sql "drop view if exists test_view_udf_drop;"
    sql """CREATE VIEW if not exists test_view_udf_drop AS (select ${db}.alias_function_create_view_test_drop(id) as c1, java_udf_create_view_test_drop('2011-01-01','2011-01-02'),
        ${db}.java_udf_create_view_test_drop('2011-01-01','2011-01-03') from create_view_table1_drop);"""
    qt_test_udf "select * from test_view_udf_drop order by 1,2,3"

    sql "DROP VIEW  if exists test_view_with_as_drop_with_columns_drop"
    sql """CREATE VIEW if not exists test_view_with_as_drop_with_columns_drop AS (
            with t1(c1,c2,c3) as (select * from mal_test_view_drop),  t2 as (select * from mal_test_view_drop),  
            t3 as (select * from mal_test_view_drop) SELECT * FROM t1);"""
    qt_test_with_as_with_columns "select * from test_view_with_as_drop_with_columns_drop order by c1,c2,c3"

    sql "drop view if exists test_having_drop"
    sql """create view test_having_drop as
    select sum(a) over(partition by a order by pk) as c1 , a from mal_test_view_drop group by grouping sets((a),(b),(pk,a)) having a>1"""
    qt_test_having_drop "select * from test_having_drop order by 1,2"

    sql "drop view if exists test_view_complicated_drop;"
    sql """create view test_view_complicated_drop as
    SELECT * FROM (
        SELECT t1.id, tt.value3, ROW_NUMBER() OVER (PARTITION BY t1.id ORDER BY tt.value3 DESC) as row_num
    FROM (SELECT id FROM create_view_table1_drop GROUP BY id) t1
    FULL OUTER JOIN (SELECT value3, id, MAX(value4) FROM create_view_table2_drop GROUP BY value3, id) tt
    ON tt.id = t1.id
    ORDER BY t1.id
    ) t
    WHERE value3 < 280 AND (id < 3 or id >8);"""
    qt_complicated_view1 "select * from test_view_complicated_drop order by 1,2,3"

    sql "drop table if exists v_t1_drop"
    sql "drop table if exists v_t2_drop"
    sql """CREATE TABLE `v_t1_drop` ( `id` VARCHAR(64) NOT NULL, `name` VARCHAR(512) NULL , `code` VARCHAR(512) NULL
    , `hid` INT NULL , `status` VARCHAR(3) NULL, `update_time` DATETIME NULL
    , `mark` VARCHAR(8) NULL ,
    `create_by` VARCHAR(64) NULL , `create_time` DATETIME NULL ,
    `update_by` VARCHAR(64) NULL , `operate_status` INT NULL DEFAULT "0" )
    ENGINE=OLAP UNIQUE KEY(`id`)  DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES
    ( "replication_allocation" = "tag.location.default: 1");"""
    sql """CREATE TABLE `v_t2_drop` ( `id` INT NULL , `name` VARCHAR(500) NULL , `hid` INT NULL , 
    `hname` VARCHAR(200) NULL , `com_id` INT NULL , `com_name` VARCHAR(200) NULL , `hsid` INT NULL ,
    `subcom_name` VARCHAR(255) NULL , `status` VARCHAR(3) NULL, `update_time` DATETIME NULL ) ENGINE=OLAP UNIQUE KEY(`id`)
     DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1");"""

    sql """drop view if exists t_view_nullable1_drop;"""
    sql """CREATE VIEW `t_view_nullable1_drop` COMMENT 'VIEW' AS SELECT `t`.`id` AS `community_id`, `t`.`name` AS `community_name`, `t2`.`com_id`
    AS `company_id`, `t`.`status` AS `del_flag`, `t`.`create_time` AS `create_time`, `t`.`update_time` AS `update_time`
    FROM `v_t1_drop` t  LEFT OUTER JOIN `v_t2_drop` t2
    ON (`t2`.`id` = `t`.`hid`) AND (`t2`.`status` != '0') GROUP BY `t`.`id`, `t`.`name`, `t2`.`com_id`, `t`.`status`,
    `t`.`create_time`, `t`.`update_time`;"""

    sql """INSERT INTO v_t2_drop (id, name, hid, hname, com_id, com_name, hsid, subcom_name, status, update_time)
    VALUES
    (100, '中心站A', 1, '供热处A', 10, '分公司A', 1000, '公司A', '1', '2024-09-01 10:00:00'),
    (101, '中心站B', 2, '供热处B', 11, '分公司B', 1001, '公司B', '1', '2024-09-01 10:00:00'),
    (102, '中心站C', 3, '供热处C', 12, '分公司C', 1002, '公司C', '0', '2024-09-01 10:00:00');
    """

    sql """INSERT INTO v_t1_drop (id, name, code, hid, status, update_time, mark, create_by, create_time, update_by, operate_status)
    VALUES
    ('1', '小区A', '001', 100, '1', '2024-09-01 10:00:00', '0', 'user1', '2024-09-01 09:00:00', 'user2', 1),
    ('2', '小区B', '002', 101, '1', '2024-09-01 10:00:00', '0', 'user1', '2024-09-01 09:00:00', 'user2', 1),
    ('3', '小区C', '003', NULL, '1', '2024-09-01 10:00:00', '0', 'user1', '2024-09-01 09:00:00', 'user2', 0);"""
    qt_nullable """SELECT * FROM t_view_nullable1_drop order by community_id;"""

    sql "drop view if exists t_view_nullable2_drop"
    sql """CREATE VIEW `t_view_nullable2_drop`(a,b,c,d,e,f) COMMENT 'VIEW' AS SELECT `t`.`id` AS `community_id`, `t`.`name` AS `community_name`, `t2`.`com_id`
    AS `company_id`, `t`.`status` AS `del_flag`, `t`.`create_time` AS `create_time`, `t`.`update_time` AS `update_time`
    FROM `v_t1_drop` t  LEFT OUTER JOIN `v_t2_drop` t2
    ON (`t2`.`id` = `t`.`hid`) AND (`t2`.`status` != '0') GROUP BY `t`.`id`, `t`.`name`, `t2`.`com_id`, `t`.`status`,
    `t`.`create_time`, `t`.`update_time`;"""
    qt_nullable_view_with_cols "select * from t_view_nullable2_drop order by a;"
    def res1 = sql "desc t_view_nullable1_drop"
    def res2 = sql "desc t_view_nullable2_drop"
    mustContain(res1[1][2], "Yes")
    mustContain(res2[1][2], "Yes")
}
