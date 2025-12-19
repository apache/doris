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

suite("test_remote_doris_agg_table_select", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    String remote_doris_thrift_port = context.config.otherConfigs.get("extFeThriftPort")

    def showres = sql "show frontends";
    remote_doris_arrow_port = showres[0][6]
    remote_doris_http_port = showres[0][3]
    remote_doris_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${remote_doris_arrow_port}, http: ${remote_doris_http_port}, thrift: ${remote_doris_thrift_port}")

    def showres2 = sql "show backends";
    log.info("show backends log = ${showres2}")

    def db_name = "test_remote_doris_agg_table_select_db"
    def catalog_name = "test_remote_doris_agg_table_select_catalog"
    def catalog_arrow_name = "test_remote_doris_agg_table_select_catalog_with_arrow"

    sql "set enable_agg_state=true"

    sql """DROP DATABASE IF EXISTS `${db_name}`"""

    sql """CREATE DATABASE IF NOT EXISTS `${db_name}`"""

    // Non-Aggregate data type
    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_agg_table_select_t` (
          `id` datetime(3) NOT NULL,
          `c_boolean` boolean NULL,
          `c_tinyint` tinyint NULL,
          `c_smallint` smallint NULL,
          `c_int` int NULL,
          `c_bigint` bigint NULL,
          `c_largeint` largeint NULL,
          `c_decimal9` decimal(9,0) NULL,
          `c_decimal18` decimal(18,0) NULL,
          `c_decimal32` decimal(32,0) NULL,
          `c_date` date NULL,
          `c_datetime` datetime NULL,
          `c_char` char(1) NULL,
          `c_varchar` varchar(65533) NULL,
        ) ENGINE=OLAP
        AGGREGATE KEY(id, c_boolean, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_decimal9, c_decimal18, c_decimal32, c_date, c_datetime, c_char, c_varchar)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t` values('2025-05-18 01:00:00.000', true, -128, -32768, -2147483648, -9223372036854775808, -1234567890123456790, -123457, -123456789012346, -1234567890123456789012345678, '1970-01-01', '1970-01-01 00:00:00', 'A', 'Hello')
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t` values('2025-05-18 02:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t` values('2025-05-18 03:00:00.000', false, 127, 32767, 2147483647, 9223372036854775807, 1234567890123456789, 123457, 123456789012346, 1234567890123456789012345678, '9999-12-31', '9999-12-31 23:59:59', '', '')
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t` values('2025-05-18 04:00:00.000', true, 0, 0, 0, 0, 0, 0, 0, 0, '2023-10-01', '2023-10-01 12:34:56', 'A', 'Hello');
    """

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_agg_table_select_t3` (
          `id` datetime NOT NULL,
          `datetime_0` datetime(0) NULL,
          `datetime_1` datetime(1) NULL,
          `datetime_2` datetime(2) NULL,
          `datetime_3` datetime(3) NULL,
          `datetime_4` datetime(4) NULL,
          `datetime_5` datetime(5) NULL,
          `datetime_6` datetime(6) NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(id, datetime_0, datetime_1, datetime_2, datetime_3, datetime_4, datetime_5, datetime_6)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t3` values('2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111');
    """

    // Aggregation data type
    sql """
        CREATE TABLE IF NOT EXISTS `${db_name}`.test_remote_doris_agg_table_select_t4
        (
            user_id             LARGEINT    NOT NULL,
            load_dt             DATE        NOT NULL,
            city                VARCHAR(20),
            last_visit_dt       DATETIME    REPLACE DEFAULT "1970-01-01 00:00:00",

            sum_agg             BIGINT      SUM DEFAULT "0",
            replace_agg         String      REPLACE DEFAULT "0",
            max_agg             INT         MAX DEFAULT "0",
            min_agg             INT         MIN DEFAULT "0",
            replace_n_null_agg  String      REPLACE_IF_NOT_NULL DEFAULT "0"
        )
        AGGREGATE KEY(user_id, load_dt, city)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        properties("replication_num" = "1");
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_agg_table_select_t4` VALUES 
          (101, '2025-12-05', 'BJ', '2024-01-01', 10, 'a', 10, 10, 'a'),
          (101, '2025-12-05', 'BJ', '2024-01-02', 20, 'b', 20, 20, 'b'),
          (101, '2025-12-05', 'BJ', '2024-01-03', 30, null, 30, 30, null),
          (102, '2025-12-05', 'SH', '2024-01-04', 10, 'a', 10, 10, 'a'),
          (102, '2025-12-05', 'SH', '2024-01-05', 20, null, 20, 20, null),
          (102, '2025-12-05', 'SH', '2024-01-06', 30, 'b', 30, 30, 'b');
    """


    sql """
        create table `${db_name}`.`test_remote_doris_agg_table_select_t5`(
            k1 int null,
            k2 agg_state<max_by(int not null,int)> generic,
            k3 agg_state<group_concat(string)> generic
        )
        AGGREGATE KEY(k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql """
        insert into `${db_name}`.`test_remote_doris_agg_table_select_t5` values
          (1,max_by_state(3,1),group_concat_state('a')),
          (1,max_by_state(2,2),group_concat_state('bb')),
          (2,max_by_state(1,3),group_concat_state('ccc'));
    """

    sql """
        create table `${db_name}`.test_remote_doris_agg_table_select_bitmap (
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

    sql """
        insert into `${db_name}`.test_remote_doris_agg_table_select_bitmap values
        (20200622, 1, to_bitmap(243)),
        (20200622, 2, bitmap_from_array([1,2,3,4,5,434543])),
        (20200622, 3, to_bitmap(287667876573));
    """

    sql """
       CREATE TABLE `${db_name}`.test_remote_doris_agg_table_select_hll (
           typ_id           BIGINT          NULL   COMMENT "ID",
           typ_name         VARCHAR(10)     NULL   COMMENT "NAME",
           pv               hll hll_union   NOT NULL   COMMENT "hll"
       )
       AGGREGATE KEY(typ_id,typ_name)
       DISTRIBUTED BY HASH(typ_id) BUCKETS 10
       properties(
         "replication_num" = "1"
       );
    """

    sql """
        insert into `${db_name}`.test_remote_doris_agg_table_select_hll values
           (1, 'aa',hll_hash(1)),
           (1, 'aa',hll_hash(1)),
           (2, 'aa',hll_hash(2)),
           (2, 'bb',hll_hash(2)),
           (3, 'cc',hll_hash(3)),
           (4, 'dd',hll_hash(4)),
           (5, 'ee',hll_hash(5));
    """

    // create catalog
    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """
    sql """
        DROP CATALOG IF EXISTS `${catalog_arrow_name}`
    """

    sql """
        CREATE CATALOG `${catalog_name}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'false'
        );
    """
    sql """
        CREATE CATALOG `${catalog_arrow_name}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    // boolean,tinyint,smallint,int,bigint,largeint,decimal,date,datetime,char,varchar
    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t` order by id
    """
    qt_sql """
        select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t` order by id
    """

    // datetime
    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t3` order by id
    """
    qt_sql """
        select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t3` order by id
    """

    // agg SUM, agg REPLACE, agg MAX, agg MIN, agg REPLACE_IF_NOT_NULL
    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t4` order by last_visit_dt
    """
    qt_sql """
        select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_agg_table_select_t4` order by last_visit_dt
    """

    // AGG_STATE
    qt_sql """
        select k1,max_by_merge(k2), array_sort(SPLIT_BY_STRING(group_concat_merge(k3), ",")) from `${catalog_name}`.`${db_name}`.test_remote_doris_agg_table_select_t5 group by k1 order by k1;
    """
    qt_sql """
        select k1,max_by_merge(k2), array_sort(SPLIT_BY_STRING(group_concat_merge(k3), ",")) from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_agg_table_select_t5 group by k1 order by k1;
    """

    // // AGG_STATE
    qt_sql """
        select max_by_merge(u2), array_sort(SPLIT_BY_STRING(group_concat_merge(u3), ",")) from (
            select k1,max_by_union(k2) as u2,group_concat_union(k3) u3 from `${catalog_name}`.`${db_name}`.test_remote_doris_agg_table_select_t5 group by k1 order by k1
        ) t;
    """
    qt_sql """
        select max_by_merge(u2), array_sort(SPLIT_BY_STRING(group_concat_merge(u3), ",")) from (
            select k1,max_by_union(k2) as u2,group_concat_union(k3) u3 from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_agg_table_select_t5 group by k1 order by k1
        ) t;
    """

    // HLL
    qt_sql """
        select typ_id, typ_name, hll_cardinality(pv) from `${catalog_name}`.`${db_name}`.test_remote_doris_agg_table_select_hll order by typ_id,typ_name
    """
    test {
        sql "select typ_id, typ_name, hll_cardinality(pv) from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_agg_table_select_hll order by typ_id,typ_name"
        // check exception message contains
        exception "[NOT_IMPLEMENTED_ERROR]read_column_from_arrow with type HLL. cur path: /dummyPath"
    }

    // BITMAP
    qt_sql """
        select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
           select hour, BITMAP_UNION(device_id) as pv
           from `${catalog_name}`.`${db_name}`.test_remote_doris_agg_table_select_bitmap
           where datekey=20200622
        group by hour order by 1
        ) final;
    """
    test {
        sql """
            select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
               select hour, BITMAP_UNION(device_id) as pv
               from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_agg_table_select_bitmap
               where datekey=20200622
            group by hour order by 1
            ) final;
        """
        // check exception message contains
        exception "[NOT_IMPLEMENTED_ERROR]read_column_from_arrow with type BITMAP. cur path: /dummyPath"
    }

    sql """ DROP DATABASE IF EXISTS `${db_name}` """
    sql """ DROP CATALOG IF EXISTS `${catalog_name}` """
}
