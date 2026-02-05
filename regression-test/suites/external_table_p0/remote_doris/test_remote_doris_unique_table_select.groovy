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

suite("test_remote_doris_unique_table_select", "p0,external,doris,external_docker,external_docker_doris") {
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

    def db_name = "test_remote_doris_unique_table_select_db"
    def catalog_name = "test_remote_doris_unique_table_select_catalog"
    def catalog_arrow_name = "test_remote_doris_unique_table_select_catalog_with_arrow"

    sql "set enable_agg_state=true"

    sql """DROP DATABASE IF EXISTS `${db_name}`"""

    sql """CREATE DATABASE IF NOT EXISTS `${db_name}`"""

    // Non-Aggregate data type
    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_unique_table_select_t` (
          `id` datetime(3) NOT NULL,
          `c_boolean` boolean NULL,
          `c_tinyint` tinyint NULL,
          `c_smallint` smallint NULL,
          `c_int` int NULL,
          `c_bigint` bigint NULL,
          `c_largeint` largeint NULL,
          `c_float` float NULL,
          `c_double` double NULL,
          `c_decimal9` decimal(9,0) NULL,
          `c_decimal18` decimal(18,0) NULL,
          `c_decimal32` decimal(32,0) NULL,
          `c_date` date NULL,
          `c_datetime` datetime NULL,
          `c_char` char(1) NULL,
          `c_varchar` varchar(65533) NULL,
          `c_string` text NULL,
          `c_array_s` array<text> NULL,
          `c_map` MAP<STRING, INT> NULL,
          `c_struct` STRUCT<f1:INT,f2:FLOAT,f3:STRING>  NULL,
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_unique_table_select_t` values('2025-05-18 01:00:00.000', true, -128, -32768, -2147483648, -9223372036854775808, -1234567890123456790, -123.456, -123456.789, -123457, -123456789012346, -1234567890123456789012345678, '1970-01-01', '1970-01-01 00:00:00', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_unique_table_select_t` values('2025-05-18 02:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_unique_table_select_t` values('2025-05-18 03:00:00.000', false, 127, 32767, 2147483647, 9223372036854775807, 1234567890123456789, 123.456, 123456.789, 123457, 123456789012346, 1234567890123456789012345678, '9999-12-31', '9999-12-31 23:59:59', '', '', '', [], {}, {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_unique_table_select_t` values('2025-05-18 04:00:00.000', true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '2023-10-01', '2023-10-01 12:34:56', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"});
    """

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_unique_table_select_t3` (
          `id` datetime NOT NULL,
          `datetime_0` datetime(0) NULL,
          `datetime_1` datetime(1) NULL,
          `datetime_2` datetime(2) NULL,
          `datetime_3` datetime(3) NULL,
          `datetime_4` datetime(4) NULL,
          `datetime_5` datetime(5) NULL,
          `datetime_6` datetime(6) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_unique_table_select_t3` values('2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111');
    """

    // Aggregation data type
    sql """
        create table `${db_name}`.test_remote_doris_unique_table_select_bitmap (
          datekey int,
          hour int,
          device_id bitmap
        )
        UNIQUE KEY (datekey, hour)
        distributed by hash(datekey, hour) buckets 1
        properties(
          "replication_num" = "1"
        );
    """

    sql """
        insert into `${db_name}`.test_remote_doris_unique_table_select_bitmap values
        (20200622, 1, to_bitmap(243)),
        (20200622, 2, bitmap_from_array([1,2,3,4,5,434543])),
        (20200622, 3, to_bitmap(287667876573));
    """

    sql """
       CREATE TABLE `${db_name}`.test_remote_doris_unique_table_select_hll (
           typ_id           BIGINT          NULL   COMMENT "ID",
           typ_name         VARCHAR(10)     NULL   COMMENT "NAME",
           pv               hll              NOT NULL   COMMENT "hll"
       )
       UNIQUE KEY(typ_id,typ_name)
       DISTRIBUTED BY HASH(typ_id) BUCKETS 10
       properties(
         "replication_num" = "1"
       );
    """

    sql """
        insert into `${db_name}`.test_remote_doris_unique_table_select_hll values
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

    // boolean,tinyint,smallint,int,bigint,largeint,decimal,date,datetime,char,varchar ···
    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_unique_table_select_t` order by id
    """
    qt_sql """
        select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_unique_table_select_t` order by id
    """

    // datetime
    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_unique_table_select_t3` order by id
    """
    qt_sql """
        select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_unique_table_select_t3` order by id
    """


    // HLL
    qt_sql """
        select typ_id, typ_name, hll_cardinality(pv) from `${catalog_name}`.`${db_name}`.test_remote_doris_unique_table_select_hll order by typ_id,typ_name
    """
    test {
        sql "select typ_id, typ_name, hll_cardinality(pv) from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_unique_table_select_hll order by typ_id,typ_name"
        // check exception message contains
        exception "[NOT_IMPLEMENTED_ERROR]read_column_from_arrow with type HLL. cur path: /dummyPath"
    }

    // BITMAP
    qt_sql """
        select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
           select hour, BITMAP_UNION(device_id) as pv
           from `${catalog_name}`.`${db_name}`.test_remote_doris_unique_table_select_bitmap
           where datekey=20200622
        group by hour order by 1
        ) final;
    """
    test {
        sql """
            select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
               select hour, BITMAP_UNION(device_id) as pv
               from `${catalog_arrow_name}`.`${db_name}`.test_remote_doris_unique_table_select_bitmap
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
