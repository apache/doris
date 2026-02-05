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

suite("test_query_remote_doris_as_olap_table_select", "p0,external,doris,external_docker,external_docker_doris") {
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

    def db_name = "test_query_remote_doris_as_olap_table_select_db"
    def catalog_name = "test_query_remote_doris_as_olap_table_select_catalog"

    sql """DROP DATABASE IF EXISTS ${db_name}"""

    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_all_types_select_t` (
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
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t` values('2025-05-18 01:00:00.000', true, -128, -32768, -2147483648, -9223372036854775808, -1234567890123456790, -123.456, -123456.789, -123457, -123456789012346, -1234567890123456789012345678, '1970-01-01', '0000-01-01 00:00:00', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t` values('2025-05-18 02:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t` values('2025-05-18 03:00:00.000', false, 127, 32767, 2147483647, 9223372036854775807, 1234567890123456789, 123.456, 123456.789, 123457, 123456789012346, 1234567890123456789012345678, '9999-12-31', '9999-12-31 23:59:59', '', '', '', [], {}, {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t` values('2025-05-18 04:00:00.000', true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '2023-10-01', '2023-10-01 12:34:56', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"});
    """

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_all_types_select_t2` (
          `id` datetime(3) NOT NULL,
          `a_boolean` array<boolean> NULL,
          `a_tinyint` array<tinyint> NULL,
          `a_smallint` array<smallint> NULL,
          `a_int` array<int> NULL,
          `a_bigint` array<bigint> NULL,
          `a_largeint` array<largeint> NULL,
          `a_float` array<float> NULL,
          `a_double` array<double> NULL,
          `a_decimal9` array<decimal(9,0)> NULL,
          `a_decimal18` array<decimal(18,0)> NULL,
          `a_decimal32` array<decimal(32,0)> NULL,
          `a_date` array<date> NULL,
          `a_datetime` array<datetime> NULL,
          `a_char` array<char(1)> NULL,
          `a_varchar` array<varchar(65533)> NULL,
          `a_string` array<text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t2` values('2025-05-18 01:00:00.000', [true], [-128], [-32768], [-2147483648], [-9223372036854775808], [-1234567890123456790], [-123.456], [-123456.789], [-123457], [-123456789012346], [-1234567890123456789012345678], ['0000-01-01'], ['0000-01-01 00:00:00'], ['A'], ['Hello'], ['Hello, Doris!'])
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t2` values('2025-05-18 02:00:00.000', [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL])
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t2` values('2025-05-18 03:00:00.000', [false], [127], [32767], [2147483647], [9223372036854775807], [1234567890123456789], [123.456], [123456.789], [123457], [123456789012346], [1234567890123456789012345678], ['9999-12-31'], ['9999-12-31 23:59:59'], [''], [''], [''])
    """
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t2` values('2025-05-18 04:00:00.000', [true], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], ['2023-10-01'], ['2023-10-01 12:34:56'], ['A'], ['Hello'], ['Hello, Doris!']);
    """

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_all_types_select_t3` (
          `id` datetime NOT NULL,
          `datetime_0` datetime(0) NULL,
          `datetime_1` datetime(1) NULL,
          `datetime_3` datetime(2) NULL,
          `datetime_4` datetime(3) NULL,
          `datetime_5` datetime(4) NULL,
          `datetime_6` datetime(5) NULL,
          `datetime_7` datetime(6) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t3` values('2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111');
    """


    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
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

    qt_sql """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t` order by id
    """

    qt_sql """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t2` order by id
    """

    qt_sql """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t3` order by id
    """

    // test select after insert also get correct data, it seems we can newly partition version
    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_all_types_select_t3` values('2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111', '2025-05-19 01:00:00.111111');
    """
    qt_query_after_insert """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t3` order by id
    """

    // test insert command
    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_all_types_insert` (
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
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    // test insert into
    explain {
        sql("insert into `${db_name}`.`test_remote_doris_all_types_insert` select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t`")
        contains("VOlapScanNode")
    }
    sql """
        insert into `${db_name}`.`test_remote_doris_all_types_insert` select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t`
    """
    qt_after_insert_cmd """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`test_remote_doris_all_types_insert` order by id
    """
    // test insert overwrite
    explain {
        sql("insert OVERWRITE table `${db_name}`.`test_remote_doris_all_types_insert` select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t`")
        contains("VOlapScanNode")
    }
    sql """
        insert OVERWRITE table `${db_name}`.`test_remote_doris_all_types_insert` select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${catalog_name}`.`${db_name}`.`test_remote_doris_all_types_select_t`
    """
    qt_after_insert_overwrite_cmd """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`test_remote_doris_all_types_insert` order by id
    """

    // test join operation
    sql """
        CREATE TABLE `${db_name}`.`left_inner_table` (
          log_type        INT            NOT NULL,
          reason       VARCHAR(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`log_type`)
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`left_inner_table` VALUES
        (1,'reason1'),
        (2,'reason2'),
        (3,'reason3');
    """
    sql """
        CREATE TABLE `${db_name}`.`right_remote_table` (
          log_time        DATE       NOT NULL,
          log_type        INT            NOT NULL,
          error_code      INT,
          error_msg       VARCHAR(1024),
          op_id           BIGINT,
          op_time         DATETIME
        ) ENGINE=OLAP
        DUPLICATE KEY(log_time, log_type, error_code)
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`right_remote_table` VALUES
        ('2023-01-01',1,100,'error1',1000,'2023-01-01 00:00:00'),
        ('2023-01-01',2,200,'error2',2000,'2023-01-01 00:00:00'),
        ('2023-01-01',3,300,'error3',3000,'2023-01-01 00:00:00');
    """
    qt_join """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`left_inner_table` a
            join `${catalog_name}`.`${db_name}`.`right_remote_table` b on a.`log_type` = b.`log_type` order by a.`log_type`
    """
    qt_join_predicate """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`left_inner_table` a
        join `${catalog_name}`.`${db_name}`.`right_remote_table` b on a.`log_type` = b.`log_type` and b.op_id=2000 order by a.`log_type`
    """

    // test partition table
    sql """
        CREATE TABLE `${db_name}`.`left_inner_table_partition` (
          log_type        INT            NOT NULL,
          `log_time`       date           NOT NULL,
          reason       VARCHAR(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`log_type`)
        PARTITION BY RANGE(`log_time`)
        (PARTITION p20230101 VALUES [('2023-01-01'), ('2023-01-02')),
        PARTITION p20230102 VALUES [('2023-01-02'), ('2023-01-03')),
        PARTITION p20230103 VALUES [('2023-01-03'), ('2023-01-04')))
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`left_inner_table_partition` VALUES
        (1,'2023-01-01','reason1'),
        (2,'2023-01-02','reason2'),
        (3,'2023-01-03','reason3');
    """
 
    sql """
        CREATE TABLE `${db_name}`.`right_remote_table_partition` (
          log_time        DATE       NOT NULL,
          log_type        INT            NOT NULL,
          error_code      INT,
          error_msg       VARCHAR(1024),
          op_id           BIGINT,
          op_time         DATETIME
        ) ENGINE=OLAP
        DUPLICATE KEY(log_time, log_type, error_code)
        PARTITION BY RANGE(`log_time`)
        (PARTITION p20230101 VALUES [('2023-01-01'), ('2023-01-02')),
        PARTITION p20230102 VALUES [('2023-01-02'), ('2023-01-03')),
        PARTITION p20230103 VALUES [('2023-01-03'), ('2023-01-04')))
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`right_remote_table_partition` VALUES
        ('2023-01-01',1,100,'error1',1000,'2023-01-01 00:00:00'),
        ('2023-01-02',2,200,'error2',2000,'2023-01-02 00:00:00'),
        ('2023-01-03',3,300,'error3',3000,'2023-01-03 00:00:00');
    """

    qt_join_partition """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`left_inner_table_partition` a
            join `${catalog_name}`.`${db_name}`.`right_remote_table_partition` b on a.`log_type` = b.`log_type` and a.`log_time` = b.`log_time` order by a.`log_type`
    """

    qt_join_partition_predicate """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`left_inner_table_partition` a
            join `${catalog_name}`.`${db_name}`.`right_remote_table_partition` b on a.`log_type` = b.`log_type` and a.`log_time` = b.`log_time` and b.log_time='2023-01-02' order by a.`log_type`
    """

    // test json and variant data type
    sql """
        CREATE TABLE `${db_name}`.`remote_json_variant` (
            id          INT     NOT NULL,
            c_json      JSON    ,
            c_variant    VARIANT<'id' : INT,'message*' : STRING,    'tags*' : ARRAY<TEXT>>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`remote_json_variant` VALUES
        (1,'{"id":1,"message":"hello"}','{"id":1,"message":"hello","tags":["tag1","tag2"]}'),
        (2,'{"id":2,"message":"world"}','{"id":2,"message":"world","tags":["tag3","tag4"]}'),
        (3,'{"id":3,"message":"doris"}','{"id":3,"message":"doris","tags":["tag5","tag6"]}');
    """
    qt_json_variant """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ id, c_json, c_variant from `${catalog_name}`.`${db_name}`.`remote_json_variant` order by id
    """
    qt_json_variant_function """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ id,
        JSON_EXTRACT_INT(c_json, '\$.id') as json_id, cast(c_variant['message'] as string) as v_msg from `${catalog_name}`.`${db_name}`.`remote_json_variant` order by id
    """

    // test unique table
    sql """
        CREATE TABLE `${db_name}`.`left_inner_unique` (
          log_type        INT            NOT NULL,
          reason       VARCHAR(1024) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`log_type`)
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`left_inner_unique` VALUES
        (1,'reason1'),
        (2,'reason2'),
        (3,'reason3');
    """

    sql """
        CREATE TABLE `${db_name}`.`right_remote_table_unique` (
          log_time        DATE       NOT NULL,
          log_type        INT            NOT NULL,
          error_code      INT,
          error_msg       VARCHAR(1024),
          op_id           BIGINT,
          op_time         DATETIME
        ) ENGINE=OLAP
        UNIQUE KEY(log_time, log_type, error_code)
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`right_remote_table_unique` VALUES
        ('2023-01-01',1,100,'error1',1000,'2023-01-01 00:00:00'),
        ('2023-01-02',2,200,'error2',2000,'2023-01-02 00:00:00'),
        ('2023-01-03',3,300,'error3',3000,'2023-01-03 00:00:00');
    """
    qt_join_unique """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ * from `${db_name}`.`left_inner_unique` a
            join `${catalog_name}`.`${db_name}`.`right_remote_table_unique` b on a.`log_type` = b.`log_type` and b.op_id=2000 order by a.`log_type`
    """

    // test aggregate table

    sql """
        CREATE TABLE `${db_name}`.`right_remote_table_aggregate` (
          log_type        INT            NOT NULL,
          count           BIGINT SUM
        ) ENGINE=OLAP
        AGGREGATE KEY(log_type)
        DISTRIBUTED BY HASH(`log_type`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO `${db_name}`.`right_remote_table_aggregate` VALUES
        (1,100),
        (1,150),
        (2,200),
        (2,250),
        (3,300),
        (3,350),
        (4,400);
     """
     qt_aggregate """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ log_type, sum(count) as count from `${catalog_name}`.`${db_name}`.`right_remote_table_aggregate` group by log_type order by log_type
    """

    qt_join_aggregate """
        select /*+ SET_VAR(enable_nereids_distribute_planner=true, enable_sql_cache=true) */ b.`log_type`, sum(b.`count`) as count from `${db_name}`.`left_inner_unique` a
            join `${catalog_name}`.`${db_name}`.`right_remote_table_aggregate` b on a.`log_type` = b.`log_type` group by b.`log_type` order by b.`log_type`
    """

    sql """ DROP DATABASE IF EXISTS ${db_name} """
    sql """ DROP CATALOG IF EXISTS `${catalog_name}` """
}