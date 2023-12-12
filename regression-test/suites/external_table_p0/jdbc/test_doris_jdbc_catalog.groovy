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

suite("test_doris_jdbc_catalog", "p0,external,doris,external_docker,external_docker_doris") {
    qt_sql """select current_catalog()"""

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


    String resource_name = "jdbc_resource_catalog_doris"
    String catalog_name = "doris_jdbc_catalog";
    String internal_db_name = "regression_test_jdbc_catalog_p0";
    String doris_port = context.config.otherConfigs.get("doris_port");
    String inDorisTable = "test_doris_jdbc_doris_in_tb";
    String hllTable = "bowen_hll_test"
    String base_table = "base";
    String arr_table = "arr";

    sql """create database if not exists ${internal_db_name}; """

    qt_sql """select current_catalog()"""
    sql """drop catalog if exists ${catalog_name} """

    sql """ CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""
    sql """use ${internal_db_name}"""
    sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
    sql  """
          CREATE TABLE ${internal_db_name}.${inDorisTable} (
            `id` INT NULL COMMENT "主键id",
            `name` string NULL COMMENT "名字"
            ) DISTRIBUTED BY HASH(id) BUCKETS 10
            PROPERTIES("replication_num" = "1");
    """
    sql """ insert into ${inDorisTable} values (1, 'doris1')"""
    sql """ insert into ${inDorisTable} values (2, 'doris2')"""
    sql """ insert into ${inDorisTable} values (3, 'doris3')"""
    sql """ insert into ${inDorisTable} values (4, 'doris4')"""
    sql """ insert into ${inDorisTable} values (5, 'doris5')"""
    sql """ insert into ${inDorisTable} values (6, 'doris6')"""

    order_qt_ex_tb1 """ select * from internal.${internal_db_name}.${inDorisTable} order by id; """

    qt_sql """select current_catalog()"""
    sql "switch ${catalog_name}"
    qt_sql """select current_catalog()"""
    sql """ use ${internal_db_name}"""
    order_qt_ex_tb1 """ select * from ${inDorisTable} order by id; """

    // test hll query
    sql "switch internal"
    sql "use ${internal_db_name}"

    sql """ drop table if exists ${hllTable}  """
    sql """ CREATE TABLE `${hllTable}` (
          `pin_id` bigint(20) NOT NULL COMMENT "",
          `pv_date` datev2 NOT NULL COMMENT "",
          `user_log_acct` hll HLL_UNION NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`pin_id`, `pv_date`)
        DISTRIBUTED BY HASH(`pin_id`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """

    sql """ insert into ${hllTable} values(1, "2023-01-01", hll_hash("1"));"""
    sql """ insert into ${hllTable} values(2, "2023-01-02", hll_hash("2"));"""
    sql """ insert into ${hllTable} values(3, "2023-01-03", hll_hash("3"));"""
    sql """ insert into ${hllTable} values(4, "2023-01-04", hll_hash("4"));"""
    sql """ insert into ${hllTable} values(5, "2023-01-05", hll_hash("5"));"""
    sql """ insert into ${hllTable} values(6, "2023-01-06", hll_hash("6"));"""

    sql """drop table if exists ${base_table}"""
    sql """
        create table ${base_table} (
            bool_col boolean,
            tinyint_col tinyint,
            smallint_col smallint,
            int_col int,
            bigint_col bigint,
            largeint_col largeint,
            float_col float,
            double_col double,
            decimal_col decimal(10, 5),
            decimal_col2 decimal(30, 10),
            date_col date,
            datetime_col datetime(3),
            char_col char(10),
            varchar_col varchar(10),
            json_col json
        )
        DUPLICATE KEY(`bool_col`)
        DISTRIBUTED BY HASH(`bool_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${base_table} values (true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1.0, '2021-01-01', '2021-01-01 00:00:00.000', 'a', 'a', '{\"a\": 1}');"""
    // insert NULL
    sql """insert into ${base_table} values (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""
    order_qt_base1 """ select * from ${base_table} order by int_col; """

    sql """drop table if exists all_null_tbl"""
    sql """
        create table all_null_tbl (
            bool_col boolean,
            tinyint_col tinyint,
            smallint_col smallint,
            int_col int,
            bigint_col bigint,
            largeint_col largeint,
            float_col float,
            double_col double,
            decimal_col decimal(10, 5),
            decimal_col2 decimal(30, 10),
            date_col date,
            datetime_col datetime(3),
            char_col char(10),
            varchar_col varchar(10),
            json_col json
        )
        DUPLICATE KEY(`bool_col`)
        DISTRIBUTED BY HASH(`bool_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into all_null_tbl values (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""
    order_qt_all_null """ select * from all_null_tbl order by int_col; """

    sql """drop table if exists ${arr_table}"""
    sql """
        create table ${arr_table} (
            int_col int,
            arr_bool_col array<boolean>,
            arr_tinyint_col array<tinyint>,
            arr_smallint_col array<smallint>,
            arr_int_col array<int>,
            arr_bigint_col array<bigint>,
            arr_largeint_col array<largeint>,
            arr_float_col array<float>,
            arr_double_col array<double>,
            arr_decimal1_col array<decimal(10, 5)>,
            arr_decimal2_col array<decimal(30, 10)>,
            arr_date_col array<date>,
            arr_datetime_col array<datetime(3)>,
            arr_char_col array<char(10)>,
            arr_varchar_col array<varchar(10)>,
            arr_string_col array<string>
        )
        DUPLICATE KEY(`int_col`)
        DISTRIBUTED BY HASH(`int_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into ${arr_table} values (1, array(true), array(1), array(1), array(1), array(1), array(1), array(1.0), array(1.0), array(1.0), array(1.0), array('2021-01-01'), array('2021-01-01 00:00:00.000'), array('a'), array('a'), array('a'));"""
    // insert NULL
    sql """insert into ${arr_table} values (2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""
    order_qt_arr1 """ select * from ${arr_table} order by int_col; """

    sql """drop table if exists test_insert_order"""

    sql """
         CREATE TABLE test_insert_order (
             gameid varchar(50) NOT NULL DEFAULT "",
             aid int(11) NOT NULL DEFAULT "0",
             bid int(11) NOT NULL DEFAULT "0",
             cid int(11) NOT NULL DEFAULT "0",
             did int(11) NOT NULL DEFAULT "0",
             pname varchar(255) NOT NULL DEFAULT "其他"
         ) ENGINE=OLAP
         UNIQUE KEY(gameid, aid, bid, cid)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(gameid) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1"
         );
    """


    sql """ set return_object_data_as_binary=true """
    order_qt_tb1 """ select pin_id, hll_union_agg(user_log_acct) from ${hllTable} group by pin_id; """

    // query with jdbc external table
    sql """ refresh catalog  ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ switch ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ use ${internal_db_name} """
    order_qt_tb2 """ select pin_id, hll_union_agg(user_log_acct) from ${catalog_name}.${internal_db_name}.${hllTable} group by pin_id; """
    order_qt_base2 """ select * from ${catalog_name}.${internal_db_name}.${base_table} order by int_col; """
    order_qt_all_null2 """ select * from ${catalog_name}.${internal_db_name}.all_null_tbl order by int_col; """
    order_qt_arr2 """ select * from ${catalog_name}.${internal_db_name}.${arr_table} order by int_col; """
    sql """ drop table if exists internal.${internal_db_name}.ctas_base; """
    sql """ drop table if exists internal.${internal_db_name}.ctas_arr; """
    order_qt_ctas_base """ create table internal.${internal_db_name}.ctas_base PROPERTIES("replication_num" = "1") as select * from ${catalog_name}.${internal_db_name}.${base_table} order by int_col; """
    order_qt_ctas_arr """ create table internal.${internal_db_name}.ctas_arr PROPERTIES("replication_num" = "1") as select * from ${catalog_name}.${internal_db_name}.${arr_table} order by int_col; """
    qt_desc_ctas_base """ desc internal.${internal_db_name}.ctas_base; """
    qt_desc_ctas_arr """ desc internal.${internal_db_name}.ctas_arr; """
    order_qt_query_ctas_base """ select * from internal.${internal_db_name}.ctas_base order by int_col; """
    order_qt_query_ctas_arr """ select * from internal.${internal_db_name}.ctas_arr order by int_col; """

    // test insert order
    sql """insert into test_insert_order(gameid,did,cid,bid,aid,pname) values('g1',4,3,2,1,'p1')""";
    sql """insert into test_insert_order(gameid,did,cid,bid,aid,pname) select 'g2',4,3,2,1,'p2'""";
    qt_sql """select * from test_insert_order order by gameid, aid, bid, cid, did;"""

    //clean
    qt_sql """select current_catalog()"""
    sql "switch internal"
    qt_sql """select current_catalog()"""
    sql "use ${internal_db_name}"
    sql """ drop table if exists ${inDorisTable} """
    sql """ drop table if exists ${hllTable} """
    sql """ drop table if exists ${base_table} """
    sql """ drop table if exists all_null_tbl """
    sql """ drop table if exists ${arr_table} """
    sql """ drop table if exists test_insert_order """

}
