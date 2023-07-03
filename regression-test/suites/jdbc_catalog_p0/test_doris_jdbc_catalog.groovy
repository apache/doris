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

suite("test_doris_jdbc_catalog", "p0") {
    qt_sql """select current_catalog()"""

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String driver_url = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar"

    String resource_name = "jdbc_resource_catalog_doris"
    String catalog_name = "doris_jdbc_catalog";
    String internal_db_name = "regression_test_jdbc_catalog_p0";
    String doris_port = 9030;
    String inDorisTable = "doris_in_tb";
    String hllTable = "bowen_hll_test"

    qt_sql """select current_catalog()"""
    sql """drop catalog if exists ${catalog_name} """

    sql """ CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.jdbc.Driver"
        )"""

    sql  """ drop table if exists ${inDorisTable} """
    sql  """
          CREATE TABLE ${inDorisTable} (
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
        COMMENT "OLAP"
        PARTITION BY RANGE(`pv_date`)
        (PARTITION pbefore201910 VALUES [('1900-01-01'), ('2019-10-01')),
        PARTITION p201910 VALUES [('2019-10-01'), ('2019-11-01')),
        PARTITION p201911 VALUES [('2019-11-01'), ('2019-12-01')),
        PARTITION p201912 VALUES [('2019-12-01'), ('2020-01-01')),
        PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),
        PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01')),
        PARTITION p202003 VALUES [('2020-03-01'), ('2020-04-01')),
        PARTITION p202004 VALUES [('2020-04-01'), ('2020-05-01')),
        PARTITION p202005 VALUES [('2020-05-01'), ('2020-06-01')),
        PARTITION p202006 VALUES [('2020-06-01'), ('2020-07-01')),
        PARTITION p202007 VALUES [('2020-07-01'), ('2020-08-01')),
        PARTITION p202008 VALUES [('2020-08-01'), ('2020-09-01')),
        PARTITION p202009 VALUES [('2020-09-01'), ('2020-10-01')),
        PARTITION p202010 VALUES [('2020-10-01'), ('2020-11-01')),
        PARTITION p202011 VALUES [('2020-11-01'), ('2020-12-01')),
        PARTITION p202012 VALUES [('2020-12-01'), ('2021-01-01')),
        PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
        PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
        PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
        PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
        PARTITION p202105 VALUES [('2021-05-01'), ('2021-06-01')),
        PARTITION p202106 VALUES [('2021-06-01'), ('2021-07-01')),
        PARTITION p202107 VALUES [('2021-07-01'), ('2021-08-01')),
        PARTITION p202108 VALUES [('2021-08-01'), ('2021-09-01')),
        PARTITION p202109 VALUES [('2021-09-01'), ('2021-10-01')),
        PARTITION p202110 VALUES [('2021-10-01'), ('2021-11-01')),
        PARTITION p202111 VALUES [('2021-11-01'), ('2021-12-01')),
        PARTITION p202112 VALUES [('2021-12-01'), ('2022-01-01')),
        PARTITION p202201 VALUES [('2022-01-01'), ('2022-02-01')),
        PARTITION p202202 VALUES [('2022-02-01'), ('2022-03-01')),
        PARTITION p202203 VALUES [('2022-03-01'), ('2022-04-01')),
        PARTITION p202204 VALUES [('2022-04-01'), ('2022-05-01')),
        PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
        PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')),
        PARTITION p202207 VALUES [('2022-07-01'), ('2022-08-01')),
        PARTITION p202208 VALUES [('2022-08-01'), ('2022-09-01')),
        PARTITION p202209 VALUES [('2022-09-01'), ('2022-10-01')),
        PARTITION p202210 VALUES [('2022-10-01'), ('2022-11-01')),
        PARTITION p202211 VALUES [('2022-11-01'), ('2022-12-01')),
        PARTITION p202212 VALUES [('2022-12-01'), ('2023-01-01')),
        PARTITION p202301 VALUES [('2023-01-01'), ('2023-02-01')),
        PARTITION p202302 VALUES [('2023-02-01'), ('2023-03-01')),
        PARTITION p202303 VALUES [('2023-03-01'), ('2023-04-01')),
        PARTITION p202304 VALUES [('2023-04-01'), ('2023-05-01')),
        PARTITION p202305 VALUES [('2023-05-01'), ('2023-06-01')),
        PARTITION p202306 VALUES [('2023-06-01'), ('2023-07-01')),
        PARTITION p202307 VALUES [('2023-07-01'), ('2023-08-01')),
        PARTITION p202308 VALUES [('2023-08-01'), ('2023-09-01')),
        PARTITION p202309 VALUES [('2023-09-01'), ('2023-10-01')))
        DISTRIBUTED BY HASH(`pin_id`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "DEFAULT"
        ); """

    sql """ insert into ${hllTable} values(1, "2023-01-01", hll_hash("1"));"""
    sql """ insert into ${hllTable} values(2, "2023-01-02", hll_hash("2"));"""
    sql """ insert into ${hllTable} values(3, "2023-01-03", hll_hash("3"));"""
    sql """ insert into ${hllTable} values(4, "2023-01-04", hll_hash("4"));"""
    sql """ insert into ${hllTable} values(5, "2023-01-05", hll_hash("5"));"""
    sql """ insert into ${hllTable} values(6, "2023-01-06", hll_hash("6"));"""

    sql """ set return_object_data_as_binary=true """
    order_qt_tb1 """ select pin_id, hll_union_agg(user_log_acct) from ${hllTable} group by pin_id; """

    // query with jdbc external table
    sql """ refresh catalog  ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ switch ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ use ${internal_db_name} """
    order_qt_tb2 """ select pin_id, hll_union_agg(user_log_acct) from ${catalog_name}.${internal_db_name}.${hllTable} group by pin_id; """

    //clean
    qt_sql """select current_catalog()"""
    sql "switch internal"
    qt_sql """select current_catalog()"""
    sql "use ${internal_db_name}"
    sql """ drop table if exists ${inDorisTable} """
    sql """ drop table if exists ${hllTable} """

}
