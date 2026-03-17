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

suite("test_doris_jdbc_catalog_query_hll_quantile", "p0,external") {
    qt_sql """select current_catalog()"""

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    // String driver_url = "mysql-connector-j-8.4.0.jar"

    String catalog_name = "doris_jdbc_catalog_query_hll_quantile";
    String internal_db_name = "regression_test_jdbc_catalog_p0_query_hll_quantile";
    String hllTable = "test_hll_table"
    String quantileTable = "test_quantile_table"

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

    // ========== Test HLL type via JDBC catalog ==========
    sql """switch internal"""
    sql """use ${internal_db_name}"""

    sql """ drop table if exists ${hllTable} """
    sql """ create table `${hllTable}` (
              datekey int,
              hour int,
              user_id hll HLL_UNION
            )
            aggregate key (datekey, hour)
            distributed by hash(datekey, hour) buckets 1
            properties(
              "replication_num" = "1"
            ); """

    sql """ insert into ${hllTable} values(20200622, 1, hll_hash(10001));"""
    sql """ insert into ${hllTable} values(20200622, 2, hll_hash(10002));"""
    sql """ insert into ${hllTable} values(20200622, 2, hll_hash(10003));"""
    sql """ insert into ${hllTable} values(20200622, 3, hll_hash(10004));"""

    sql """ set return_object_data_as_binary=true """
    order_qt_hll_internal """ select hour, HLL_UNION_AGG(user_id) as cnt
                       from `${hllTable}`
                       where datekey=20200622
                    group by hour order by hour; """

    // Query HLL via JDBC external catalog
    sql """ refresh catalog ${catalog_name} """
    sql """ switch ${catalog_name} """
    sql """ use ${internal_db_name} """

    order_qt_hll_jdbc """ select hour, HLL_UNION_AGG(user_id) as cnt
                       from ${catalog_name}.${internal_db_name}.${hllTable}
                       where datekey=20200622
                    group by hour order by hour; """

    // ========== Test QUANTILE_STATE type via JDBC catalog ==========
    sql """switch internal"""
    sql """use ${internal_db_name}"""

    sql """ drop table if exists ${quantileTable} """
    sql """ create table `${quantileTable}` (
              datekey int,
              hour int,
              pv quantile_state QUANTILE_UNION
            )
            aggregate key (datekey, hour)
            distributed by hash(datekey, hour) buckets 1
            properties(
              "replication_num" = "1"
            ); """

    sql """ insert into ${quantileTable} values(20200622, 1, to_quantile_state(100, 2048));"""
    sql """ insert into ${quantileTable} values(20200622, 2, to_quantile_state(200, 2048));"""
    sql """ insert into ${quantileTable} values(20200622, 2, to_quantile_state(300, 2048));"""
    sql """ insert into ${quantileTable} values(20200622, 3, to_quantile_state(400, 2048));"""

    sql """ set return_object_data_as_binary=true """
    order_qt_quantile_internal """ select hour, quantile_percent(quantile_union(pv), 0.5) as median_val
                       from `${quantileTable}`
                       where datekey=20200622
                    group by hour order by hour; """

    // Query quantile_state via JDBC external catalog
    sql """ refresh catalog ${catalog_name} """
    sql """ switch ${catalog_name} """
    sql """ use ${internal_db_name} """

    order_qt_quantile_jdbc """ select hour, quantile_percent(quantile_union(pv), 0.5) as median_val
                       from ${catalog_name}.${internal_db_name}.${quantileTable}
                       where datekey=20200622
                    group by hour order by hour; """

    // clean
    sql """switch internal"""
    sql """use ${internal_db_name}"""
    sql """ drop table if exists ${hllTable} """
    sql """ drop table if exists ${quantileTable} """
    sql """drop catalog if exists ${catalog_name} """
}
