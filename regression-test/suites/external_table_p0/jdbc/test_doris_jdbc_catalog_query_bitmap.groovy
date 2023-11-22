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

suite("test_doris_jdbc_catalog_query_bitmap", "p0,external,doris,external_docker,external_docker_doris") {
    qt_sql """select current_catalog()"""

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


    String resource_name = "jdbc_resource_catalog_doris_query_bitmap"
    String catalog_name = "doris_jdbc_catalog_query_bitmap";
    String internal_db_name = "regression_test_jdbc_catalog_p0_query_bitmap";
    String doris_port = 9030;
    String inDorisTable = "test_doris_jdbc_doris_in_tb_metric_table";
    String bitmapTable = "metric_table"

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

    sql """ drop table if exists ${bitmapTable}  """
    sql """ create table `${bitmapTable}` (
              datekey int,
              hour int,
              device_id bitmap BITMAP_UNION
            )
            aggregate key (datekey, hour)
            distributed by hash(datekey, hour) buckets 1
            properties(
              "replication_num" = "1"
            ); """

    sql """ insert into ${bitmapTable} values(20200622, 1, to_bitmap(243));"""
    sql """ insert into ${bitmapTable} values(20200622, 2, bitmap_from_array([1,2,3,4,5,434543]));"""
    sql """ insert into ${bitmapTable} values(20200622, 3, to_bitmap(287667876573));"""

    sql """ set return_object_data_as_binary=true """
    order_qt_tb1 """ select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
                       select hour, BITMAP_UNION(device_id) as pv
                       from `${bitmapTable}`
                       where datekey=20200622
                    group by hour order by 1
                    ) final; """

    // query with jdbc external table
    sql """ refresh catalog  ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ switch ${catalog_name} """
    qt_sql """select current_catalog()"""
    sql """ use ${internal_db_name} """
    //order_qt_tb2 """ select pin_id, hll_union_agg(user_log_acct) from ${catalog_name}.${internal_db_name}.${hllTable} group by pin_id; """
    order_qt_tb2 """ select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
                       select hour, BITMAP_UNION(device_id) as pv
                       from ${catalog_name}.${internal_db_name}.${bitmapTable}
                       where datekey=20200622
                    group by hour order by 1
                    ) final; """

    //clean
    qt_sql """select current_catalog()"""
    sql "switch internal"
    qt_sql """select current_catalog()"""
    sql "use ${internal_db_name}"
    sql """ drop table if exists ${inDorisTable} """
    sql """ drop table if exists ${bitmapTable} """

}
