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

suite("test_jni_complex_type", "p0,external,doris,external_docker,external_docker_doris") {
    qt_sql """select current_catalog()"""

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


    String resource_name = "jdbc_resource_catalog_doris"
    String catalog_name = "doris_jdbc_catalog_test_jni";
    String internal_db_name = "regression_test_jdbc_catalog_p0";
    String doris_port = context.config.otherConfigs.get("doris_port");
    String inDorisTable = "test_jni_complex_type";
    String base_table = "base";

    sql """create database if not exists ${internal_db_name}; """

    sql """use ${internal_db_name}"""
    sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
    sql  """
          CREATE TABLE ${internal_db_name}.${inDorisTable} (
                `id` int(11) NULL,
                `arr_text` ARRAY<string> NOT NULL,
                `name` string NULL,
                `age` int(11) NULL
            ) DISTRIBUTED BY HASH(id) BUCKETS 10
            PROPERTIES("replication_num" = "1");
    """
    sql """ insert into ${inDorisTable} values (1, [], 'doris', 18);"""
    sql """ insert into ${inDorisTable} values (2, [], 'nereids', 20);"""
    sql """ insert into ${inDorisTable} values (3, [], 'yy', 19);"""
    sql """ insert into ${inDorisTable} values (4, [], 'xx', 21);"""
    sql """ insert into ${inDorisTable} values (5, [], null, null);"""

    qt_sql """select current_catalog()"""
    order_qt_inner_tb """ select * from internal.${internal_db_name}.${inDorisTable} order by id; """

    sql """drop catalog if exists ${catalog_name} """
    sql """ CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""
    sql "switch ${catalog_name}"
    qt_sql """select current_catalog()"""
    sql """ use ${internal_db_name}"""
    order_qt_ex_tb1 """ select * from ${inDorisTable} order by id; """
}