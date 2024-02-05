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

suite("test_jdbc_call", "p0,external,doris,external_docker,external_docker_doris") {
    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    String catalog_name = "jdbc_call";
    String non_jdbc_catalog_name = "non_jdbc_catalog";
    String internal_db_name = "jdbc_call_db";
    String internal_tbl_name = "jdbc_call_tbl";
    String internal_tbl_name2 = "jdbc_call_tbl2";

    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""

    // 1. create internal db/tbl
    sql """drop database if exists ${internal_db_name};"""
    sql """create database if not exists ${internal_db_name}; """

    sql """create table ${internal_db_name}.${internal_tbl_name} (k1 int, k2 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1");"""
    sql """insert into ${internal_db_name}.${internal_tbl_name} values(1, 2);"""

    // 2. create catalog
    sql """drop catalog if exists ${catalog_name} """
    sql """ CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""

    sql """drop catalog if exists ${non_jdbc_catalog_name}"""
    sql """create catalog if not exists ${non_jdbc_catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://127.0.0.1:9083'
    );"""
    

    // 3. execute call
    order_qt_sql1 """select * from ${catalog_name}.${internal_db_name}.${internal_tbl_name}""";

    test {
        sql """call execute_stmt()"""
        exception "EXECUTE_STMT function must have 2 arguments"
    }

    test {
        sql """call execute_stmt("jdbc")"""
        exception "EXECUTE_STMT function must have 2 arguments"
    }

    test {
        sql """call execute_stmt("${non_jdbc_catalog_name}", "select 1")"""
        exception "Only support JDBC catalog"
    }

    test {
        sql """call execute_stmt("xxx", "select 1")"""
        exception "catalog not found: xxx"
    }

    test {
        sql """call execute_stmt("${catalog_name}", "select 1")"""
        exception "Can not issue SELECT via executeUpdate() or executeLargeUpdate()"
    }

    // execute insert
    sql """call execute_stmt("${catalog_name}", "insert into ${internal_db_name}.${internal_tbl_name} values (3, 4)")"""
    order_qt_sql2 """select * from ${catalog_name}.${internal_db_name}.${internal_tbl_name}"""
    order_qt_sql3 """select * from internal.${internal_db_name}.${internal_tbl_name}"""

    // execute delete
    sql """call execute_stmt("${catalog_name}", "delete from ${internal_db_name}.${internal_tbl_name} where k1 = 1")"""
    order_qt_sql4 """select * from ${catalog_name}.${internal_db_name}.${internal_tbl_name}"""
    order_qt_sql5 """select * from internal.${internal_db_name}.${internal_tbl_name}"""

    // execute create table and insert
    sql """call execute_stmt("${catalog_name}", "create table ${internal_db_name}.${internal_tbl_name2} (c1 int, c2 int) distributed by hash(c1) buckets 1 properties('replication_num' = '1')")"""
    sql """refresh catalog ${catalog_name}"""
    sql """call execute_stmt("${catalog_name}", "insert into ${internal_db_name}.${internal_tbl_name2} values (5, 6), (7, 8)")"""
    order_qt_sql6 """select * from ${catalog_name}.${internal_db_name}.${internal_tbl_name2}"""
    order_qt_sql7 """select * from internal.${internal_db_name}.${internal_tbl_name2}"""

    // test priv
    // only user with load priv can execute call
    String user1 = "normal_jdbc_user";
    String user2 = "load_jdbc_user";
    sql """drop user if exists ${user1}""";
    sql """create user ${user1}""";
    sql """grant select_priv on *.*.* to ${user1}"""
    sql """drop user if exists ${user2}""";
    sql """create user ${user2}""";
    sql """grant load_priv, select_priv on *.*.* to ${user2}"""

    def result1 = connect(user="${user1}", password="", url=context.config.jdbcUrl) {
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        test {
            sql """call execute_stmt("${catalog_name}", "insert into ${internal_db_name}.${internal_tbl_name} values (3, 4)")"""
            exception """has no privilege to execute stmt in catalog"""
        }
    }

    def result2 = connect(user="${user2}", password="", url=context.config.jdbcUrl) {
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        sql """call execute_stmt("${catalog_name}", "insert into ${internal_db_name}.${internal_tbl_name} values (9, 10)")"""
        order_qt_sql8 """select * from internal.${internal_db_name}.${internal_tbl_name}"""
    }
}
