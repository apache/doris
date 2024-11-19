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

suite("test_nereids_refresh_catalog", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "refresh_catalog_mysql_catalog_nereids";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String ex_tb0 = "ex_tb0";
        String new_mysql_db = "new_mysql_db";

        sql """drop catalog if exists ${catalog_name} """

	    sql """set enable_nereids_planner=true;"""
	    sql """set enable_fallback_to_original_planner=false;"""

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """switch ${catalog_name}"""
        sql """CALL EXECUTE_STMT("${catalog_name}", "drop database if exists ${new_mysql_db}");"""

        qt_sql """show databases;"""
        sql """ use ${ex_db_name}"""

        qt_ex_tb0_where """select id from ${ex_tb0} where id = 111;"""
        order_qt_ex_tb0  """ select id, name from ${ex_tb0} order by id; """
        // create database in mysql
        sql """CALL EXECUTE_STMT("${catalog_name}", "create database  ${new_mysql_db} ;");"""
        qt_sql """show databases;"""
        checkNereidsExecute("refresh catalog ${catalog_name} ;")
        qt_sql """show databases;"""

        checkNereidsExecute("refresh catalog ${catalog_name} properties ('invalid_cache'='true');")

        sql """CALL EXECUTE_STMT("${catalog_name}", "drop database if exists ${new_mysql_db} ;");"""
        qt_sql """show databases;"""

        checkNereidsExecute("refresh catalog ${catalog_name} properties ('invalid_cache'='true');")
        qt_sql """show databases;"""

        sql """ drop catalog if exists ${catalog_name} ;"""
    }
}
