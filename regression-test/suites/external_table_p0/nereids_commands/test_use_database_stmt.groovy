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

suite("test_use_database_stmt", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "use_db_nereids";
        String internal_catalog = "internal";
        String internal_db_name = "testdb";
        String ex_db_name = "testdb";
        String user = "kevin"
        String pwd = "doris@123456"
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String[] tokens = context.config.jdbcUrl.split('/')
        String url=tokens[0] + "//" + tokens[2] + "/" + "${internal_db_name}" + "?"

        sql """drop catalog if exists ${catalog_name}; """
        sql """drop database if exists ${internal_db_name};"""

        sql """switch internal;"""
        sql """create database ${internal_db_name};"""
        sql """use ${internal_db_name};"""

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """CALL EXECUTE_STMT("${catalog_name}", "drop database if exists ${ex_db_name}");"""
        sql """CALL EXECUTE_STMT("${catalog_name}", "create database if not exists ${ex_db_name}");"""
        sql """switch ${internal_catalog};"""

        try_sql("DROP USER ${user}")
        sql """CREATE USER ${user}@'%' IDENTIFIED BY '${pwd}';"""
        sql """GRANT SELECT_PRIV ON ${internal_catalog}.*.* TO '${user}'@'%';"""

        connect(user, pwd, url) {
            try {
                sql """switch internal"""
                sql """use ${internal_db_name}"""
                qt_sql """select current_catalog()"""
                sql """use ${catalog_name}.${ex_db_name}"""
                exception"Access denied for user '${user}' to database '${ex_db_name}'";
            } catch (Exception e) {
                log.info(e.getMessage())
            }
            qt_sql """select current_catalog()"""
        }
        sql """switch ${internal_catalog}"""
        sql """drop database if exists ${internal_db_name};"""
        sql """CALL EXECUTE_STMT("${catalog_name}", "drop database if exists ${ex_db_name}");"""
        sql """ drop catalog if exists ${catalog_name} ;"""
    }
}
