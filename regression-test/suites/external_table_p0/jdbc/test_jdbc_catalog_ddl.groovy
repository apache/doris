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

suite("test_jdbc_catalog_ddl", "p0,external,mysql,external_docker,external_docker_mysql") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-5.1.49.jar"
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    // String driver_url = "mysql-connector-java-5.1.49.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "test_jdbc_catalog_ddl";

        for (String useMetaCache : ["true", "false"]) {
            sql """drop catalog if exists ${catalog_name} """
            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.jdbc.Driver",
                "use_meta_cache" = "${useMetaCache}"
            );"""
            def res = sql(""" show databases from ${catalog_name}; """).collect {x -> x[0] as String}
            println("show databases result " + res);
            def containedDb = ['mysql', 'doris_test', 'information_schema']
            for (final def db in containedDb) {
                assertTrue(res.contains(db), 'Not contains db: `' + db + '` in mysql catalog')
            }

            // test wrong catalog and db
            test {
                sql """switch unknown_catalog"""
                exception "Unknown catalog 'unknown_catalog'"
            }
            test {
                sql """use unknown_catalog.db1"""
                exception """Unknown catalog 'unknown_catalog'"""
            }
            test {
                sql """use ${catalog_name}.unknown_db"""
                exception """Unknown database 'unknown_db'"""
            }

            // create a database in mysql
            sql """CALL EXECUTE_STMT("${catalog_name}",  "drop database if exists temp_database")"""
            sql """CALL EXECUTE_STMT("${catalog_name}",  "create database temp_database")"""
            sql """CALL EXECUTE_STMT("${catalog_name}",  "drop table if exists temp_database.temp_table")"""
            sql """CALL EXECUTE_STMT("${catalog_name}",  "create table temp_database.temp_table (k1 int)")"""
            sql """CALL EXECUTE_STMT("${catalog_name}",  "insert into temp_database.temp_table values(12345)")"""

            if (useMetaCache.equals("false")) {
                sql """refresh catalog ${catalog_name}"""
            }
            sql "use ${catalog_name}.temp_database"
            qt_sql01 """select * from temp_table"""
            sql """CALL EXECUTE_STMT("${catalog_name}",  "drop database if exists temp_database")"""
        }
    }
}

