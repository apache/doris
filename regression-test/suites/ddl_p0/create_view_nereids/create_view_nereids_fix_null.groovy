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

suite("create_view_nereids_fix_null") {
    sql "drop view if exists test_null"
    sql "CREATE VIEW test_null COMMENT '测试null类型' AS SELECT  NULL AS `col1`; "
    def res = sql "desc test_null"
    mustContain(res[0][1], "tinyint")

    sql "drop view if exists test_null_array"
    sql "CREATE VIEW test_null_array COMMENT '测试null类型' AS SELECT  [NULL, NULL] AS `col1`; "
    def res2 = sql "desc test_null_array"
    mustContain(res2[0][1], "array<tinyint>")
    mustContain(res2[0][2], "No")

    String s3_endpoint = getS3Endpoint()
    logger.info("s3_endpoint: " + s3_endpoint)
    String bucket = getS3BucketName()
    logger.info("bucket: " + bucket)
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    String dbname = context.config.getDbNameByFile(context.file)
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    logger.info("jdbcUser: " + jdbcUser)
    String jdbcPassword = context.config.jdbcPassword
    logger.info("jdbcPassword: " + jdbcPassword)
    sql "drop catalog if exists create_view_nereids_fix_null_catalog"
    sql """
        CREATE CATALOG create_view_nereids_fix_null_catalog PROPERTIES (
         "type"="jdbc",
         "user"="${jdbcUser}",
         "password"="${jdbcPassword}",
         "jdbc_url"="${jdbcUrl}",
         "driver_url"="${driver_url}",
         "driver_class"="com.mysql.cj.jdbc.Driver"
        );
    """
    sql "switch create_view_nereids_fix_null_catalog"
    sql "use ${dbname}"
    qt_test_null "select * from test_null"
    qt_test_null_array "select * from test_null_array"
    sql "drop catalog create_view_nereids_fix_null_catalog;"
}