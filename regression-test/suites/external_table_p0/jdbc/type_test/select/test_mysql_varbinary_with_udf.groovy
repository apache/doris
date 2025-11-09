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

suite("test_mysql_varbinary_with_udf", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists mysql_varbinary_udf_catalog """
        sql """create catalog if not exists mysql_varbinary_udf_catalog properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use mysql_varbinary_udf_catalog.test_varbinary_db"""
        qt_desc_varbinary_type """desc test_varbinary_udf;"""
        qt_select_varbinary_type """select * from test_varbinary_udf order by id;"""

        sql """switch internal"""
        sql """create database if not exists test_mysql_udf;"""
        sql """use internal.test_mysql_udf;"""

        def jarPath = """${context.file.parent}/../../../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
        scp_udf_file_to_all_be(jarPath)
        log.info("Jar path: ${jarPath}".toString())

        try_sql("DROP FUNCTION IF EXISTS udf_test_varbinary(varbinary);")
        sql """ CREATE FUNCTION udf_test_varbinary(varbinary) RETURNS varbinary PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.VarBinaryTest",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        try_sql("DROP FUNCTION IF EXISTS udf_test_varbinary2(varbinary);")
        sql """ CREATE FUNCTION udf_test_varbinary2(varbinary) RETURNS varbinary PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.VarBinaryTest2",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        qt_select_varbinary_type4 """select id, varbinary_c, udf_test_varbinary(varbinary_c) as col_varbinary_reversed from mysql_varbinary_udf_catalog.test_varbinary_db.test_varbinary_udf order by id;"""
        qt_select_varbinary_type5 """select id, varbinary_c, udf_test_varbinary2(varbinary_c) as col_varbinary_reversed from mysql_varbinary_udf_catalog.test_varbinary_db.test_varbinary_udf order by id;"""

        sql """drop catalog if exists mysql_varbinary_udf_catalog """
    }
}
