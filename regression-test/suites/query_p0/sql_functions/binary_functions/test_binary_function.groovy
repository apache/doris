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

suite("test_binary_function", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
        String catalog_name = "mysql_varbinary_catalog";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String test_table = "binary_test_function_table";

     

        sql """drop catalog if exists ${catalog_name}"""

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "enable.mapping.varbinary" = "true"
        );"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false") {
            try_sql """DROP TABLE IF EXISTS ${test_table}"""

            sql """CREATE TABLE ${test_table} (
                id int,
                vb varbinary(100),
                vc VARCHAR(100)
            )"""

            sql """INSERT INTO ${test_table} VALUES 
                (1, 'hello world', 'hello world'),
                (2, '', ''),
                (3, 'special chars: !@#%', 'special chars: !@#%'),
                (4, '__123hehe1', '__123hehe1'),
                (5, 'ABB', 'ABB'),
                (6, '5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh', '5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh'),
                (7, 'SEVMTE8sIV4l', 'SEVMTE8sIV4l')
            """
        }

        sql """switch ${catalog_name}"""
        sql """use ${ex_db_name}"""

        def length_result = sql """select id, length(vb), length(vc) from ${test_table} order by id"""
        for (int i = 0; i < length_result.size(); i++) {
            assertTrue(length_result[i][1] == length_result[i][2], 
                "length mismatch for row ${length_result[i][0]}: VarBinary=${length_result[i][1]}, VARCHAR=${length_result[i][2]}")
        }

        def from_base64_result = sql """select id, from_binary(from_base64_binary(vc)), hex(from_base64(vc)) from ${test_table} order by id"""
        for (int i = 0; i < from_base64_result.size(); i++) {
            def bin = from_base64_result[i][1]
            def str = from_base64_result[i][2]
            assertTrue(bin == str,
                "from_base64 mismatch for row ${from_base64_result[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

        def to_base64_result = sql """select id, to_base64_binary(vb), to_base64(vc) from ${test_table} order by id"""
        for (int i = 0; i < to_base64_result.size(); i++) {
            assertTrue(to_base64_result[i][1] == to_base64_result[i][2],
                "to_base64 mismatch for row ${to_base64_result[i][0]}: VarBinary=${to_base64_result[i][1]}, VARCHAR=${to_base64_result[i][2]}")
        }

        def sub_binary_3args_result = sql """select id, from_binary(sub_binary(vb, 1, 5)), hex(substr(vc, 1, 5)) from ${test_table} order by id"""
        for (int i = 0; i < sub_binary_3args_result.size(); i++) {
            def bin = sub_binary_3args_result[i][1]
            def str = sub_binary_3args_result[i][2]
            assertTrue(bin == str,
                "sub_binary_3args mismatch for row ${sub_binary_3args_result[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

        def sub_binary_3args_result_2 = sql """select id, from_binary(sub_binary(vb, -1, 5)), hex(substr(vc, -1, 5)) from ${test_table} order by id"""
        for (int i = 0; i < sub_binary_3args_result_2.size(); i++) {
            def bin = sub_binary_3args_result_2[i][1]
            def str = sub_binary_3args_result_2[i][2]
            assertTrue(bin == str,
                "sub_binary_3args_2 mismatch for row ${sub_binary_3args_result_2[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

        def sub_binary_2args_result = sql """select id, from_binary(sub_binary(vb, 1)), hex(substr(vc, 1)) from ${test_table} order by id"""
        for (int i = 0; i < sub_binary_2args_result.size(); i++) {
            def bin = sub_binary_2args_result[i][1]
            def str = sub_binary_2args_result[i][2]
            assertTrue(bin == str,
                "sub_binary_2args mismatch for row ${sub_binary_2args_result[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

        def sub_binary_2args_result_2 = sql """select id, from_binary(sub_binary(vb, -1)), hex(substr(vc, -1)) from ${test_table} order by id"""
        for (int i = 0; i < sub_binary_2args_result_2.size(); i++) {
            def bin = sub_binary_2args_result_2[i][1]
            def str = sub_binary_2args_result_2[i][2]
            assertTrue(bin == str,
                "sub_binary_2args_2 mismatch for row ${sub_binary_2args_result_2[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

        def sub_binary_nest_2args_result = sql """select id, from_binary(sub_binary(sub_binary(vb, 1), 1)), hex(substr(substr(vc, 1), 1)) from ${test_table} order by id"""
        for (int i = 0; i < sub_binary_nest_2args_result.size(); i++) {
            def bin = sub_binary_nest_2args_result[i][1]
            def str = sub_binary_nest_2args_result[i][2]
            assertTrue(bin == str,
                "sub_binary_nest_2args mismatch for row ${sub_binary_nest_2args_result[i][0]}: VarBinary=${bin}, VARCHAR=${str}")
        }

    }
}
