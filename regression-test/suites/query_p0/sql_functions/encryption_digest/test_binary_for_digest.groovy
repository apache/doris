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

suite("test_binary_for_digest", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mysql_varbinary_hash_catalog";
        String ex_db_name = "binary_for_digest_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String test_table = "binary_test_digiest_function_table";

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

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?useSSL=false") {
            try_sql """DROP DATABASE IF EXISTS ${ex_db_name}"""
            sql """CREATE DATABASE ${ex_db_name}"""
            sql """USE ${ex_db_name}"""
            
            sql """CREATE TABLE ${test_table} (
                id int,
                vb varbinary(100),
                vc VARCHAR(100)
            )"""

            sql """INSERT INTO ${test_table} VALUES 
                (1, 'hello world', 'hello world'),
                (2, 'test data', 'test data'),
                (3, 'hash test', 'hash test'),
                (4, '', ''),
                (5, 'special chars: !@#%', 'special chars: !@#%')"""
        }

        sql """switch ${catalog_name}"""
        sql """use ${ex_db_name}"""

        def sha1_result = sql """select id, sha1(vb), sha1(vc) from ${test_table} order by id"""
        for (int i = 0; i < sha1_result.size(); i++) {
            assertTrue(sha1_result[i][1] == sha1_result[i][2], 
                "SHA1 hash mismatch for row ${sha1_result[i][0]}: VarBinary=${sha1_result[i][1]}, VARCHAR=${sha1_result[i][2]}")
        }

        def sha2_256_result = sql """select id, sha2(vb, 256), sha2(vc, 256) from ${test_table} order by id"""
        for (int i = 0; i < sha2_256_result.size(); i++) {
            assertTrue(sha2_256_result[i][1] == sha2_256_result[i][2],
                "SHA2-256 hash mismatch for row ${sha2_256_result[i][0]}: VarBinary=${sha2_256_result[i][1]}, VARCHAR=${sha2_256_result[i][2]}")
        }

        def sha2_224_result = sql """select id, sha2(vb, 224), sha2(vc, 224) from ${test_table} order by id"""
        for (int i = 0; i < sha2_224_result.size(); i++) {
            assertTrue(sha2_224_result[i][1] == sha2_224_result[i][2],
                "SHA2-224 hash mismatch for row ${sha2_224_result[i][0]}: VarBinary=${sha2_224_result[i][1]}, VARCHAR=${sha2_224_result[i][2]}")
        }

        def sha2_384_result = sql """select id, sha2(vb, 384), sha2(vc, 384) from ${test_table} order by id"""
        for (int i = 0; i < sha2_384_result.size(); i++) {
            assertTrue(sha2_384_result[i][1] == sha2_384_result[i][2],
                "SHA2-384 hash mismatch for row ${sha2_384_result[i][0]}: VarBinary=${sha2_384_result[i][1]}, VARCHAR=${sha2_384_result[i][2]}")
        }

        def sha2_512_result = sql """select id, sha2(vb, 512), sha2(vc, 512) from ${test_table} order by id"""
        for (int i = 0; i < sha2_512_result.size(); i++) {
            assertTrue(sha2_512_result[i][1] == sha2_512_result[i][2],
                "SHA2-512 hash mismatch for row ${sha2_512_result[i][0]}: VarBinary=${sha2_512_result[i][1]}, VARCHAR=${sha2_512_result[i][2]}")
        }

        def md5_result = sql """select id, md5(vb), md5(vc) from ${test_table} order by id"""
        for (int i = 0; i < md5_result.size(); i++) {
            assertTrue(md5_result[i][1] == md5_result[i][2],
                "MD5 hash mismatch for row ${md5_result[i][0]}: VarBinary=${md5_result[i][1]}, VARCHAR=${md5_result[i][2]}")
        }

        def md5sum_result = sql """select id, md5sum(vb, vb, vb), md5sum(vc, vc, vc) from ${test_table} order by id"""
        for (int i = 0; i < md5sum_result.size(); i++) {
            assertTrue(md5sum_result[i][1] == md5sum_result[i][2],
                "MD5SUM hash mismatch for row ${md5sum_result[i][0]}: VarBinary=${md5sum_result[i][1]}, VARCHAR=${md5sum_result[i][2]}")
        }

        def sm3_result = sql """select id, sm3(vb), sm3(vc) from ${test_table} order by id"""
        for (int i = 0; i < sm3_result.size(); i++) {
            assertTrue(sm3_result[i][1] == sm3_result[i][2],
                "SM3 hash mismatch for row ${sm3_result[i][0]}: VarBinary=${sm3_result[i][1]}, VARCHAR=${sm3_result[i][2]}")
        }

        def sm3sum_result = sql """select id, sm3sum(vb, vb, vb), sm3sum(vc, vc, vc) from ${test_table} order by id"""
        for (int i = 0; i < sm3sum_result.size(); i++) {
            assertTrue(sm3sum_result[i][1] == sm3sum_result[i][2],
                "SM3SUM hash mismatch for row ${sm3sum_result[i][0]}: VarBinary=${sm3sum_result[i][1]}, VARCHAR=${sm3sum_result[i][2]}")
        }

        def xxhash32_result = sql """select id, xxhash_32(vb), xxhash_32(vc) from ${test_table} order by id"""
        for (int i = 0; i < xxhash32_result.size(); i++) {
            assertTrue(xxhash32_result[i][1] == xxhash32_result[i][2],
                "xxHash32 mismatch for row ${xxhash32_result[i][0]}: VarBinary=${xxhash32_result[i][1]}, VARCHAR=${xxhash32_result[i][2]}")
        }

        def xxhash64_result = sql """select id, xxhash_64(vb), xxhash_64(vc) from ${test_table} order by id"""
        for (int i = 0; i < xxhash64_result.size(); i++) {
            assertTrue(xxhash64_result[i][1] == xxhash64_result[i][2],
                "xxHash64 mismatch for row ${xxhash64_result[i][0]}: VarBinary=${xxhash64_result[i][1]}, VARCHAR=${xxhash64_result[i][2]}")
        }

        def variadic_xxhash32_result = sql """select id, xxhash_32(vb, vb), xxhash_32(vc, vc) from ${test_table} order by id"""
        for (int i = 0; i < variadic_xxhash32_result.size(); i++) {
            assertTrue(variadic_xxhash32_result[i][1] != null && variadic_xxhash32_result[i][2] != null,
                "Variadic xxHash32 should work with mixed VarBinary and VARCHAR arguments for row ${variadic_xxhash32_result[i][0]}")
        }

        def variadic_xxhash64_result = sql """select id, xxhash_64(vb, vb), xxhash_64(vc, vc) from ${test_table} order by id"""
        for (int i = 0; i < variadic_xxhash64_result.size(); i++) {
            assertTrue(variadic_xxhash64_result[i][1] != null && variadic_xxhash64_result[i][2] != null,
                "Variadic xxHash64 should work with mixed VarBinary and VARCHAR arguments for row ${variadic_xxhash64_result[i][0]}")
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?useSSL=false") {
            try_sql """DROP DATABASE IF EXISTS ${ex_db_name}"""
        }

        sql """drop catalog if exists ${catalog_name}"""
    }
}