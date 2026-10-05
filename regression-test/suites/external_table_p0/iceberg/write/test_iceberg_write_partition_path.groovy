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

suite("test_iceberg_write_partition_path", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_write_partition_path"
    String db_name = "test_partition_path_db"
    String table_name = "test_partition_path_tbl"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${rest_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1"
        )
    """

    try {
        sql """switch ${catalog_name}"""
        sql """drop database if exists ${db_name} force"""
        sql """create database ${db_name}"""
        sql """use ${db_name}"""

        sql """drop table if exists ${table_name}"""
        sql """
            create table ${table_name} (
                id bigint,
                part_col string
            ) engine=iceberg
            partition by list (part_col) ()
            properties (
                "format-version" = "2",
                "write-format" = "parquet",
                "write.format.default" = "parquet"
            )
        """

        sql """
            insert into ${table_name} values
            (1, concat('with', unhex('CC81'), 'combining character')),
            (2, 'slash/colon:equals=percent%question?')
        """

        List<List<Object>> rows = sql """select id, hex(part_col) from ${table_name} order by id"""
        assertEquals(2, rows.size())
        assertEquals("1", rows[0][0].toString())
        assertEquals("77697468CC81636F6D62696E696E6720636861726163746572", rows[0][1].toString())
        assertEquals("2", rows[1][0].toString())
        assertEquals("736C6173682F636F6C6F6E3A657175616C733D70657263656E74257175657374696F6E3F", rows[1][1].toString())

        List<String> filePaths = sql("""select file_path from ${table_name}\$files order by file_path""")
                .collect { row -> row[0].toString() }
        logger.info("Iceberg partition file paths: ${filePaths}")
        assertTrue(filePaths.any { path -> path.contains("part_col=with%CC%81combining+character") },
                "Expected Iceberg URL-encoded UTF-8 partition path, actual paths: ${filePaths}")
        assertTrue(filePaths.any { path -> path.contains("part_col=slash%2Fcolon%3Aequals%3Dpercent%25question%3F") },
                "Expected Iceberg URL-encoded special-character partition path, actual paths: ${filePaths}")
    } finally {
        sql """drop database if exists ${catalog_name}.${db_name} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
