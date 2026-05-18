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

import org.apache.doris.regression.action.ProfileAction

suite("test_iceberg_variant_table_path", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    def enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    def catalogName = "test_iceberg_variant_table_path"
    def dbName = "test_iceberg_variant_table_path_db"
    def restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    def minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def profileAction = new ProfileAction(context)
    def getProfileByToken = { token ->
        for (int i = 0; i < 60; ++i) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    def profileText = profileAction.getProfile(profileItem["Profile ID"].toString()).toString()
                    if (profileText.contains("ParquetReadColumnPaths")) {
                        return profileText
                    }
                }
            }
            Thread.sleep(1000)
        }
        assertTrue(false)
    }
    def getParquetReadColumnPathSet = { profileText ->
        def parquetReadColumnPaths = profileText.readLines().find { it.contains("ParquetReadColumnPaths") }
        assertTrue(parquetReadColumnPaths != null)
        logger.info("Iceberg variant table path ${parquetReadColumnPaths}")
        def separatorIndex = parquetReadColumnPaths.indexOf(":")
        assertTrue(separatorIndex >= 0)
        return parquetReadColumnPaths.substring(separatorIndex + 1)
                .split(",")
                .collect { it.trim() }
                .findAll { !it.isEmpty() } as Set
    }

    sql """drop catalog if exists ${catalogName}"""
    spark_iceberg """CREATE DATABASE IF NOT EXISTS demo.${dbName}"""
    spark_iceberg """DROP TABLE IF EXISTS demo.${dbName}.variant_table_path"""

    try {
        spark_iceberg_multi """
            CREATE TABLE demo.${dbName}.variant_table_path (
                id INT,
                v VARIANT
            ) USING iceberg
            TBLPROPERTIES (
                'format-version' = '3',
                'write.format.default' = 'parquet'
            );

            INSERT INTO demo.${dbName}.variant_table_path
            SELECT 1, parse_json('{"metric":10,"nested":{"x":"a"},"items":[1,2]}')
            UNION ALL
            SELECT 2, parse_json('{"metric":20,"nested":{"x":"b"},"items":[3,4]}')
            UNION ALL
            SELECT 3, parse_json('null');
        """, 300

        sql """
            create catalog if not exists ${catalogName} properties (
                "type" = "iceberg",
                "iceberg.catalog.type" = "rest",
                "uri" = "http://${externalEnvIp}:${restPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1"
            )
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        def rows = sql """
            select id,
                   cast(v['metric'] as bigint) as metric,
                   cast(v['nested']['x'] as string) as nested_x,
                   cast(v['missing'] as string) is null as missing_is_null
            from variant_table_path
            order by id
        """
        assertEquals(3, rows.size())
        assertEquals("1", rows[0][0].toString())
        assertEquals("10", rows[0][1].toString())
        assertEquals("a", rows[0][2].toString())
        assertEquals("true", rows[0][3].toString())
        assertEquals("2", rows[1][0].toString())
        assertEquals("20", rows[1][1].toString())
        assertEquals("b", rows[1][2].toString())
        assertEquals("true", rows[1][3].toString())
        assertEquals("3", rows[2][0].toString())
        assertEquals(null, rows[2][1])
        assertEquals(null, rows[2][2])
        assertEquals("true", rows[2][3].toString())

        sql """ set enable_profile = true """
        sql """ set profile_level = 2 """
        def profileToken = UUID.randomUUID().toString()
        sql """
            select "${profileToken}", sum(cast(v['metric'] as bigint))
            from variant_table_path
        """
        def profile = getProfileByToken(profileToken)
        def columnPaths = getParquetReadColumnPathSet(profile)
        assertTrue(columnPaths.contains("v.metadata"))
        assertTrue(columnPaths.contains("v.value"))
    } finally {
        sql """ set enable_profile = false """
        sql """ set profile_level = 0 """
        sql """drop catalog if exists ${catalogName}"""
    }
}
