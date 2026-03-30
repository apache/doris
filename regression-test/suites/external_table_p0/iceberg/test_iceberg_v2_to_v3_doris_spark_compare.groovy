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

suite("test_iceberg_v2_to_v3_doris_spark_compare", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    def enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    def catalogName = "test_iceberg_v2_to_v3_doris_spark_compare"
    def dbName = "test_v2_to_v3_doris_spark_compare_db"
    def restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    def minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def formats = ["parquet", "orc"]

    def tableNameForFormat = { baseName, format ->
        return format == "parquet" ? baseName : "${baseName}_orc"
    }

    sql """drop catalog if exists ${catalogName}"""
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
    sql """set enable_fallback_to_original_planner = false"""

    try {
        def assertV2RowsAreNullAfterUpgrade = { tableName ->
            def rows = sql """
                select id, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            assertEquals(2, rows.size())
            rows.each { row ->
                assertTrue(row[1] == null,
                        "_row_id should be null for v2 rows after upgrade in ${tableName}, row=${row}")
                assertTrue(row[2] == null,
                        "_last_updated_sequence_number should be null for v2 rows after upgrade in ${tableName}, row=${row}")
            }
        }

        def assertV23RowsNotNullAfterUpd = { tableName ->
            def rows = sql """
                select id, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            rows.each { row ->
                assertTrue(row[1] != null,
                    "_row_id should be non-null after Doris operator for ${tableName}")                        
                assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null after Doris operator for ${tableName}")

            }
        }

        def upgradeV3DorisOperationInsert = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)

            sql """
                insert into ${tableName} values
                (4, 'post_v3_i', 400, date '2024-01-04')
            """

            def rows = sql """
                select id, tag, score, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            assertEquals(3, rows.size())
            assertEquals(4, rows[2][0].toString().toInteger())
            assertEquals("post_v3_i", rows[2][1])
            assertV23RowsNotNullAfterUpd(tableName)
        }

        def upgradeV3DorisOperationDelete = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)

            sql """
                delete from ${tableName}
                where id = 3
            """

            def rows = sql """
                select id, tag, score
                from ${tableName}
                order by id
            """
            assertEquals(1, rows.size())
            assertEquals(1, rows[0][0].toString().toInteger())
            assertV23RowsNotNullAfterUpd(tableName)

        }

        def upgradeV3DorisOperationUpdate = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)

            sql """
                update ${tableName}
                set tag = 'post_v3_u', score = score + 20
                where id = 1
            """

            def rows = sql """
                select id, tag, score
                from ${tableName}
                order by id
            """
            assertEquals(2, rows.size())
            assertEquals(1, rows[0][0].toString().toInteger())
            assertEquals("post_v3_u", rows[0][1])
            assertV23RowsNotNullAfterUpd(tableName)
        }

        def upgradeV3DorisOperationRewrite = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)

            def rewriteResult = sql("""
                alter table ${catalogName}.${dbName}.${tableName}
                execute rewrite_data_files(
                    "target-file-size-bytes" = "10485760",
                    "min-input-files" = "1"
                )
            """)
            assertTrue(rewriteResult.size() > 0,
                    "rewrite_data_files should return summary rows for ${tableName}")

            def rowCount = sql """
                select count(*)
                from ${tableName}
            """
            assertEquals(2, rowCount[0][0].toString().toInteger())
            assertV23RowsNotNullAfterUpd(tableName)
        }

        formats.each { format ->
            def rowLineageNullTable = tableNameForFormat("v2v3_row_lineage_null_after_upgrade", format)
            def sparkReferenceTable = tableNameForFormat("v2v3_spark_ops_reference", format)
            def dorisTargetTable = tableNameForFormat("v2v3_doris_ops_target", format)
            log.info("Run v2-to-v3 Doris/Spark compare test with format ${format}")

            def scenario1Rows = sql """
                select id, _row_id, _last_updated_sequence_number
                from ${rowLineageNullTable}
                order by id
            """
            assertEquals(3, scenario1Rows.size())
            scenario1Rows.each { row ->
                assertTrue(row[1] == null,
                        "_row_id should be null for rows written before v3 upgrade, row=${row}")
                assertTrue(row[2] == null,
                        "_last_updated_sequence_number should be null for rows written before v3 upgrade, row=${row}")
            }

            sql """
                update ${dorisTargetTable}
                set tag = 'post_v3_u', score = score + 20
                where id = 2
            """

            sql """
                insert into ${dorisTargetTable} values
                (4, 'post_v3_i', 400, date '2024-02-04')
            """

            def dorisRewriteResult = sql("""
                alter table ${catalogName}.${dbName}.${dorisTargetTable}
                execute rewrite_data_files(
                    "target-file-size-bytes" = "10485760",
                    "min-input-files" = "1"
                )
            """)
            assertTrue(dorisRewriteResult.size() > 0,
                    "Doris rewrite_data_files should return summary rows")

            check_sqls_result_equal """
                select *
                from ${dorisTargetTable}
                order by id
            """, """
                select *
                from ${sparkReferenceTable}
                order by id
            """

            upgradeV3DorisOperationInsert(tableNameForFormat("v2v3_doris_upd_case1", format))
            upgradeV3DorisOperationDelete(tableNameForFormat("v2v3_doris_upd_case2", format))
            upgradeV3DorisOperationUpdate(tableNameForFormat("v2v3_doris_upd_case3", format))
            upgradeV3DorisOperationRewrite(tableNameForFormat("v2v3_doris_upd_case4", format))
        }

    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
