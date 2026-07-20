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
        return "${baseName}_${format}"
    }

    def createSparkUpgradeFixture = { tableName, format, partitioned, datePrefix, deleteBeforeUpgrade ->
        def partitionClause = partitioned ? "partitioned by (days(dt))" : ""
        def deleteClause = deleteBeforeUpgrade ? """
            delete from ${tableName}
            where id = 2;
        """ : ""
        spark_iceberg_multi """
            use demo.${dbName};

            drop table if exists ${tableName};

            create table ${tableName} (
                id int,
                tag string,
                score int,
                dt date
            ) using iceberg
            ${partitionClause}
            tblproperties (
                'format-version' = '2',
                'write.format.default' = '${format}',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            );

            insert into ${tableName} values
            (1, 'base', 100, date '${datePrefix}-01'),
            (2, 'base', 200, date '${datePrefix}-02');

            insert into ${tableName} values
            (3, 'base', 300, date '${datePrefix}-03');

            update ${tableName}
            set tag = 'base_u', score = score + 10
            where id = 1;

            ${deleteClause}
            alter table ${tableName}
            set tblproperties ('format-version' = '3');
        """
    }

    def createSparkReferenceFixture = { tableName, format ->
        createSparkUpgradeFixture(tableName, format, true, "2024-02", false)
        spark_iceberg_multi """
            use demo.${dbName};

            update ${tableName}
            set tag = 'post_v3_u', score = score + 20
            where id = 2;

            insert into ${tableName} values
            (4, 'post_v3_i', 400, date '2024-02-04');

            call demo.system.rewrite_data_files(
                table => 'demo.${dbName}.${tableName}',
                options => map('target-file-size-bytes', '10485760', 'min-input-files', '1')
            );
        """
    }

    spark_iceberg """create database if not exists demo.${dbName}"""
    formats.each { format ->
        createSparkUpgradeFixture(
                tableNameForFormat("v2v3_row_lineage_null_after_upgrade", format),
                format,
                true,
                "2024-01",
                false)
        createSparkReferenceFixture(tableNameForFormat("v2v3_spark_ops_reference", format), format)
        createSparkUpgradeFixture(
                tableNameForFormat("v2v3_doris_ops_target", format),
                format,
                true,
                "2024-02",
                false)
        (1..5).each { caseIndex ->
            createSparkUpgradeFixture(
                    tableNameForFormat("v2v3_doris_upd_case${caseIndex}", format),
                    format,
                    true,
                    "2024-01",
                    true)
            createSparkUpgradeFixture(
                    tableNameForFormat("v2v3_doris_unpart_case${caseIndex}", format),
                    format,
                    false,
                    "2024-01",
                    true)
        }
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
            sql """refresh table ${dbName}.${tableName}"""
            def rows = sql """
                select id, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            if (rows.size() != 2) {
                def dorisBusinessRows = sql """
                    select id, tag, score, dt
                    from ${tableName}
                    order by id, tag, score
                """
                spark_iceberg """refresh table demo.${dbName}.${tableName}"""
                def sparkBusinessRows = spark_iceberg """
                    select id, tag, score, dt
                    from demo.${dbName}.${tableName}
                    order by id, tag, score
                """
                log.info("Unexpected v2-to-v3 precheck row count for ${tableName}: lineageRows=${rows}, " + "dorisBusinessRows=${dorisBusinessRows}, sparkBusinessRows=${sparkBusinessRows}")
            }
            assertEquals(2, rows.size())
            rows.each { row ->
                assertTrue(row[1] == null,
                    "_row_id should be null for v2 rows after upgrade in ${tableName}, row=${row}")
                assertTrue(row[2] == null,
                    "_last_updated_sequence_number should be null for v2 rows after upgrade in ${tableName}, row=${row}")
            }
        }

        def assertLineageState = { tableName, nonNullIds, nullIds ->
            def rows = sql """
                select id, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            log.info("Lineage state for ${tableName}: ${rows}")
            Map<Integer, List<Object>> rowsById = [:]
            rows.each { row -> rowsById[row[0].toString().toInteger()] = row
            }
            nonNullIds.each { id ->
                assertTrue(rowsById.containsKey(id), "id=${id} should exist in ${tableName}, rows=${rows}")
                def row = rowsById[id]
                assertTrue(row[1] != null,
                    "_row_id should be non-null after Doris operator for ${tableName}, row=${row}")
                assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null after Doris operator for ${tableName}, row=${row}")
            }
            nullIds.each { id ->
                assertTrue(rowsById.containsKey(id), "id=${id} should exist in ${tableName}, rows=${rows}")
                def row = rowsById[id]
                assertTrue(row[1] == null,
                    "_row_id should stay null for historical v2 row in ${tableName}, row=${row}")
                assertTrue(row[2] == null,
                    "_last_updated_sequence_number should stay null for historical v2 row in ${tableName}, row=${row}")
            }
        }

        def normalizeBusinessRows = { rows ->
            return rows.collect { row ->
                [row[0].toString().toInteger(),
                 row[1].toString(),
                 row[2].toString().toInteger(),
                 row[3].toString()]
            }
        }

        def assertSparkBusinessRowsEqual = { tableName ->
            sql """refresh table ${dbName}.${tableName}"""
            spark_iceberg """refresh table demo.${dbName}.${tableName}"""
            def sparkRows = spark_iceberg """
                select id, tag, score, dt
                from demo.${dbName}.${tableName}
                order by id
            """
            def dorisRows = sql """
                select id, tag, score, dt
                from ${tableName}
                order by id
            """
            log.info("Doris business rows for ${tableName}: ${dorisRows}")
            log.info("Spark business rows for ${tableName}: ${sparkRows}")
            assertEquals(normalizeBusinessRows(dorisRows), normalizeBusinessRows(sparkRows))
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
            assertLineageState(tableName, [1, 3, 4], [])
            assertSparkBusinessRowsEqual(tableName)
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
            assertLineageState(tableName, [1], [])
            assertSparkBusinessRowsEqual(tableName)

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
            assertLineageState(tableName, [1, 3], [])
            assertSparkBusinessRowsEqual(tableName)
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
            sql """refresh table ${dbName}.${tableName}"""

            def rowCount = sql """
                select count(*)
                from ${tableName}
            """
            assertEquals(2, rowCount[0][0].toString().toInteger())
            assertLineageState(tableName, [1, 3], [])
            assertSparkBusinessRowsEqual(tableName)
        }

        def validateV3UpgradeReadCompatibility = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)
            assertSparkBusinessRowsEqual(tableName)
        }

        def upgradeV3DorisOperationMerge = { tableName ->
            assertV2RowsAreNullAfterUpgrade(tableName)

            sql """
                merge into ${tableName} t
                using (
                    select 1 as id, 'post_v3_m_u' as tag, 101 as score, date '2024-01-01' as dt, 'U' as flag
                    union all
                    select 3, 'base3', 30, date '2024-01-03', 'D'
                    union all
                    select 4, 'post_v3_m_i', 40, date '2024-01-04', 'I'
                ) s
                on t.id = s.id
                when matched and s.flag = 'D' then delete
                when matched then update set tag = s.tag, score = s.score
                when not matched then insert (id, tag, score, dt)
                values (s.id, s.tag, s.score, s.dt)
            """

            def rows = sql """
                select id, tag, score, _row_id, _last_updated_sequence_number
                from ${tableName}
                order by id
            """
            assertEquals(2, rows.size())
            assertEquals(1, rows[0][0].toString().toInteger())
            assertEquals("post_v3_m_u", rows[0][1].toString())
            assertEquals(101, rows[0][2].toString().toInteger())
            assertEquals(4, rows[1][0].toString().toInteger())
            assertEquals("post_v3_m_i", rows[1][1].toString())
            rows.each { row ->
                assertTrue(row[3] != null, "_row_id should be non-null after MERGE for ${tableName}, row=${row}")
                assertTrue(row[4] != null,
                    "_last_updated_sequence_number should be non-null after MERGE for ${tableName}, row=${row}")
            }
            assertSparkBusinessRowsEqual(tableName)
        }

        formats.each { format ->
            def rowLineageNullTable = tableNameForFormat("v2v3_row_lineage_null_after_upgrade", format)
            def sparkReferenceTable = tableNameForFormat("v2v3_spark_ops_reference", format)
            def dorisTargetTable = tableNameForFormat("v2v3_doris_ops_target", format)
            log.info("Run v2-to-v3 Doris/Spark compare test with format ${format}")

            sql """refresh table ${dbName}.${rowLineageNullTable}"""
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
            assertLineageState(dorisTargetTable, [1, 2, 3, 4], [])

            sql """refresh table ${dbName}.${dorisTargetTable}"""
            sql """refresh table ${dbName}.${sparkReferenceTable}"""
            check_sqls_result_equal """
                select *
                from ${dorisTargetTable}
                order by id
            """, """
                select *
                from ${sparkReferenceTable}
                order by id
            """
            assertSparkBusinessRowsEqual(dorisTargetTable)

            upgradeV3DorisOperationInsert(tableNameForFormat("v2v3_doris_upd_case1", format))
            upgradeV3DorisOperationDelete(tableNameForFormat("v2v3_doris_upd_case2", format))
            upgradeV3DorisOperationUpdate(tableNameForFormat("v2v3_doris_upd_case3", format))
            upgradeV3DorisOperationRewrite(tableNameForFormat("v2v3_doris_upd_case4", format))
            upgradeV3DorisOperationMerge(tableNameForFormat("v2v3_doris_upd_case5", format))

            upgradeV3DorisOperationInsert(tableNameForFormat("v2v3_doris_unpart_case1", format))
            upgradeV3DorisOperationDelete(tableNameForFormat("v2v3_doris_unpart_case2", format))
            upgradeV3DorisOperationUpdate(tableNameForFormat("v2v3_doris_unpart_case3", format))
            validateV3UpgradeReadCompatibility(tableNameForFormat("v2v3_doris_unpart_case4", format))
            upgradeV3DorisOperationMerge(tableNameForFormat("v2v3_doris_unpart_case5", format))
        }

    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
