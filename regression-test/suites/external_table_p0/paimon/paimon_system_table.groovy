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

suite("paimon_system_table", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalog_name = "paimon_timestamp_types"
    try {

        String db_name = "flink_paimon"
        String tableName = "ts_scale_orc"
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use ${db_name};"""
        logger.info("use " + db_name)

        // 1. test Paimon data system table
        logger.info("query data from paimon system table")
        List<List<Object>> paimonTableList = sql """ show tables; """
        boolean targetTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(tableName)
        }
        assertTrue(targetTableExists, "Target table '${tableName}' not found in database '${db_name}'")

        // test all paimon system table
        List<String> paimonSystemTableList = new ArrayList<>(Arrays.asList("manifests", "snapshots", "options", "schemas",
                "partitions", "buckets", "files", "tags", "branches", "consumers", "aggregation_fields",
                "statistics", "table_indexes"))

        // Iterate through all system tables and verify queryability via $ syntax
        for (String systemTable : paimonSystemTableList) {
            logger.info("Testing system table: " + systemTable)

            List<List<Object>> systemTableQueryResult = sql """select * from ${tableName}\$${systemTable}"""
            assertNotNull(systemTableQueryResult, "System table '${systemTable}' result should not be null")
            logger.info("System table ${systemTable} query passed, result size: ${systemTableQueryResult.size()}")
        }

        // 2 Verify system table projection and predicate functionality
        // 2.1 Column projection tests
        qt_direct_query__snapshots_result """select * from ${tableName}\$snapshots order by snapshot_id"""
        // column name
        qt_paimon_snapshots_core_fields_direct_query """
            select snapshot_id,
                       schema_id,
                       commit_user,
                       commit_identifier,
                       commit_kind,
                       base_manifest_list,
                       delta_manifest_list,
                       changelog_manifest_list,
                       total_record_count,
                       delta_record_count,
                       changelog_record_count from ${tableName}\$snapshots
                       order by snapshot_id;
        """

        qt_paimon_snapshots_reordered_filed_direct_query """
                    select  schema_id,
                               snapshot_id,
                               commit_user,
                               commit_identifier,
                               commit_kind,
                               base_manifest_list,
                               delta_manifest_list,
                               changelog_manifest_list,
                               total_record_count,
                               delta_record_count,
                               changelog_record_count from ${tableName}\$snapshots
                               order by snapshot_id;
                """
        // 2.2 Predicate filtering tests
        List<List<Object>> res1 = sql """ select snapshot_id from ${tableName}\$snapshots order by snapshot_id;"""

        qt_snapshot_id_direct_query """select snapshot_id from ${tableName}\$snapshots order by snapshot_id;
                                    """

        assertTrue(res1.size() > 0, "Direct query should return data")
        String direct_query_snapshot_id = String.valueOf(res1[0][0]);
        logger.info("snapshot_id=" + direct_query_snapshot_id)
        qt_direct_query_snapshot_id_predicate """select  schema_id,
                                                snapshot_id,
                                                commit_user,
                                                commit_identifier,
                                                commit_kind,
                                                base_manifest_list,
                                                delta_manifest_list,
                                                changelog_manifest_list,
                                                total_record_count,
                                                delta_record_count,
                                                changelog_record_count from ${tableName}\$snapshots
                                                where snapshot_id=${direct_query_snapshot_id}
                                                order by snapshot_id;

        """

        //2.3  Aggregation functions
        qt_direct_query_snapshot_id_count """
        select count(*) from ${tableName}\$snapshots
                            where snapshot_id=${direct_query_snapshot_id}
        """
        //2.4 Join operations between system tables
        qt_direct_query_snapshots_join """
        SELECT s.snapshot_id, t.schema_id, t.fields
            FROM ${tableName}\$snapshots s JOIN ${tableName}\$schemas t
            ON s.schema_id=t.schema_id where s.snapshot_id=${direct_query_snapshot_id};
        """

        //2.5 Table description queries
        qt_desc_direct_query_ctl_db_table """
                desc ${catalog_name}.${db_name}.${tableName}\$snapshots
                """
        qt_desc_direct_query_db_table """
                desc ${db_name}.${tableName}\$snapshots
                """

        qt_desc_direct_query_table """
                        desc ${tableName}\$snapshots
                        """

        // 2.6 system table does not support time travel
        test {
            sql """select * from ${tableName}\$snapshots FOR VERSION AS OF 1"""
            exception "Paimon system tables do not support time travel"
        }
        test {
            sql """select * from ${tableName}\$snapshots FOR TIME AS OF "2024-07-11 16:01:57.425" """
            exception "Paimon system tables do not support time travel"
        }
        test {
            sql """select * from ${tableName}\$snapshots@incr('startSnapshotId'=1, 'endSnapshotId'=2)"""
            exception "Paimon system tables do not support scan params"
        }

    } catch (Exception e) {
        logger.error("Paimon system table test failed: " + e.getMessage())
        throw e
    } finally {
        // clean resource
        try {
            sql """drop catalog if exists ${catalog_name}"""
        } catch (Exception e) {
            logger.warn("Failed to cleanup catalog: " + e.getMessage())
        }
    }
}
