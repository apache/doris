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

suite("test_iceberg_historical_filter_planning", "p0,external,doris,external_docker,external_docker_doris") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String baseCatalog = "iceberg_historical_filter_planning"
    String cacheCatalog = "iceberg_historical_filter_planning_cache"
    String dbName = "historical_filter_planning_db"
    def catalogs = [baseCatalog, cacheCatalog]
    def oldBatchMode = sql("show variables like 'enable_external_table_batch_mode'")[0][1]
    def oldBatchSize = sql("show variables like 'num_files_in_batch_mode'")[0][1]

    try {
        catalogs.each { catalog ->
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'rest',
                'uri' = 'http://${externalEnvIp}:${restPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.region' = 'us-east-1',
                'meta.cache.iceberg.manifest.enable' = '${catalog == cacheCatalog}'
            )"""
        }
        sql "create database if not exists ${baseCatalog}.${dbName}"
        sql "set num_files_in_batch_mode = 1"

        [false, true].each { partitioned ->
            ["rename", "drop"].each { change ->
                String tableName = "${change}_${partitioned ? 'partitioned' : 'unpartitioned'}"
                String table = "${baseCatalog}.${dbName}.${tableName}"
                sql "drop table if exists ${table}"
                sql """create table ${table} (id int, x int, part int)
                    ${partitioned ? 'partition by list (part) ()' : ''}
                    properties ('format-version' = '2')"""
                sql "insert into ${table} values (1, 1, 1), (2, 1, 2), (3, 2, 2)"
                def snapshotId = sql("select snapshot_id from ${table}\$snapshots")[0][0]
                sql "alter table ${table} create tag before_change"
                if (change == "rename") {
                    sql "alter table ${table} rename column x renamed_x"
                } else {
                    sql "alter table ${table} drop column x"
                }

                def assertHistoricalFilters = {
                    catalogs.each { catalog ->
                        sql "refresh table ${catalog}.${dbName}.${tableName}"
                        [false, true].each { batchMode ->
                            sql "set enable_external_table_batch_mode = ${batchMode}"
                            [" for version as of ${snapshotId}", "@tag(before_change)"].each { ref ->
                                String historical = "${catalog}.${dbName}.${tableName}${ref}"
                                assertEquals([[1, 1, 1], [2, 1, 2], [3, 2, 2]],
                                        sql("select id, x, part from ${historical} order by id"))
                                assertEquals([[1, 1, 1], [2, 1, 2]],
                                        sql("select id, x, part from ${historical} where x = 1 order by id"))
                                assertEquals([[2, 1, 2]],
                                        sql("select id, x, part from ${historical} where x = 1 and part = 2 order by id"))
                            }
                        }
                    }
                }

                // Schema-only changes retain the data snapshot ID; SDK-only time-travel tests
                // that append immediately after DDL miss this case and Doris batch-mode pruning.
                assertEquals(snapshotId, sql("select snapshot_id from ${table}\$snapshots")[0][0])
                assertHistoricalFilters()

                if (change == "rename") {
                    sql "insert into ${table} values (4, 1, 3)"
                } else {
                    sql "insert into ${table} values (4, 3)"
                }
                assertHistoricalFilters()

                // The same name with a new field ID must not change historical predicate binding.
                sql "alter table ${table} add column x int"
                assertHistoricalFilters()
            }
        }
    } finally {
        sql "set enable_external_table_batch_mode = ${oldBatchMode}"
        sql "set num_files_in_batch_mode = ${oldBatchSize}"
        sql "drop database if exists ${baseCatalog}.${dbName} force"
        catalogs.reverseEach { catalog -> sql "drop catalog if exists ${catalog}" }
    }
}
