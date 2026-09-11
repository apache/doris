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

// This suite requires a pre-provisioned Databricks Unity Catalog fixture. It is disabled unless the
// environment explicitly sets enableDatabricksAzureIcebergE2E=true; no credential is stored in the test.
suite("test_databricks_azure_vended_credentials_e2e", "p0,external,iceberg") {
    String enabled = context.config.otherConfigs.get("enableDatabricksAzureIcebergE2E")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Databricks Azure Iceberg E2E test is disabled")
        return
    }

    String catalogName = context.config.otherConfigs.get("databricksAzureIcebergCatalog")
            ?: "dbx_azure_iceberg"
    String dbName = context.config.otherConfigs.get("databricksAzureIcebergDatabase")
            ?: "iceberg_e2e"
    String dmlTable = context.config.otherConfigs.get("databricksAzureIcebergDmlTable")
            ?: "matrix_dml_v3_20260820"
    String overwriteTable = context.config.otherConfigs.get("databricksAzureIcebergOverwriteTable")
            ?: "matrix_merge_fresh_20260820"
    String partitionTable = context.config.otherConfigs.get("databricksAzureIcebergPartitionTable")
            ?: "matrix_partition_prune_2304"
    String partitionKeysTable = context.config.otherConfigs.get("databricksAzureIcebergPartitionKeysTable")
            ?: "matrix_partition_filter_keys_2304"
    String nestedTable = context.config.otherConfigs.get("databricksAzureIcebergNestedTable")
            ?: "matrix_nested_prune_2304"
    String nestedArrayTable = context.config.otherConfigs.get("databricksAzureIcebergNestedArrayTable")
            ?: "matrix_nested_array_prune_2304"
    String nestedMapTable = context.config.otherConfigs.get("databricksAzureIcebergNestedMapTable")
            ?: "matrix_nested_map_prune_2304"

    sql "SWITCH ${catalogName}"
    sql "USE ${dbName}"

    def databases = sql "SHOW DATABASES FROM ${catalogName}"
    assertTrue(databases.any { row -> row[0].toString().equalsIgnoreCase(dbName) },
            "Expected database ${catalogName}.${dbName}")
    def tables = sql "SHOW TABLES FROM ${catalogName}.${dbName}"
    [dmlTable, overwriteTable, partitionTable, partitionKeysTable, nestedTable, nestedArrayTable, nestedMapTable].each {
        String expectedTable ->
            assertTrue(tables.any { row -> row[0].toString().equalsIgnoreCase(expectedTable) },
                    "Expected table ${catalogName}.${dbName}.${expectedTable}")
    }

    qt_basic_vended_azure_read """
        SELECT COUNT(*) > 0
        FROM ${dmlTable}
    """

    def createTableRows = sql "SHOW CREATE TABLE ${dmlTable}"
    assertEquals(1, createTableRows.size())
    String createTableDdl = createTableRows[0][1].toString()
    assertTrue(createTableDdl.contains("ENGINE=iceberg"), createTableDdl)
    assertTrue(createTableDdl.contains("LOCATION 'abfss://"), createTableDdl)
    def createDatabaseRows = sql "SHOW CREATE DATABASE ${dbName}"
    assertEquals(1, createDatabaseRows.size())
    assertTrue(createDatabaseRows[0][1].toString().contains("CREATE DATABASE"),
            createDatabaseRows[0][1].toString())

    def snapshotRows = sql """
        SELECT snapshot_id,
               DATE_FORMAT(DATE_ADD(committed_at, INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s')
        FROM ${dmlTable}\$snapshots
        ORDER BY committed_at
    """
    assertTrue(snapshotRows.size() > 0, "Expected at least one Iceberg snapshot")
    String oldestSnapshotId = snapshotRows[0][0].toString()
    String oldestSnapshotTime = snapshotRows[0][1].toString()

    qt_version_time_travel """
        SELECT COUNT(*) > 0
        FROM ${dmlTable} FOR VERSION AS OF ${oldestSnapshotId}
    """
    qt_timestamp_time_travel """
        SELECT COUNT(*) > 0
        FROM ${dmlTable} FOR TIME AS OF '${oldestSnapshotTime}'
    """

    order_qt_system_tables """
        SELECT 'all_data_files', COUNT(*) > 0 FROM ${dmlTable}\$all_data_files
        UNION ALL SELECT 'all_delete_files', COUNT(*) > 0 FROM ${dmlTable}\$all_delete_files
        UNION ALL SELECT 'all_entries', COUNT(*) > 0 FROM ${dmlTable}\$all_entries
        UNION ALL SELECT 'all_files', COUNT(*) > 0 FROM ${dmlTable}\$all_files
        UNION ALL SELECT 'all_manifests', COUNT(*) > 0 FROM ${dmlTable}\$all_manifests
        UNION ALL SELECT 'data_files', COUNT(*) > 0 FROM ${dmlTable}\$data_files
        UNION ALL SELECT 'delete_files', COUNT(*) > 0 FROM ${dmlTable}\$delete_files
        UNION ALL SELECT 'entries', COUNT(*) > 0 FROM ${dmlTable}\$entries
        UNION ALL SELECT 'files', COUNT(*) > 0 FROM ${dmlTable}\$files
        UNION ALL SELECT 'history', COUNT(*) > 0 FROM ${dmlTable}\$history
        UNION ALL SELECT 'manifests', COUNT(*) > 0 FROM ${dmlTable}\$manifests
        UNION ALL SELECT 'metadata_log_entries', COUNT(*) > 0 FROM ${dmlTable}\$metadata_log_entries
        UNION ALL SELECT 'partitions', COUNT(*) > 0 FROM ${dmlTable}\$partitions
        UNION ALL SELECT 'refs', COUNT(*) > 0 FROM ${dmlTable}\$refs
        UNION ALL SELECT 'snapshots', COUNT(*) > 0 FROM ${dmlTable}\$snapshots
    """

    qt_position_deletes """
        SELECT COUNT(*) > 0,
               MIN(file_path LIKE 'abfss://%'),
               MIN(delete_file_path LIKE 'abfss://%')
        FROM ${dmlTable}\$position_deletes
    """

    qt_identity_partition_pruning """
        SELECT id, p, name
        FROM ${partitionTable}
        WHERE p = 2
        ORDER BY id
    """
    explain {
        sql """VERBOSE SELECT id, p, name FROM ${partitionTable} WHERE p = 2 ORDER BY id"""
        contains("inputSplitNum=1")
        contains("partition=1/3")
    }

    qt_runtime_filter_partition_query """
        SELECT t.id, t.p, t.name
        FROM ${partitionTable} t
        JOIN ${partitionKeysTable} k ON t.p = k.p
        ORDER BY t.id
    """

    qt_nested_struct_projection """
        SELECT id, info.metric, info.label
        FROM ${nestedTable}
        WHERE info.metric >= 10
        ORDER BY id
    """
    qt_nested_array_projection """
        SELECT id, events[1].score
        FROM ${nestedArrayTable}
        ORDER BY id
    """
    qt_nested_map_projection """
        SELECT id, attrs['k'].code
        FROM ${nestedMapTable}
        ORDER BY id
    """

    explain {
        sql """VERBOSE SELECT name, p FROM ${partitionTable} ORDER BY id LIMIT 3"""
        contains("VTOP-N")
        contains("isTopMaterializeNode: true")
    }
    qt_topn_lazy_materialization """
        SELECT name, p
        FROM ${partitionTable}
        ORDER BY id
        LIMIT 3
    """

    // Use a reserved ID range so the suite is repeatable against the retained Azure fixture.
    sql "DELETE FROM ${dmlTable} WHERE id BETWEEN 99101 AND 99199"
    sql """INSERT INTO ${dmlTable} (id, name, age) VALUES (99101, 'azure_values', 101)"""
    sql """
        INSERT INTO ${dmlTable} (id, name, age)
        SELECT 99102, 'azure_select', 102
    """
    sql """
        UPDATE ${dmlTable}
        SET name = 'azure_updated', age = 111
        WHERE id = 99101
    """
    sql """
        MERGE INTO ${dmlTable} t
        USING (
            SELECT 99102 AS id, 'azure_merged' AS name, 112 AS age
            UNION ALL
            SELECT 99103, 'azure_merge_insert', 113
        ) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age
        WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (s.id, s.name, s.age)
    """
    sql "DELETE FROM ${dmlTable} WHERE id = 99103"
    qt_row_level_dml """
        SELECT id, name, age
        FROM ${dmlTable}
        WHERE id BETWEEN 99101 AND 99199
        ORDER BY id
    """

    sql """
        INSERT OVERWRITE TABLE ${overwriteTable} (id, name, age) VALUES
            (1, 'alice_merge', 27),
            (2, 'bob', 30),
            (4, 'dora', 40)
    """
    qt_insert_overwrite """
        SELECT id, name, age
        FROM ${overwriteTable}
        ORDER BY id
    """

    long rowsBeforeManifestRewrite = ((Number) sql("SELECT COUNT(*) FROM ${dmlTable}")[0][0]).longValue()
    def rewriteResult = sql "ALTER TABLE ${catalogName}.${dbName}.${dmlTable} EXECUTE rewrite_manifests()"
    assertEquals(1, rewriteResult.size())
    assertEquals(2, rewriteResult[0].size())
    assertTrue(((Number) rewriteResult[0][0]).longValue() >= 0)
    assertTrue(((Number) rewriteResult[0][1]).longValue() >= 0)
    long rowsAfterManifestRewrite = ((Number) sql("SELECT COUNT(*) FROM ${dmlTable}")[0][0]).longValue()
    assertEquals(rowsBeforeManifestRewrite, rowsAfterManifestRewrite,
            "Manifest rewrite should preserve every logical row in ${dmlTable}")
}
