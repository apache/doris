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

suite("test_native_delta_path", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        return
    }

    def sourceTable = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toPath()
    // Negative write tests must not damage the shared sample if a write guard regresses.
    def copyCustomerTable = { String prefix ->
        def copy = java.nio.file.Files.createTempDirectory(prefix).resolve("customer")
        java.nio.file.Files.walk(sourceTable).withCloseable { paths ->
            paths.forEach { source ->
                def target = copy.resolve(sourceTable.relativize(source))
                if (java.nio.file.Files.isDirectory(source)) {
                    java.nio.file.Files.createDirectories(target)
                } else {
                    java.nio.file.Files.copy(source, target)
                }
            }
        }
        copy
    }
    def catalogName = "test_native_delta_path"
    def tablePath = copyCustomerTable("doris-native-delta-read-").toUri().toString()

    sql "DROP CATALOG IF EXISTS ${catalogName}"
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'customer',
            'delta.table.path' = '${tablePath}',
            'test_connection' = 'false'
        )
    """

    qt_schema "DESC ${catalogName}.`default`.customer"
    order_qt_count "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
    order_qt_filtered """
        SELECT c_custkey, c_name
        FROM ${catalogName}.`default`.customer
        WHERE c_custkey < 100
        ORDER BY c_custkey
        LIMIT 3
    """

    def createdTablePath = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-create-").resolve("created_events")
    def createCatalogName = "test_native_delta_path_create"
    sql "DROP CATALOG IF EXISTS ${createCatalogName}"
    sql """
        CREATE CATALOG ${createCatalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'created_events',
            'delta.table.path' = '${createdTablePath.toUri()}',
            'delta.write.enabled' = 'true',
            'test_connection' = 'false'
        )
    """
    sql """
        CREATE TABLE ${createCatalogName}.`default`.created_events (
            id BIGINT NOT NULL,
            payload STRING NULL
        )
    """
    sql "INSERT INTO ${createCatalogName}.`default`.created_events VALUES (1, 'created')"
    sql """
        INSERT OVERWRITE TABLE ${createCatalogName}.`default`.created_events
        SELECT 2, 'overwritten'
    """
    order_qt_created_after_overwrite """
        SELECT id, payload
        FROM ${createCatalogName}.`default`.created_events
        ORDER BY id
    """
    sql """
        INSERT INTO ${createCatalogName}.`default`.created_events VALUES
        (3, 'keep'), (4, NULL)
    """
    order_qt_deleted_matching """
        DELETE FROM ${createCatalogName}.`default`.created_events
        WHERE payload = 'overwritten'
    """
    order_qt_created_after_delete """
        SELECT id, payload
        FROM ${createCatalogName}.`default`.created_events
        ORDER BY id
    """
    order_qt_deleted_all "DELETE FROM ${createCatalogName}.`default`.created_events"
    order_qt_created_after_delete_all """
        SELECT COUNT(*)
        FROM ${createCatalogName}.`default`.created_events
    """
    sql """
        INSERT INTO ${createCatalogName}.`default`.created_events VALUES
        (10, 'alpha'), (11, 'beta'), (12, NULL)
    """
    order_qt_updated_matching """
        UPDATE ${createCatalogName}.`default`.created_events AS e
        SET e.id = e.id + 100, e.payload = concat(e.payload, '-u')
        WHERE e.id = 10
    """
    order_qt_updated_null """
        UPDATE ${createCatalogName}.`default`.created_events
        SET payload = 'null-updated'
        WHERE payload IS NULL
    """
    order_qt_updated_none """
        UPDATE ${createCatalogName}.`default`.created_events
        SET payload = 'missing'
        WHERE id = 99
    """
    order_qt_updated_all """
        UPDATE ${createCatalogName}.`default`.created_events
        SET id = id + 1000
    """
    order_qt_created_after_update """
        SELECT id, payload
        FROM ${createCatalogName}.`default`.created_events
        ORDER BY id
    """
    sql "DROP TABLE IF EXISTS test_native_delta_merge_source"
    sql """
        CREATE TABLE test_native_delta_merge_source (
            id BIGINT NOT NULL,
            payload STRING NULL,
            action STRING NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true'
        )
    """
    sql """
        INSERT INTO test_native_delta_merge_source VALUES
        (1011, 'beta-merged', 'U'),
        (1012, 'deleted', 'D'),
        (2000, 'inserted', 'I'),
        (9999, 'ignored', 'X')
    """
    order_qt_merged_rows """
        MERGE INTO ${createCatalogName}.`default`.created_events t
        USING test_native_delta_merge_source s
        ON t.id = s.id
        WHEN MATCHED AND s.action = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET payload = s.payload
        WHEN NOT MATCHED AND s.action = 'I' THEN INSERT (id, payload)
        VALUES (s.id, s.payload)
    """
    order_qt_created_after_merge """
        SELECT id, payload
        FROM ${createCatalogName}.`default`.created_events
        ORDER BY id
    """

    def writableTable = copyCustomerTable("doris-native-delta-write-")

    def writeCatalogName = "test_native_delta_path_write"
    sql "DROP CATALOG IF EXISTS ${writeCatalogName}"
    sql """
        CREATE CATALOG ${writeCatalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'customer',
            'delta.table.path' = '${writableTable.toUri()}',
            'delta.write.enabled' = 'true',
            'test_connection' = 'false'
        )
    """

    sql """
        INSERT INTO ${writeCatalogName}.`default`.customer VALUES
        (200001, 'Customer#000200001', 'Doris native Delta', 1,
         '10-100-100-1000', 12.34, 'BUILDING', 'native write')
    """
    order_qt_count_after_insert "SELECT COUNT(*) FROM ${writeCatalogName}.`default`.customer"
    order_qt_inserted_row """
        SELECT c_custkey, c_name, c_acctbal
        FROM ${writeCatalogName}.`default`.customer
        WHERE c_custkey = 200001
    """
    test {
        sql "DROP TABLE ${createCatalogName}.`default`.created_events"
        exception "Drop table is not supported for catalog"
    }
    order_qt_created_after_rejected_drop """
        SELECT COUNT(*)
        FROM ${createCatalogName}.`default`.created_events
    """

    test {
        sql "TRUNCATE TABLE ${catalogName}.`default`.customer"
        exception "Truncate table is not supported for catalog"
    }
    order_qt_read_only_after_rejected_truncate "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
    test {
        sql "TRUNCATE TABLE ${createCatalogName}.`default`.created_events PARTITION (p1)"
        exception "Plugin connector TRUNCATE TABLE supports full tables only"
    }
    order_qt_created_after_rejected_partition_truncate """
        SELECT id, payload FROM ${createCatalogName}.`default`.created_events ORDER BY id
    """

    sql "TRUNCATE TABLE ${createCatalogName}.`default`.created_events"
    order_qt_created_after_truncate "SELECT COUNT(*) FROM ${createCatalogName}.`default`.created_events"
    order_qt_created_history_after_truncate """
        SELECT id, payload FROM ${createCatalogName}.`default`.created_events
        FOR VERSION AS OF 1 ORDER BY id
    """
    sql "TRUNCATE TABLE ${createCatalogName}.`default`.created_events"
    order_qt_created_after_repeated_truncate "SELECT COUNT(*) FROM ${createCatalogName}.`default`.created_events"
    sql "INSERT INTO ${createCatalogName}.`default`.created_events VALUES (4000, 'after-truncate'), (4001, NULL)"
    order_qt_created_append_after_truncate """
        SELECT id, payload FROM ${createCatalogName}.`default`.created_events ORDER BY id
    """
}
