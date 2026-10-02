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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Catalog-specific control-plane operations used by the Delta connector. */
public interface DeltaCatalogAdapter {

    /** Lists databases visible through this adapter. */
    List<String> listDatabaseNames();

    /** Checks whether a database is visible through this adapter. */
    boolean databaseExists(String databaseName);

    /** Lists tables visible in a database. */
    List<String> listTableNames(String databaseName);

    /** Resolves a table name to a handle pinned to a Delta snapshot version. */
    Optional<DeltaTableHandle> getTableHandle(String databaseName, String tableName);

    /** Loads the snapshot pinned by a table handle. */
    DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle);

    /** Resolves a version or timestamp boundary to a schema-compatible pinned handle. */
    default DeltaTableHandle applyTableSnapshot(
            DeltaTableHandle tableHandle, DeltaTableSnapshot snapshot) {
        throw new UnsupportedOperationException(
                "This Delta catalog adapter does not support time travel");
    }

    /** Creates a Delta table and returns true when IF NOT EXISTS matched it. */
    default boolean createTable(ConnectorCreateTableRequest request) {
        throw new UnsupportedOperationException(
                "This Delta catalog adapter does not support CREATE TABLE");
    }

    default boolean supportsCreateTable() {
        return false;
    }

    /** Drops a catalog registration without directly deleting object-storage paths. */
    default void dropTable(DeltaTableHandle tableHandle) {
        throw new UnsupportedOperationException(
                "This Delta catalog adapter does not support DROP TABLE");
    }

    default boolean supportsDropTable() {
        return false;
    }

    /** Historical scans use the already-bound current schema, so schema evolution must fail closed. */
    static void requireCompatibleSchema(
            DeltaKernelSnapshot current, DeltaKernelSnapshot requested) {
        if (!current.getSchema().equals(requested.getSchema())
                || !current.getPartitionColumnNames().equals(
                requested.getPartitionColumnNames())) {
            throw new UnsupportedOperationException(
                    "Delta time travel across schema or partition evolution is not supported; "
                            + "current version=" + current.getVersion()
                            + ", requested version=" + requested.getVersion());
        }
    }

    /** Resolves one logical Delta scan file and its delete metadata for Doris BE. */
    default DeltaScanFile getBackendScanFile(DeltaTableHandle tableHandle, DeltaScanFile file) {
        return file;
    }

    /** Returns short-lived storage settings consumed by the backend for this table. */
    default Map<String, String> getBackendStorageProperties(DeltaTableHandle tableHandle) {
        return Map.of();
    }

    /** Returns storage settings with the scope required for a connector write. */
    default Map<String, String> getBackendStoragePropertiesForWrite(
            DeltaTableHandle tableHandle) {
        return getBackendStorageProperties(tableHandle);
    }

    /** Starts a catalog-specific append transaction when the adapter owns the credentials. */
    default DeltaInsertHandle beginInsert(DeltaTableHandle tableHandle) {
        throw new UnsupportedOperationException(
                "This Delta catalog adapter does not support INSERT");
    }

    /** Starts an append transaction with a query-scoped idempotency identifier. */
    default DeltaInsertHandle beginInsert(DeltaTableHandle tableHandle,
            String applicationId) {
        return beginInsert(tableHandle);
    }

    /** Starts a full-table overwrite transaction using catalog-specific commit semantics. */
    default DeltaInsertHandle beginOverwrite(DeltaTableHandle tableHandle,
            String applicationId) {
        throw new UnsupportedOperationException(
                "This Delta catalog adapter does not support INSERT OVERWRITE");
    }

    /** Whether this adapter can start an append transaction for at least one table. */
    default boolean supportsInsert() {
        return false;
    }

    default boolean supportsOverwrite() {
        return false;
    }

    /** Verifies the catalog control plane or configured path. */
    String testConnection();
}
