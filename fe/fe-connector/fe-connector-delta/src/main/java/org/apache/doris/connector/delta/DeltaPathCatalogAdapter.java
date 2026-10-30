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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;

import io.delta.kernel.exceptions.TableNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Path-only catalog adapter for a single configured Delta table.
 *
 * <p>This adapter deliberately has no table discovery service.  It exposes the
 * configured database/table pair and uses the Delta log at the configured path
 * as the source of snapshot metadata.  A Unity Catalog adapter can implement
 * the same {@link DeltaCatalogAdapter} contract later.</p>
 */
public class DeltaPathCatalogAdapter implements DeltaCatalogAdapter {

    private final String databaseName;
    private final String tableName;
    private final String tablePath;
    private final DeltaKernelSnapshotLoader snapshotLoader;
    private final DeltaKernelWriter writer;

    public DeltaPathCatalogAdapter(String databaseName, String tableName,
            String tablePath, DeltaKernelSnapshotLoader snapshotLoader) {
        this(databaseName, tableName, tablePath, snapshotLoader, null);
    }

    public DeltaPathCatalogAdapter(String databaseName, String tableName,
            String tablePath, DeltaKernelSnapshotLoader snapshotLoader,
            DeltaKernelWriter writer) {
        this.databaseName = requireNonBlank(databaseName, "databaseName");
        this.tableName = requireNonBlank(tableName, "tableName");
        this.tablePath = requireNonBlank(tablePath, "tablePath");
        this.snapshotLoader = snapshotLoader;
        this.writer = writer;
    }

    @Override
    public List<String> listDatabaseNames() {
        return Collections.singletonList(databaseName);
    }

    @Override
    public boolean databaseExists(String requestedDatabaseName) {
        return databaseName.equals(requestedDatabaseName);
    }

    @Override
    public List<String> listTableNames(String requestedDatabaseName) {
        if (!databaseExists(requestedDatabaseName)) {
            return Collections.emptyList();
        }
        return getTableHandle(databaseName, tableName).isPresent()
                ? Collections.singletonList(tableName) : Collections.emptyList();
    }

    @Override
    public Optional<DeltaTableHandle> getTableHandle(
            String requestedDatabaseName, String requestedTableName) {
        if (!databaseName.equals(requestedDatabaseName)
                || !tableName.equals(requestedTableName)) {
            return Optional.empty();
        }
        try {
            DeltaKernelSnapshot snapshot = snapshotLoader.loadLatest(tablePath);
            return Optional.of(new DeltaTableHandle(databaseName, tableName, tablePath,
                    snapshot.getVersion(), null, false, true, writer != null)
                    .withPinnedSnapshot(snapshot));
        } catch (TableNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load latest Delta snapshot at '" + tablePath + "'", e);
        }
    }

    @Override
    public DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle) {
        validateHandle(tableHandle);
        if (tableHandle.getPinnedSnapshot() != null) {
            return tableHandle.getPinnedSnapshot();
        }
        try {
            return snapshotLoader.loadVersion(tablePath, tableHandle.getSnapshotVersion());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load Delta snapshot version " + tableHandle.getSnapshotVersion()
                            + " at '" + tablePath + "'", e);
        }
    }

    @Override
    public DeltaTableHandle applyTableSnapshot(
            DeltaTableHandle tableHandle, DeltaTableSnapshot snapshot) {
        validateHandle(tableHandle);
        DeltaKernelSnapshot requested;
        try {
            requested = snapshot.getType() == DeltaTableSnapshot.Type.VERSION
                    ? snapshotLoader.loadVersion(tablePath, snapshot.getValue())
                    : snapshotLoader.loadTimestamp(tablePath, snapshot.getValue());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load requested Delta snapshot at '" + tablePath + "'", e);
        }
        DeltaCatalogAdapter.requireCompatibleSchema(loadSnapshot(tableHandle), requested);
        return tableHandle.withSnapshotVersion(requested.getVersion())
                .withPinnedSnapshot(requested);
    }

    @Override
    public boolean createTable(ConnectorCreateTableRequest request) {
        if (!databaseName.equals(request.getDbName())
                || !tableName.equals(request.getTableName())) {
            throw new IllegalArgumentException(
                    "CREATE TABLE coordinates do not match the configured Delta path table");
        }
        Optional<DeltaTableHandle> existing = getTableHandle(databaseName, tableName);
        if (existing.isPresent()) {
            if (request.isIfNotExists()) {
                return true;
            }
            throw new DorisConnectorException(
                    "Delta table already exists: " + databaseName + "." + tableName);
        }
        writer.createTable(tablePath,
                DeltaTypeMapping.toDeltaSchema(request.getColumns()),
                request.getProperties(), DeltaCreateTableValidator.partitionColumns(request));
        return false;
    }

    @Override
    public boolean supportsCreateTable() {
        return writer != null;
    }

    /** Loads the current latest snapshot for connectivity checks and handle creation. */
    public DeltaKernelSnapshot loadLatestSnapshot() {
        try {
            return snapshotLoader.loadLatest(tablePath);
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load latest Delta snapshot at '" + tablePath + "'", e);
        }
    }

    private void validateHandle(DeltaTableHandle tableHandle) {
        if (!databaseName.equals(tableHandle.getDatabaseName())
                || !tableName.equals(tableHandle.getTableName())
                || !tablePath.equals(tableHandle.getTablePath())) {
            throw new IllegalArgumentException(
                    "Table handle does not belong to this Delta path adapter");
        }
    }

    @Override
    public String testConnection() {
        Optional<DeltaTableHandle> handle = getTableHandle(databaseName, tableName);
        if (handle.isEmpty()) {
            if (writer == null) {
                throw new DorisConnectorException(
                        "Delta table does not exist at '" + tablePath + "'");
            }
            return "Delta path is ready for CREATE TABLE";
        }
        DeltaKernelSnapshot snapshot = loadSnapshot(handle.get());
        return "Delta snapshot version " + snapshot.getVersion() + " is readable";
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
