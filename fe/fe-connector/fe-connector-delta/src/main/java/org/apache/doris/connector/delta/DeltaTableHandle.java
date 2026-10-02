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

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.Objects;
import java.util.OptionalLong;

/** Serializable coordinates for a Delta table pinned to one snapshot version. */
public final class DeltaTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final String tablePath;
    private final long snapshotVersion;
    private final String catalogTableId;
    private final boolean catalogManaged;
    private final boolean externalTable;
    private final boolean externalWriteSupported;
    private final transient DeltaKernelSnapshot pinnedSnapshot;

    public DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion) {
        this(databaseName, tableName, tablePath, snapshotVersion, null, false, true);
    }

    public DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion, String catalogTableId,
            boolean catalogManaged) {
        this(databaseName, tableName, tablePath, snapshotVersion, catalogTableId,
                catalogManaged, !catalogManaged);
    }

    public DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion, String catalogTableId,
            boolean catalogManaged, boolean externalTable) {
        this(databaseName, tableName, tablePath, snapshotVersion, catalogTableId,
                catalogManaged, externalTable, false, null);
    }

    public DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion, String catalogTableId,
            boolean catalogManaged, boolean externalTable, boolean externalWriteSupported) {
        this(databaseName, tableName, tablePath, snapshotVersion, catalogTableId,
                catalogManaged, externalTable, externalWriteSupported, null);
    }

    private DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion, String catalogTableId,
            boolean catalogManaged, boolean externalTable,
            boolean externalWriteSupported,
            DeltaKernelSnapshot pinnedSnapshot) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath");
        if (snapshotVersion < 0) {
            throw new IllegalArgumentException("Delta snapshot version must be non-negative");
        }
        this.snapshotVersion = snapshotVersion;
        this.catalogTableId = catalogTableId;
        this.catalogManaged = catalogManaged;
        this.externalTable = externalTable;
        this.externalWriteSupported = externalWriteSupported;
        this.pinnedSnapshot = pinnedSnapshot;
        if (catalogManaged && catalogTableId == null) {
            throw new IllegalArgumentException(
                    "Catalog-managed Delta table handle requires a catalog table ID");
        }
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

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    @Override
    public OptionalLong getCopyOnWriteSnapshotVersion() {
        return OptionalLong.of(snapshotVersion);
    }

    public String getCatalogTableId() {
        return catalogTableId;
    }

    public boolean isCatalogManaged() {
        return catalogManaged;
    }

    public boolean isExternalTable() {
        return externalTable;
    }

    public boolean supportsExternalWrite() {
        return externalWriteSupported;
    }

    public DeltaKernelSnapshot getPinnedSnapshot() {
        return pinnedSnapshot;
    }

    public DeltaTableHandle withPinnedSnapshot(DeltaKernelSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.getVersion() != snapshotVersion) {
            throw new IllegalArgumentException(
                    "Pinned Delta snapshot version does not match table handle");
        }
        return new DeltaTableHandle(databaseName, tableName, tablePath, snapshotVersion,
                catalogTableId, catalogManaged, externalTable, externalWriteSupported, snapshot);
    }

    public DeltaTableHandle withSnapshotVersion(long version) {
        return new DeltaTableHandle(databaseName, tableName, tablePath, version,
                catalogTableId, catalogManaged, externalTable, externalWriteSupported);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DeltaTableHandle)) {
            return false;
        }
        DeltaTableHandle that = (DeltaTableHandle) other;
        return snapshotVersion == that.snapshotVersion
                && catalogManaged == that.catalogManaged
                && externalTable == that.externalTable
                && externalWriteSupported == that.externalWriteSupported
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && tablePath.equals(that.tablePath)
                && Objects.equals(catalogTableId, that.catalogTableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tablePath, snapshotVersion,
                catalogTableId, catalogManaged, externalTable, externalWriteSupported);
    }

    @Override
    public String toString() {
        return "DeltaTableHandle{" + databaseName + "." + tableName
                + ", path=" + tablePath + ", version=" + snapshotVersion
                + ", catalogManaged=" + catalogManaged + ", external=" + externalTable
                + ", externalWrite=" + externalWriteSupported + "}";
    }
}
