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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.NameMapping;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;

import java.util.Objects;
import java.util.Optional;

/** Stable identity for an Iceberg snapshot projection built from one frozen metadata generation. */
public final class IcebergSnapshotEntryKey {
    private final NameMapping nameMapping;
    private final String tableUuid;
    private final String metadataFileLocation;
    private final long snapshotId;
    private final int schemaId;
    private final int defaultSpecId;

    private IcebergSnapshotEntryKey(NameMapping nameMapping, String tableUuid, String metadataFileLocation,
            long snapshotId, int schemaId, int defaultSpecId) {
        this.nameMapping = Objects.requireNonNull(nameMapping, "nameMapping can not be null");
        this.tableUuid = Objects.requireNonNull(tableUuid, "tableUuid can not be null");
        this.metadataFileLocation = Objects.requireNonNull(
                metadataFileLocation, "metadataFileLocation can not be null");
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.defaultSpecId = defaultSpecId;
    }

    /**
     * Build a key from the same retained table generation that will be used by the value loader.
     * Tables without a stable metadata location intentionally bypass the snapshot cache.
     */
    public static Optional<IcebergSnapshotEntryKey> tryCreate(NameMapping nameMapping, Table retainedTable) {
        if (!(retainedTable instanceof HasTableOperations)) {
            return Optional.empty();
        }
        TableMetadata metadata = ((HasTableOperations) retainedTable).operations().current();
        if (metadata == null || metadata.uuid() == null || metadata.uuid().isEmpty()
                || metadata.metadataFileLocation() == null
                || metadata.metadataFileLocation().isEmpty()) {
            return Optional.empty();
        }
        Snapshot snapshot = metadata.currentSnapshot();
        long snapshotId = snapshot == null ? IcebergUtils.UNKNOWN_SNAPSHOT_ID : snapshot.snapshotId();
        return Optional.of(new IcebergSnapshotEntryKey(nameMapping, metadata.uuid(), metadata.metadataFileLocation(),
                snapshotId, metadata.currentSchemaId(), metadata.defaultSpecId()));
    }

    public NameMapping getNameMapping() {
        return nameMapping;
    }

    public String getMetadataFileLocation() {
        return metadataFileLocation;
    }

    public String getTableUuid() {
        return tableUuid;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getDefaultSpecId() {
        return defaultSpecId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IcebergSnapshotEntryKey)) {
            return false;
        }
        IcebergSnapshotEntryKey that = (IcebergSnapshotEntryKey) object;
        return snapshotId == that.snapshotId
                && schemaId == that.schemaId
                && defaultSpecId == that.defaultSpecId
                && nameMapping.equals(that.nameMapping)
                && tableUuid.equals(that.tableUuid)
                && metadataFileLocation.equals(that.metadataFileLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameMapping, tableUuid, metadataFileLocation, snapshotId, schemaId, defaultSpecId);
    }
}
