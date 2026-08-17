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
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;

import java.util.Objects;
import java.util.Optional;

public class IcebergTableCacheValue {
    private volatile Table icebergTable;
    private String retainedCurrentSnapshotJson;
    private volatile boolean queryIsolationPrepared;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;

    public IcebergTableCacheValue(Table icebergTable) {
        this.icebergTable = IcebergSnapshotCacheValue.retainTableGeneration(icebergTable);
    }

    public Table getIcebergTable() {
        Table retainedTable = icebergTable;
        return queryIsolationPrepared || IcebergSnapshotCacheValue.isNonGrowingGeneration(retainedTable)
                ? IcebergSnapshotCacheValue.createQueryScopedTable(
                        retainedTable, retainedCurrentSnapshotJson)
                : retainedTable;
    }

    public Table getWritableIcebergTable(Table liveTable) {
        Table retainedTable = icebergTable;
        return IcebergSnapshotCacheValue.createWritableTable(retainedTable, liveTable);
    }

    synchronized MetaCacheSizeEstimate prepareForCachePublication(NameMapping key) {
        if (sizeEstimate == null) {
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("iceberg_table_preparation_failed",
                    () -> {
                        // Order matters: serializing a v1 snapshot materializes its transient
                        // manifest list, which the payload accounting rejects, so account first.
                        retainedTablePayloadBytes =
                                IcebergCacheSizeEstimator.retainedTablePayloadBytes(icebergTable);
                        retainedCurrentSnapshotJson =
                                IcebergSnapshotCacheValue.retainCurrentSnapshotJson(icebergTable);
                        return IcebergCacheSizeEstimator.estimateTableEntry(key, this);
                    });
            if (sizeEstimate.isComplete()) {
                icebergTable = IcebergSnapshotCacheValue.retainNonGrowingGeneration(icebergTable);
                queryIsolationPrepared = true;
            }
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }

    Table getRetainedIcebergTable() {
        return icebergTable;
    }

    synchronized Table newQueryScopedTable() {
        if (!queryIsolationPrepared) {
            // A failed optional size preparation must only reject weighted cache admission. Do not
            // repeat the same unsupported metadata access on the query path and turn it into a
            // table-load failure; this value is not retained by the weighted cache in that case.
            if (sizeEstimate != null && !sizeEstimate.isComplete()) {
                return icebergTable;
            }
            retainedCurrentSnapshotJson =
                    IcebergSnapshotCacheValue.retainCurrentSnapshotJson(icebergTable);
            icebergTable = IcebergSnapshotCacheValue.retainNonGrowingGeneration(icebergTable);
            queryIsolationPrepared = true;
        }
        return IcebergSnapshotCacheValue.createQueryScopedTable(
                icebergTable, retainedCurrentSnapshotJson);
    }

    String getRetainedCurrentSnapshotJson() {
        return retainedCurrentSnapshotJson;
    }

    boolean isQueryIsolationPrepared() {
        return queryIsolationPrepared;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    long getRetainedCurrentSnapshotPayloadBytes() {
        return IcebergSnapshotCacheValue.retainedSnapshotJsonBytes(
                retainedCurrentSnapshotJson);
    }

    Optional<String> getTableUuid() {
        TableMetadata metadata = retainedMetadata();
        return metadata == null || metadata.uuid() == null || metadata.uuid().isEmpty()
                ? Optional.empty() : Optional.of(metadata.uuid());
    }

    boolean isSamePhysicalGeneration(IcebergTableCacheValue other) {
        if (other == null) {
            return false;
        }
        TableMetadata left = retainedMetadata();
        TableMetadata right = other.retainedMetadata();
        return left != null && right != null
                && Objects.equals(left.uuid(), right.uuid())
                && Objects.equals(left.metadataFileLocation(), right.metadataFileLocation());
    }

    private TableMetadata retainedMetadata() {
        Table retainedTable = icebergTable;
        return retainedTable instanceof HasTableOperations
                ? ((HasTableOperations) retainedTable).operations().current() : null;
    }
}
