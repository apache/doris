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

package org.apache.doris.datasource.metacache.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonReaderOptions;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSchemaCacheValue;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves the latest snapshot runtime projection from the base table entry.
 */
public final class PaimonLatestSnapshotProjectionLoader {
    @FunctionalInterface
    public interface SchemaValueLoader {
        PaimonSchemaCacheValue load(NameMapping nameMapping, long schemaId);
    }

    private final PaimonPartitionInfoLoader partitionInfoLoader;
    private final SchemaValueLoader schemaValueLoader;

    public PaimonLatestSnapshotProjectionLoader(PaimonPartitionInfoLoader partitionInfoLoader,
            SchemaValueLoader schemaValueLoader) {
        this.partitionInfoLoader = partitionInfoLoader;
        this.schemaValueLoader = schemaValueLoader;
    }

    public PaimonSnapshotCacheValue load(NameMapping nameMapping, Table paimonTable) {
        try {
            PaimonSnapshot latestSnapshot = resolveLatestSnapshot(paimonTable, true);
            List<Column> partitionColumns = schemaValueLoader.load(nameMapping, latestSnapshot.getSchemaId())
                    .getPartitionColumns();
            PaimonPartitionInfo partitionInfo =
                    partitionInfoLoader.load(nameMapping, latestSnapshot.getTable(), partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo, latestSnapshot);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }

    public PaimonSnapshotCacheValue loadFence(NameMapping nameMapping, Table paimonTable) {
        try {
            // A statement fence needs version/schema identity only; enumerating partitions here
            // can fail before relation-level options have replaced an unsafe physical setting.
            return new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, resolveLatestSnapshot(paimonTable, false));
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot fence %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }

    public PaimonSnapshotCacheValue loadAtFence(NameMapping nameMapping, PaimonSnapshot fence) {
        return loadEffectiveAtFence(nameMapping, fence.getTable(), fence);
    }

    public PaimonSnapshotCacheValue loadEffectiveAtFence(
            NameMapping nameMapping, Table effectiveTable, PaimonSnapshot fence) {
        try {
            // The fence owns both version and table generation. Reopening the catalog here can pair
            // the old snapshot id with a newer schema or branch after invalidation.
            FileStoreTable latestSchemaTable = (FileStoreTable) effectiveTable;
            Table snapshotTable = latestSchemaTable;
            if (fence.getSnapshotId() != PaimonSnapshot.INVALID_SNAPSHOT_ID) {
                // Pin data at the statement fence without replaying time travel, which would
                // discard a schema-only ALTER that happened after the last data snapshot.
                snapshotTable = PaimonReaderOptions.runtimeSafeTable(
                        latestSchemaTable.copyWithoutTimeTravel(
                                PaimonScanParams.isolateSnapshotRead(fence.getSnapshotId())));
            }
            List<Column> partitionColumns = schemaValueLoader.load(nameMapping, fence.getSchemaId())
                    .getPartitionColumns();
            PaimonPartitionInfo partitionInfo =
                    partitionInfoLoader.load(nameMapping, snapshotTable, partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo,
                    new PaimonSnapshot(fence.getSnapshotId(), fence.getSchemaId(), snapshotTable));
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot at fence %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }

    private PaimonSnapshot resolveLatestSnapshot(Table paimonTable, boolean normalizeForPartitionLoad) {
        FileStoreTable latestSchemaTable = ((FileStoreTable) paimonTable).copyWithLatestSchema();
        Table snapshotTable = latestSchemaTable;
        long latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID;
        Optional<Snapshot> optionalSnapshot = latestSchemaTable.latestSnapshot();
        Map<String, String> projectionOptions = Collections.emptyMap();
        if (optionalSnapshot.isPresent()) {
            latestSnapshotId = optionalSnapshot.get().id();
            // Pin the data snapshot for MVCC while retaining the latest table schema. A normal
            // copy applies time travel and falls back to the snapshot's schema, which can be stale
            // immediately after a schema change that has not produced a new data snapshot.
            projectionOptions = PaimonScanParams.isolateSnapshotRead(latestSnapshotId);
        }
        if (!projectionOptions.isEmpty()) {
            snapshotTable = latestSchemaTable.copyWithoutTimeTravel(projectionOptions);
        }
        if (normalizeForPartitionLoad) {
            // Full projection enumerates partitions immediately, so normalize the complete
            // planning tree after pinning; the lightweight fence remains relation-neutral.
            snapshotTable = PaimonReaderOptions.runtimeSafeTable(snapshotTable);
        }
        DataTable dataTable = (DataTable) latestSchemaTable;
        long latestSchemaId = dataTable.schemaManager().latest().map(TableSchema::id).orElse(0L);
        return new PaimonSnapshot(latestSnapshotId, latestSchemaId, snapshotTable);
    }
}
