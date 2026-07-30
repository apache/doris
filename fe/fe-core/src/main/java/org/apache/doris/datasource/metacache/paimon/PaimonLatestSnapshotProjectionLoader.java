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
            PaimonSnapshot latestSnapshot = resolveLatestSnapshot(paimonTable);
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

    private PaimonSnapshot resolveLatestSnapshot(Table paimonTable) {
        FileStoreTable latestSchemaTable = ((FileStoreTable) paimonTable).copyWithLatestSchema();
        Table snapshotTable = latestSchemaTable;
        long latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID;
        Optional<Snapshot> optionalSnapshot = latestSchemaTable.latestSnapshot();
        if (optionalSnapshot.isPresent()) {
            latestSnapshotId = optionalSnapshot.get().id();
            // Pin the data snapshot for MVCC while retaining the latest table schema. A normal
            // copy applies time travel and falls back to the snapshot's schema, which can be stale
            // immediately after a schema change that has not produced a new data snapshot.
            Map<String, String> projectionOptions = PaimonReaderOptions.runtimeSafeCopyOptions(
                    latestSchemaTable, PaimonScanParams.isolateSnapshotRead(latestSnapshotId));
            snapshotTable = latestSchemaTable.copyWithoutTimeTravel(projectionOptions);
        } else {
            Map<String, String> runtimeOptions = PaimonReaderOptions.runtimeSafeCopyOptions(
                    latestSchemaTable, Collections.emptyMap());
            if (!runtimeOptions.isEmpty()) {
                // The shared cache stays relation-neutral; only its planning projection receives
                // CPU-local caps so persisted catalog semantics remain replay-stable.
                snapshotTable = latestSchemaTable.copyWithoutTimeTravel(runtimeOptions);
            }
        }
        DataTable dataTable = (DataTable) latestSchemaTable;
        long latestSchemaId = dataTable.schemaManager().latest().map(TableSchema::id).orElse(0L);
        return new PaimonSnapshot(latestSnapshotId, latestSchemaId, snapshotTable);
    }
}
