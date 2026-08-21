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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

import javax.annotation.Nullable;

public class PaimonSnapshotCacheValue {

    private final PaimonPartitionInfo partitionInfo;
    private final PaimonSnapshot snapshot;
    private final boolean schemaFromSnapshotTable;
    private final long tableGeneration;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;
    /**
     * Execution context captured from the table generation this value retains. Later projection
     * or schema hydration of the retained physical table must run under this context, never under
     * the catalog's current one: a concurrent property/credential ALTER may already have replaced
     * the catalog resources while this statement still operates the retained generation.
     * Not part of the accounted payload; it references catalog-generation-lifetime resources.
     */
    @Nullable
    private transient volatile ExecutionAuthenticator capturedAuthenticator;

    public PaimonSnapshotCacheValue(PaimonPartitionInfo partitionInfo, PaimonSnapshot snapshot) {
        this(partitionInfo, snapshot, false, 0L);
    }

    public PaimonSnapshotCacheValue(PaimonPartitionInfo partitionInfo, PaimonSnapshot snapshot,
            boolean schemaFromSnapshotTable) {
        this(partitionInfo, snapshot, schemaFromSnapshotTable, 0L);
    }

    public PaimonSnapshotCacheValue(PaimonPartitionInfo partitionInfo, PaimonSnapshot snapshot,
            boolean schemaFromSnapshotTable, long tableGeneration) {
        this.partitionInfo = partitionInfo;
        this.snapshot = snapshot;
        this.schemaFromSnapshotTable = schemaFromSnapshotTable;
        this.tableGeneration = tableGeneration;
    }

    public PaimonPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public PaimonSnapshot getSnapshot() {
        return snapshot;
    }

    public PaimonSnapshotCacheValue bindCapturedAuthenticator(@Nullable ExecutionAuthenticator authenticator) {
        this.capturedAuthenticator = authenticator;
        return this;
    }

    @Nullable
    public ExecutionAuthenticator getCapturedAuthenticator() {
        return capturedAuthenticator;
    }

    public boolean isSchemaFromSnapshotTable() {
        return schemaFromSnapshotTable;
    }

    public long getTableGeneration() {
        return tableGeneration;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    MetaCacheSizeEstimate prepareForCachePublication(PaimonSnapshotEntryKey key) {
        if (sizeEstimate == null) {
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("paimon_snapshot_preparation_failed",
                    () -> {
                        retainedTablePayloadBytes =
                                PaimonCacheSizeEstimator.retainedTablePayloadBytes(snapshot.getTable());
                        return PaimonCacheSizeEstimator.estimateSnapshotEntry(key, this);
                    });
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }
}
