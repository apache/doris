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

import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

public class PaimonSnapshotCacheValue {

    private final PaimonPartitionInfo partitionInfo;
    private final PaimonSnapshot snapshot;
    private final boolean schemaFromSnapshotTable;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;

    public PaimonSnapshotCacheValue(PaimonPartitionInfo partitionInfo, PaimonSnapshot snapshot) {
        this(partitionInfo, snapshot, false);
    }

    public PaimonSnapshotCacheValue(PaimonPartitionInfo partitionInfo, PaimonSnapshot snapshot,
            boolean schemaFromSnapshotTable) {
        this.partitionInfo = partitionInfo;
        this.snapshot = snapshot;
        this.schemaFromSnapshotTable = schemaFromSnapshotTable;
    }

    public PaimonPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public PaimonSnapshot getSnapshot() {
        return snapshot;
    }

    public boolean isSchemaFromSnapshotTable() {
        return schemaFromSnapshotTable;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    MetaCacheSizeEstimate prepareForCachePublication(PaimonSnapshotEntryKey key) {
        if (sizeEstimate == null) {
            retainedTablePayloadBytes = PaimonCacheSizeEstimator.retainedTablePayloadBytes(snapshot.getTable());
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("paimon_snapshot_preparation_failed",
                    () -> PaimonCacheSizeEstimator.estimateSnapshotEntry(key, this));
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }
}
