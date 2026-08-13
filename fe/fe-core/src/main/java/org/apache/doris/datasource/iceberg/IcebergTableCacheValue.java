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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

import org.apache.iceberg.Table;

public class IcebergTableCacheValue {
    private Table icebergTable;
    private String retainedCurrentSnapshotJson;
    private volatile boolean queryIsolationPrepared;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;

    public IcebergTableCacheValue(Table icebergTable) {
        this.icebergTable = IcebergSnapshotCacheValue.retainTableGeneration(icebergTable);
    }

    IcebergTableCacheValue(Table icebergTable, ExecutionAuthenticator authenticator) {
        this.icebergTable = IcebergSnapshotCacheValue.retainTableGeneration(
                icebergTable, authenticator);
    }

    public Table getIcebergTable() {
        return queryIsolationPrepared
                ? IcebergSnapshotCacheValue.createQueryScopedTable(
                        icebergTable, retainedCurrentSnapshotJson)
                : icebergTable;
    }

    public Table getWritableIcebergTable(Table liveTable) {
        return IcebergSnapshotCacheValue.createWritableTable(
                icebergTable, liveTable, queryIsolationPrepared);
    }

    MetaCacheSizeEstimate prepareForCachePublication(NameMapping key) {
        if (sizeEstimate == null) {
            retainedCurrentSnapshotJson =
                    IcebergSnapshotCacheValue.retainCurrentSnapshotJson(icebergTable);
            retainedTablePayloadBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(icebergTable);
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("iceberg_table_preparation_failed",
                    () -> IcebergCacheSizeEstimator.estimateTableEntry(key, this));
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
}
