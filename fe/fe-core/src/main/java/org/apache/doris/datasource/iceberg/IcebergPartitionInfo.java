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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IcebergPartitionInfo {
    // Each RangePartitionItem endpoint holds one LiteralExpr per partition column beyond the
    // first (createPartitionKey fills the vacancy with an infinity literal): literal, its lazy
    // supplier, children list and array. Calibrated against JOL in IcebergExternalMetaCacheTest.
    private static final long RANGE_KEY_EXTRA_COLUMN_BYTES =
            MetaCacheWeightUtils.estimatedObjectBytes(208L);
    private static final long RANGE_ENDPOINTS_PER_ITEM = 2L;
    // A merged-overlap alias group is a HashSet of the enclosed physical partition names; the
    // names themselves are shared with the partition maps.
    private static final long HASH_SET_BYTES = MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);

    private final Map<String, PartitionItem> nameToPartitionItem;
    private final Map<String, IcebergPartition> nameToIcebergPartition;
    private final Map<String, Set<String>> nameToIcebergPartitionNames;
    private final long retainedPayloadBytes;

    private static final IcebergPartitionInfo EMPTY = new IcebergPartitionInfo();

    private IcebergPartitionInfo() {
        this.nameToPartitionItem = Collections.emptyMap();
        this.nameToIcebergPartition = Collections.emptyMap();
        this.nameToIcebergPartitionNames = Collections.emptyMap();
        this.retainedPayloadBytes = 0L;
    }

    public IcebergPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
                                Map<String, IcebergPartition> nameToIcebergPartition,
                                Map<String, Set<String>> nameToIcebergPartitionNames) {
        this(nameToPartitionItem, nameToIcebergPartition, nameToIcebergPartitionNames,
                MetaCacheWeightUtils.saturatedAdd(
                        retainedPayloadBytes(nameToIcebergPartition),
                        partitionAliasBytes(nameToIcebergPartitionNames)));
    }

    public IcebergPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
                                Map<String, IcebergPartition> nameToIcebergPartition,
                                Map<String, Set<String>> nameToIcebergPartitionNames,
                                long retainedPayloadBytes) {
        this.nameToPartitionItem = nameToPartitionItem;
        this.nameToIcebergPartition = nameToIcebergPartition;
        this.nameToIcebergPartitionNames = nameToIcebergPartitionNames;
        this.retainedPayloadBytes = retainedPayloadBytes;
    }

    static IcebergPartitionInfo empty() {
        return EMPTY;
    }

    public Map<String, PartitionItem> getNameToPartitionItem() {
        return nameToPartitionItem;
    }

    public Map<String, IcebergPartition> getNameToIcebergPartition() {
        return nameToIcebergPartition;
    }

    Map<String, Set<String>> getNameToIcebergPartitionNames() {
        return nameToIcebergPartitionNames;
    }

    public long getRetainedPayloadBytes() {
        return retainedPayloadBytes;
    }

    private static long retainedPayloadBytes(Map<String, IcebergPartition> partitions) {
        if (partitions == null) {
            return 0L;
        }
        long bytes = 0L;
        for (IcebergPartition partition : partitions.values()) {
            if (partition != null) {
                bytes = MetaCacheWeightUtils.saturatedAdd(
                        bytes, partition.getRetainedPayloadBytes());
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes, partitionItemColumnBytes(
                        partition.getPartitionValues() == null
                                ? 0 : partition.getPartitionValues().size()));
            }
        }
        return bytes;
    }

    /**
     * Retained bytes of the merged-overlap alias sets: every group keeps one HashSet with one
     * node per enclosed physical partition name (the estimator's per-group constant covers only
     * the outer map entry and the empty set object).
     */
    static long partitionAliasBytes(Map<String, Set<String>> nameToIcebergPartitionNames) {
        if (nameToIcebergPartitionNames == null) {
            return 0L;
        }
        long bytes = 0L;
        for (Set<String> aliases : nameToIcebergPartitionNames.values()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, HASH_SET_BYTES);
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                    MetaCacheWeightUtils.estimatedHashMapBytes(aliases == null ? 0L : aliases.size()));
        }
        return bytes;
    }

    /**
     * Structural bytes a partition item retains for every partition column beyond the first;
     * the fixed per-partition constants of the estimator cover a single column. The width is
     * taken from the loaded metadata generation, so a spec that grew after the related-table
     * check was cached is still charged for its full width.
     */
    static long partitionItemColumnBytes(long partitionColumnCount) {
        if (partitionColumnCount <= 1L) {
            return 0L;
        }
        return MetaCacheWeightUtils.saturatedMultiply(
                MetaCacheWeightUtils.saturatedMultiply(
                        partitionColumnCount - 1L, RANGE_ENDPOINTS_PER_ITEM),
                RANGE_KEY_EXTRA_COLUMN_BYTES);
    }

    public long getLatestSnapshotId(String partitionName) {
        Set<String> icebergPartitionNames = nameToIcebergPartitionNames.get(partitionName);
        if (icebergPartitionNames == null) {
            return nameToIcebergPartition.get(partitionName).getLastSnapshotId();
        }
        long latestSnapshotId = -1;
        long latestUpdateTime = -1;
        for (String name : icebergPartitionNames) {
            IcebergPartition partition = nameToIcebergPartition.get(name);
            long lastUpdateTime = partition.getLastUpdateTime();
            // Skip partitions with invalid update time (<= 0 means unknown/invalid)
            if (lastUpdateTime <= 0) {
                continue;
            }
            if (latestUpdateTime < lastUpdateTime) {
                latestUpdateTime = lastUpdateTime;
                latestSnapshotId = partition.getLastSnapshotId();
            }
        }
        return latestSnapshotId;
    }
}
