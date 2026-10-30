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

package org.apache.doris.nereids;

import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Metadata and partition dependencies of a partitioned OLAP SQL cache entry. */
public class OlapTableSqlCacheVersion {
    private final long tableVersion;
    private final long baseIndexId;
    private final long topologyVersion;
    private final Map<Long, Pair<Integer, Integer>> schemas;
    private final Map<Long, String> partitionNames = new TreeMap<>();
    // Include empty partitions: pruning can remove their scan nodes before they receive their first rows.
    private final Map<Long, Pair<Long, Long>> partitionVersions = new TreeMap<>();
    private boolean hasScan;
    private boolean usable = true;
    private volatile long validatedTableVersion;

    /** Capture table metadata while the planner holds the table read lock. */
    public OlapTableSqlCacheVersion(OlapTable table, long tableVersion) {
        this.tableVersion = tableVersion;
        this.validatedTableVersion = tableVersion;
        this.baseIndexId = table.getBaseIndexId();
        this.topologyVersion = table.getPartitionTopologyVersion();
        this.schemas = schemaVersions(table);
    }

    private static Map<Long, Pair<Integer, Integer>> schemaVersions(OlapTable table) {
        Map<Long, Pair<Integer, Integer>> versions = new TreeMap<>();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta meta = entry.getValue();
            versions.put(entry.getKey(), Pair.of(meta.getSchemaVersion(), meta.getSchemaHash()));
        }
        return versions;
    }

    private static Pair<Long, Long> version(Partition partition, long version) {
        // MS has no update timestamp for an empty partition and FE may synthesize one on every refresh.
        return Pair.of(version, version == Partition.PARTITION_INIT_VERSION ? 0L : partition.getVisibleVersionTime());
    }

    /** Track negative dependencies removed by PruneEmptyPartition, not unrelated empty partitions. */
    public void addEmptyPartitions(OlapTable table, Set<Long> partitionIds) {
        for (Long id : partitionIds) {
            Partition partition = table.getPartition(id);
            if (table.isTemporaryPartition(id)
                    || partition.getCachedVisibleVersion() != Partition.PARTITION_INIT_VERSION) {
                usable = false;
                return;
            }
            partitionNames.put(id, partition.getName());
            partitionVersions.put(id, version(partition, Partition.PARTITION_INIT_VERSION));
        }
    }

    private boolean metadataMatches(OlapTable table) {
        return table.getBaseIndexId() == baseIndexId
                && table.getPartitionTopologyVersion() == topologyVersion
                && schemas.equals(schemaVersions(table));
    }

    /** Record the versions actually assigned to the BE scan, before executing the query. */
    public void addScan(OlapTable table, Map<Long, Long> scanVersions) throws RpcException {
        if (!usable) {
            return;
        }
        long currentVersion = table.getVisibleVersion();
        // Native versions are protected by the statement's table read lock; the extra lock is cloud-only.
        if (Config.isCloudMode()) {
            table.versionReadLock();
        }
        try {
            // Do not attach a newer snapshot to a plan analyzed before a concurrent commit or DDL.
            if (currentVersion != tableVersion || !metadataMatches(table)) {
                usable = false;
                return;
            }
            for (Map.Entry<Long, Long> entry : scanVersions.entrySet()) {
                Partition partition = table.getPartition(entry.getKey());
                if (table.isTemporaryPartition(entry.getKey())
                        || partition.getCachedVisibleVersion() != entry.getValue()) {
                    usable = false;
                    return;
                }
                partitionNames.put(entry.getKey(), partition.getName());
                // Singleton cloud refreshes can advance the cache despite the table's version read lock.
                // Keep the version assigned to the scan, rather than reading the mutable cache again.
                Pair<Long, Long> scanned = version(partition, entry.getValue());
                Pair<Long, Long> previous = partitionVersions.putIfAbsent(entry.getKey(), scanned);
                if (previous != null && !previous.equals(scanned)) {
                    usable = false;
                    return;
                }
                hasScan = true;
            }
        } finally {
            if (Config.isCloudMode()) {
                table.versionReadUnlock();
            }
        }
    }

    /** Keep table-wide invalidation when some dependencies have no ordinary scan ranges. */
    public void useTableVersion() {
        usable = false;
    }

    public boolean isUsable() {
        return usable && hasScan;
    }

    /** Check metadata first, and refresh partition dependencies when the table's data version changes. */
    public boolean matches(OlapTable table, long currentVersion) throws RpcException {
        if (!metadataMatches(table)) {
            return false;
        }
        if (currentVersion == validatedTableVersion) {
            return true;
        }
        List<Partition> partitions = new ArrayList<>(partitionVersions.size());
        for (Long id : partitionVersions.keySet()) {
            partitions.add(table.getPartition(id));
        }
        List<Long> versions = new ArrayList<>(partitions.size());
        if (Config.isCloudMode()) {
            List<CloudPartition> cloudPartitions = new ArrayList<>(partitions.size());
            for (Partition partition : partitions) {
                cloudPartitions.add((CloudPartition) partition);
            }
            // A fresh table version must not be validated against stale FE partition-version caches.
            versions = CloudPartition.getSnapshotVisibleVersionFromMs(cloudPartitions);
        } else {
            for (Partition partition : partitions) {
                versions.add(partition.getVisibleVersion());
            }
        }
        // Native versions are protected by the statement's table read lock; the extra lock is cloud-only.
        if (Config.isCloudMode()) {
            table.versionReadLock();
        }
        try {
            for (int i = 0; i < partitions.size(); i++) {
                Pair<Long, Long> cached = partitionVersions.get(partitions.get(i).getId());
                if (!cached.first.equals(versions.get(i))
                        || (cached.first != Partition.PARTITION_INIT_VERSION
                                && cached.second != partitions.get(i).getVisibleVersionTime())) {
                    return false;
                }
            }
            validatedTableVersion = currentVersion;
            return true;
        } finally {
            if (Config.isCloudMode()) {
                table.versionReadUnlock();
            }
        }
    }

    /** A stable BE key for identical metadata and partition dependencies, independent of other writes. */
    public String getCacheKey() {
        return "partitions-v1:" + baseIndexId + ":" + schemas + ":" + partitionNames + ":" + partitionVersions;
    }
}
