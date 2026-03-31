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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.foundation.util.ConcurrentLong2LongHashMap;
import org.apache.doris.foundation.util.ConcurrentLong2ObjectHashMap;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CloudTabletInvertedIndex extends TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(CloudTabletInvertedIndex.class);

    // tablet id -> replica
    // for cloud mode, no need to know the replica's backend
    private Long2ObjectOpenHashMap<Replica> replicaMetaMap = new Long2ObjectOpenHashMap<>();

    // Centralized cluster-to-BE mappings (moved from per-CloudReplica ConcurrentHashMaps).
    // Outer key: clusterId (typically 1-3 clusters). Inner key: replicaId. Value: beId or timestamp.
    private final ConcurrentHashMap<String, ConcurrentLong2LongHashMap> clusterPrimaryBeMap
            = new ConcurrentHashMap<>();
    // Stores [beId, timestamp] atomically per replicaId to avoid TOCTOU races between separate maps.
    private final ConcurrentHashMap<String, ConcurrentLong2ObjectHashMap<long[]>> clusterSecondaryMap
            = new ConcurrentHashMap<>();

    public CloudTabletInvertedIndex() {
        super();
    }

    @Override
    public List<Replica> getReplicas(Long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaMap.containsKey(tabletId)) {
                return Collections.singletonList(replicaMetaMap.get(tabletId));
            }
            return Collections.emptyList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public void deleteTablet(long tabletId) {
        long stamp = writeLock();
        try {
            Replica replica = replicaMetaMap.remove(tabletId);
            if (replica != null) {
                long replicaId = replica.getId();
                clearPrimaryBeForReplica(replicaId);
                clearSecondaryBeForReplica(replicaId);
            }
            tabletMetaMap.remove(tabletId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void addReplica(long tabletId, Replica replica) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, replica " + replica.getId());
            replicaMetaMap.put(tabletId, replica);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add replica {} of tablet {}", replica.getId(), tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void deleteReplica(long tabletId, long backendId) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), "tablet " + tabletId + " not exists");
            if (replicaMetaMap.containsKey(tabletId)) {
                Replica replica = replicaMetaMap.remove(tabletId);
                long replicaId = replica.getId();
                clearPrimaryBeForReplica(replicaId);
                clearSecondaryBeForReplica(replicaId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete replica {} of tablet {}", replicaId, tabletId);
                }
            } else {
                // this may happen when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public Replica getReplica(long tabletId, long backendId) {
        long stamp = readLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), "tablet " + tabletId + " not exists");
            return replicaMetaMap.get(tabletId);
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public List<Replica> getReplicasByTabletId(long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaMap.containsKey(tabletId)) {
                return Collections.singletonList(replicaMetaMap.get(tabletId));
            }
            return Collections.emptyList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    protected void innerClear() {
        replicaMetaMap.clear();
        clusterPrimaryBeMap.clear();
        clusterSecondaryMap.clear();
    }

    // ---- Central cluster-to-BE mapping accessors ----

    private ConcurrentLong2LongHashMap getOrCreateClusterMap(
            ConcurrentHashMap<String, ConcurrentLong2LongHashMap> outer, String clusterId) {
        return outer.computeIfAbsent(clusterId, k -> new ConcurrentLong2LongHashMap());
    }

    // -- Primary BE --

    public long getPrimaryBeId(String clusterId, long replicaId) {
        ConcurrentLong2LongHashMap inner = clusterPrimaryBeMap.get(clusterId);
        if (inner == null) {
            return -1L;
        }
        return inner.getOrDefault(replicaId, -1L);
    }

    public void setPrimaryBeId(String clusterId, long replicaId, long beId) {
        getOrCreateClusterMap(clusterPrimaryBeMap, clusterId).put(replicaId, beId);
    }

    public void removePrimaryBeId(String clusterId, long replicaId) {
        ConcurrentLong2LongHashMap inner = clusterPrimaryBeMap.get(clusterId);
        if (inner != null) {
            inner.remove(replicaId);
        }
    }

    public void clearPrimaryBeForReplica(long replicaId) {
        for (ConcurrentLong2LongHashMap inner : clusterPrimaryBeMap.values()) {
            inner.remove(replicaId);
        }
    }

    public Map<String, Long> getAllPrimaryClusterBeIds(long replicaId) {
        Map<String, Long> result = new HashMap<>();
        for (Map.Entry<String, ConcurrentLong2LongHashMap> entry : clusterPrimaryBeMap.entrySet()) {
            long beId = entry.getValue().getOrDefault(replicaId, -1L);
            if (beId != -1L) {
                result.put(entry.getKey(), beId);
            }
        }
        return result;
    }

    // -- Secondary BE (beId + timestamp stored atomically as long[2]) --

    private ConcurrentLong2ObjectHashMap<long[]> getOrCreateSecondaryMap(String clusterId) {
        return clusterSecondaryMap.computeIfAbsent(clusterId, k -> new ConcurrentLong2ObjectHashMap<>());
    }

    /** Returns [beId, timestamp] or null if no secondary BE is set. */
    public long[] getSecondaryBe(String clusterId, long replicaId) {
        ConcurrentLong2ObjectHashMap<long[]> inner = clusterSecondaryMap.get(clusterId);
        if (inner == null) {
            return null;
        }
        return inner.get(replicaId);
    }

    public long getSecondaryBeId(String clusterId, long replicaId) {
        long[] pair = getSecondaryBe(clusterId, replicaId);
        return pair != null ? pair[0] : -1L;
    }

    public long getSecondaryTimestamp(String clusterId, long replicaId) {
        long[] pair = getSecondaryBe(clusterId, replicaId);
        return pair != null ? pair[1] : -1L;
    }

    public void setSecondaryBe(String clusterId, long replicaId, long beId, long timestamp) {
        getOrCreateSecondaryMap(clusterId).put(replicaId, new long[]{beId, timestamp});
    }

    public void removeSecondaryBe(String clusterId, long replicaId) {
        ConcurrentLong2ObjectHashMap<long[]> inner = clusterSecondaryMap.get(clusterId);
        if (inner != null) {
            inner.remove(replicaId);
        }
    }

    public void clearSecondaryBeForReplica(long replicaId) {
        for (ConcurrentLong2ObjectHashMap<long[]> inner : clusterSecondaryMap.values()) {
            inner.remove(replicaId);
        }
    }
}
