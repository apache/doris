// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.thrift.TStorageMedium;
import com.baidu.palo.thrift.TTablet;
import com.baidu.palo.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * this class stores a inverted index
 * key is tablet id. value is the related ids of this tablet
 */
public class TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // tablet id -> tablet meta
    private Map<Long, TabletMeta> tabletMetaMap = Maps.newHashMap();
    
    /*
     *  we use this to save memory.
     *  we do not need create TabletMeta instance for each tablet,
     *  cause tablets in one (Partition-MaterializedIndex) has same parent info
     *      (dbId, tableId, partitionId, indexId, schemaHash)
     *  we use 'tabletMetaTable' to do the update things
     *      (eg. update schema hash in TabletMeta)
     *  partition id -> (index id -> tablet meta)
     */
    private Table<Long, Long, TabletMeta> tabletMetaTable = HashBasedTable.create();
    
    // tablet id -> (backend id -> replica)
    private Table<Long, Long, Replica> replicaMetaTable = HashBasedTable.create();
    // backing replica table, for visiting backend replicas faster.
    // backend id -> (tablet id -> replica)
    private Table<Long, Long, Replica> backingReplicaMetaTable = HashBasedTable.create();

    public TabletInvertedIndex() {

    }

    private final void readLock() {
        this.lock.readLock().lock();
    }

    private final void readUnlock() {
        this.lock.readLock().unlock();
    }

    private final void writeLock() {
        this.lock.writeLock().lock();
    }

    private final void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> foundTabletsWithValidSchema,
                             Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap) {

        long start = 0L;
        readLock();
        try {
            LOG.info("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            start = System.currentTimeMillis();
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                // traverse replicas in meta with this backend
                for (Map.Entry<Long, Replica> entry : replicaMetaWithBackend.entrySet()) {
                    long tabletId = entry.getKey();
                    Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
                    TabletMeta tabletMeta = tabletMetaMap.get(tabletId);

                    if (backendTablets.containsKey(tabletId)) {
                        TTablet backendTablet = backendTablets.get(tabletId);
                        Replica replica = entry.getValue();
                        for (TTabletInfo backendTabletInfo : backendTablet.getTablet_infos()) {
                            if (tabletMeta.containsSchemaHash(backendTabletInfo.getSchema_hash())) {
                                foundTabletsWithValidSchema.add(tabletId);
                                // 1. (intersection)
                                if (checkSync(replica, backendTabletInfo.getVersion(),
                                              backendTabletInfo.getVersion_hash())) {
                                    // need sync
                                    tabletSyncMap.put(tabletMeta.getDbId(), tabletId);
                                }

                                // check if need migration
                                long partitionId = tabletMeta.getPartitionId();
                                TStorageMedium storageMedium = storageMediumMap.get(partitionId);
                                if (storageMedium != null && backendTabletInfo.isSetStorage_medium()) {
                                    if (storageMedium != backendTabletInfo.getStorage_medium()) {
                                        tabletMigrationMap.put(storageMedium, tabletId);
                                    }
                                }

                                // update replicas's version count
                                // no need to write log, and no need to get db lock.
                                if (backendTabletInfo.isSetVersion_count()) {
                                    replica.setVersionCount(backendTabletInfo.getVersion_count());
                                }
                            } else {
                                // tablet with invalid schemahash
                                foundTabletsWithInvalidSchema.put(tabletId, backendTabletInfo);
                            }
                        } // end for be tablet info
                    } else {
                        // 2. (meta - be)
                        // may need delete from meta
                        LOG.debug("backend[{}] does not report tablet[{}-{}]", backendId, tabletId, tabletMeta);
                        tabletDeleteFromMeta.put(tabletMeta.getDbId(), tabletId);
                    }
                } // end for replicaMetaWithBackend
            }
        } finally {
            readUnlock();
        }

        long end = System.currentTimeMillis();
        LOG.info("finished to do tablet diff with backend[{}]. sync: {}. metaDel: {}. foundValid: {}. foundInvalid: {}."
                         + " migration: {}." + " cost: {} ms", backendId, tabletSyncMap.size(),
                 tabletDeleteFromMeta.size(), foundTabletsWithValidSchema.size(), foundTabletsWithInvalidSchema.size(),
                 tabletMigrationMap.size(), (end - start));
    }

    public long getDbId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getDbId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public long getTableId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getTableId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public long getPartitionId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getPartitionId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public long getIndexId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getIndexId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public int getEffectiveSchemaHash(long tabletId) {
        // always get old schema hash(as effective one)
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getOldSchemaHash();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public TabletMeta getTabletMeta(long tabletId) {
        readLock();
        try {
            return tabletMetaMap.get(tabletId);
        } finally {
            readUnlock();
        }
    }

    private boolean checkSync(Replica replicaMeta, long backendVersion, long backendVersionHash) {
        long metaVersion = replicaMeta.getVersion();
        long metaVersionHash = replicaMeta.getVersionHash();
        if (metaVersion < backendVersion || (metaVersion == backendVersion && metaVersionHash != backendVersionHash)) {
            return true;
        }
        return false;
    }

    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        // always add tablet before adding replicas
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return;
            }
            tabletMetaMap.put(tabletId, tabletMeta);
            if (!tabletMetaTable.contains(tabletMeta.getPartitionId(), tabletMeta.getIndexId())) {
                tabletMetaTable.put(tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletMeta);
            }
        } finally {
            writeUnlock();
        }
    }

    public void deleteTablet(long tabletId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Map<Long, Replica> replicas = replicaMetaTable.rowMap().remove(tabletId);
            if (replicas != null) {
                for (long backendId : replicas.keySet()) {
                    backingReplicaMetaTable.remove(backendId, tabletId);
                }
            }
            TabletMeta tabletMeta = tabletMetaMap.remove(tabletId);
            if (tabletMeta != null) {
                tabletMetaTable.remove(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
            }
        } finally {
            writeUnlock();
        }
    }

    public void addReplica(long tabletId, Replica replica) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            replicaMetaTable.put(tabletId, replica.getBackendId(), replica);
            backingReplicaMetaTable.put(replica.getBackendId(), tabletId, replica);
        } finally {
            writeUnlock();
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            // Preconditions.checkState(replicaMetaTable.containsRow(tabletId));
            if (replicaMetaTable.containsRow(tabletId)) {
                replicaMetaTable.remove(tabletId, backendId);
                backingReplicaMetaTable.remove(backendId, tabletId);
                LOG.debug("delete tablet[{}] in backend[{}]", tabletId, backendId);
            } else {
                // this may happend when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock();
        }
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        readLock();
        try {
            if (replicaMetaTable.containsRow(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.row(tabletId).values());
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    public void setNewSchemaHash(long partitionId, long indexId, int newSchemaHash) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaTable.contains(partitionId, indexId));
            tabletMetaTable.get(partitionId, indexId).setNewSchemaHash(newSchemaHash);
        } finally {
            writeUnlock();
        }
    }

    public void updateToNewSchemaHash(long partitionId, long indexId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaTable.contains(partitionId, indexId));
            tabletMetaTable.get(partitionId, indexId).updateToNewSchemaHash();
        } finally {
            writeUnlock();
        }
    }

    public void deleteNewSchemaHash(long partitionId, long indexId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            TabletMeta tabletMeta = tabletMetaTable.get(partitionId, indexId);
            if (tabletMeta != null) {
                tabletMeta.deleteNewSchemaHash();
            }
        } finally {
            writeUnlock();
        }
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        List<Long> tabletIds = Lists.newArrayList();
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIds.addAll(replicaMetaWithBackend.keySet());
            }
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public int getTabletNumByBackendId(long backendId) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                return replicaMetaWithBackend.size();
            }
        } finally {
            readUnlock();
        }
        return 0;
    }

    // just for test
    public void clear() {
        writeLock();
        try {
            tabletMetaMap.clear();
            tabletMetaTable.clear();
            replicaMetaTable.clear();
            backingReplicaMetaTable.clear();
        } finally {
            writeUnlock();
        }
    }
}

