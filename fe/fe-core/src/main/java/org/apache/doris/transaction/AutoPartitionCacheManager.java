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

package org.apache.doris.transaction;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AutoPartitionCacheManager is used to manage the cache of auto partition info.
 * To distinguish the idempotence of the createPartition RPC during incremental partition creation
 * for automatic partitioned tables, record the dbId -> txnId -> tabletId -> BE id
 */
public class AutoPartitionCacheManager {
    // dbId -> txnId -> tabletId -> BE id
    private final Map<Long, Map<Long, Multimap<Long, Long>>> autoPartitionInfo = new ConcurrentHashMap<>();
    // Per-transaction locks: "dbId:txnId" -> lock
    private final Map<String, ReentrantLock> transactionLocks = new ConcurrentHashMap<>();

    private ReentrantLock getTransactionLock(Long dbId, Long txnId) {
        String key = dbId + ":" + txnId;
        return transactionLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    private void removeTransactionLock(Long dbId, Long txnId) {
        String key = dbId + ":" + txnId;
        transactionLocks.remove(key);
    }

    /**
     * Get or set auto partition info for a transaction.
     *
     * @param dbId database id
     * @param txnId transaction id
     * @param inMap input map: partitionId -> tabletId -> BeId
     * @param outMap output map: tabletId -> BeId
     */
    public void getOrSetAutoPartitionInfo(Long dbId, Long txnId,
                    Map<Long, Multimap<Long, Long>> inMap, Multimap<Long, Long> outMap) {
        ReentrantLock txnLock = getTransactionLock(dbId, txnId);
        txnLock.lock();
        try {
            Map<Long, Multimap<Long, Long>> txnMap = autoPartitionInfo.computeIfAbsent(dbId,
                    k -> new ConcurrentHashMap<>());
            Multimap<Long, Long> tabletMap = txnMap.get(txnId);
            // first create partition in this txn, so just use the tablet info in inMap
            if (tabletMap == null) {
                tabletMap = HashMultimap.create();
                for (Map.Entry<Long, Multimap<Long, Long>> entry : inMap.entrySet()) {
                    Multimap<Long, Long> partitionTabletMap = entry.getValue();
                    for (Map.Entry<Long, Long> tabletEntry : partitionTabletMap.entries()) {
                        Long tabletId = tabletEntry.getKey();
                        Long beId = tabletEntry.getValue();
                        tabletMap.put(tabletId, beId);
                        outMap.put(tabletId, beId);
                    }
                }
                txnMap.put(txnId, tabletMap);
            } else {
                // not first create partition, check every partition in inMap
                for (Map.Entry<Long, Multimap<Long, Long>> entry : inMap.entrySet()) {
                    Multimap<Long, Long> partitionTabletMap = entry.getValue();
                    if (partitionTabletMap.isEmpty()) {
                        continue;
                    }
                    Long firstTabletId = partitionTabletMap.keys().iterator().next();
                    // the tablets in this partition have been cached
                    if (tabletMap.containsKey(firstTabletId)) {
                        for (Long tabletId : partitionTabletMap.keySet()) {
                            if (tabletMap.containsKey(tabletId)) {
                                outMap.putAll(tabletId, tabletMap.get(tabletId));
                            }
                        }
                    } else {
                        // newly create partition
                        for (Map.Entry<Long, Long> tabletEntry : partitionTabletMap.entries()) {
                            Long tabletId = tabletEntry.getKey();
                            Long beId = tabletEntry.getValue();
                            tabletMap.put(tabletId, beId);
                            outMap.put(tabletId, beId);
                        }
                    }
                }
            }
        } finally {
            txnLock.unlock();
        }
    }

    public void clearAutoPartitionInfo(Long dbId, Long txnId) {
        ReentrantLock txnLock = getTransactionLock(dbId, txnId);
        txnLock.lock();
        try {
            Map<Long, Multimap<Long, Long>> txnMap = autoPartitionInfo.get(dbId);
            if (txnMap != null) {
                txnMap.remove(txnId);
                if (txnMap.isEmpty()) {
                    autoPartitionInfo.remove(dbId);
                }
            }
        } finally {
            txnLock.unlock();
            removeTransactionLock(dbId, txnId);
        }
    }
}

