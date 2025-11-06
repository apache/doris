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

import org.apache.doris.thrift.TTabletLocation;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AutoPartitionCacheManager is used to manage the cache of auto partition info.
 * To distinguish the idempotence of the createPartition RPC during incremental partition creation
 * for automatic partitioned tables, cache tablet locations per partition.
 */
public class AutoPartitionCacheManager {

    /**
     * Cache structure to store tablet and slave tablet locations.
     */
    public static class PartitionTabletCache {
        public final List<TTabletLocation> tablets;
        public final List<TTabletLocation> slaveTablets;

        public PartitionTabletCache(List<TTabletLocation> tablets, List<TTabletLocation> slaveTablets) {
            this.tablets = tablets;
            this.slaveTablets = slaveTablets;
        }
    }

    // dbId -> txnId -> partitionId -> PartitionTabletCache
    private final ConcurrentHashMap<Long,
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, PartitionTabletCache>>> autoPartitionInfo
                    = new ConcurrentHashMap<>();

    /**
     * Get or set auto partition info for a transaction.
     * For each partition, if it's already cached, use the cached tablet locations;
     * otherwise, cache the new tablet locations from tempResultTablets.
     *
     * @param dbId database id
     * @param txnId transaction id
     * @param tempResultTablets input map: partitionId -> (tabletId -> Set of BEids)
     * @param isWriteSingleReplica whether to write single replica (randomly select master)
     * @return PartitionTabletCache containing tablets and slaveTablets
     */
    public PartitionTabletCache getOrSetAutoPartitionInfo(Long dbId, Long txnId,
                    Map<Long, Map<Long, Set<Long>>> tempResultTablets, boolean isWriteSingleReplica) {
        List<TTabletLocation> allTablets = new ArrayList<>();
        List<TTabletLocation> allSlaveTablets = new ArrayList<>();

        ConcurrentHashMap<Long, ConcurrentHashMap<Long, PartitionTabletCache>> txnMap =
                autoPartitionInfo.computeIfAbsent(dbId, k -> new ConcurrentHashMap<>());

        ConcurrentHashMap<Long, PartitionTabletCache> partitionMap =
                txnMap.computeIfAbsent(txnId, k -> new ConcurrentHashMap<>());

        for (Map.Entry<Long, Map<Long, Set<Long>>> entry : tempResultTablets.entrySet()) {
            Long partitionId = entry.getKey();
            Map<Long, Set<Long>> tabletBeMap = entry.getValue();

            if (tabletBeMap == null || tabletBeMap.isEmpty()) {
                continue;
            }

            // This ensures that for the same partition, tablet locations are created only once
            // which is critical for write_single_replica mode where master selection must be consistent
            PartitionTabletCache tabletBeMapCache = partitionMap.computeIfAbsent(partitionId, k -> {
                List<TTabletLocation> tablets = new ArrayList<>();
                List<TTabletLocation> slaveTablets = new ArrayList<>();

                for (Map.Entry<Long, Set<Long>> tabletEntry : tabletBeMap.entrySet()) {
                    Long tabletId = tabletEntry.getKey();
                    Set<Long> beIds = tabletEntry.getValue();

                    if (beIds == null || beIds.isEmpty()) {
                        continue;
                    }

                    if (isWriteSingleReplica) {
                        // Randomly select one BE as master, others as slaves
                        Long[] nodes = beIds.toArray(new Long[0]);
                        Random random = new SecureRandom();
                        Long masterNode = nodes[random.nextInt(nodes.length)];
                        List<Long> masterNodes = new ArrayList<>();
                        masterNodes.add(masterNode);

                        List<Long> slaveNodes = new ArrayList<>();
                        for (Long beId : beIds) {
                            if (!beId.equals(masterNode)) {
                                slaveNodes.add(beId);
                            }
                        }

                        tablets.add(new TTabletLocation(tabletId, masterNodes));
                        if (!slaveNodes.isEmpty()) {
                            slaveTablets.add(new TTabletLocation(tabletId, slaveNodes));
                        }
                    } else {
                        // All BEs as nodes
                        tablets.add(new TTabletLocation(tabletId, new ArrayList<>(beIds)));
                    }
                }

                return new PartitionTabletCache(tablets, slaveTablets);
            });

            allTablets.addAll(tabletBeMapCache.tablets);
            allSlaveTablets.addAll(tabletBeMapCache.slaveTablets);
        }

        return new PartitionTabletCache(allTablets, allSlaveTablets);
    }

    public void clearAutoPartitionInfo(Long dbId, Long txnId) {
        autoPartitionInfo.computeIfPresent(dbId, (key, txnMap) -> {
            txnMap.remove(txnId);
            return txnMap.isEmpty() ? null : txnMap;
        });
    }
}

