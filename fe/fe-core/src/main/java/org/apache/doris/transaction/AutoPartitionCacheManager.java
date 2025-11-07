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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

    // txnId -> partitionId -> PartitionTabletCache
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, PartitionTabletCache>> autoPartitionInfo
                    = new ConcurrentHashMap<>();

    /**
     * Get or set auto partition info for a transaction.
     * For a partition, if it's already cached, use the cached tablet locations;
     * otherwise, cache the new tablet locations.
     */
    public void getOrSetAutoPartitionInfo(Long txnId, Long partitionId,
            List<TTabletLocation> partitionTablets, List<TTabletLocation> partitionSlaveTablets) {
        ConcurrentHashMap<Long, PartitionTabletCache> partitionMap =
                autoPartitionInfo.computeIfAbsent(txnId, k -> new ConcurrentHashMap<>());

        final AtomicBoolean needUpdate = new AtomicBoolean(false);
        PartitionTabletCache cached = partitionMap.computeIfAbsent(partitionId, k -> {
            needUpdate.set(true);
            return new PartitionTabletCache(
                    new ArrayList<>(partitionTablets),
                    new ArrayList<>(partitionSlaveTablets)
            );
        });

        if (!needUpdate.get()) {
            partitionTablets.clear();
            partitionTablets.addAll(cached.tablets);
            partitionSlaveTablets.clear();
            partitionSlaveTablets.addAll(cached.slaveTablets);
        }
    }

    public void clearAutoPartitionInfo(Long txnId) {
        autoPartitionInfo.remove(txnId);
    }
}

