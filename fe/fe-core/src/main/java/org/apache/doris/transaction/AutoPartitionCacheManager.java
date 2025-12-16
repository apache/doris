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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/*
    ** this class AutoPartitionCacheManager is used for solve the follow question :
    **
    * RPC [P1, P2]              RPC [P2, P3]
    *       |                         |
    *    P1:t1, t2                    |
    *       ↓                         |
    *    P2:t3, t4                    |
    *                                 ↓
    *                             P2:exist
    *                                 ↓
    *                             P3:t5,t6
    * --------------------------------------
    *       tablet rebalance during ...
    *     t1 - be1                 t3 - be1 <-
    *     t2 - be2                 t4 - be1
    *     t3 - be2 <-              t5 - be2
    *     t4 - be1                 t6 - be2
    * --------------------------------------
    * We ensure that only one view of the replica distribution in P2:t3,t4 above takes effect for this txn
    * to avoid tablets being written to multiple instances within the same transaction (assuming single replica)
*/

// AutoPartitionCacheManager is used to manage the cache of auto partition info.
// To distinguish the idempotence of the createPartition RPC during incremental partition creation
// for automatic partitioned tables, cache tablet locations per partition.
public class AutoPartitionCacheManager {
    private static Logger LOG = LogManager.getLogger(AutoPartitionCacheManager.class);

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

    // return true if cached, else false, this function only read cache
    public boolean getAutoPartitionInfo(Long txnId, Long partitionId,
            List<TTabletLocation> partitionTablets, List<TTabletLocation> partitionSlaveTablets) {
        ConcurrentHashMap<Long, PartitionTabletCache> partitionMap = autoPartitionInfo.get(txnId);
        if (partitionMap == null) {
            return false;
        }

        PartitionTabletCache cached = partitionMap.get(partitionId);
        if (cached == null) {
            return false;
        }

        partitionTablets.clear();
        partitionTablets.addAll(cached.tablets);
        partitionSlaveTablets.clear();
        partitionSlaveTablets.addAll(cached.slaveTablets);
        return true;
    }

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
            partitionSlaveTablets.clear();
            partitionTablets.addAll(cached.tablets);
            partitionSlaveTablets.addAll(cached.slaveTablets);
            LOG.debug("Get cached auto partition info from cache, txnId: {}, partitionId: {}, "
                    + "tablets: {}, slaveTablets: {}", txnId, partitionId,
                    cached.tablets.size(), cached.slaveTablets.size());
        }
    }

    public void clearAutoPartitionInfo(Long txnId) {
        autoPartitionInfo.remove(txnId);
    }
}

