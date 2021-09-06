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

package org.apache.doris.clone;

import org.apache.doris.common.Pair;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * MovesCacheMap stores MovesCache for every cluster and medium.
 * MovesCache is a simple encapsulation of Guava Cache. Use it by calling MovesCache.get().
 * MovesCache's expireAfterAccess can be reset when updating the cache mapping. If expireAfterAccess reset,
 * all MovesCaches will be cleared and recreated.
 */
public class MovesCacheMap {
    private static final Logger LOG = LogManager.getLogger(MovesCacheMap.class);

    // cluster -> tag -> medium -> MovesCache
    private final Table<String, Tag, Map<TStorageMedium, MovesCache>> cacheMap = HashBasedTable.create();
    private long lastExpireConfig = -1L;

    // TabletId -> Pair<Move, ToDeleteReplicaId>, 'ToDeleteReplicaId == -1' means this move haven't been scheduled successfully.
    public static class MovesCache {
        Cache<Long, Pair<PartitionRebalancer.TabletMove, Long>> cache;

        MovesCache(long duration, TimeUnit unit) {
            cache = CacheBuilder.newBuilder().expireAfterAccess(duration, unit).build();
        }

        public Cache<Long, Pair<PartitionRebalancer.TabletMove, Long>> get() {
            return cache;
        }
    }

    // Cyclical update the cache mapping, cuz the cluster may be deleted, we should delete the corresponding cache too.
    public void updateMapping(Table<String, Tag, ClusterLoadStatistic> statisticMap, long expireAfterAccessSecond) {
        if (expireAfterAccessSecond > 0 && lastExpireConfig != expireAfterAccessSecond) {
            LOG.debug("Reset expireAfterAccess, last {} s, now {} s. Moves will be cleared.", lastExpireConfig, expireAfterAccessSecond);
            cacheMap.clear();
            lastExpireConfig = expireAfterAccessSecond;
        }

        cacheMap.cellSet().stream().filter(c -> !statisticMap.contains(c.getRowKey(), c.getColumnKey())).forEach(
                c -> cacheMap.remove(c.getRowKey(), c.getColumnKey()));

        List<Table.Cell<String, Tag, ClusterLoadStatistic>> toAdd = statisticMap.cellSet().stream()
                .filter(c -> !cacheMap.contains(c.getRowKey(), c.getColumnKey()))
                .collect(Collectors.toList());
        for (Table.Cell<String, Tag, ClusterLoadStatistic> cell : toAdd) {
            Map<TStorageMedium, MovesCache> newCacheMap = Maps.newHashMap();
            Arrays.stream(TStorageMedium.values()).forEach(m -> newCacheMap.put(m, new MovesCache(expireAfterAccessSecond, TimeUnit.SECONDS)));
            this.cacheMap.put(cell.getRowKey(), cell.getColumnKey(), newCacheMap);
        }
    }

    public MovesCache getCache(String clusterName, Tag tag, TStorageMedium medium) {
        Map<TStorageMedium, MovesCache> clusterMoves = cacheMap.get(clusterName, tag);
        if (clusterMoves != null) {
            return clusterMoves.get(medium);
        }
        return null;
    }

    // For given tablet ctx, find it in cacheMap
    public Pair<PartitionRebalancer.TabletMove, Long> getTabletMove(TabletSchedCtx tabletCtx) {
        Map<Tag, Map<TStorageMedium, MovesCache>> tagMap = cacheMap.row(tabletCtx.getCluster());
        if (tagMap == null) {
            return null;
        }
        for (Map<TStorageMedium, MovesCache> mediumMap : tagMap.values()) {
            MovesCache cache = mediumMap.get(tabletCtx.getStorageMedium());
            if (cache == null) {
                continue;
            }
            return cache.get().getIfPresent(tabletCtx.getTabletId());
        }
        return null;
    }


    // For each MovesCache, performs any pending maintenance operations needed by the cache.
    public void maintain() {
        cacheMap.values().forEach(maps -> maps.values().forEach(map -> map.get().cleanUp()));
    }

    public long size() {
        return cacheMap.values().stream().mapToLong(maps -> maps.values().stream().mapToLong(map -> map.get().size()).sum()).sum();
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner("\n", "MovesInProgress detail:\n", "");
        cacheMap.cellSet().forEach(c -> c.getValue().forEach((k, v)
                -> sj.add("(" + c.getRowKey() + "-" + c.getColumnKey() + "-" + k + ": " + v.get().asMap() + ")")));
        return sj.toString();
    }
}
