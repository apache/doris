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

package org.apache.doris.statistics.query;

import org.apache.doris.common.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TabletStats is used to store the query stats of a tablet.
 */
public class TabletStats {
    private ConcurrentHashMap<Long, AtomicLong> tabletQueryStats;

    public TabletStats() {
        tabletQueryStats = new ConcurrentHashMap<>();
    }

    /**
     * Add the query statistics of the tablet.
     *
     * @param replicaIds The tablet replica ids
     */
    public void addStats(List<Long> replicaIds) {
        for (Long replicaId : replicaIds) {
            AtomicLong l = tabletQueryStats.get(replicaId);
            if (l == null) {
                l = new AtomicLong(0);
                AtomicLong old = tabletQueryStats.putIfAbsent(replicaId, l);
                if (old == null) {
                    l.updateAndGet(Util.overflowSafeIncrement());
                } else {
                    old.updateAndGet(Util.overflowSafeIncrement());
                }
            } else {
                l.updateAndGet(Util.overflowSafeIncrement());
            }
        }
    }

    /**
     * Get the query statistics of the tablet.
     *
     * @param replicaIds The tablet replica ids
     * @return The query statistics of the tablets
     */
    public Map<Long, Long> getTabletQueryStats(List<Long> replicaIds) {
        Map<Long, Long> stats = new HashMap<>();
        for (Long replicaId : replicaIds) {
            AtomicLong l = this.tabletQueryStats.get(replicaId);
            if (l != null) {
                stats.put(replicaId, l.get());
            } else {
                stats.put(replicaId, 0L);
            }
        }
        return stats;
    }

    /**
     * Get the query statistics of the tablet.
     * @param replicaId The tablet replica id
     * @return The query statistics of the tablet
     */
    public long getTabletQueryStats(long replicaId) {
        AtomicLong l = this.tabletQueryStats.get(replicaId);
        if (l != null) {
            return l.get();
        } else {
            return 0L;
        }
    }

    public Map<Long, Long> getTabletQueryStats() {
        Map<Long, Long> stats = new HashMap<>();
        for (Map.Entry<Long, AtomicLong> entry : this.tabletQueryStats.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().get());
        }
        return stats;
    }

    public void clear() {
        this.tabletQueryStats.clear();
    }

    public void clear(long replicaId) {
        this.tabletQueryStats.remove(replicaId);
    }

    public void clear(List<Long> replicaIds) {
        for (Long replicaId : replicaIds) {
            this.tabletQueryStats.remove(replicaId);
        }
    }

    public boolean isEmpty() {
        return tabletQueryStats.isEmpty();
    }
}
