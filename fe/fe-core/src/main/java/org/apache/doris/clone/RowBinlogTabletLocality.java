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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class RowBinlogTabletLocality {
    private RowBinlogTabletLocality() {
    }

    public static class RowBinlogHealthResult {
        private final TabletHealth tabletHealth;
        private final Tablet baseTablet;
        private final Map<Long, Long> requiredDestPathHashByBackend;

        private RowBinlogHealthResult(TabletHealth tabletHealth, Tablet baseTablet,
                Map<Long, Long> requiredDestPathHashByBackend) {
            this.tabletHealth = tabletHealth;
            this.baseTablet = baseTablet;
            this.requiredDestPathHashByBackend = requiredDestPathHashByBackend;
        }

        public TabletHealth getTabletHealth() {
            return tabletHealth;
        }

        public Tablet getBaseTablet() {
            return baseTablet;
        }

        public Map<Long, Long> getRequiredDestPathHashByBackend() {
            return requiredDestPathHashByBackend;
        }

        public Set<Long> getRequiredBackends() {
            return Sets.newHashSet(requiredDestPathHashByBackend.keySet());
        }

        public void applyTo(TabletSchedCtx tabletCtx) {
            tabletCtx.setColocateGroupBackendIds(getRequiredBackends());
            tabletCtx.setRowBinlogRequiredDestPathHashByBackend(requiredDestPathHashByBackend);
        }
    }

    public static RowBinlogHealthResult getRowBinlogHealth(Partition partition, Tablet rowBinlogTablet,
            ReplicaAllocation replicaAlloc, long visibleVersion) {
        Tablet baseTablet = partition.getBaseIndex().getTablet(rowBinlogTablet.getAlignedTabletId());
        if (baseTablet == null) {
            TabletHealth tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
            return new RowBinlogHealthResult(tabletHealth, null, Maps.newHashMap());
        }

        Map<Long, Long> requiredDestPathHashByBackend = getEffectiveBaseReplicaPathByBackend(
                baseTablet, visibleVersion, false);
        TabletHealth tabletHealth;
        if (requiredDestPathHashByBackend.isEmpty()) {
            tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
        } else {
            tabletHealth = rowBinlogTablet.getColocateHealth(
                    visibleVersion, replicaAlloc, requiredDestPathHashByBackend.keySet());
            if (tabletHealth.status != TabletStatus.UNRECOVERABLE
                    && hasWrongPathReplica(rowBinlogTablet, requiredDestPathHashByBackend)) {
                tabletHealth.status = TabletStatus.COLOCATE_REDUNDANT;
            }
            if (tabletHealth.status != TabletStatus.HEALTHY
                    && tabletHealth.status != TabletStatus.UNRECOVERABLE
                    && tabletHealth.priority == Priority.NORMAL) {
                tabletHealth.priority = Priority.HIGH;
            }
        }
        return new RowBinlogHealthResult(tabletHealth, baseTablet, requiredDestPathHashByBackend);
    }

    public static Map<Long, Long> getEffectiveBaseReplicaPathByBackend(Tablet baseTablet,
            long visibleVersion, boolean requireVersionComplete) {
        Map<Long, Long> pathHashByBackend = Maps.newHashMap();
        for (Replica replica : baseTablet.getReplicas()) {
            if (!isEffectiveReplica(replica, visibleVersion, requireVersionComplete, true)) {
                continue;
            }
            pathHashByBackend.put(replica.getBackendIdWithoutException(), replica.getPathHash());
        }
        return pathHashByBackend;
    }

    public static Map<Long, Long> getPreferredBaseRepairPathByBackend(Partition partition, Tablet baseTablet,
            long visibleVersion) {
        Tablet rowBinlogTablet = getRowBinlogTablet(partition, baseTablet);
        Map<Long, Long> preferredPathHashByBackend = Maps.newHashMap();
        if (rowBinlogTablet == null) {
            return preferredPathHashByBackend;
        }
        for (Replica replica : rowBinlogTablet.getReplicas()) {
            if (!isEffectiveReplica(replica, visibleVersion, false, false)) {
                continue;
            }
            preferredPathHashByBackend.put(replica.getBackendIdWithoutException(), replica.getPathHash());
        }
        return preferredPathHashByBackend;
    }

    public static Tablet getRowBinlogTablet(Partition partition, Tablet baseTablet) {
        long rowBinlogTabletId = baseTablet.getAlignedTabletId();
        if (rowBinlogTabletId <= 0) {
            return null;
        }
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE_WITH_ROW_BINLOG)) {
            if (!index.isRowBinlog()) {
                continue;
            }
            Tablet rowBinlogTablet = index.getTablet(rowBinlogTabletId);
            if (rowBinlogTablet != null) {
                return rowBinlogTablet;
            }
        }
        return null;
    }

    public static int getCompletePairCount(Tablet baseTablet, Tablet rowBinlogTablet, long visibleVersion,
            boolean requireSamePath) {
        int completePairCount = 0;
        for (Replica baseReplica : baseTablet.getReplicas()) {
            if (!isEffectiveReplica(baseReplica, visibleVersion, true, true)) {
                continue;
            }
            Replica rowBinlogReplica = rowBinlogTablet.getReplicaByBackendId(
                    baseReplica.getBackendIdWithoutException());
            if (rowBinlogReplica == null || !isEffectiveReplica(rowBinlogReplica, visibleVersion, true, false)) {
                continue;
            }
            if (requireSamePath && baseReplica.getPathHash() != rowBinlogReplica.getPathHash()) {
                continue;
            }
            completePairCount++;
        }
        return completePairCount;
    }

    public static boolean isCompletePair(Replica baseReplica, Replica rowBinlogReplica, long visibleVersion,
            boolean requireSamePath) {
        if (rowBinlogReplica == null) {
            return false;
        }
        if (!isEffectiveReplica(baseReplica, visibleVersion, true, true)
                || !isEffectiveReplica(rowBinlogReplica, visibleVersion, true, false)) {
            return false;
        }
        return !requireSamePath || baseReplica.getPathHash() == rowBinlogReplica.getPathHash();
    }

    private static boolean hasWrongPathReplica(Tablet rowBinlogTablet, Map<Long, Long> requiredPathHashByBackend) {
        for (Replica replica : rowBinlogTablet.getReplicas()) {
            long beId = replica.getBackendIdWithoutException();
            Long requiredPathHash = requiredPathHashByBackend.get(beId);
            if (requiredPathHash == null || requiredPathHash == -1L || replica.getPathHash() == -1L) {
                continue;
            }
            if (replica.getPathHash() != requiredPathHash) {
                return true;
            }
        }
        return false;
    }

    private static boolean isEffectiveReplica(Replica replica, long visibleVersion, boolean requireVersionComplete,
            boolean skipBinlogMissing) {
        if (replica.isBad() || replica.tooSlow()) {
            return false;
        }
        if (skipBinlogMissing && replica.isBinlogMissing()) {
            return false;
        }
        if (!replica.isAlive() || !replica.isScheduleAvailable()) {
            return false;
        }
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Backend backend = infoService.getBackend(replica.getBackendIdWithoutException());
        if (backend == null || !backend.isAlive() || !backend.isMixNode()) {
            return false;
        }
        if (requireVersionComplete && (replica.getLastFailedVersion() > 0
                || replica.getVersion() < visibleVersion)) {
            return false;
        }
        return true;
    }
}
