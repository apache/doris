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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class RowBinlogTabletLocality {
    private static final Logger LOG = LogManager.getLogger(RowBinlogTabletLocality.class);

    private RowBinlogTabletLocality() {
    }

    /**
     * Returns whether a local tablet may be moved without coordinating another tablet.
     * Callers must not hold the tablet inverted-index lock while this method takes the table read lock.
     */
    public static boolean canMoveTabletIndependently(TabletMeta tabletMeta) {
        if (tabletMeta == null) {
            return false;
        }
        Database db = Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId());
        if (db == null) {
            return false;
        }
        Table table = db.getTableNullable(tabletMeta.getTableId());
        if (!(table instanceof OlapTable)) {
            return false;
        }

        OlapTable olapTable = (OlapTable) table;
        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(tabletMeta.getPartitionId());
            if (partition == null) {
                return false;
            }
            MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
            return canMoveTabletIndependently(partition, index);
        } finally {
            olapTable.readUnlock();
        }
    }

    static boolean canMoveTabletIndependently(Partition partition, MaterializedIndex index) {
        if (index == null || index.isRowBinlog()) {
            return false;
        }
        if (index.getId() != partition.getBaseIndex().getId()) {
            return true;
        }
        return partition.getMaterializedIndices(IndexExtState.ALL, true).stream()
                .noneMatch(MaterializedIndex::isRowBinlog);
    }

    static class RowBinlogTabletPair {
        private final Tablet baseTablet;
        private final Tablet rowBinlogTablet;

        private RowBinlogTabletPair(Tablet baseTablet, Tablet rowBinlogTablet) {
            this.baseTablet = baseTablet;
            this.rowBinlogTablet = rowBinlogTablet;
        }

        Tablet getBaseTablet() {
            return baseTablet;
        }

        Tablet getRowBinlogTablet() {
            return rowBinlogTablet;
        }
    }

    public static class RowBinlogHealthResult {
        private final TabletHealth tabletHealth;
        private final Tablet baseTablet;
        private final RowBinlogRepairReason repairReason;
        private final Map<Long, Long> requiredDestPathHashByBackend;
        private final Map<Long, Long> observedPathHashByBackend;

        private RowBinlogHealthResult(TabletHealth tabletHealth, Tablet baseTablet,
                RowBinlogRepairReason repairReason, Map<Long, Long> requiredDestPathHashByBackend,
                Map<Long, Long> observedPathHashByBackend) {
            this.tabletHealth = tabletHealth;
            this.baseTablet = baseTablet;
            this.repairReason = repairReason;
            this.requiredDestPathHashByBackend = Maps.newHashMap(requiredDestPathHashByBackend);
            this.observedPathHashByBackend = Maps.newHashMap(observedPathHashByBackend);
        }

        public TabletHealth getTabletHealth() {
            return tabletHealth;
        }

        public Tablet getBaseTablet() {
            return baseTablet;
        }

        public RowBinlogRepairReason getRepairReason() {
            return repairReason;
        }

        public Map<Long, Long> getRequiredDestPathHashByBackend() {
            return requiredDestPathHashByBackend;
        }

        public Map<Long, Long> getObservedPathHashByBackend() {
            return observedPathHashByBackend;
        }

        public Set<Long> getRequiredBackends() {
            return Sets.newHashSet(requiredDestPathHashByBackend.keySet());
        }

        public void applyTo(TabletSchedCtx tabletCtx) {
            // Reuse the scheduler's colocate backend-set carrier for the effective backends of the
            // paired base tablet. This does not mean that the row-binlog tablet belongs to, or follows,
            // a user-defined colocate group layout.
            tabletCtx.setColocateGroupBackendIds(getRequiredBackends());
            tabletCtx.setRowBinlogBaseTabletId(baseTablet == null ? -1L : baseTablet.getId());
            tabletCtx.setRowBinlogRepairReason(repairReason);
            tabletCtx.setRowBinlogRequiredDestPathHashByBackend(requiredDestPathHashByBackend);
            tabletCtx.setRowBinlogObservedPathHashByBackend(observedPathHashByBackend);
        }
    }

    public static RowBinlogHealthResult getRowBinlogHealth(Partition partition, Tablet rowBinlogTablet,
            ReplicaAllocation replicaAlloc, long visibleVersion) {
        RowBinlogTabletPair tabletPair;
        try {
            tabletPair = resolvePairForRowBinlogTablet(partition, rowBinlogTablet);
        } catch (IllegalStateException e) {
            LOG.warn("invalid row binlog tablet pair for tablet {} in partition {}: {}",
                    rowBinlogTablet.getId(), partition.getId(), e.getMessage());
            TabletHealth tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
            return new RowBinlogHealthResult(tabletHealth, null, RowBinlogRepairReason.NONE,
                    Maps.newHashMap(), getReplicaPathHashByBackend(rowBinlogTablet));
        }
        Tablet baseTablet = tabletPair.getBaseTablet();
        rowBinlogTablet = tabletPair.getRowBinlogTablet();

        Map<Long, Long> requiredDestPathHashByBackend = getEffectiveBaseReplicaPathByBackend(
                baseTablet, visibleVersion, false);
        Map<Long, Long> observedPathHashByBackend = getReplicaPathHashByBackend(rowBinlogTablet);
        RowBinlogRepairReason repairReason = RowBinlogRepairReason.NONE;
        TabletHealth tabletHealth;
        if (requiredDestPathHashByBackend.isEmpty()) {
            tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
        } else {
            tabletHealth = rowBinlogTablet.getColocateHealth(
                    visibleVersion, replicaAlloc, requiredDestPathHashByBackend.keySet());
            if (tabletHealth.status == TabletStatus.COLOCATE_MISMATCH) {
                repairReason = RowBinlogRepairReason.BACKEND_MISMATCH;
            } else if (tabletHealth.status == TabletStatus.COLOCATE_REDUNDANT) {
                repairReason = RowBinlogRepairReason.REDUNDANT;
            } else if (tabletHealth.status == TabletStatus.HEALTHY
                    && hasWrongPathReplica(rowBinlogTablet, requiredDestPathHashByBackend)) {
                tabletHealth.status = TabletStatus.COLOCATE_MISMATCH;
                repairReason = RowBinlogRepairReason.PATH_MISMATCH;
            }
            if (tabletHealth.status != TabletStatus.HEALTHY
                    && tabletHealth.status != TabletStatus.UNRECOVERABLE
                    && tabletHealth.priority == Priority.NORMAL) {
                tabletHealth.priority = Priority.HIGH;
            }
        }
        return new RowBinlogHealthResult(tabletHealth, baseTablet, repairReason,
                requiredDestPathHashByBackend, observedPathHashByBackend);
    }

    private static Map<Long, Long> getReplicaPathHashByBackend(Tablet tablet) {
        Map<Long, Long> pathHashByBackend = Maps.newHashMap();
        for (Replica replica : tablet.getReplicas()) {
            pathHashByBackend.put(replica.getBackendIdWithoutException(), replica.getPathHash());
        }
        return pathHashByBackend;
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
        RowBinlogTabletPair tabletPair = resolvePairForBaseTablet(partition, baseTablet);
        Map<Long, Long> preferredPathHashByBackend = Maps.newHashMap();
        if (tabletPair == null) {
            return preferredPathHashByBackend;
        }
        Tablet rowBinlogTablet = tabletPair.getRowBinlogTablet();
        for (Replica replica : rowBinlogTablet.getReplicas()) {
            if (!isEffectiveReplica(replica, visibleVersion, false, false)) {
                continue;
            }
            preferredPathHashByBackend.put(replica.getBackendIdWithoutException(), replica.getPathHash());
        }
        return preferredPathHashByBackend;
    }

    public static Tablet getRowBinlogTablet(Partition partition, Tablet baseTablet) {
        RowBinlogTabletPair tabletPair = resolvePairForBaseTablet(partition, baseTablet);
        return tabletPair == null ? null : tabletPair.getRowBinlogTablet();
    }

    static RowBinlogTabletPair resolvePairForRowBinlogTablet(Partition partition, Tablet rowBinlogTablet) {
        MaterializedIndex rowBinlogIndex = getRowBinlogIndex(partition);
        if (rowBinlogIndex == null) {
            throw invalidPair(partition, "row binlog index does not exist");
        }
        Tablet indexedRowBinlogTablet = rowBinlogIndex.getTablet(rowBinlogTablet.getId());
        if (indexedRowBinlogTablet == null) {
            throw invalidPair(partition, "row binlog tablet " + rowBinlogTablet.getId() + " does not exist");
        }
        if (!indexedRowBinlogTablet.hasRowBinlogBaseTabletId()) {
            throw invalidPair(partition, "row binlog tablet " + indexedRowBinlogTablet.getId()
                    + " has no base tablet link");
        }
        Tablet baseTablet = partition.getBaseIndex().getTablet(indexedRowBinlogTablet.getRowBinlogBaseTabletId());
        if (baseTablet == null) {
            throw invalidPair(partition, "base tablet " + indexedRowBinlogTablet.getRowBinlogBaseTabletId()
                    + " does not exist");
        }
        validateBidirectionalPair(partition, baseTablet, indexedRowBinlogTablet);
        return new RowBinlogTabletPair(baseTablet, indexedRowBinlogTablet);
    }

    static RowBinlogTabletPair resolvePairForBaseTablet(Partition partition, Tablet baseTablet) {
        MaterializedIndex rowBinlogIndex = getRowBinlogIndex(partition);
        if (rowBinlogIndex == null) {
            return null;
        }
        Tablet indexedBaseTablet = partition.getBaseIndex().getTablet(baseTablet.getId());
        if (indexedBaseTablet == null) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " does not exist");
        }
        if (!indexedBaseTablet.hasRowBinlogTabletId()) {
            throw invalidPair(partition, "base tablet " + indexedBaseTablet.getId()
                    + " has no row binlog tablet link");
        }
        Tablet rowBinlogTablet = rowBinlogIndex.getTablet(indexedBaseTablet.getRowBinlogTabletId());
        if (rowBinlogTablet == null) {
            throw invalidPair(partition, "row binlog tablet " + indexedBaseTablet.getRowBinlogTabletId()
                    + " does not exist");
        }
        validateBidirectionalPair(partition, indexedBaseTablet, rowBinlogTablet);
        return new RowBinlogTabletPair(indexedBaseTablet, rowBinlogTablet);
    }

    private static MaterializedIndex getRowBinlogIndex(Partition partition) {
        MaterializedIndex rowBinlogIndex = null;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE, true)) {
            if (index.isRowBinlog()) {
                if (rowBinlogIndex != null) {
                    throw invalidPair(partition, "multiple row binlog indexes exist");
                }
                rowBinlogIndex = index;
            }
        }
        return rowBinlogIndex;
    }

    private static void validateBidirectionalPair(Partition partition, Tablet baseTablet, Tablet rowBinlogTablet) {
        if (!baseTablet.hasRowBinlogTabletId()) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " has no row binlog tablet link");
        }
        if (baseTablet.getRowBinlogTabletId() != rowBinlogTablet.getId()) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " points to row binlog tablet "
                    + baseTablet.getRowBinlogTabletId() + " instead of " + rowBinlogTablet.getId());
        }
        if (!rowBinlogTablet.hasRowBinlogBaseTabletId()) {
            throw invalidPair(partition, "row binlog tablet " + rowBinlogTablet.getId()
                    + " has no base tablet link");
        }
        if (rowBinlogTablet.getRowBinlogBaseTabletId() != baseTablet.getId()) {
            throw invalidPair(partition, "row binlog tablet " + rowBinlogTablet.getId() + " points to base tablet "
                    + rowBinlogTablet.getRowBinlogBaseTabletId() + " instead of " + baseTablet.getId());
        }
    }

    private static IllegalStateException invalidPair(Partition partition, String reason) {
        return new IllegalStateException("invalid row binlog tablet pair in partition " + partition.getId()
                + ": " + reason);
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
