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

package org.apache.doris.changelog;

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
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class ChangelogTabletLocality {
    private static final Logger LOG = LogManager.getLogger(ChangelogTabletLocality.class);

    private ChangelogTabletLocality() {
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

    public static boolean canMoveTabletIndependently(Partition partition, MaterializedIndex index) {
        if (index == null || index.isChangelog()) {
            return false;
        }
        if (index.getId() != partition.getBaseIndex().getId()) {
            return true;
        }
        return partition.getMaterializedIndices(IndexExtState.ALL, true).stream()
                .noneMatch(MaterializedIndex::isChangelog);
    }

    public static class ChangelogTabletPair {
        private final Tablet baseTablet;
        private final Tablet changelogTablet;

        private ChangelogTabletPair(Tablet baseTablet, Tablet changelogTablet) {
            this.baseTablet = baseTablet;
            this.changelogTablet = changelogTablet;
        }

        public Tablet getBaseTablet() {
            return baseTablet;
        }

        public Tablet getChangelogTablet() {
            return changelogTablet;
        }
    }

    public static class ChangelogHealthResult {
        private final TabletHealth tabletHealth;
        private final Tablet baseTablet;
        private final ChangelogRepairReason repairReason;
        private final Map<Long, Long> requiredDestPathHashByBackend;
        private final Map<Long, Long> observedPathHashByBackend;

        private ChangelogHealthResult(TabletHealth tabletHealth, Tablet baseTablet,
                ChangelogRepairReason repairReason, Map<Long, Long> requiredDestPathHashByBackend,
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

        public ChangelogRepairReason getRepairReason() {
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
            // paired base tablet. This does not mean that the changelog tablet belongs to, or follows,
            // a user-defined colocate group layout.
            tabletCtx.setColocateGroupBackendIds(getRequiredBackends());
            tabletCtx.setChangelogBaseTabletId(baseTablet == null ? -1L : baseTablet.getId());
            tabletCtx.setChangelogRepairReason(repairReason);
            tabletCtx.setChangelogRequiredDestPathHashByBackend(requiredDestPathHashByBackend);
            tabletCtx.setChangelogObservedPathHashByBackend(observedPathHashByBackend);
        }
    }

    public static ChangelogHealthResult getChangelogHealth(Partition partition, Tablet changelogTablet,
            ReplicaAllocation replicaAlloc, long visibleVersion) {
        ChangelogTabletPair tabletPair;
        try {
            tabletPair = resolvePairForChangelogTablet(partition, changelogTablet);
        } catch (IllegalStateException e) {
            LOG.warn("invalid changelog tablet pair for tablet {} in partition {}: {}",
                    changelogTablet.getId(), partition.getId(), e.getMessage());
            TabletHealth tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
            return new ChangelogHealthResult(tabletHealth, null, ChangelogRepairReason.NONE,
                    Maps.newHashMap(), getReplicaPathHashByBackend(changelogTablet));
        }
        Tablet baseTablet = tabletPair.getBaseTablet();
        changelogTablet = tabletPair.getChangelogTablet();

        Map<Long, Long> requiredDestPathHashByBackend = getEffectiveBaseReplicaPathByBackend(
                baseTablet, visibleVersion, false);
        Map<Long, Long> observedPathHashByBackend = getReplicaPathHashByBackend(changelogTablet);
        ChangelogRepairReason repairReason = ChangelogRepairReason.NONE;
        TabletHealth tabletHealth;
        if (requiredDestPathHashByBackend.isEmpty()) {
            tabletHealth = new TabletHealth();
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
        } else {
            tabletHealth = changelogTablet.getColocateHealth(
                    visibleVersion, replicaAlloc, requiredDestPathHashByBackend.keySet());
            if (tabletHealth.status == TabletStatus.COLOCATE_MISMATCH) {
                repairReason = ChangelogRepairReason.BACKEND_MISMATCH;
            } else if (tabletHealth.status == TabletStatus.COLOCATE_REDUNDANT) {
                repairReason = ChangelogRepairReason.REDUNDANT;
            } else if (tabletHealth.status == TabletStatus.HEALTHY
                    && hasWrongPathReplica(changelogTablet, requiredDestPathHashByBackend)) {
                tabletHealth.status = TabletStatus.COLOCATE_MISMATCH;
                repairReason = ChangelogRepairReason.PATH_MISMATCH;
            }
            if (tabletHealth.status != TabletStatus.HEALTHY
                    && tabletHealth.status != TabletStatus.UNRECOVERABLE
                    && tabletHealth.priority == Priority.NORMAL) {
                tabletHealth.priority = Priority.HIGH;
            }
        }
        return new ChangelogHealthResult(tabletHealth, baseTablet, repairReason,
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
        ChangelogTabletPair tabletPair = resolvePairForBaseTablet(partition, baseTablet);
        Map<Long, Long> preferredPathHashByBackend = Maps.newHashMap();
        if (tabletPair == null) {
            return preferredPathHashByBackend;
        }
        Tablet changelogTablet = tabletPair.getChangelogTablet();
        for (Replica replica : changelogTablet.getReplicas()) {
            if (!isEffectiveReplica(replica, visibleVersion, false, false)) {
                continue;
            }
            preferredPathHashByBackend.put(replica.getBackendIdWithoutException(), replica.getPathHash());
        }
        return preferredPathHashByBackend;
    }

    public static Tablet getChangelogTablet(Partition partition, Tablet baseTablet) {
        ChangelogTabletPair tabletPair = resolvePairForBaseTablet(partition, baseTablet);
        return tabletPair == null ? null : tabletPair.getChangelogTablet();
    }

    public static ChangelogTabletPair resolvePairForChangelogTablet(Partition partition, Tablet changelogTablet) {
        MaterializedIndex changelogIndex = getChangelogIndex(partition);
        if (changelogIndex == null) {
            throw invalidPair(partition, "changelog index does not exist");
        }
        Tablet indexedChangelogTablet = changelogIndex.getTablet(changelogTablet.getId());
        if (indexedChangelogTablet == null) {
            throw invalidPair(partition, "changelog tablet " + changelogTablet.getId() + " does not exist");
        }
        if (!indexedChangelogTablet.hasChangelogBaseTabletId()) {
            throw invalidPair(partition, "changelog tablet " + indexedChangelogTablet.getId()
                    + " has no base tablet link");
        }
        Tablet baseTablet = partition.getBaseIndex().getTablet(indexedChangelogTablet.getChangelogBaseTabletId());
        if (baseTablet == null) {
            throw invalidPair(partition, "base tablet " + indexedChangelogTablet.getChangelogBaseTabletId()
                    + " does not exist");
        }
        validateBidirectionalPair(partition, baseTablet, indexedChangelogTablet);
        return new ChangelogTabletPair(baseTablet, indexedChangelogTablet);
    }

    static ChangelogTabletPair resolvePairForBaseTablet(Partition partition, Tablet baseTablet) {
        MaterializedIndex changelogIndex = getChangelogIndex(partition);
        if (changelogIndex == null) {
            return null;
        }
        Tablet indexedBaseTablet = partition.getBaseIndex().getTablet(baseTablet.getId());
        if (indexedBaseTablet == null) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " does not exist");
        }
        if (!indexedBaseTablet.hasChangelogTabletId()) {
            throw invalidPair(partition, "base tablet " + indexedBaseTablet.getId()
                    + " has no changelog tablet link");
        }
        Tablet changelogTablet = changelogIndex.getTablet(indexedBaseTablet.getChangelogTabletId());
        if (changelogTablet == null) {
            throw invalidPair(partition, "changelog tablet " + indexedBaseTablet.getChangelogTabletId()
                    + " does not exist");
        }
        validateBidirectionalPair(partition, indexedBaseTablet, changelogTablet);
        return new ChangelogTabletPair(indexedBaseTablet, changelogTablet);
    }

    private static MaterializedIndex getChangelogIndex(Partition partition) {
        MaterializedIndex changelogIndex = null;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE, true)) {
            if (index.isChangelog()) {
                if (changelogIndex != null) {
                    throw invalidPair(partition, "multiple changelog indexes exist");
                }
                changelogIndex = index;
            }
        }
        return changelogIndex;
    }

    private static void validateBidirectionalPair(Partition partition, Tablet baseTablet, Tablet changelogTablet) {
        if (!baseTablet.hasChangelogTabletId()) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " has no changelog tablet link");
        }
        if (baseTablet.getChangelogTabletId() != changelogTablet.getId()) {
            throw invalidPair(partition, "base tablet " + baseTablet.getId() + " points to changelog tablet "
                    + baseTablet.getChangelogTabletId() + " instead of " + changelogTablet.getId());
        }
        if (!changelogTablet.hasChangelogBaseTabletId()) {
            throw invalidPair(partition, "changelog tablet " + changelogTablet.getId()
                    + " has no base tablet link");
        }
        if (changelogTablet.getChangelogBaseTabletId() != baseTablet.getId()) {
            throw invalidPair(partition, "changelog tablet " + changelogTablet.getId() + " points to base tablet "
                    + changelogTablet.getChangelogBaseTabletId() + " instead of " + baseTablet.getId());
        }
    }

    private static IllegalStateException invalidPair(Partition partition, String reason) {
        return new IllegalStateException("invalid changelog tablet pair in partition " + partition.getId()
                + ": " + reason);
    }

    public static int getCompletePairCount(Tablet baseTablet, Tablet changelogTablet, long visibleVersion,
            boolean requireSamePath) {
        int completePairCount = 0;
        for (Replica baseReplica : baseTablet.getReplicas()) {
            if (!isEffectiveReplica(baseReplica, visibleVersion, true, true)) {
                continue;
            }
            Replica changelogReplica = changelogTablet.getReplicaByBackendId(
                    baseReplica.getBackendIdWithoutException());
            if (changelogReplica == null || !isEffectiveReplica(changelogReplica, visibleVersion, true, false)) {
                continue;
            }
            if (requireSamePath && baseReplica.getPathHash() != changelogReplica.getPathHash()) {
                continue;
            }
            completePairCount++;
        }
        return completePairCount;
    }

    public static boolean isCompletePair(Replica baseReplica, Replica changelogReplica, long visibleVersion,
            boolean requireSamePath) {
        if (changelogReplica == null) {
            return false;
        }
        if (!isEffectiveReplica(baseReplica, visibleVersion, true, true)
                || !isEffectiveReplica(changelogReplica, visibleVersion, true, false)) {
            return false;
        }
        return !requireSamePath || baseReplica.getPathHash() == changelogReplica.getPathHash();
    }

    private static boolean hasWrongPathReplica(Tablet changelogTablet, Map<Long, Long> requiredPathHashByBackend) {
        for (Replica replica : changelogTablet.getReplicas()) {
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
            boolean skipChangelogMissing) {
        if (replica.isBad() || replica.tooSlow()) {
            return false;
        }
        if (skipChangelogMissing && replica.isChangelogMissing()) {
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
