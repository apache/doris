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

package org.apache.doris.catalog;

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * This class represents the olap tablet related metadata.
 */
public class Tablet extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);
    // if current version count of replica is mor than
    // QUERYABLE_TIMES_OF_MIN_VERSION_COUNT times the minimum version count,
    // then the replica would not be considered as queryable.
    private static final int QUERYABLE_TIMES_OF_MIN_VERSION_COUNT = 3;

    public enum TabletStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num.
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing.
        REPLICA_RELOCATING, // replica is healthy, but is under relocating (eg. BE is decommission).
        REDUNDANT, // too much replicas.
        REPLICA_MISSING_FOR_TAG, // not enough healthy replicas in backend with specified tag.
        FORCE_REDUNDANT, // some replica is missing or bad, but there is no other backends for repair,
        // at least one replica has to be deleted first to make room for new replica.
        COLOCATE_MISMATCH, // replicas do not all locate in right colocate backends set.
        COLOCATE_REDUNDANT, // replicas match the colocate backends set, but redundant.
        NEED_FURTHER_REPAIR, // one of replicas need a definite repair.
        UNRECOVERABLE,   // none of replicas are healthy
        REPLICA_COMPACTION_TOO_SLOW // one replica's version count is much more than other replicas;
    }

    public static class TabletHealth {
        public TabletStatus status;
        public TabletSchedCtx.Priority priority;

        // num of alive replica with version complete
        public int aliveAndVersionCompleteNum;

        // NEED_FURTHER_REPAIR replica id
        public long needFurtherRepairReplicaId;

        // has alive replica with version incomplete, prior to repair these replica
        public boolean hasAliveAndVersionIncomplete;

        // this tablet recent write failed, then increase its sched priority
        public boolean hasRecentLoadFailed;

        // this tablet want to add new replica, but not found target backend.
        public boolean noPathForNewReplica;

        public TabletHealth() {
            status = null; // don't set for balance task
            priority = TabletSchedCtx.Priority.NORMAL;
            aliveAndVersionCompleteNum = 0;
            needFurtherRepairReplicaId = -1L;
            hasAliveAndVersionIncomplete = false;
            hasRecentLoadFailed = false;
            noPathForNewReplica = false;
        }
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "replicas")
    private List<Replica> replicas;
    @SerializedName(value = "checkedVersion")
    private long checkedVersion;
    @Deprecated
    @SerializedName(value = "checkedVersionHash")
    private long checkedVersionHash;
    @SerializedName(value = "isConsistent")
    private boolean isConsistent;

    // cooldown conf
    @SerializedName(value = "cooldownReplicaId")
    private long cooldownReplicaId = -1;
    @SerializedName(value = "cooldownTerm")
    private long cooldownTerm = -1;
    private MonitoredReentrantReadWriteLock cooldownConfLock = new MonitoredReentrantReadWriteLock();

    // last time that the tablet checker checks this tablet.
    // no need to persist
    private long lastStatusCheckTime = -1;

    // last time for load data fail
    private long lastLoadFailedTime = -1;

    // if tablet want to add a new replica, but cann't found any backend to locate the new replica.
    // then mark this tablet. For later repair, even try and try to repair this tablet, sched will always fail.
    // For example, 1 tablet contains 3 replicas, if 1 backend is dead, then tablet's healthy status
    // is REPLICA_MISSING. But since no other backend can held the new replica, then sched always fail.
    // So don't increase this tablet's sched priority if it has no path for new replica.
    private long lastTimeNoPathForNewReplica = -1;

    public Tablet() {
        this(0L, new ArrayList<>());
    }

    public Tablet(long tabletId) {
        this(tabletId, new ArrayList<>());
    }

    public Tablet(long tabletId, List<Replica> replicas) {
        this.id = tabletId;
        this.replicas = replicas;
        if (this.replicas == null) {
            this.replicas = new ArrayList<>();
        }

        checkedVersion = -1L;

        isConsistent = true;
    }

    public void setIdForRestore(long tabletId) {
        this.id = tabletId;
    }

    public long getId() {
        return this.id;
    }

    public long getCheckedVersion() {
        return this.checkedVersion;
    }

    public void setCheckedVersion(long checkedVersion) {
        this.checkedVersion = checkedVersion;
    }

    public void setIsConsistent(boolean good) {
        this.isConsistent = good;
    }

    public boolean isConsistent() {
        return isConsistent;
    }

    public void setCooldownConf(long cooldownReplicaId, long cooldownTerm) {
        cooldownConfLock.writeLock().lock();
        this.cooldownReplicaId = cooldownReplicaId;
        this.cooldownTerm = cooldownTerm;
        cooldownConfLock.writeLock().unlock();
    }

    public long getCooldownReplicaId() {
        return cooldownReplicaId;
    }

    public Pair<Long, Long> getCooldownConf() {
        cooldownConfLock.readLock().lock();
        try {
            return Pair.of(cooldownReplicaId, cooldownTerm);
        } finally {
            cooldownConfLock.readLock().unlock();
        }
    }

    private boolean deleteRedundantReplica(long backendId, long version) {
        boolean delete = false;
        boolean hasBackend = false;
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendId() == backendId) {
                hasBackend = true;
                if (replica.getVersion() <= version) {
                    iterator.remove();
                    delete = true;
                }
            }
        }

        return delete || !hasBackend;
    }

    public void addReplica(Replica replica, boolean isRestore) {
        if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
            replicas.add(replica);
            if (!isRestore) {
                Env.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

    public void addReplica(Replica replica) {
        addReplica(replica, false);
    }

    public List<Replica> getReplicas() {
        return this.replicas;
    }

    public Set<Long> getBackendIds() {
        Set<Long> beIds = Sets.newHashSet();
        for (Replica replica : replicas) {
            beIds.add(replica.getBackendId());
        }
        return beIds;
    }

    public List<Long> getNormalReplicaBackendIds() {
        return Lists.newArrayList(getNormalReplicaBackendPathMap().keySet());
    }

    // return map of (BE id -> path hash) of normal replicas
    // for load plan.
    public Multimap<Long, Long> getNormalReplicaBackendPathMap() {
        Multimap<Long, Long> map = HashMultimap.create();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            if (!infoService.checkBackendAlive(replica.getBackendId())) {
                continue;
            }

            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (state.canLoad()
                    || (state == ReplicaState.DECOMMISSION
                            && replica.getPostWatermarkTxnId() < 0
                            && replica.getLastFailedVersion() < 0)) {
                map.put(replica.getBackendId(), replica.getPathHash());
            }
        }
        return map;
    }

    // for query
    public List<Replica> getQueryableReplicas(long visibleVersion, Map<Long, Set<Long>> backendAlivePathHashs,
            boolean allowFailedVersion) {
        List<Replica> allQueryableReplica = Lists.newArrayListWithCapacity(replicas.size());
        List<Replica> auxiliaryReplica = Lists.newArrayListWithCapacity(replicas.size());
        List<Replica> deadPathReplica = Lists.newArrayList();
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            // Skip the missing version replica
            if (replica.getLastFailedVersion() > 0 && !allowFailedVersion) {
                continue;
            }

            if (!replica.checkVersionCatchUp(visibleVersion, false)) {
                continue;
            }

            Set<Long> thisBeAlivePaths = backendAlivePathHashs.get(replica.getBackendId());
            ReplicaState state = replica.getState();
            // if thisBeAlivePaths contains pathHash = 0, it mean this be hadn't report disks state.
            // should ignore this case.
            if (replica.getPathHash() != -1 && thisBeAlivePaths != null
                    && !thisBeAlivePaths.contains(replica.getPathHash())
                    && !thisBeAlivePaths.contains(0L)) {
                deadPathReplica.add(replica);
            } else if (state.canQuery()) {
                allQueryableReplica.add(replica);
            } else if (state == ReplicaState.DECOMMISSION) {
                auxiliaryReplica.add(replica);
            }
        }

        if (allQueryableReplica.isEmpty()) {
            allQueryableReplica = auxiliaryReplica;
        }
        if (allQueryableReplica.isEmpty()) {
            allQueryableReplica = deadPathReplica;
        }

        if (Config.skip_compaction_slower_replica && allQueryableReplica.size() > 1) {
            long minVersionCount = allQueryableReplica.stream().mapToLong(Replica::getVisibleVersionCount)
                    .filter(count -> count != -1).min().orElse(Long.MAX_VALUE);
            long maxVersionCount = Config.min_version_count_indicate_replica_compaction_too_slow;
            if (minVersionCount != Long.MAX_VALUE) {
                maxVersionCount = Math.max(maxVersionCount, minVersionCount * QUERYABLE_TIMES_OF_MIN_VERSION_COUNT);
            }

            final long finalMaxVersionCount = maxVersionCount;
            return allQueryableReplica.stream()
                    .filter(replica -> replica.getVisibleVersionCount() < finalMaxVersionCount)
                    .collect(Collectors.toList());
        }
        return allQueryableReplica;
    }

    public String getDetailsStatusForQuery(long visibleVersion) {
        StringBuilder sb = new StringBuilder("Visible Replicas:");
        sb.append("Visible version: ").append(visibleVersion);
        sb.append(", Replicas: ");
        sb.append(Joiner.on(", ").join(replicas.stream().map(replica -> replica.toStringSimple(true))
                .collect(Collectors.toList())));
        sb.append(".");

        return sb.toString();
    }

    public Replica getReplicaById(long replicaId) {
        for (Replica replica : replicas) {
            if (replica.getId() == replicaId) {
                return replica;
            }
        }
        return null;
    }

    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) {
            if (replica.getBackendId() == backendId) {
                return replica;
            }
        }
        return null;
    }

    public boolean deleteReplica(Replica replica) {
        if (replicas.contains(replica)) {
            replicas.remove(replica);
            Env.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendId());
            return true;
        }
        return false;
    }

    public boolean deleteReplicaByBackendId(long backendId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendId() == backendId) {
                iterator.remove();
                Env.getCurrentInvertedIndex().deleteReplica(id, backendId);
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public Replica deleteReplicaById(long replicaId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getId() == replicaId) {
                LOG.info("delete replica[" + replica.getId() + "]");
                iterator.remove();
                return replica;
            }
        }
        return null;
    }

    // for test,
    // and for some replay cases
    public void clearReplica() {
        this.replicas.clear();
    }

    public void setTabletId(long tabletId) {
        this.id = tabletId;
    }

    public static void sortReplicaByVersionDesc(List<Replica> replicas) {
        // sort replicas by version. higher version in the tops
        replicas.sort(Replica.VERSION_DESC_COMPARATOR);
    }

    @Override
    public String toString() {
        return "tabletId=" + this.id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        int replicaCount = in.readInt();
        for (int i = 0; i < replicaCount; ++i) {
            Replica replica = Replica.read(in);
            if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
                replicas.add(replica);
            }
        }

        checkedVersion = in.readLong();
        checkedVersionHash = in.readLong();
        isConsistent = in.readBoolean();
    }

    public static Tablet read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_115) {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, Tablet.class);
        }

        Tablet tablet = new Tablet();
        tablet.readFields(in);
        return tablet;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Tablet)) {
            return false;
        }

        Tablet tablet = (Tablet) obj;

        if (replicas != tablet.replicas) {
            if (replicas.size() != tablet.replicas.size()) {
                return false;
            }
            int size = replicas.size();
            for (int i = 0; i < size; i++) {
                if (!tablet.replicas.contains(replicas.get(i))) {
                    return false;
                }
            }
        }
        return id == tablet.id;
    }

    public long getDataSize(boolean singleReplica) {
        LongStream s = replicas.stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .mapToLong(Replica::getDataSize);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
    }

    public long getRemoteDataSize() {
        // if CooldownReplicaId is not init
        if (cooldownReplicaId <= 0) {
            return 0;
        }
        for (Replica r : replicas) {
            if (r.getId() == cooldownReplicaId) {
                return r.getRemoteDataSize();
            }
        }
        // return replica with max remoteDataSize
        return replicas.stream().max(Comparator.comparing(Replica::getRemoteDataSize)).get().getRemoteDataSize();
    }

    public long getRowCount(boolean singleReplica) {
        LongStream s = replicas.stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .mapToLong(Replica::getRowCount);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
    }

    /**
     * A replica is healthy only if
     * 1. the backend is available
     * 2. replica version is caught up, and last failed version is -1
     * <p>
     * A tablet is healthy only if
     * 1. healthy replica num is equal to replicationNum
     * 2. all healthy replicas are in right tag
     */
    public TabletHealth getHealth(SystemInfoService systemInfoService,
            long visibleVersion, ReplicaAllocation replicaAlloc, List<Long> aliveBeIds) {
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        Map<Tag, Short> stableAllocMap = Maps.newHashMap();
        Map<Tag, Short> stableVersionCompleteAllocMap = Maps.newHashMap();

        short replicationNum = replicaAlloc.getTotalReplicaNum();
        int alive = 0;
        int aliveAndVersionComplete = 0;
        int stable = 0;

        Replica needFurtherRepairReplica = null;
        boolean hasAliveAndVersionIncomplete = false;
        Set<String> hosts = Sets.newHashSet();
        ArrayList<Long> versions = new ArrayList<>();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (!isReplicaAndBackendAlive(replica, backend, hosts)) {
                continue;
            }

            alive++;

            boolean versionCompleted = replica.getLastFailedVersion() < 0 && replica.getVersion() >= visibleVersion;
            if (versionCompleted) {
                aliveAndVersionComplete++;
            }

            if (replica.isScheduleAvailable()) {
                if (replica.needFurtherRepair() && (needFurtherRepairReplica == null || !versionCompleted)) {
                    needFurtherRepairReplica = replica;
                }

                short allocNum = stableAllocMap.getOrDefault(backend.getLocationTag(), (short) 0);
                stableAllocMap.put(backend.getLocationTag(), (short) (allocNum + 1));

                if (versionCompleted) {
                    stable++;
                    versions.add(replica.getVisibleVersionCount());

                    allocNum = stableVersionCompleteAllocMap.getOrDefault(backend.getLocationTag(), (short) 0);
                    stableVersionCompleteAllocMap.put(backend.getLocationTag(), (short) (allocNum + 1));
                } else {
                    hasAliveAndVersionIncomplete = true;
                }
            }
        }

        TabletHealth tabletHealth = new TabletHealth();
        initTabletHealth(tabletHealth);
        tabletHealth.aliveAndVersionCompleteNum = aliveAndVersionComplete;
        tabletHealth.hasAliveAndVersionIncomplete = hasAliveAndVersionIncomplete;
        if (needFurtherRepairReplica != null) {
            tabletHealth.needFurtherRepairReplicaId = needFurtherRepairReplica.getId();
        }

        // 0. We can not choose a good replica as src to repair this tablet.
        if (aliveAndVersionComplete == 0) {
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
            return tabletHealth;
        } else if (aliveAndVersionComplete < replicationNum && hasAliveAndVersionIncomplete) {
            // not enough good replica, and there exists schedule available replicas and  version incomplete,
            // no matter whether they tag is proper right, fix them immediately.
            tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
            tabletHealth.priority = TabletSchedCtx.Priority.VERY_HIGH;
            return tabletHealth;
        }

        // 1. alive replicas are not enough
        int aliveBackendsNum = aliveBeIds.size();
        if (alive < replicationNum && replicas.size() >= aliveBackendsNum
                && aliveBackendsNum >= replicationNum && replicationNum > 1) {
            // there is no enough backend for us to create a new replica, so we have to delete an existing replica,
            // so there can be available backend for us to create a new replica.
            // And if there is only one replica, we will not handle it(maybe need human interference)
            // condition explain:
            // 1. alive < replicationNum: replica is missing or bad
            // 2. replicas.size() >= aliveBackendsNum: the existing replicas occupies all available backends
            // 3. aliveBackendsNum >= replicationNum: make sure after deleting,
            //    there will be at least one backend for new replica.
            // 4. replicationNum > 1: if replication num is set to 1, do not delete any replica, for safety reason
            tabletHealth.status = TabletStatus.FORCE_REDUNDANT;
            tabletHealth.priority = TabletSchedCtx.Priority.VERY_HIGH;
            return tabletHealth;
        } else if (alive < replicationNum) {
            tabletHealth.status = TabletStatus.REPLICA_MISSING;
            tabletHealth.priority = alive < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.VERY_HIGH
                    : TabletSchedCtx.Priority.NORMAL;
            return tabletHealth;
        }

        // 2. version complete replicas are not enough
        if (aliveAndVersionComplete < replicationNum) {
            tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
            tabletHealth.priority = alive < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.HIGH
                    : TabletSchedCtx.Priority.NORMAL;
            return tabletHealth;
        } else if (aliveAndVersionComplete > replicationNum) {
            if (needFurtherRepairReplica != null) {
                tabletHealth.status = TabletStatus.NEED_FURTHER_REPAIR;
                tabletHealth.priority = TabletSchedCtx.Priority.HIGH;
            } else {
                // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
                tabletHealth.status = TabletStatus.REDUNDANT;
                tabletHealth.priority = TabletSchedCtx.Priority.VERY_HIGH;
            }
            return tabletHealth;
        }

        // 3. replica is under relocating
        if (stable < replicationNum) {
            Set<Long> replicaBeIds = replicas.stream().map(Replica::getBackendId).collect(Collectors.toSet());
            List<Long> availableBeIds = aliveBeIds.stream().filter(systemInfoService::checkBackendScheduleAvailable)
                    .collect(Collectors.toList());
            if (replicaBeIds.containsAll(availableBeIds)
                    && availableBeIds.size() >= replicationNum
                    && replicationNum > 1) { // No BE can be choose to create a new replica
                tabletHealth.status = TabletStatus.FORCE_REDUNDANT;
                tabletHealth.priority = stable < (replicationNum / 2) + 1
                                ? TabletSchedCtx.Priority.NORMAL : TabletSchedCtx.Priority.LOW;
                return tabletHealth;
            }

            if (stable < replicationNum) {
                tabletHealth.status = TabletStatus.REPLICA_RELOCATING;
                tabletHealth.priority = stable < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.NORMAL
                        : TabletSchedCtx.Priority.LOW;
                return tabletHealth;
            }
        }

        // 4. got enough healthy replicas, check tag
        for (Map.Entry<Tag, Short> alloc : allocMap.entrySet()) {
            if (stableVersionCompleteAllocMap.getOrDefault(alloc.getKey(), (short) 0) < alloc.getValue()) {
                if (stableAllocMap.getOrDefault(alloc.getKey(), (short) 0) >= alloc.getValue()) {
                    tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
                } else {
                    tabletHealth.status = TabletStatus.REPLICA_MISSING_FOR_TAG;
                }
                tabletHealth.priority = TabletSchedCtx.Priority.NORMAL;
                return tabletHealth;
            }
        }

        if (replicas.size() > replicationNum) {
            if (needFurtherRepairReplica != null) {
                tabletHealth.status = TabletStatus.NEED_FURTHER_REPAIR;
                tabletHealth.priority = TabletSchedCtx.Priority.HIGH;
            } else {
                // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
                tabletHealth.status = TabletStatus.REDUNDANT;
                tabletHealth.priority = TabletSchedCtx.Priority.VERY_HIGH;
            }
            return tabletHealth;
        }

        // 5. find a replica's version count is much more than others, and drop it
        if (Config.repair_slow_replica && versions.size() == replicas.size() && versions.size() > 1) {
            // sort version
            Collections.sort(versions);
            // get the max version diff
            long delta = versions.get(versions.size() - 1) - versions.get(0);
            double ratio = (double) delta / versions.get(versions.size() - 1);
            if (versions.get(versions.size() - 1) >= Config.min_version_count_indicate_replica_compaction_too_slow
                    && ratio > Config.valid_version_count_delta_ratio_between_replicas) {
                tabletHealth.status = TabletStatus.REPLICA_COMPACTION_TOO_SLOW;
                tabletHealth.priority = Priority.HIGH;
                return tabletHealth;
            }
        }

        // 6. healthy
        tabletHealth.status = TabletStatus.HEALTHY;
        tabletHealth.priority = TabletSchedCtx.Priority.NORMAL;

        return tabletHealth;
    }

    private void initTabletHealth(TabletHealth tabletHealth) {
        long endTime = System.currentTimeMillis() - Config.tablet_recent_load_failed_second * 1000L;
        tabletHealth.hasRecentLoadFailed = lastLoadFailedTime > endTime;
        tabletHealth.noPathForNewReplica = lastTimeNoPathForNewReplica > endTime;
    }

    private boolean isReplicaAndBackendAlive(Replica replica, Backend backend, Set<String> hosts) {
        if (backend == null || !backend.isAlive() || !replica.isAlive()
                || checkHost(hosts, backend) || replica.tooSlow() || !backend.isMixNode()) {
            // this replica is not alive,
            // or if this replica is on same host with another replica, we also treat it as 'dead',
            // so that Tablet Scheduler will create a new replica on different host.
            // ATTN: Replicas on same host is a bug of previous Doris version, so we fix it by this way.
            return false;
        } else {
            return true;
        }
    }

    private boolean checkHost(Set<String> hosts, Backend backend) {
        return !Config.allow_replica_on_same_host && !FeConstants.runningUnitTest && !hosts.add(backend.getHost());
    }

    /**
     * Check colocate table's tablet health
     * 1. Mismatch:
     *      backends set:       1,2,3
     *      tablet replicas:    1,2,5
     *
     *      backends set:       1,2,3
     *      tablet replicas:    1,2
     *
     *      backends set:       1,2,3
     *      tablet replicas:    1,2,4,5
     *
     * 2. Version incomplete:
     *      backend matched, but some replica(in backends set)'s version is incomplete
     *
     * 3. Redundant:
     *      backends set:       1,2,3
     *      tablet replicas:    1,2,3,4
     *
     * No need to check if backend is available. We consider all backends in 'backendsSet' are available,
     * If not, unavailable backends will be relocated by CalocateTableBalancer first.
     */
    public TabletHealth getColocateHealth(long visibleVersion,
            ReplicaAllocation replicaAlloc, Set<Long> backendsSet) {
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        short replicationNum = replicaAlloc.getTotalReplicaNum();
        boolean hasAliveAndVersionIncomplete = false;
        int aliveAndVersionComplete = 0;
        Set<String> hosts = Sets.newHashSet();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (!isReplicaAndBackendAlive(replica, backend, hosts)) {
                continue;
            }

            boolean versionCompleted = replica.getLastFailedVersion() < 0 && replica.getVersion() >= visibleVersion;
            if (versionCompleted) {
                aliveAndVersionComplete++;
            }

            if (replica.isScheduleAvailable()) {
                if (!versionCompleted) {
                    hasAliveAndVersionIncomplete = true;
                }
            }
        }

        TabletHealth tabletHealth = new TabletHealth();
        initTabletHealth(tabletHealth);
        tabletHealth.aliveAndVersionCompleteNum = aliveAndVersionComplete;
        tabletHealth.hasAliveAndVersionIncomplete = hasAliveAndVersionIncomplete;
        tabletHealth.priority = TabletSchedCtx.Priority.NORMAL;

        // 0. We can not choose a good replica as src to repair this tablet.
        if (aliveAndVersionComplete == 0) {
            tabletHealth.status = TabletStatus.UNRECOVERABLE;
            return tabletHealth;
        } else if (aliveAndVersionComplete < replicationNum && hasAliveAndVersionIncomplete) {
            // not enough good replica, and there exists schedule available replicas and  version incomplete,
            // no matter whether they tag is proper right, fix them immediately.
            tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
            tabletHealth.priority = TabletSchedCtx.Priority.VERY_HIGH;
            return tabletHealth;
        }

        // Here we don't need to care about tag. Because the replicas of the colocate table has been confirmed
        // in ColocateTableCheckerAndBalancer.
        Short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
        // 1. check if replicas' backends are mismatch
        //    There is no REPLICA_MISSING status for colocate table's tablet.
        //    Because if the following check doesn't pass, the COLOCATE_MISMATCH will return.
        Set<Long> replicaBackendIds = getBackendIds();
        if (!replicaBackendIds.containsAll(backendsSet)) {
            tabletHealth.status = TabletStatus.COLOCATE_MISMATCH;
            return tabletHealth;
        }

        // 2. check version completeness
        for (Replica replica : replicas) {
            if (!backendsSet.contains(replica.getBackendId())) {
                // We don't care about replicas that are not in backendsSet.
                // eg:  replicaBackendIds=(1,2,3,4); backendsSet=(1,2,3),
                //      then replica 4 should be skipped here and then goto ```COLOCATE_REDUNDANT``` in step 3
                continue;
            }

            if (!replica.isAlive()) {
                if (replica.isBad()) {
                    // If this replica is bad but located on one of backendsSet,
                    // we have drop it first, or we can find any other BE for new replica.
                    tabletHealth.status = TabletStatus.COLOCATE_REDUNDANT;
                } else {
                    // maybe in replica's DECOMMISSION state
                    // Here we return VERSION_INCOMPLETE,
                    // and the tablet scheduler will finally set it's state to NORMAL.
                    tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
                }
                return tabletHealth;
            }

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                tabletHealth.status = TabletStatus.VERSION_INCOMPLETE;
                return tabletHealth;
            }
        }

        // 3. check redundant
        if (replicas.size() > totalReplicaNum) {
            tabletHealth.status = TabletStatus.COLOCATE_REDUNDANT;
            return tabletHealth;
        }

        tabletHealth.status = TabletStatus.HEALTHY;
        return tabletHealth;
    }

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORMAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    public boolean readyToBeRepaired(SystemInfoService infoService, TabletSchedCtx.Priority priority) {
        if (FeConstants.runningUnitTest) {
            return true;
        }

        if (priority == Priority.VERY_HIGH) {
            return true;
        }

        boolean allBeAliveOrDecommissioned = true;
        for (Replica replica : replicas) {
            Backend backend = infoService.getBackend(replica.getBackendId());
            if (backend == null || (!backend.isAlive() && !backend.isDecommissioned())) {
                allBeAliveOrDecommissioned = false;
                break;
            }
        }

        if (allBeAliveOrDecommissioned) {
            return true;
        }

        long currentTime = System.currentTimeMillis();

        // first check, wait for next round
        if (lastStatusCheckTime == -1) {
            lastStatusCheckTime = currentTime;
            return false;
        }

        boolean ready = false;
        switch (priority) {
            case HIGH:
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000 * 1;
                break;
            case NORMAL:
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000 * 2;
                break;
            case LOW:
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000 * 3;
                break;
            default:
                break;
        }

        return ready;
    }

    public void setLastStatusCheckTime(long lastStatusCheckTime) {
        this.lastStatusCheckTime = lastStatusCheckTime;
    }

    public long getLastLoadFailedTime() {
        return lastLoadFailedTime;
    }

    public void setLastLoadFailedTime(long lastLoadFailedTime) {
        this.lastLoadFailedTime = lastLoadFailedTime;
    }

    public void setLastTimeNoPathForNewReplica(long lastTimeNoPathForNewReplica) {
        this.lastTimeNoPathForNewReplica = lastTimeNoPathForNewReplica;
    }
}
