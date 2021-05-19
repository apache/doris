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
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents the olap tablet related metadata.
 */
public class Tablet extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);
    
    public enum TabletStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num.
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing.
        REPLICA_RELOCATING, // replica is healthy, but is under relocating (eg. BE is decommission).
        REDUNDANT, // too much replicas.
        REPLICA_MISSING_IN_CLUSTER, // not enough healthy replicas in correct cluster.
        FORCE_REDUNDANT, // some replica is missing or bad, but there is no other backends for repair,
                         // at least one replica has to be deleted first to make room for new replica.
        COLOCATE_MISMATCH, // replicas do not all locate in right colocate backends set.
        COLOCATE_REDUNDANT, // replicas match the colocate backends set, but redundant.
        NEED_FURTHER_REPAIR, // one of replicas need a definite repair.
        UNRECOVERABLE   // non of replicas are healthy
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "replicas")
    private List<Replica> replicas;
    @SerializedName(value = "checkedVersion")
    private long checkedVersion;
    @SerializedName(value = "checkedVersionHash")
    private long checkedVersionHash;
    @SerializedName(value = "isConsistent")
    private boolean isConsistent;

    // last time that the tablet checker checks this tablet.
    // no need to persist
    private long lastStatusCheckTime = -1;
    
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
        checkedVersionHash = -1L;

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

    public long getCheckedVersionHash() {
        return this.checkedVersionHash;
    }

    public void setCheckedVersion(long checkedVersion, long checkedVersionHash) {
        this.checkedVersion = checkedVersion;
        this.checkedVersionHash = checkedVersionHash;
    }

    public void setIsConsistent(boolean good) {
        this.isConsistent = good;
    }

    public boolean isConsistent() {
        return isConsistent;
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
                Catalog.getCurrentInvertedIndex().addReplica(id, replica);
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

    // for loading data
    public List<Long> getNormalReplicaBackendIds() {
        List<Long> beIds = Lists.newArrayList();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }
            
            ReplicaState state = replica.getState();
            if (infoService.checkBackendAlive(replica.getBackendId()) && state.canLoad()) {
                beIds.add(replica.getBackendId());
            }
        }
        return beIds;
    }

    // return map of (BE id -> path hash) of normal replicas
    public Multimap<Long, Long> getNormalReplicaBackendPathMap() {
        Multimap<Long, Long> map = HashMultimap.create();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (infoService.checkBackendAlive(replica.getBackendId())
                    && (state == ReplicaState.NORMAL || state == ReplicaState.ALTER)) {
                map.put(replica.getBackendId(), replica.getPathHash());
            }
        }
        return map;
    }

    // for query
    public void getQueryableReplicas(List<Replica> allQuerableReplica, List<Replica> localReplicas,
            long visibleVersion, long visibleVersionHash, long localBeId, int schemaHash) {
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            // Skip the missing version replica
            if (replica.getLastFailedVersion() > 0) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (state.canQuery()) {
                // replica.getSchemaHash() == -1 is for compatibility
                if (replica.checkVersionCatchUp(visibleVersion, visibleVersionHash, false)
                        && (replica.getSchemaHash() == -1 || replica.getSchemaHash() == schemaHash)) {
                    allQuerableReplica.add(replica);
                    if (localBeId != -1 && replica.getBackendId() == localBeId) {
                        localReplicas.add(replica);
                    }
                }
            }
        }
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
            Catalog.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendId());
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
                Catalog.getCurrentInvertedIndex().deleteReplica(id, backendId);
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
        super.write(out);

        out.writeLong(id);
        int replicaCount = replicas.size();
        out.writeInt(replicaCount);
        for (int i = 0; i < replicaCount; ++i) {
            replicas.get(i).write(out);
        }

        out.writeLong(checkedVersion);
        out.writeLong(checkedVersionHash);
        out.writeBoolean(isConsistent);
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

        if (Catalog.getCurrentCatalogJournalVersion() >= 6) {
            checkedVersion = in.readLong();
            checkedVersionHash = in.readLong();
            isConsistent = in.readBoolean();
        }
    }
    
    public static Tablet read(DataInput in) throws IOException {
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
        long dataSize = 0;
        int count = 0;
        for (Replica replica : getReplicas()) {
            if (replica.getState() == ReplicaState.NORMAL
                    || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                dataSize += replica.getDataSize();
                count++;
            }
        }
        if (count == 0) {
            return 0;
        }

        if (singleReplica) {
            // get the avg replica size
            dataSize /= count;
        }

        return dataSize;
    }

    /**
     * A replica is healthy only if
     * 1. the backend is available
     * 2. replica version is caught up, and last failed version is -1
     *
     * A tablet is healthy only if
     * 1. healthy replica num is equal to replicationNum
     * 2. all healthy replicas are in right cluster
     */
    public Pair<TabletStatus, TabletSchedCtx.Priority> getHealthStatusWithPriority(
            SystemInfoService systemInfoService, String clusterName,
            long visibleVersion, long visibleVersionHash, ReplicaAllocation replicaAlloc,
            List<Long> aliveBeIdsInCluster) {

        // FIXME(cmy): should be aware of tag info
        short replicationNum = replicaAlloc.getTotalReplicaNum();
        int alive = 0;
        int aliveAndVersionComplete = 0;
        int stable = 0;
        int availableInCluster = 0;

        Replica needFurtherRepairReplica = null;
        Set<String> hosts = Sets.newHashSet();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (backend == null || !backend.isAlive() || replica.getState() == ReplicaState.CLONE
                    || replica.getState() == ReplicaState.DECOMMISSION
                    || replica.isBad() || !hosts.add(backend.getHost())) {
                // this replica is not alive,
                // or if this replica is on same host with another replica, we also treat it as 'dead',
                // so that Tablet Scheduler will create a new replica on different host.
                // ATTN: Replicas on same host is a bug of previous Doris version, so we fix it by this way.
                continue;
            }
            alive++;

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                continue;
            }
            aliveAndVersionComplete++;

            if (!backend.isAvailable()) {
                // this replica is alive, version complete, but backend is not available
                continue;
            }
            stable++;

            if (!backend.getOwnerClusterName().equals(clusterName)) {
                // this replica is available, version complete, but not in right cluster
                continue;
            }
            availableInCluster++;

            if (replica.needFurtherRepair() && needFurtherRepairReplica == null) {
                needFurtherRepairReplica = replica;
            }
        }

        // 1. alive replicas are not enough
        int aliveBackendsNum = aliveBeIdsInCluster.size();
        if (alive == 0) {
            return Pair.create(TabletStatus.UNRECOVERABLE, Priority.VERY_HIGH);
        } else if (alive < replicationNum && replicas.size() >= aliveBackendsNum
                && aliveBackendsNum >= replicationNum && replicationNum > 1) {
            // there is no enough backend for us to create a new replica, so we have to delete an existing replica,
            // so there can be available backend for us to create a new replica.
            // And if there is only one replica, we will not handle it(maybe need human interference)
            // condition explain:
            // 1. alive < replicationNum: replica is missing or bad
            // 2. replicas.size() >= aliveBackendsNum: the existing replicas occupies all available backends
            // 3. aliveBackendsNum >= replicationNum: make sure after deleting, there will be at least one backend for new replica.
            // 4. replicationNum > 1: if replication num is set to 1, do not delete any replica, for safety reason
            return Pair.create(TabletStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        } else if (alive < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.HIGH);
        } else if (alive < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.NORMAL);
        }

        // 2. version complete replicas are not enough
        if (aliveAndVersionComplete == 0) {
            return Pair.create(TabletStatus.UNRECOVERABLE, Priority.VERY_HIGH);
        } else if (aliveAndVersionComplete < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.HIGH);
        } else if (aliveAndVersionComplete < replicationNum) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.NORMAL);
        } else if (aliveAndVersionComplete > replicationNum) {
            if (needFurtherRepairReplica != null) {
                return Pair.create(TabletStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
            }
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return Pair.create(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        }

        // 3. replica is under relocating
        if (stable < replicationNum) {
            List<Long> replicaBeIds = replicas.stream()
                    .map(Replica::getBackendId).collect(Collectors.toList());
            List<Long> availableBeIds = aliveBeIdsInCluster.stream()
                    .filter(systemInfoService::checkBackendAvailable)
                    .collect(Collectors.toList());
            if (replicaBeIds.containsAll(availableBeIds)
                    && availableBeIds.size() >= replicationNum
                    && replicationNum > 1) { // No BE can be choose to create a new replica
                return Pair.create(TabletStatus.FORCE_REDUNDANT,
                        stable < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.NORMAL : TabletSchedCtx.Priority.LOW);
            }
            if (stable < (replicationNum / 2) + 1) {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.NORMAL);
            } else if (stable < replicationNum) {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.LOW);
            }
        }

        // 4. healthy replicas in cluster are not enough
        if (availableInCluster < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING_IN_CLUSTER, TabletSchedCtx.Priority.LOW);
        } else if (replicas.size() > replicationNum) {
            if (needFurtherRepairReplica != null) {
                return Pair.create(TabletStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
            }
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return Pair.create(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        }

        // 5. healthy
        return Pair.create(TabletStatus.HEALTHY, TabletSchedCtx.Priority.NORMAL);
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
    public TabletStatus getColocateHealthStatus(long visibleVersion, ReplicaAllocation replicaAlloc, Set<Long> backendsSet) {
        // FIXME(cmy): need to be aware of the tag info
        Short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
        // 1. check if replicas' backends are mismatch
        Set<Long> replicaBackendIds = getBackendIds();
        if (!replicaBackendIds.containsAll(backendsSet)) {
            return TabletStatus.COLOCATE_MISMATCH;
        }

        // 2. check version completeness
        for (Replica replica : replicas) {
            if (!backendsSet.contains(replica.getBackendId())) {
                // We don't care about replicas that are not in backendsSet.
                // eg:  replicaBackendIds=(1,2,3,4); backendsSet=(1,2,3), then replica 4 should be skipped here and then goto ```COLOCATE_REDUNDANT``` in step 3
                continue;
            }
            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                return TabletStatus.VERSION_INCOMPLETE;
            }
        }

        // 3. check redundant
        if (replicas.size() > totalReplicaNum) {
            return TabletStatus.COLOCATE_REDUNDANT;
        }

        return TabletStatus.HEALTHY;
    }

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORMAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    public boolean readyToBeRepaired(TabletSchedCtx.Priority priority) {
        if (priority == Priority.VERY_HIGH) {
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
}
