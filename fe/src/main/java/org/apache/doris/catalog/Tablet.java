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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class represents the olap tablet related metadata.
 */
public class Tablet extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);
    
    public enum TabletStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing
        REDUNDANT, // too much replicas
        REPLICA_MISSING_IN_CLUSTER, // not enough healthy replicas in correct cluster
    }

    private long id;
    private List<Replica> replicas;

    private long checkedVersion;
    private long checkedVersionHash;

    private boolean isConsistent;

    // last time that the tablet checker checks this tablet.
    // no need to persist
    private long lastStatusCheckTime = -1;

    public Tablet() {
        this(0L, new ArrayList<Replica>());
    }
    
    public Tablet(long tabletId) {
        this(tabletId, new ArrayList<Replica>());
    }
    
    public Tablet(long tabletId, List<Replica> replicas) {
        this.id = tabletId;
        this.replicas = replicas;
        if (this.replicas == null) {
            this.replicas = new ArrayList<Replica>();
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

    public List<Long> getBackendIdsList() {
        List<Long> beIds = Lists.newArrayList();
        for (Replica replica : replicas) {
            beIds.add(replica.getBackendId());
        }
        return beIds;
    }

    // for query
    public void getQueryableReplicas(List<Replica> allQuerableReplica, List<Replica> localReplicas,
            long visibleVersion, long visibleVersionHash, long localBeId, int schemaHash) {
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (state == ReplicaState.NORMAL || state == ReplicaState.SCHEMA_CHANGE) {
                // replica.getSchemaHash() == -1 is for compatibility
                if (replica.checkVersionCatchUp(visibleVersion, visibleVersionHash)
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
        Collections.sort(replicas, Replica.VERSION_DESC_COMPARATOR);
    }
 
    public String toString() {
        return "tabletId=" + this.id;
    }

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

    /*
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
            long visibleVersion, long visibleVersionHash, int replicationNum) {

        int aliveReplicaNum = 0;
        int healthyReplicaNum = 0;
        int healthyReplicaNumInCluster = 0;

        for (Replica replica : replicas) {
            long backendId = replica.getBackendId();
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend == null || !backend.isAvailable() || replica.getState() == ReplicaState.CLONE
                    || replica.isBad()) {
                // replica missing or bad
                continue;
            }
            ++aliveReplicaNum;

            if (replica.getLastFailedVersion() > 0 ||
                    replica.getVersion() < visibleVersion
                    || (replica.getVersion() == visibleVersion 
                    && replica.getVersionHash() != visibleVersionHash) ) {
                // version incomplete
                continue;
            }

            ++healthyReplicaNum;
            if (backend.getOwnerClusterName().equals(clusterName)) {
                ++healthyReplicaNumInCluster;
            }
        }

        // 1. alive replicas are not enough
        if (aliveReplicaNum < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.HIGH);
        } else if (aliveReplicaNum < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.NORMAL);
        }
        
        // 2. healthy replicas are not enough
        if (healthyReplicaNum < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.HIGH);
        } else if (healthyReplicaNum < replicationNum) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.NORMAL);
        }

        // 3. healthy replicas in cluster are not enough
        if (healthyReplicaNumInCluster < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING_IN_CLUSTER, TabletSchedCtx.Priority.LOW);
        } else if (replicas.size() > replicationNum) {
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return Pair.create(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        }

        // 4. healthy
        return Pair.create(TabletStatus.HEALTHY, TabletSchedCtx.Priority.NORMAL);
    }

    /*
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORNAL:  delay Config.tablet_repair_delay_factor_second * 2;
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
