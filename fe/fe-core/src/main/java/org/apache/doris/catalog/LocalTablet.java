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
import org.apache.doris.common.Pair;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LocalTablet extends Tablet {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

    @SerializedName(value = "rs", alternate = {"replicas"})
    private volatile List<Replica> replicas;
    @SerializedName(value = "lastCheckTime")
    private long lastCheckTime;

    // cooldown conf
    @SerializedName(value = "cri", alternate = {"cooldownReplicaId"})
    private long cooldownReplicaId = -1;
    @SerializedName(value = "ctm", alternate = {"cooldownTerm"})
    private long cooldownTerm = -1;
    private final Object cooldownConfLock = new Object();

    @SerializedName(value = "cv", alternate = {"checkedVersion"})
    private long checkedVersion;
    @SerializedName(value = "ic", alternate = {"isConsistent"})
    private boolean isConsistent;

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

    public LocalTablet() {
        this(0);
    }

    public LocalTablet(long tabletId) {
        super(tabletId);
        if (this.replicas == null) {
            this.replicas = new ArrayList<>();
        }
        checkedVersion = -1L;
        isConsistent = true;
    }

    @Override
    public void setCooldownConf(long cooldownReplicaId, long cooldownTerm) {
        synchronized (cooldownConfLock) {
            this.cooldownReplicaId = cooldownReplicaId;
            this.cooldownTerm = cooldownTerm;
        }
    }

    @Override
    public long getCooldownReplicaId() {
        return cooldownReplicaId;
    }

    @Override
    public Pair<Long, Long> getCooldownConf() {
        synchronized (cooldownConfLock) {
            return Pair.of(cooldownReplicaId, cooldownTerm);
        }
    }

    @Override
    public long getRemoteDataSize() {
        // if CooldownReplicaId is not init
        if (cooldownReplicaId <= 0) {
            return 0;
        }
        List<Replica> snapshot = replicas; // single volatile read; reuse below
        for (Replica r : snapshot) {
            if (r.getId() == cooldownReplicaId) {
                return r.getRemoteDataSize();
            }
        }
        // return replica with max remoteDataSize
        return snapshot.stream().max(Comparator.comparing(Replica::getRemoteDataSize)).get().getRemoteDataSize();
    }

    @Override
    public Replica getReplicaById(long replicaId) {
        for (Replica replica : getReplicas()) {
            if (replica.getId() == replicaId) {
                return replica;
            }
        }
        return null;
    }

    // ATTN: Replica::getDataSize may zero in cloud and non-cloud
    // due to dataSize not write to image
    @Override
    public long getDataSize(boolean singleReplica, boolean filterSizeZero) {
        long dataSize = 0;
        int replicaNum = 0;
        for (Replica replica : getReplicas()) {
            if (replica.getState() != ReplicaState.NORMAL) {
                continue;
            }
            long replicaDataSize = replica.getDataSize();
            if (filterSizeZero && replicaDataSize <= 0) {
                continue;
            }
            dataSize += replicaDataSize;
            replicaNum++;
        }
        return singleReplica && replicaNum > 0 ? dataSize / replicaNum : dataSize;
    }

    @Override
    public long getRowCount(boolean singleReplica) {
        long rowCount = 0;
        int replicaNum = 0;
        for (Replica replica : getReplicas()) {
            if (replica.getState() != ReplicaState.NORMAL) {
                continue;
            }
            rowCount += replica.getRowCount();
            replicaNum++;
        }
        return singleReplica && replicaNum > 0 ? rowCount / replicaNum : rowCount;
    }

    // Get the least row count among all valid replicas.
    // The replica with the least row count is the most accurate one. Because it performs most compaction.
    @Override
    public long getMinReplicaRowCount(long version) {
        long minRowCount = Long.MAX_VALUE;
        long maxReplicaVersion = 0;
        for (Replica r : getReplicas()) {
            if (r.isAlive()
                    && r.checkVersionCatchUp(version, false)
                    && (r.getVersion() > maxReplicaVersion
                    || r.getVersion() == maxReplicaVersion && r.getRowCount() < minRowCount)) {
                minRowCount = r.getRowCount();
                maxReplicaVersion = r.getVersion();
            }
        }
        return minRowCount == Long.MAX_VALUE ? 0 : minRowCount;
    }

    @Override
    public long getCheckedVersion() {
        return this.checkedVersion;
    }

    @Override
    public void setCheckedVersion(long checkedVersion) {
        this.checkedVersion = checkedVersion;
    }

    @Override
    public void setIsConsistent(boolean good) {
        this.isConsistent = good;
    }

    @Override
    public boolean isConsistent() {
        return isConsistent;
    }

    @Override
    protected long getLastStatusCheckTime() {
        return lastStatusCheckTime;
    }

    @Override
    public void setLastStatusCheckTime(long lastStatusCheckTime) {
        this.lastStatusCheckTime = lastStatusCheckTime;
    }

    @Override
    public long getLastLoadFailedTime() {
        return lastLoadFailedTime;
    }

    @Override
    public void setLastLoadFailedTime(long lastLoadFailedTime) {
        this.lastLoadFailedTime = lastLoadFailedTime;
    }

    @Override
    protected long getLastTimeNoPathForNewReplica() {
        return lastTimeNoPathForNewReplica;
    }

    @Override
    public void setLastTimeNoPathForNewReplica(long lastTimeNoPathForNewReplica) {
        this.lastTimeNoPathForNewReplica = lastTimeNoPathForNewReplica;
    }

    @Override
    public long getLastCheckTime() {
        return lastCheckTime;
    }

    @Override
    public void setLastCheckTime(long lastCheckTime) {
        this.lastCheckTime = lastCheckTime;
    }

    // Writers are synchronized on this tablet to prevent concurrent lost-update:
    // some callers (e.g. InternalCatalog.createTablets, RestoreJob) do NOT hold
    // the OlapTable write lock when modifying replicas.
    // Readers capture the volatile reference once and iterate freely — no lock needed.

    @Override
    public synchronized void addReplica(Replica replica, boolean isRestore) {
        long version = replica.getVersion();
        long backendId = replica.getBackendIdWithoutException();
        boolean hasBackend = false;
        boolean deletedOld = false;
        List<Replica> current = replicas;
        List<Replica> next = new ArrayList<>(current.size() + 1);
        for (Replica r : current) {
            if (r.getBackendIdWithoutException() == backendId) {
                hasBackend = true;
                if (r.getVersion() <= version) {
                    deletedOld = true;
                    continue; // drop stale replica
                }
            }
            next.add(r);
        }
        if (deletedOld || !hasBackend) {
            next.add(replica);
            replicas = next; // volatile write; readers see the new immutable snapshot
            if (!isRestore) {
                Env.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

    @Override
    public void clearReplicas() {
        replicas = new ArrayList<>();
    }

    @Override
    public List<Replica> getReplicas() {
        // Volatile read: returns the current immutable snapshot; callers iterate without locking.
        return Collections.unmodifiableList(replicas);
    }

    @Override
    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) { // single volatile read
            if (replica.getBackendIdWithoutException() == backendId) {
                return replica;
            }
        }
        return null;
    }

    @Override
    public synchronized boolean deleteReplica(Replica replica) {
        List<Replica> current = replicas;
        if (current.contains(replica)) {
            List<Replica> next = new ArrayList<>(current);
            next.remove(replica);
            replicas = next; // volatile write
            Env.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendIdWithoutException());
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean deleteReplicaByBackendId(long backendId) {
        List<Replica> current = replicas;
        List<Replica> next = new ArrayList<>(current.size());
        Replica found = null;
        for (Replica replica : current) {
            if (replica.getBackendIdWithoutException() == backendId) {
                found = replica;
            } else {
                next.add(replica);
            }
        }
        if (found != null) {
            replicas = next; // volatile write
            Env.getCurrentInvertedIndex().deleteReplica(id, backendId);
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LocalTablet)) {
            return false;
        }

        LocalTablet tablet = (LocalTablet) obj;

        // Capture one snapshot per side so a concurrent writer cannot publish
        // a different list between size/contains/get calls below.
        List<Replica> thisReplicas = replicas;
        List<Replica> otherReplicas = tablet.replicas;
        if (thisReplicas != otherReplicas) {
            if (thisReplicas.size() != otherReplicas.size()) {
                return false;
            }
            int size = thisReplicas.size();
            for (int i = 0; i < size; i++) {
                if (!otherReplicas.contains(thisReplicas.get(i))) {
                    return false;
                }
            }
        }
        return id == tablet.id;
    }

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORMAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    @Override
    public boolean readyToBeRepaired(SystemInfoService infoService, TabletSchedCtx.Priority priority) {
        if (FeConstants.runningUnitTest) {
            return true;
        }

        if (priority == Priority.VERY_HIGH) {
            return true;
        }

        boolean allBeAliveOrDecommissioned = true;
        for (Replica replica : replicas) { // single volatile read; iteration on the snapshot
            Backend backend = infoService.getBackend(replica.getBackendIdWithoutException());
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
        if (getLastStatusCheckTime() == -1) {
            setLastStatusCheckTime(currentTime);
            return false;
        }

        boolean ready = false;
        switch (priority) {
            case HIGH:
                ready = currentTime - getLastStatusCheckTime() > Config.tablet_repair_delay_factor_second * 1000 * 1;
                break;
            case NORMAL:
                ready = currentTime - getLastStatusCheckTime() > Config.tablet_repair_delay_factor_second * 1000 * 2;
                break;
            case LOW:
                ready = currentTime - getLastStatusCheckTime() > Config.tablet_repair_delay_factor_second * 1000 * 3;
                break;
            default:
                break;
        }

        return ready;
    }
}
