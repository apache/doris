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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

public class LocalTablet extends Tablet {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

    @SerializedName(value = "rs", alternate = {"replicas"})
    private List<Replica> replicas;
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
        for (Replica r : replicas) {
            if (r.getId() == cooldownReplicaId) {
                return r.getRemoteDataSize();
            }
        }
        // return replica with max remoteDataSize
        return replicas.stream().max(Comparator.comparing(Replica::getRemoteDataSize)).get().getRemoteDataSize();
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
        LongStream s = getReplicas().stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .filter(r -> !filterSizeZero || r.getDataSize() > 0)
                .mapToLong(Replica::getDataSize);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
    }

    @Override
    public long getRowCount(boolean singleReplica) {
        LongStream s = getReplicas().stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .mapToLong(Replica::getRowCount);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
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

    private boolean isLatestReplicaAndDeleteOld(Replica newReplica) {
        boolean delete = false;
        boolean hasBackend = false;
        long version = newReplica.getVersion();
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendIdWithoutException() == newReplica.getBackendIdWithoutException()) {
                hasBackend = true;
                if (replica.getVersion() <= version) {
                    iterator.remove();
                    delete = true;
                }
            }
        }

        return delete || !hasBackend;
    }

    @Override
    public void addReplica(Replica replica, boolean isRestore) {
        if (isLatestReplicaAndDeleteOld(replica)) {
            replicas.add(replica);
            if (!isRestore) {
                Env.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

    @Override
    public List<Replica> getReplicas() {
        return this.replicas;
    }

    @Override
    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) {
            if (replica.getBackendIdWithoutException() == backendId) {
                return replica;
            }
        }
        return null;
    }

    @Override
    public boolean deleteReplica(Replica replica) {
        if (replicas.contains(replica)) {
            replicas.remove(replica);
            Env.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendIdWithoutException());
            return true;
        }
        return false;
    }

    @Override
    public boolean deleteReplicaByBackendId(long backendId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendIdWithoutException() == backendId) {
                iterator.remove();
                Env.getCurrentInvertedIndex().deleteReplica(id, backendId);
                return true;
            }
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
        for (Replica replica : replicas) {
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
