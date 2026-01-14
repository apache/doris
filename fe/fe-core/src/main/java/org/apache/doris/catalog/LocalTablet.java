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

import org.apache.doris.common.Pair;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;

public class LocalTablet extends Tablet {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

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
}
