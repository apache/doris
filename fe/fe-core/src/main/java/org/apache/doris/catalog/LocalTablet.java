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

    // cooldown conf
    @SerializedName(value = "cri", alternate = {"cooldownReplicaId"})
    private long cooldownReplicaId = -1;
    @SerializedName(value = "ctm", alternate = {"cooldownTerm"})
    private long cooldownTerm = -1;
    private final Object cooldownConfLock = new Object();

    public LocalTablet() {
    }

    public LocalTablet(long tabletId) {
        super(tabletId);
    }

    public void setCooldownConf(long cooldownReplicaId, long cooldownTerm) {
        synchronized (cooldownConfLock) {
            this.cooldownReplicaId = cooldownReplicaId;
            this.cooldownTerm = cooldownTerm;
        }
    }

    public long getCooldownReplicaId() {
        return cooldownReplicaId;
    }

    public Pair<Long, Long> getCooldownConf() {
        synchronized (cooldownConfLock) {
            return Pair.of(cooldownReplicaId, cooldownTerm);
        }
    }

    @Override
    public long getRemoteDataSize() {
        // if CooldownReplicaId is not init
        // [fix](fe) move some variables from Tablet to LocalTablet which are not used in CloudTablet
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
}
