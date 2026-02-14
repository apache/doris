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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletSlidingWindowAccessStats;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Multimap;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CloudTablet extends Tablet implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(CloudTablet.class);

    // cloud only has one replica, use replica instead of replicas
    @Deprecated
    @SerializedName(value = "rs", alternate = {"replicas"})
    private List<Replica> replicas;
    @SerializedName(value = "r")
    private Replica replica;

    public CloudTablet() {
        this(0);
    }

    public CloudTablet(long tabletId) {
        super(tabletId);
    }

    @Override
    public Replica getReplicaByBackendId(long backendId) {
        return replica;
    }

    private Multimap<Long, Long> backendPathMapReprocess(Multimap<Long, Long> pathMap) throws UserException {
        if (pathMap.containsKey(-1L)) {
            pathMap.removeAll(-1L);
            if (pathMap.isEmpty()) {
                throw new UserException(InternalErrorCode.META_NOT_FOUND_ERR,
                        SystemInfoService.NOT_USING_VALID_CLUSTER_MSG);
            }
        }

        return pathMap;
    }

    @Override
    public Multimap<Long, Long> getNormalReplicaBackendPathMap() throws UserException {
        Multimap<Long, Long> pathMap = super.getNormalReplicaBackendPathMap();
        return backendPathMapReprocess(pathMap);
    }

    public Multimap<Long, Long> getNormalReplicaBackendPathMap(String beEndpoint) throws UserException {
        TabletSlidingWindowAccessStats.recordTablet(getId());
        Multimap<Long, Long> pathMap = super.getNormalReplicaBackendPathMapImpl(beEndpoint,
                (rep, be) -> ((CloudReplica) rep).getBackendId(be));
        return backendPathMapReprocess(pathMap);
    }

    @Override
    public void addReplica(Replica replica, boolean isRestore) {
        this.replica = replica;
        if (!isRestore) {
            Env.getCurrentInvertedIndex().addReplica(id, replica);
        }
    }

    @Override
    public List<Replica> getReplicas() {
        if (replica == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(replica);
    }

    @Override
    public Replica getReplicaById(long replicaId) {
        if (replica != null && replica.getId() == replicaId) {
            return replica;
        }
        return null;
    }

    public CloudReplica getCloudReplica() {
        if (replica == null) {
            return null;
        }
        return (CloudReplica) replica;
    }

    @Override
    public long getDataSize(boolean singleReplica, boolean filterSizeZero) {
        if (replica != null && replica.getState() == ReplicaState.NORMAL) {
            return replica.getDataSize();
        }
        return 0;
    }

    @Override
    public long getRowCount(boolean singleReplica) {
        if (replica != null && replica.getState() == ReplicaState.NORMAL) {
            return replica.getRowCount();
        }
        return 0;
    }

    @Override
    public long getMinReplicaRowCount(long version) {
        if (replica != null && replica.isAlive() && replica.checkVersionCatchUp(version, false)) {
            return replica.getRowCount();
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CloudTablet)) {
            return false;
        }
        CloudTablet tablet = (CloudTablet) obj;
        return id == tablet.id && Objects.equals(replica, tablet.replica);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert replicas to replica for CloudTablet: {}, replica: {}, replicas: {}", this.id,
                    this.replica, this.replicas);
        }
        if (replicas != null && !replicas.isEmpty()) {
            this.replica = replicas.get(0);
            this.replicas = null;
        }
    }
}
