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

package org.apache.doris.system;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.rpc.RpcException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Cluster-wide fail-closed gate for the irreversible Row TTL metadata feature. */
public final class RowTtlFeatureGate {
    private static final Object ACTIVATION_LOCK = new Object();

    private RowTtlFeatureGate() {
    }

    public static void activateForMutation() throws DdlException {
        synchronized (ACTIVATION_LOCK) {
            ensureNoUnactivatedRowTtlMetadata();
            ensureClusterSupportsRowTtl();
            Env.getCurrentEnv().activateRowTtlMetaVersion();
        }
    }

    public static void ensureReadyForUse() throws DdlException {
        if (!Env.getCurrentEnv().isRowTtlActivated()) {
            throw new DdlException("Row TTL metadata predates the required activation barrier; "
                    + "upgrade every FE, BE and Meta Service before using this table");
        }
        ensureRuntimeNodesSupportRowTtl();
    }

    public static void ensureClusterSupportsRowTtl() throws DdlException {
        ensureRuntimeNodesSupportRowTtl();
        if (Config.isCloudMode()) {
            ensureMetaServiceSupportsRowTtl();
        }
    }

    /**
     * Check only heartbeat state. This method is safe to call while a planner holds table read locks;
     * the Meta Service capability RPC is reserved for the lock-free activation path.
     */
    public static void ensureRuntimeNodesSupportRowTtl() throws DdlException {
        Env env = Env.getCurrentEnv();
        long now = System.currentTimeMillis();
        long maxHeartbeatAgeMs = TimeUnit.SECONDS.toMillis(Config.heartbeat_interval_second
                * Math.max(3L, Config.max_backend_heartbeat_failure_tolerance_count + 1));
        List<String> incompatibleNodes = new ArrayList<>();

        for (Frontend frontend : env.getFrontends(null)) {
            boolean isSelf = frontend.getHost().equals(env.getSelfNode().getHost())
                    && frontend.getEditLogPort() == env.getSelfNode().getPort();
            if (isSelf) {
                if (!env.isReady()) {
                    incompatibleNodes.add("FE " + frontend.getNodeName() + " is not ready");
                }
            } else if (!frontend.isAlive()) {
                incompatibleNodes.add("FE " + frontend.getNodeName() + " is not alive");
            } else if (!isFresh(now, frontend.getLastUpdateTime(), maxHeartbeatAgeMs)) {
                incompatibleNodes.add("FE " + frontend.getNodeName() + " has no fresh heartbeat");
            } else if (frontend.isNodeFeatureIncompatible()
                    || !frontend.supportsNodeFeature(NodeFeature.ROW_TTL)) {
                incompatibleNodes.add("FE " + frontend.getNodeName() + " does not support Row TTL");
            }
        }

        for (Backend backend : env.getClusterInfo().getAllClusterBackends(false)) {
            if (!backend.isAlive()) {
                incompatibleNodes.add("BE " + backend.getId() + " is not alive");
            } else if (!isFresh(now, backend.getLastUpdateMs(), maxHeartbeatAgeMs)) {
                incompatibleNodes.add("BE " + backend.getId() + " has no fresh heartbeat");
            } else if (backend.isNodeFeatureIncompatible()
                    || !backend.supportsNodeFeature(NodeFeature.ROW_TTL)) {
                incompatibleNodes.add("BE " + backend.getId() + " does not support Row TTL");
            }
        }

        if (!incompatibleNodes.isEmpty()) {
            throw new DdlException("Row TTL requires every registered FE and BE to support the feature: "
                    + String.join("; ", incompatibleNodes));
        }
    }

    private static void ensureNoUnactivatedRowTtlMetadata() throws DdlException {
        Env env = Env.getCurrentEnv();
        if (env.isRowTtlActivated()) {
            return;
        }
        for (Database database : env.getInternalCatalog().getDbs()) {
            for (Table table : database.getTables()) {
                if (table instanceof OlapTable && ((OlapTable) table).hasRowTtl()) {
                    throw new DdlException("Row TTL table " + table.getQualifiedName()
                            + " predates the metadata activation barrier; it cannot be silently adopted");
                }
            }
        }
        if (Env.getCurrentRecycleBin().containsRowTtlTable()) {
            throw new DdlException("Recycled Row TTL metadata predates the activation barrier; "
                    + "recover or permanently erase it with a compatible cluster before enabling Row TTL");
        }
    }

    private static void ensureMetaServiceSupportsRowTtl() throws DdlException {
        Cloud.GetMetaServiceCapabilityResponse response;
        try {
            response = MetaServiceProxy.getInstance().getMetaServiceCapability(
                    Cloud.GetMetaServiceCapabilityRequest.newBuilder().build());
        } catch (RpcException e) {
            throw new DdlException("Failed to verify Meta Service Row TTL capability: " + e.getMessage(), e);
        }

        if (!response.hasStatus() || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            String message = response.hasStatus() ? response.getStatus().getMsg() : "missing response status";
            throw new DdlException("Failed to verify Meta Service Row TTL capability: " + message);
        }
        if ((response.getFeatureFlags()
                & Cloud.MetaServiceFeature.META_SERVICE_FEATURE_ROW_TTL_VALUE) == 0) {
            throw new DdlException("Every active Meta Service instance must support Row TTL");
        }
    }

    private static boolean isFresh(long now, long heartbeatTime, long maxHeartbeatAgeMs) {
        return heartbeatTime > 0 && heartbeatTime <= now && now - heartbeatTime <= maxHeartbeatAgeMs;
    }
}
