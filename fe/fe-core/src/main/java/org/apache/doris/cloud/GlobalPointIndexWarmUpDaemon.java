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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the GLOBAL_POINT index files of every alive BE in its file cache (cloud mode).
 *
 * <p>Plan-time pruning reads one bloom per rowset of every probed tablet, and a bloom that is
 * not cached costs a remote read on the planning path. Each round, this master-only daemon sends
 * every alive BE the tablets of GLOBAL_POINT-indexed tables that map to it, in one of two modes:
 * <ul>
 *   <li>initial warm-up, the first time a BE process is seen (its lastStartTime changed): the BE
 *   syncs the rowsets and caches every file;
 *   <li>repair sweep, at most every {@code Config.global_point_index_repair_interval_sec} for a BE
 *   already warmed: the BE uses its cached rowsets only, which is cheap when the files are still
 *   cached, and brings back what was evicted since.
 * </ul>
 *
 * <p>Everything is best effort and idempotent. A failed round is retried in the next one, and an
 * FE restart simply warms every BE once more. Query results never depend on it.
 */
public class GlobalPointIndexWarmUpDaemon extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(GlobalPointIndexWarmUpDaemon.class);

    private static final long INTERVAL_MS = 60 * 1000L;
    private static final long RPC_ACK_TIMEOUT_SEC = 10;

    // beId -> the Backend.lastStartTime last warmed. Selects initial warm-up or repair sweep.
    private final Map<Long, Long> warmedStartTime = Maps.newHashMap();

    // beId -> time of the last successful repair sweep of that BE.
    private final Map<Long, Long> lastRepairTimeMs = Maps.newHashMap();

    public GlobalPointIndexWarmUpDaemon() {
        super("global-point-index-warmup", INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.isCloudMode() || !Config.enable_global_point_index_warmup) {
            return;
        }
        ImmutableMap<Long, Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (Exception e) {
            LOG.warn("GLOBAL_POINT warm-up: failed to list backends", e);
            return;
        }
        List<Backend> aliveBackends = new ArrayList<>();
        for (Backend be : backends.values()) {
            if (be.isAlive()) {
                aliveBackends.add(be);
            }
        }
        CollectResult collected = collectTabletsForAllBackends(aliveBackends);
        long now = System.currentTimeMillis();
        long repairIntervalMs = Math.max(1, Config.global_point_index_repair_interval_sec) * 1000L;
        for (Backend be : aliveBackends) {
            if (collected.unresolvedBeIds.contains(be.getId())) {
                // Not in a compute group yet (right after startup); retry next round.
                continue;
            }
            List<Long> tabletIds = collected.tabletsByBe.getOrDefault(be.getId(), new ArrayList<>());
            Long warmed = warmedStartTime.get(be.getId());
            if (warmed == null || warmed != be.getLastStartTime()) {
                if (warmBackend(be, tabletIds, false)) {
                    warmedStartTime.put(be.getId(), be.getLastStartTime());
                    lastRepairTimeMs.put(be.getId(), now);
                }
                continue;
            }
            if (!Config.enable_global_point_index_repair) {
                continue;
            }
            Long lastRepair = lastRepairTimeMs.get(be.getId());
            if (lastRepair != null && now - lastRepair < repairIntervalMs) {
                continue;
            }
            if (warmBackend(be, tabletIds, true)) {
                lastRepairTimeMs.put(be.getId(), now);
            }
        }
        warmedStartTime.keySet().retainAll(backends.keySet());
        lastRepairTimeMs.keySet().retainAll(backends.keySet());
    }

    /**
     * Returns true only if every batch was accepted in full. Otherwise the BE is retried in the
     * next round; a rejected tablet means the BE's warm-up queue was full.
     */
    private boolean warmBackend(Backend be, List<Long> tabletIds, boolean isRepairSweep) {
        if (tabletIds.isEmpty()) {
            return true;
        }
        LOG.info("GLOBAL_POINT warm-up: {} BE {} ({}), {} tablets", isRepairSweep ? "repair sweep of" : "warming",
                be.getId(), be.getAddress(), tabletIds.size());
        boolean allOk = true;
        int batchSize = Math.max(1, Config.global_point_index_warmup_batch_size);
        for (int from = 0; from < tabletIds.size(); from += batchSize) {
            List<Long> batch = tabletIds.subList(from, Math.min(from + batchSize, tabletIds.size()));
            InternalService.PGpIdxWarmUpRequest request = InternalService.PGpIdxWarmUpRequest.newBuilder()
                    .addAllTabletIds(batch)
                    .setIsRepairSweep(isRepairSweep)
                    .build();
            try {
                Future<InternalService.PGpIdxWarmUpResponse> future = BackendServiceProxy.getInstance()
                        .warmUpGlobalPointIndexAsync(be.getBrpcAddress(), request);
                if (future == null) {
                    allOk = false;
                    continue;
                }
                InternalService.PGpIdxWarmUpResponse response = future.get(RPC_ACK_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (response.getStatus().getStatusCode() != 0 || response.getRejectedTablets() > 0) {
                    LOG.warn("GLOBAL_POINT warm-up: BE {} did not accept the whole batch, status={} accepted={}"
                                    + " rejected={}, retrying next round", be.getId(),
                            response.getStatus().getStatusCode(), response.getAcceptedTablets(),
                            response.getRejectedTablets());
                    allOk = false;
                }
                logPreviousSweep(be, response);
            } catch (Exception e) {
                LOG.warn("GLOBAL_POINT warm-up: RPC to BE {} failed, retrying next round", be.getId(), e);
                allOk = false;
            }
        }
        return allOk;
    }

    private static void logPreviousSweep(Backend be, InternalService.PGpIdxWarmUpResponse response) {
        if (response.hasPrevSweepChecked()) {
            LOG.info("GLOBAL_POINT warm-up: BE {} previous sweep checked={} resident={} repaired={} failed={}",
                    be.getId(), response.getPrevSweepChecked(), response.getPrevSweepResident(),
                    response.getPrevSweepRepaired(), response.getPrevSweepFailed());
        }
        // A saturated bloom answers every probe without error, so this is the only sign that
        // pruning stopped working for some rowsets. The BE exports the same numbers as metrics;
        // this line tells which BE.
        if (response.getPrevSweepBloomsUndersized() > 0) {
            LOG.warn("GLOBAL_POINT warm-up: BE {} found {} undersized blooms out of {}; pruning is lost for"
                    + " those rowsets, results are not affected", be.getId(),
                    response.getPrevSweepBloomsUndersized(), response.getPrevSweepBloomsSeen());
        }
        // Ambiguous: an all-NULL indexed column also records no value. Worth a look, not an alert.
        if (response.getPrevSweepBloomsEmpty() > 0) {
            LOG.info("GLOBAL_POINT warm-up: BE {} saw {} blooms with no value (an all-NULL column, or a bloom"
                    + " that was never fed)", be.getId(), response.getPrevSweepBloomsEmpty());
        }
        // Normal after ADD INDEX until the historical rowsets are rewritten; should trend to zero.
        if (response.getPrevSweepBloomsMissing() > 0) {
            LOG.info("GLOBAL_POINT warm-up: BE {} saw {} rowsets without a GLOBAL_POINT descriptor", be.getId(),
                    response.getPrevSweepBloomsMissing());
        }
    }

    private static final class CollectResult {
        final Map<Long, List<Long>> tabletsByBe = new HashMap<>();
        final Set<Long> unresolvedBeIds = new HashSet<>();
    }

    /**
     * Tablets of GLOBAL_POINT-indexed tables, grouped by the BE each maps to, in one catalog walk.
     *
     * <p>A tablet's BE depends only on the compute group, so the cluster id of each alive BE is
     * resolved once and each tablet is mapped once per compute group. A BE whose cluster id cannot
     * be resolved yet is reported in {@link CollectResult#unresolvedBeIds}, to be retried.
     */
    private CollectResult collectTabletsForAllBackends(List<Backend> aliveBackends) {
        CollectResult result = new CollectResult();
        if (aliveBackends.isEmpty()) {
            return result;
        }
        CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
        Set<String> clusterIds = new HashSet<>();
        for (Backend be : aliveBackends) {
            String clusterId = resolveClusterId(infoService, be.getAddress());
            if (clusterId == null) {
                result.unresolvedBeIds.add(be.getId());
            } else {
                clusterIds.add(clusterId);
            }
        }
        if (clusterIds.isEmpty()) {
            return result;
        }
        for (Long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            for (Table table : db.getTables()) {
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                List<Index> indexes = olapTable.getIndexes();
                if (indexes == null
                        || indexes.stream().noneMatch(idx -> idx.getIndexType() == IndexType.GLOBAL_POINT)) {
                    continue;
                }
                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : index.getTablets()) {
                                addTablet(result, tablet, clusterIds);
                            }
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        return result;
    }

    private static void addTablet(CollectResult result, Tablet tablet, Set<String> clusterIds) {
        if (tablet.getReplicas().isEmpty() || !(tablet.getReplicas().get(0) instanceof CloudReplica)) {
            return;
        }
        CloudReplica replica = (CloudReplica) tablet.getReplicas().get(0);
        for (String clusterId : clusterIds) {
            long mappedBeId;
            try {
                mappedBeId = replica.getBackendIdWithClusterId(clusterId);
            } catch (ComputeGroupException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GLOBAL_POINT warm-up: cannot map tablet {} in cluster {}", tablet.getId(),
                            clusterId, e);
                }
                continue;
            }
            if (mappedBeId >= 0) {
                result.tabletsByBe.computeIfAbsent(mappedBeId, k -> new ArrayList<>()).add(tablet.getId());
            }
        }
    }

    /** The same resolution as CloudReplica.getBackendId(String). Returns null when not ready yet. */
    private static String resolveClusterId(CloudSystemInfoService infoService, String beEndpoint) {
        String clusterName = infoService.getClusterNameByBeAddr(beEndpoint);
        if (clusterName == null) {
            return null;
        }
        try {
            return infoService.resolveClusterIdByName(infoService.getPhysicalCluster(clusterName));
        } catch (ComputeGroupException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("GLOBAL_POINT warm-up: cannot resolve the cluster id of BE {}", beEndpoint, e);
            }
            return null;
        }
    }
}
