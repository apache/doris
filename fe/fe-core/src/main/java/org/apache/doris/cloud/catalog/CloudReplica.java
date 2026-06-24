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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CloudReplica extends Replica implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    // In the future, a replica may be mapped to multiple BEs in a cluster,
    // so this value is be list
    @SerializedName(value = "bes")
    private ConcurrentHashMap<String, List<Long>> primaryClusterToBackends
            = new ConcurrentHashMap<String, List<Long>>();
    @SerializedName(value = "be")
    private ConcurrentHashMap<String, Long> primaryClusterToBackend = null;
    @SerializedName(value = "dbId")
    private long dbId = -1;
    @SerializedName(value = "tableId")
    private long tableId = -1;
    @SerializedName(value = "partitionId")
    private long partitionId = -1;
    @SerializedName(value = "indexId")
    private long indexId = -1;
    @SerializedName(value = "idx")
    private long idx = -1;
    // last time to get tablet stats
    @Getter
    @Setter
    long lastGetTabletStatsTime = 0;
    /**
     * The index of {@link org.apache.doris.catalog.CloudTabletStatMgr#DEFAULT_INTERVAL_LADDER_MS} array.
     * Used to control the interval of getting tablet stats.
     * When get tablet stats:
     * if the stats is unchanged, will update this index to next value to get stats less frequently;
     * if the stats is changed, will update this index to 0 to get stats more frequently.
     */
    @Getter
    @Setter
    int statsIntervalIndex = 0;

    private static final Random rand = new Random();

    private Map<String, List<Long>> memClusterToBackends = new ConcurrentHashMap<String, List<Long>>();

    // clusterId, secondaryBe, changeTimestamp
    private Map<String, Pair<Long, Long>> secondaryClusterToBackends
            = new ConcurrentHashMap<String, Pair<Long, Long>>();

    public CloudReplica() {
    }

    public CloudReplica(ReplicaContext context) {
        this(context.replicaId, context.backendId, context.state, context.version,
                context.schemaHash, context.dbId, context.tableId, context.partitionId,
                context.indexId,
                context.originReplica != null ? ((CloudReplica) context.originReplica).getIdx() : -1);
    }

    public CloudReplica(long replicaId, Long backendId, ReplicaState state, long version, int schemaHash,
            long dbId, long tableId, long partitionId, long indexId, long idx) {
        super(replicaId, -1, state, version, schemaHash);
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.idx = idx;
    }

    private boolean isColocated() {
        return Env.getCurrentColocateIndex().isColocateTableNoLock(tableId);
    }

    public long getColocatedBeId(String clusterId) throws ComputeGroupException {
        CloudSystemInfoService infoService = ((CloudSystemInfoService) Env.getCurrentSystemInfo());
        List<Backend> bes = infoService.getBackendsByClusterId(clusterId).stream()
                .filter(be -> be.isQueryAvailable()).collect(Collectors.toList());
        String clusterName = infoService.getClusterNameByClusterId(clusterId);
        if (bes.isEmpty()) {
            LOG.warn("failed to get available be, cluster: {}-{}", clusterName, clusterId);
            throw new ComputeGroupException(
                String.format("There are no Backend nodes in the current compute group %s", clusterName),
                ComputeGroupException.FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE);
        }
        List<Backend> availableBes = new ArrayList<>();
        List<Backend> decommissionAvailBes = new ArrayList<>();
        for (Backend be : bes) {
            if (be.isAlive()) {
                if (be.isDecommissioned()) {
                    decommissionAvailBes.add(be);
                } else {
                    availableBes.add(be);
                }
            }
        }
        if (availableBes.isEmpty()) {
            availableBes = decommissionAvailBes;
        }
        if (availableBes.isEmpty()) {
            LOG.warn("failed to get available backend due to all backend dead, cluster: {}-{}", clusterName, clusterId);
            throw new ComputeGroupException(
                String.format("All the Backend nodes in the current compute group %s are in an abnormal state",
                        clusterName),
                ComputeGroupException.FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE);
        }

        GroupId groupId = Env.getCurrentColocateIndex().getGroupNoLock(tableId);
        HashCode hashCode = Hashing.murmur3_128().hashLong(groupId.grpId);
        if (availableBes.size() != bes.size()) {
            // some be is dead recently, still hash tablets on all backends.
            long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
            if (bes.stream().anyMatch(be -> !be.isAlive() && be.getLastUpdateMs() > needRehashDeadTime)) {
                List<Backend> beAliveOrDeadShort = bes.stream()
                        .filter(be -> be.isAlive() || be.getLastUpdateMs() > needRehashDeadTime)
                        .collect(Collectors.toList());
                long index = getIndexByBeNum(hashCode.asLong() + idx, beAliveOrDeadShort.size());
                Backend be = beAliveOrDeadShort.get((int) index);
                if (be.isAlive() && !be.isDecommissioned()) {
                    return be.getId();
                }
            }
        }

        // Tablets with the same idx will be hashed to the same BE, which
        // meets the requirements of colocated table.
        long index = getIndexByBeNum(hashCode.asLong() + idx, availableBes.size());
        long pickedBeId = availableBes.get((int) index).getId();

        return pickedBeId;
    }

    @Override
    public long getBackendId() throws ComputeGroupException {
        return getBackendIdImpl(cloudInfoService().getCurrentClusterId());
    }

    // Variant for callers that have already resolved the cluster id once per request
    // and want to skip the per-replica ConnectContext/priv/status/autoStart/existence pipeline.
    public long getBackendIdWithClusterId(String clusterId) throws ComputeGroupException {
        return getBackendIdImpl(clusterId);
    }

    public long getBackendId(String beEndpoint) {
        try {
            CloudSystemInfoService infoService = cloudInfoService();
            String clusterName = infoService.getClusterNameByBeAddr(beEndpoint);
            String physicalClusterName = infoService.getPhysicalCluster(clusterName);
            String clusterId = infoService.resolveClusterIdByName(physicalClusterName);
            return getBackendIdImpl(clusterId);
        } catch (ComputeGroupException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("failed to get compute group name for endpoint {}", beEndpoint, e);
            }
            return -1;
        }
    }

    public long getPrimaryBackendId() {
        String clusterId;
        try {
            clusterId = cloudInfoService().getCurrentClusterId();
        } catch (ComputeGroupException e) {
            return -1L;
        }

        if (Strings.isNullOrEmpty(clusterId)) {
            return -1L;
        }

        return getClusterPrimaryBackendId(clusterId);
    }

    // Returns the CloudSystemInfoService instance, or throws ComputeGroupException when
    // Env was set up with a base SystemInfoService (typically a unit test that mocks the
    // base type). Production cloud-mode FE never hits the throw branch.
    private static CloudSystemInfoService cloudInfoService() throws ComputeGroupException {
        SystemInfoService info = Env.getCurrentSystemInfo();
        if (info instanceof CloudSystemInfoService) {
            return (CloudSystemInfoService) info;
        }
        throw new ComputeGroupException(
                "current system info service is not cloud-aware",
                ComputeGroupException.FailedTypeEnum.CONNECT_CONTEXT_NOT_SET);
    }

    public long getClusterPrimaryBackendId(String clusterId) {
        if (isColocated()) {
            try {
                return getColocatedBeId(clusterId);
            } catch (ComputeGroupException e) {
                return -1L;
            }
        }

        List<Long> backendIds = primaryClusterToBackends.get(clusterId);
        if (backendIds != null && !backendIds.isEmpty()) {
            return backendIds.get(0);
        }

        return -1L;
    }

    private long getBackendIdImpl(String clusterId) throws ComputeGroupException {
        if (Strings.isNullOrEmpty(clusterId)) {
            return -1L;
        }

        if (isColocated()) {
            return getColocatedBeId(clusterId);
        }

        if (Config.enable_cloud_multi_replica) {
            int indexRand = rand.nextInt(Config.cloud_replica_num);
            int coldReadRand = rand.nextInt(100);
            boolean allowColdRead = coldReadRand < Config.cloud_cold_read_percent;
            boolean replicaEnough = memClusterToBackends.get(clusterId) != null
                    && memClusterToBackends.get(clusterId).size() > indexRand;

            long backendId = -1;
            if (replicaEnough) {
                backendId = memClusterToBackends.get(clusterId).get(indexRand);
            }

            if (!replicaEnough && !allowColdRead && primaryClusterToBackends.containsKey(clusterId)) {
                backendId = primaryClusterToBackends.get(clusterId).get(0);
            }

            if (backendId > 0) {
                Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
                if (be != null && be.isQueryAvailable()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("backendId={} ", backendId);
                    }
                    return backendId;
                }
            }

            List<Long> res = hashReplicaToBes(clusterId, false, Config.cloud_replica_num);
            if (res.size() < indexRand + 1) {
                if (res.isEmpty()) {
                    return -1;
                } else {
                    return res.get(0);
                }
            } else {
                return res.get(indexRand);
            }
        }

        // use primaryClusterToBackends, if find be normal
        Backend be = getPrimaryBackend(clusterId, false);
        if (be != null && be.isQueryAvailable()) {
            return be.getId();
        }

        if (!Config.enable_immediate_be_assign) {
            // use secondaryClusterToBackends, if find be normal
            be = getSecondaryBackend(clusterId);
            if (be != null && be.isQueryAvailable()) {
                return be.getId();
            }
        }

        if (DebugPointUtil.isEnable("CloudReplica.getBackendIdImpl.primaryClusterToBackends")) {
            LOG.info("Debug Point enable CloudReplica.getBackendIdImpl.primaryClusterToBackends");
            return -1;
        }

        // be abnormal, rehash it. configure settings to different maps
        long pickBeId = hashReplicaToBe(clusterId, false);
        if (Config.enable_immediate_be_assign) {
            updateClusterToPrimaryBe(clusterId, pickBeId);
        } else {
            updateClusterToSecondaryBe(clusterId, pickBeId);
        }
        return pickBeId;
    }

    public Backend getPrimaryBackend(String clusterId, boolean setIfAbsent) {
        long beId = getClusterPrimaryBackendId(clusterId);
        if (beId != -1L) {
            return Env.getCurrentSystemInfo().getBackend(beId);
        } else {
            // Load event warmup requires knowing the mapping relationship
            // between the downstream be and tablet of the shadow index
            if (setIfAbsent) {
                try {
                    beId = getBackendIdImpl(clusterId);
                    updateClusterToPrimaryBe(clusterId, beId);
                    return Env.getCurrentSystemInfo().getBackend(beId);
                } catch (ComputeGroupException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    public Backend getSecondaryBackend(String clusterId) {
        Pair<Long, Long> secondBeAndChangeTimestamp = secondaryClusterToBackends.get(clusterId);
        if (secondBeAndChangeTimestamp == null) {
            return null;
        }
        long beId = secondBeAndChangeTimestamp.key();
        long changeTimestamp = secondBeAndChangeTimestamp.value();
        if (LOG.isDebugEnabled()) {
            LOG.debug("in secondaryClusterToBackends clusterId {}, beId {}, changeTimestamp {}, replica info {}",
                    clusterId, beId, changeTimestamp, this);
        }
        return Env.getCurrentSystemInfo().getBackend(secondBeAndChangeTimestamp.first);
    }

    public long hashReplicaToBe(String clusterId, boolean isBackGround) throws ComputeGroupException {
        // TODO(luwei) list should be sorted
        List<Backend> clusterBes = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterId(clusterId);
        String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getClusterNameByClusterId(clusterId);
        if (clusterBes.isEmpty()) {
            throw new ComputeGroupException(
                String.format("There are no Backend nodes in the current compute group %s", clusterName),
                ComputeGroupException.FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE);
        }
        // use alive be to exec sql
        List<Backend> availableBes = new ArrayList<>();
        List<Backend> decommissionAvailBes = new ArrayList<>();
        for (Backend be : clusterBes) {
            if (be.isQueryAvailable() && !be.isSmoothUpgradeSrc()) {
                if (be.isDecommissioned()) {
                    decommissionAvailBes.add(be);
                } else {
                    availableBes.add(be);
                }
            }
        }
        if (availableBes.isEmpty()) {
            availableBes = decommissionAvailBes;
        }
        if (availableBes.isEmpty()) {
            if (!isBackGround) {
                LOG.warn("failed to get available be, clusterId: {}", clusterId);
            }
            throw new ComputeGroupException(
                String.format("All the Backend nodes in the current compute group %s are in an abnormal state",
                    clusterName),
                ComputeGroupException.FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("availableBes={}", availableBes);
        }
        long index = -1;
        HashCode hashCode = null;
        if (idx == -1) {
            index = getId() % availableBes.size();
        } else {
            hashCode = Hashing.murmur3_128().hashLong(partitionId);
            index = getIndexByBeNum(hashCode.asLong() + idx, availableBes.size());
        }
        long pickedBeId = availableBes.get((int) index).getId();
        LOG.info("picked clusterName {} beId {}, replicaId {}, partitionId {}, beNum {}, "
                + "replicaIdx {}, picked Index {}, hashVal {}",
                clusterName, pickedBeId, getId(), partitionId, availableBes.size(), idx, index,
                hashCode == null ? -1 : hashCode.asLong());

        return pickedBeId;
    }

    private long getIndexByBeNum(long hashValue, int beNum) {
        // hashValue may be a negative value, so we
        // need to take the modulus of beNum again to ensure
        // that result is a positive value
        return (hashValue % beNum + beNum) % beNum;
    }

    public List<Long> hashReplicaToBes(String clusterId, boolean isBackGround, int replicaNum)
            throws ComputeGroupException {
        // TODO(luwei) list should be sorted
        List<Backend> clusterBes = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterId(clusterId);
        String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getClusterNameByClusterId(clusterId);
        if (clusterBes.isEmpty()) {
            throw new ComputeGroupException(
                String.format("There are no Backend nodes in the current compute group %s", clusterName),
                ComputeGroupException.FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE);
        }
        // use alive be to exec sql
        List<Backend> availableBes = new ArrayList<>();
        List<Backend> decommissionAvailBes = new ArrayList<>();
        for (Backend be : clusterBes) {
            long lastUpdateMs = be.getLastUpdateMs();
            long missTimeMs = Math.abs(lastUpdateMs - System.currentTimeMillis());
            // be core or restart must in heartbeat_interval_second
            if ((be.isAlive() || missTimeMs <= Config.heartbeat_interval_second * 1000L)
                    && !be.isSmoothUpgradeSrc()) {
                if (be.isDecommissioned()) {
                    decommissionAvailBes.add(be);
                } else {
                    availableBes.add(be);
                }
            }
        }
        if (availableBes.isEmpty()) {
            availableBes = decommissionAvailBes;
        }
        if (availableBes.isEmpty()) {
            if (!isBackGround) {
                LOG.warn("failed to get available be, clusterId: {}", clusterId);
            }
            throw new ComputeGroupException(
                String.format("All the Backend nodes in the current compute group %s are in an abnormal state",
                    clusterName),
                ComputeGroupException.FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("availableBes={}", availableBes);
        }

        int realReplicaNum = Math.min(replicaNum, availableBes.size());
        List<Long> bes = new ArrayList<Long>();
        for (int i = 0; i < realReplicaNum; ++i) {
            long index = -1;
            HashCode hashCode = null;
            if (idx == -1) {
                index = getId() % availableBes.size();
            } else {
                hashCode = Hashing.murmur3_128().hashLong(partitionId + i);
                index = getIndexByBeNum(hashCode.asLong() + idx, availableBes.size());
            }
            long pickedBeId = availableBes.get((int) index).getId();
            availableBes.remove((int) index);
            LOG.info("picked beId {}, replicaId {}, partId {}, beNum {}, replicaIdx {}, picked Index {}, hashVal {}",
                    pickedBeId, getId(), partitionId, availableBes.size(), idx, index,
                    hashCode == null ? -1 : hashCode.asLong());
            // save to memClusterToBackends map
            bes.add(pickedBeId);
        }

        memClusterToBackends.put(clusterId, bes);

        return bes;
    }

    @Override
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        // ATTN: expectedVersion is not used here, and OlapScanNode.addScanRangeLocations
        // depends this feature to implement snapshot partition version. See comments in
        // OlapScanNode.addScanRangeLocations for details.
        return true;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getIdx() {
        return idx;
    }

    public void updateClusterToPrimaryBe(String cluster, long beId) {
        primaryClusterToBackends.put(cluster, Lists.newArrayList(beId));
        secondaryClusterToBackends.remove(cluster);
    }

    /**
     * Set secondary BE for the cluster. Used as query fallback when primary is unavailable.
     * Also used during smooth upgrade: after migrating primary from old BE to new BE,
     * set old BE as secondary so queries can still use old BE until new BE is alive.
     */
    public void updateClusterToSecondaryBe(String cluster, long beId) {
        long changeTimestamp = System.currentTimeMillis();
        if (LOG.isDebugEnabled()) {
            LOG.debug("add to secondary clusterId {}, beId {}, changeTimestamp {}, replica info {}",
                    cluster, beId, changeTimestamp, this);
        }
        secondaryClusterToBackends.put(cluster, Pair.of(beId, changeTimestamp));
    }

    public void clearClusterToBe(String cluster) {
        primaryClusterToBackends.remove(cluster);
        secondaryClusterToBackends.remove(cluster);
    }

    // ATTN: This func is only used by redundant tablet report clean in bes.
    // Only the master node will do the diff logic,
    // so just only need to clean up secondaryClusterToBackends on the master node.
    public void checkAndClearSecondaryClusterToBe(String clusterId, long expireTimestamp) {
        Pair<Long, Long> secondBeAndChangeTimestamp = secondaryClusterToBackends.get(clusterId);
        if (secondBeAndChangeTimestamp == null) {
            return;
        }
        long beId = secondBeAndChangeTimestamp.key();
        long changeTimestamp = secondBeAndChangeTimestamp.value();

        if (changeTimestamp < expireTimestamp) {
            LOG.debug("remove clusterId {} secondary beId {} changeTimestamp {} expireTimestamp {} replica info {}",
                    clusterId, beId, changeTimestamp, expireTimestamp, this);
            secondaryClusterToBackends.remove(clusterId);
            return;
        }
    }

    public List<Backend> getAllPrimaryBes() {
        List<Backend> result = new ArrayList<Backend>();
        primaryClusterToBackends.keySet().forEach(clusterId -> {
            List<Long> backendIds = primaryClusterToBackends.get(clusterId);
            if (backendIds == null || backendIds.isEmpty()) {
                return;
            }
            Long beId = backendIds.get(0);
            if (beId != -1) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                result.add(backend);
            }
        });
        return result;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert CloudReplica: {}, primaryClusterToBackend: {}, primaryClusterToBackends: {}",
                    this.getId(), this.primaryClusterToBackend, this.primaryClusterToBackends);
        }
        if (primaryClusterToBackend != null) {
            for (Map.Entry<String, Long> entry : primaryClusterToBackend.entrySet()) {
                primaryClusterToBackends.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
            }
            this.primaryClusterToBackend = null;
        }
    }
}
