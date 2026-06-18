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
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CloudReplica extends Replica implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(CloudReplica.class);

    // a replica is mapped to one BE in a cluster, use primaryClusterToBackend instead of primaryClusterToBackends
    @Deprecated
    @SerializedName(value = "bes")
    private ConcurrentHashMap<String, List<Long>> primaryClusterToBackends = null;
    @SerializedName(value = "be")
    private ConcurrentHashMap<String, Long> primaryClusterToBackend = new ConcurrentHashMap<>();
    @SerializedName(value = "tableId")
    private long tableId = -1;
    @SerializedName(value = "partitionId")
    private long partitionId = -1;
    @SerializedName(value = "idx")
    private long idx = -1;
    // last time to get tablet stats
    @Getter
    @Setter
    @SerializedName(value = "gst")
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
    @SerializedName(value = "sii")
    int statsIntervalIndex = 0;

    private static final Random rand = new Random();

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
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.idx = idx;
    }

    private boolean isColocated() {
        return Env.getCurrentColocateIndex().isColocateTableNoLock(tableId);
    }

    private boolean isDecommissioningOrDecommissioned(Backend be) {
        boolean decommissioning = be.isDecommissioning();
        boolean decommissioned = be.isDecommissioned();
        if ((decommissioning || decommissioned) && LOG.isDebugEnabled()) {
            LOG.debug("backend {} is filtered by decommission state, decommissioning={}, decommissioned={}, "
                            + "replica info {}",
                    be.getId(), decommissioning, decommissioned, this);
        }
        return decommissioning || decommissioned;
    }

    private boolean isQueryAvailableAndNotDecommissioning(Backend be) {
        return be != null && be.isQueryAvailable() && !isDecommissioningOrDecommissioned(be);
    }

    public long getColocatedBeId(String clusterId) throws ComputeGroupException {
        List<Backend> clusterBackends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterId(clusterId);
        return getColocatedBeId(clusterId, clusterBackends);
    }

    // Same as getColocatedBeId(clusterId) but reuses an already fetched backend list of
    // the compute group. Lets callers that resolve many replicas across the same compute
    // groups (e.g. the colocate proc display) fetch each group's backends only once.
    public long getColocatedBeId(String clusterId, List<Backend> clusterBackends) throws ComputeGroupException {
        CloudSystemInfoService infoService = ((CloudSystemInfoService) Env.getCurrentSystemInfo());
        // Do not filter by isQueryAvailable() here. Recently-dead BEs must stay in
        // the hash ring during the grace period to avoid unnecessary full rehash.
        List<Backend> bes = clusterBackends;
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
                if (isDecommissioningOrDecommissioned(be)) {
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
        if (availableBes.size() != bes.size()) {
            long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
            if (bes.stream().anyMatch(be -> !be.isAlive() && be.getLastUpdateMs() > needRehashDeadTime)) {
                List<Backend> beAliveOrDeadShort = bes.stream()
                        .filter(be -> be.isAlive() || be.getLastUpdateMs() > needRehashDeadTime)
                        .collect(Collectors.toList());
                Backend be = pickColocatedBackendForDeadGrace(infoService, groupId, clusterId, beAliveOrDeadShort);
                if (be.isAlive() && !isDecommissioningOrDecommissioned(be)) {
                    return be.getId();
                }
            }
        }

        return pickColocatedBackend(infoService, groupId, clusterId, availableBes).getId();
    }

    private Backend pickColocatedBackendForDeadGrace(CloudSystemInfoService infoService, GroupId groupId,
            String clusterId, List<Backend> availableBes) {
        if (!Config.enable_cloud_colocate_consistent_hash) {
            return pickColocatedBackend(infoService, groupId, clusterId, availableBes);
        }
        checkColocatedBucketIdx(groupId);
        long[] availableBeIds = availableBes.stream().mapToLong(Backend::getId).toArray();
        long pickedBeId = CloudColocatePlacement.pickBackendId(groupId.grpId, idx, availableBeIds);
        return findPickedBackend(pickedBeId, groupId, clusterId, availableBes);
    }

    Backend pickColocatedBackend(CloudSystemInfoService infoService, GroupId groupId, String clusterId,
            List<Backend> availableBes) {
        if (Config.enable_cloud_colocate_consistent_hash) {
            int bucketNum = checkColocatedBucketIdx(groupId);
            List<Long> availableBeIds = availableBes.stream().map(Backend::getId).collect(Collectors.toList());
            long pickedBeId = infoService.getCloudColocateHrwBeId(groupId, clusterId, availableBeIds, bucketNum, idx);
            return findPickedBackend(pickedBeId, groupId, clusterId, availableBes);
        }

        HashCode hashCode = Hashing.murmur3_128().hashLong(groupId.grpId);
        long index = getIndexByBeNum(hashCode.asLong() + idx, availableBes.size());
        return availableBes.get((int) index);
    }

    private Backend findPickedBackend(long pickedBeId, GroupId groupId, String clusterId, List<Backend> availableBes) {
        return availableBes.stream().filter(be -> be.getId() == pickedBeId).findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "picked colocate backend %s is not in candidate set, group %s, cluster %s, bucket idx %s, "
                                + "candidate backend ids %s",
                        pickedBeId, groupId, clusterId, idx,
                        availableBes.stream().map(Backend::getId).collect(Collectors.toList()))));
    }

    private int checkColocatedBucketIdx(GroupId groupId) {
        int bucketNum = Env.getCurrentColocateIndex().getBucketsNumNoLock(groupId);
        if (idx < 0 || idx >= bucketNum) {
            throw new IllegalStateException(String.format(
                    "colocate bucket idx %s is outside bucket num %s for group %s", idx, bucketNum, groupId));
        }
        return bucketNum;
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

        return primaryClusterToBackend.getOrDefault(clusterId, -1L);
    }

    // For proc display only. In cloud mode a replica is hashed to a different BE in each
    // compute group, so expose a clusterId -> backendId mapping; the proc display builds
    // a separate bucket sequence per compute group from it so each group's sequence is
    // self-consistent. Do not collapse this into a single BE (e.g. the first one):
    // backends differ across compute groups and would not match.
    //
    // ATTN: colocated replicas do NOT use primaryClusterToBackend (see getBackendIdImpl /
    // getClusterPrimaryBackendId, which short-circuit to getColocatedBeId), so that cache
    // is empty for them. Resolve their placement per compute group on the fly instead.
    // This reads CloudSystemInfoService / the colocate index, so callers must invoke it
    // OUTSIDE any table lock to avoid nested lock acquisition. It does not auto-start any
    // compute group: getColocatedBeId only reads the already-known backends of a clusterId.
    // computeGroupBackendCache maps compute group id -> getBackendsByClusterId() result and
    // is shared across all replicas resolved in a single proc call, so each compute group's
    // backend list is fetched only once instead of once per replica.
    @Override
    public Map<String, Long> getClusterToBackendForProcDisplay(Map<String, List<Backend>> computeGroupBackendCache) {
        if (!isColocated()) {
            return new HashMap<>(primaryClusterToBackend);
        }
        Map<String, Long> result = new HashMap<>();
        CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
        for (String clusterId : infoService.getCloudClusterIds()) {
            try {
                List<Backend> clusterBackends =
                        computeGroupBackendCache.computeIfAbsent(clusterId, infoService::getBackendsByClusterId);
                long backendId = getColocatedBeId(clusterId, clusterBackends);
                if (backendId != -1L) {
                    result.put(clusterId, backendId);
                }
            } catch (ComputeGroupException e) {
                // Skip compute groups that currently have no available backend.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip compute group {} for colocate proc display, replica {}",
                            clusterId, getId(), e);
                }
            }
        }
        return result;
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

            List<Long> res = hashReplicaToBes(clusterId, false, Config.cloud_replica_num);
            if (LOG.isDebugEnabled()) {
                LOG.debug("rehash multi replica backend, clusterId {}, replica info {}, indexRand {}, hashedBes {}",
                        clusterId, this, indexRand, res);
            }
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

        // use primaryClusterToBackend, if find be normal
        Backend be = getPrimaryBackend(clusterId, false);
        if (isQueryAvailableAndNotDecommissioning(be)) {
            return be.getId();
        }

        if (!Config.enable_immediate_be_assign) {
            // use secondaryClusterToBackends, if find be normal
            be = getSecondaryBackend(clusterId);
            if (isQueryAvailableAndNotDecommissioning(be)) {
                return be.getId();
            }
        }

        if (DebugPointUtil.isEnable("CloudReplica.getBackendIdImpl.primaryClusterToBackends")) {
            LOG.info("Debug Point enable CloudReplica.getBackendIdImpl.primaryClusterToBackends");
            return -1;
        }

        // be abnormal, rehash it. configure settings to different maps
        long pickBeId = hashReplicaToBe(clusterId, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("rehash replica backend, clusterId {}, pickedBeId {}, immediateAssign {}, replica info {}, "
                            + "primaryBackend {}, secondaryBackend {}",
                    clusterId, pickBeId, Config.enable_immediate_be_assign, this, getPrimaryBackend(clusterId, false),
                    getSecondaryBackend(clusterId));
        }
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
                if (isDecommissioningOrDecommissioned(be)) {
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

    private List<Long> hashReplicaToBes(String clusterId, boolean isBackGround, int replicaNum)
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
                if (isDecommissioningOrDecommissioned(be)) {
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
            bes.add(pickedBeId);
        }

        return bes;
    }

    @Override
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        // ATTN: expectedVersion is not used here, and OlapScanNode.addScanRangeLocations
        // depends this feature to implement snapshot partition version. See comments in
        // OlapScanNode.addScanRangeLocations for details.
        return true;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIdx() {
        return idx;
    }

    public void updateClusterToPrimaryBe(String cluster, long beId) {
        primaryClusterToBackend.put(cluster, beId);
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
        primaryClusterToBackend.remove(cluster);
        secondaryClusterToBackends.remove(cluster);
    }

    /**
     * Returns the set of compute group IDs that have primary backends for this replica.
     * Used by lazy fetch path to also collect secondary backends per compute group.
     */
    public Set<String> getPrimaryComputeGroupIds() {
        return primaryClusterToBackend.keySet();
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
        if (isColocated()) {
            List<String> clusterIds = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIds();
            for (String clusterId : clusterIds) {
                try {
                    long beId = getColocatedBeId(clusterId);
                    if (beId != -1) {
                        Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                        if (backend != null) {
                            result.add(backend);
                        }
                    }
                } catch (ComputeGroupException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("failed to get colocated be for cluster {}, replica {}", clusterId, this, e);
                    }
                }
            }
            return result;
        }
        primaryClusterToBackend.forEach((clusterId, beId) -> {
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
            LOG.debug("convert CloudReplica: {}, primaryClusterToBackends: {}, primaryClusterToBackend: {}",
                    this.getId(), this.primaryClusterToBackends, this.primaryClusterToBackend);
        }
        if (primaryClusterToBackends != null) {
            for (Map.Entry<String, List<Long>> entry : primaryClusterToBackends.entrySet()) {
                String clusterId = entry.getKey();
                List<Long> beIds = entry.getValue();
                if (beIds != null && !beIds.isEmpty()) {
                    primaryClusterToBackend.put(clusterId, beIds.get(0));
                }
            }
            this.primaryClusterToBackends = null;
        }
    }
}
