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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TCheckWarmUpCacheAsyncRequest;
import org.apache.doris.thrift.TCheckWarmUpCacheAsyncResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWarmUpCacheAsyncRequest;
import org.apache.doris.thrift.TWarmUpCacheAsyncResponse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToTabletsGlobal =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToColocateTabletsGlobal =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    // used for cloud tablet report
    private volatile ConcurrentHashMap<Long, Set<Tablet>> beToTabletsGlobalInSecondary =
            new ConcurrentHashMap<Long, Set<Tablet>>();

    private Map<Long, Set<Tablet>> futureBeToTabletsGlobal;

    private Map<String, List<Long>> clusterToBes;

    private Set<Long> allBes;

    // partitionId -> indexId -> be -> tablet
    private Map<Long, Map<Long, Map<Long, Set<Tablet>>>> partitionToTablets;

    private Map<Long, Map<Long, Map<Long, Set<Tablet>>>> futurePartitionToTablets;

    // tableId -> be -> tablet
    private Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTable;

    private Map<Long, Map<Long, Set<Tablet>>> futureBeToTabletsInTable;

    private Map<Long, Long> beToDecommissionedTime = new HashMap<Long, Long>();

    private Random rand = new Random();

    private boolean indexBalanced = true;

    private boolean tableBalanced = true;

    private LinkedBlockingQueue<Pair<Long, Long>> tabletsMigrateTasks = new LinkedBlockingQueue<Pair<Long, Long>>();

    private Map<InfightTablet, InfightTask> tabletToInfightTask = new HashMap<>();

    private CloudSystemInfoService cloudSystemInfoService;

    public CloudTabletRebalancer(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud tablet rebalancer", Config.cloud_tablet_rebalancer_interval_second * 1000);
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    private interface Operator {
        void op(Database db, Table table, Partition partition, MaterializedIndex index, String cluster);
    }

    public enum BalanceType {
        GLOBAL,
        TABLE,
        PARTITION
    }

    @Getter
    private class InfightTablet {
        private final Long tabletId;
        private final String clusterId;

        public InfightTablet(Long tabletId, String clusterId) {
            this.tabletId = tabletId;
            this.clusterId = clusterId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InfightTablet that = (InfightTablet) o;
            return tabletId.equals(that.tabletId) && clusterId.equals(that.clusterId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tabletId, clusterId);
        }
    }

    private class InfightTask {
        public Tablet pickedTablet;
        public long srcBe;
        public long destBe;
        public Map<Long, Set<Tablet>> beToTablets;
        public long startTimestamp;
        BalanceType balanceType;
    }

    private class TransferPairInfo {
        public long srcBe;
        public long destBe;
        public long minTabletsNum;
        public long maxTabletsNum;
    }

    public Set<Long> getSnapshotTabletsInPrimaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        Set<Tablet> tablets = beToTabletsGlobal.get(beId);
        if (tablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(tablets)) {
                tabletIds.add(tablet.getId());
            }
        }

        Set<Tablet> colocateTablets = beToColocateTabletsGlobal.get(beId);
        if (colocateTablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(colocateTablets)) {
                tabletIds.add(tablet.getId());
            }
        }

        return tabletIds;
    }

    public Set<Long> getSnapshotTabletsInSecondaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        Set<Tablet> tablets = beToTabletsGlobalInSecondary.get(beId);
        if (tablets != null) {
            //  Create a copy
            for (Tablet tablet : new HashSet<>(tablets)) {
                tabletIds.add(tablet.getId());
            }
        }
        return tabletIds;
    }

    public Set<Long> getSnapshotTabletsInPrimaryAndSecondaryByBeId(Long beId) {
        Set<Long> tabletIds = Sets.newHashSet();
        tabletIds.addAll(getSnapshotTabletsInPrimaryByBeId(beId));
        tabletIds.addAll(getSnapshotTabletsInSecondaryByBeId(beId));
        return tabletIds;
    }

    public int getTabletNumByBackendId(long beId) {
        Set<Tablet> tablets = beToTabletsGlobal.get(beId);
        Set<Tablet> colocateTablets = beToColocateTabletsGlobal.get(beId);

        int tabletsSize = (tablets == null) ? 0 : tablets.size();
        int colocateTabletsSize = (colocateTablets == null) ? 0 : colocateTablets.size();

        return tabletsSize + colocateTabletsSize;
    }

    // 1 build cluster to backends info
    // 2 complete route info
    // 3 check whether the inflight preheating task has been completed
    // 4 migrate tablet for smooth upgrade
    // 5 statistics be to tablets mapping information
    // 6 partition-level balance
    // 7 if tablets in partition-level already balanced, perform table balance
    // 8 if tablets in partition-level and table-level already balanced, perform global balance
    // 9 check whether all tablets of decomission node have been migrated
    @Override
    protected void runAfterCatalogReady() {
        if (Config.enable_cloud_multi_replica) {
            LOG.info("Tablet balance is temporarily not supported when multi replica enabled");
            return;
        }

        LOG.info("cloud tablet rebalance begin");
        long start = System.currentTimeMillis();

        buildClusterToBackendMap();
        if (!completeRouteInfo()) {
            return;
        }

        checkInflightWarmUpCacheAsync();
        statRouteInfo();
        migrateTabletsForSmoothUpgrade();
        statRouteInfo();

        indexBalanced = true;
        tableBalanced = true;

        performBalancing();

        checkDecommissionState(clusterToBes);
        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    private void buildClusterToBackendMap() {
        clusterToBes = new HashMap<>();
        allBes = new HashSet<>();
        for (Long beId : cloudSystemInfoService.getAllBackendIds()) {
            Backend be = cloudSystemInfoService.getBackend(beId);
            if (be == null) {
                LOG.info("backend {} not found", beId);
                continue;
            }
            clusterToBes.putIfAbsent(be.getCloudClusterId(), new ArrayList<>());
            clusterToBes.get(be.getCloudClusterId()).add(beId);
            allBes.add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);
    }

    private void migrateTabletsForSmoothUpgrade() {
        Pair<Long, Long> pair;
        while (!tabletsMigrateTasks.isEmpty()) {
            try {
                pair = tabletsMigrateTasks.take();
                LOG.debug("begin tablets migration from be {} to be {}", pair.first, pair.second);
                migrateTablets(pair.first, pair.second);
            } catch (InterruptedException e) {
                LOG.warn("migrate tablets failed", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void performBalancing() {
        // ATTN: In general, the order of `balance` should follow `partition`, `table`, and `global`.
        // This is because performing `global` scheduling first and then `partition` scheduling may
        // lead to ineffective scheduling. Specifically, `global` scheduling might place multiple tablets belonging
        // to the same table or partition onto the same BE, while `partition` scheduling later requires these tablets
        // to be dispersed across different BEs, resulting in unnecessary scheduling.
        if (Config.enable_cloud_partition_balance) {
            balanceAllPartitions();
        }
        if (Config.enable_cloud_table_balance && indexBalanced) {
            balanceAllTables();
        }
        if (Config.enable_cloud_global_balance && indexBalanced && tableBalanced) {
            globalBalance();
        }
    }

    public void balanceAllPartitions() {
        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("before partition balance be {} tablet num(current + pre heating inflight) {}",
                     entry.getKey(), entry.getValue().size());
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        // balance in partitions/index
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInPartition(entry.getValue(), entry.getKey(), infos);
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos);
        LOG.info("collect to editlog partitions before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void balanceAllTables() {
        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before table balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("before table balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        // balance in partitions/index
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceInTable(entry.getValue(), entry.getKey(), infos);
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos);
        LOG.info("collect to editlog table before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after table balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after table balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void globalBalance() {
        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("before global balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            balanceImpl(entry.getValue(), entry.getKey(), futureBeToTabletsGlobal, BalanceType.GLOBAL, infos);
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos);
        LOG.info("collect to editlog global before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        for (Map.Entry<Long, Set<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, Set<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after global balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void checkInflightWarmUpCacheAsync() {
        Map<Long, List<InfightTask>> beToInfightTasks = new HashMap<Long, List<InfightTask>>();

        for (Map.Entry<InfightTablet, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToInfightTasks.putIfAbsent(entry.getValue().destBe, new ArrayList<>());
            beToInfightTasks.get(entry.getValue().destBe).add(entry.getValue());
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
        for (Map.Entry<Long, List<InfightTask>> entry : beToInfightTasks.entrySet()) {
            LOG.info("before pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
            Backend destBackend = cloudSystemInfoService.getBackend(entry.getKey());
            if (DebugPointUtil.isEnable("CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull")) {
                LOG.info("debug point CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull, be {}", destBackend);
                destBackend = null;
            }
            if (destBackend == null || (!destBackend.isAlive() && destBackend.getLastUpdateMs() < needRehashDeadTime)) {
                List<InfightTablet> toRemove = new LinkedList<>();
                for (InfightTask task : entry.getValue()) {
                    for (InfightTablet key : tabletToInfightTask.keySet()) {
                        toRemove.add(new InfightTablet(task.pickedTablet.getId(), key.clusterId));
                    }
                }
                for (InfightTablet key : toRemove) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("remove tablet {}-{}", key.getClusterId(), key.getTabletId());
                    }
                    tabletToInfightTask.remove(key);
                }
                continue;
            }
            if (!destBackend.isAlive()) {
                continue;
            }
            List<Long> tablets = entry.getValue().stream()
                    .map(task -> task.pickedTablet.getId()).collect(Collectors.toList());
            Map<Long, Boolean> taskDone = sendCheckWarmUpCacheAsyncRpc(tablets, entry.getKey());
            if (taskDone == null) {
                LOG.warn("sendCheckWarmUpCacheAsyncRpc return null be {}, inFight tasks {}",
                        entry.getKey(), entry.getValue());
                continue;
            }
            String clusterId = cloudSystemInfoService.getBackend(entry.getKey()).getCloudClusterId();
            for (Map.Entry<Long, Boolean> result : taskDone.entrySet()) {
                InfightTask task = tabletToInfightTask
                        .getOrDefault(new InfightTablet(result.getKey(), clusterId), null);
                if (task != null && (result.getValue() || System.currentTimeMillis() / 1000 - task.startTimestamp
                            > Config.cloud_pre_heating_time_limit_sec)) {
                    if (!result.getValue()) {
                        LOG.info("{} pre cache timeout, forced to change the mapping", result.getKey());
                    }
                    updateClusterToBeMap(task.pickedTablet, task.destBe, clusterId, infos);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("remove tablet {}-{}", clusterId, task.pickedTablet.getId());
                    }
                    tabletToInfightTask.remove(new InfightTablet(task.pickedTablet.getId(), clusterId));
                }
            }
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos);
        LOG.info("collect to editlog warmup before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        // recalculate inflight beToTablets, just for print the log
        beToInfightTasks.clear();
        for (Map.Entry<InfightTablet, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToInfightTasks.putIfAbsent(entry.getValue().destBe, new ArrayList<>());
            beToInfightTasks.get(entry.getValue().destBe).add(entry.getValue());
        }

        for (Map.Entry<Long, List<InfightTask>> entry : beToInfightTasks.entrySet()) {
            LOG.info("after pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
        }
    }

    public void checkDecommissionState(Map<String, List<Long>> clusterToBes) {
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            List<Long> beList = entry.getValue();
            for (long beId : beList) {
                Set<Tablet> tablets = beToTabletsGlobal.get(beId);
                int tabletNum = tablets == null ? 0 : tablets.size();
                Backend backend = cloudSystemInfoService.getBackend(beId);
                if (backend == null) {
                    LOG.info("backend {} not found", beId);
                    continue;
                }
                if (!backend.isDecommissioning()) {
                    continue;
                }
                // here check wal
                long walNum = Env.getCurrentEnv().getGroupCommitManager().getAllWalQueueSize(backend);
                LOG.info("check decommissioning be {} state {} tabletNum {} isActive {} beList {}, wal num {}",
                        backend.getId(), backend.isDecommissioning(), tabletNum, backend.isActive(), beList, walNum);
                if ((tabletNum != 0 || backend.isActive() || walNum != 0) && beList.size() != 1) {
                    continue;
                }
                if (beToDecommissionedTime.containsKey(beId)) {
                    continue;
                }
                LOG.info("prepare to notify meta service be {} decommissioned", backend.getAddress());
                Cloud.AlterClusterRequest.Builder builder =
                        Cloud.AlterClusterRequest.newBuilder();
                builder.setCloudUniqueId(Config.cloud_unique_id);
                builder.setOp(Cloud.AlterClusterRequest.Operation.NOTIFY_DECOMMISSIONED);

                Cloud.ClusterPB.Builder clusterBuilder =
                        Cloud.ClusterPB.newBuilder();
                clusterBuilder.setClusterName(backend.getCloudClusterName());
                clusterBuilder.setClusterId(backend.getCloudClusterId());
                clusterBuilder.setType(Cloud.ClusterPB.Type.COMPUTE);

                Cloud.NodeInfoPB.Builder nodeBuilder = Cloud.NodeInfoPB.newBuilder();
                nodeBuilder.setIp(backend.getHost());
                nodeBuilder.setHeartbeatPort(backend.getHeartbeatPort());
                nodeBuilder.setCloudUniqueId(backend.getCloudUniqueId());
                nodeBuilder.setStatus(Cloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONED);

                clusterBuilder.addNodes(nodeBuilder);
                builder.setCluster(clusterBuilder);

                Cloud.AlterClusterResponse response;
                try {
                    response = MetaServiceProxy.getInstance().alterCluster(builder.build());
                    if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                        LOG.warn("notify decommission response: {}", response);
                        continue;
                    }
                    LOG.info("notify decommission response: {} ", response);
                } catch (RpcException e) {
                    LOG.warn("failed to notify decommission", e);
                    continue;
                }
                beToDecommissionedTime.put(beId, System.currentTimeMillis() / 1000);
            }
        }
    }

    private boolean completeRouteInfo() {
        List<UpdateCloudReplicaInfo> updateReplicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        long[] assignedErrNum = {0L};
        long needRehashDeadTime = System.currentTimeMillis() - Config.rehash_tablet_after_be_dead_seconds * 1000L;
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean assigned = false;
            List<Long> beIds = new ArrayList<Long>();
            List<Long> tabletIds = new ArrayList<Long>();
            boolean isColocated = Env.getCurrentColocateIndex().isColocateTable(table.getId());
            for (Tablet tablet : index.getTablets()) {
                for (Replica r : tablet.getReplicas()) {
                    CloudReplica replica = (CloudReplica) r;
                    // clean secondary map
                    replica.checkAndClearSecondaryClusterToBe(cluster, needRehashDeadTime);
                    InfightTablet taskKey = new InfightTablet(tablet.getId(), cluster);
                    // colocate table no need to update primary backends
                    if (isColocated) {
                        replica.clearClusterToBe(cluster);
                        tabletToInfightTask.remove(taskKey);
                        continue;
                    }

                    // primary backend is alive or dead not long
                    Backend be = replica.getPrimaryBackend(cluster);
                    if (be != null && (be.isQueryAvailable()
                            || (!be.isQueryDisabled() && be.getLastUpdateMs() > needRehashDeadTime))) {
                        beIds.add(be.getId());
                        tabletIds.add(tablet.getId());
                        continue;
                    }

                    // primary backend not available too long, change one
                    long beId = -1L;
                    be = replica.getSecondaryBackend(cluster);
                    if (be != null && be.isQueryAvailable()) {
                        beId = be.getId();
                    } else {
                        InfightTask task = tabletToInfightTask.get(taskKey);
                        be = task == null ? null : Env.getCurrentSystemInfo().getBackend(task.destBe);
                        if (be != null && be.isQueryAvailable()) {
                            beId = be.getId();
                        } else {
                            try {
                                beId = ((CloudReplica) replica).hashReplicaToBe(cluster, true);
                            } catch (ComputeGroupException e) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("failed to hash replica to be {}", cluster, e);
                                }
                                beId = -1;
                            }
                        }
                    }

                    if (beId <= 0) {
                        assignedErrNum[0]++;
                        continue;
                    }

                    tabletToInfightTask.remove(taskKey);

                    ((CloudReplica) replica).updateClusterToPrimaryBe(cluster, beId);
                    beIds.add(beId);
                    tabletIds.add(tablet.getId());
                    assigned = true;
                }
            }

            if (assigned) {
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(db.getId(), table.getId(),
                        partition.getId(), index.getId(), cluster, beIds, tabletIds);
                updateReplicaInfos.add(info);
            }
        });

        LOG.info("collect to editlog route {} infos, error num {}", updateReplicaInfos.size(), assignedErrNum[0]);

        if (updateReplicaInfos.isEmpty()) {
            return true;
        }

        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(updateReplicaInfos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return false;
        }

        return true;
    }

    public void fillBeToTablets(long be, long tableId, long partId, long indexId, Tablet tablet,
            Map<Long, Set<Tablet>> globalBeToTablets,
            Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTable,
            Map<Long, Map<Long, Map<Long, Set<Tablet>>>> partToTablets) {
        // global
        globalBeToTablets.putIfAbsent(be, new HashSet<Tablet>());
        globalBeToTablets.get(be).add(tablet);

        // table
        beToTabletsInTable.putIfAbsent(tableId, new HashMap<Long, Set<Tablet>>());
        Map<Long, Set<Tablet>> beToTabletsOfTable = beToTabletsInTable.get(tableId);
        beToTabletsOfTable.putIfAbsent(be, new HashSet<Tablet>());
        beToTabletsOfTable.get(be).add(tablet);

        // partition
        partToTablets.putIfAbsent(partId, new HashMap<Long, Map<Long, Set<Tablet>>>());
        Map<Long, Map<Long, Set<Tablet>>> indexToTablets = partToTablets.get(partId);
        indexToTablets.putIfAbsent(indexId, new HashMap<Long, Set<Tablet>>());
        Map<Long, Set<Tablet>> beToTabletsOfIndex = indexToTablets.get(indexId);
        beToTabletsOfIndex.putIfAbsent(be, new HashSet<Tablet>());
        beToTabletsOfIndex.get(be).add(tablet);
    }

    public void statRouteInfo() {
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToTabletsGlobal = new ConcurrentHashMap<Long, Set<Tablet>>();
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToTabletsGlobalInSecondary
                = new ConcurrentHashMap<Long, Set<Tablet>>();
        ConcurrentHashMap<Long, Set<Tablet>> tmpBeToColocateTabletsGlobal
                = new ConcurrentHashMap<Long, Set<Tablet>>();

        futureBeToTabletsGlobal = new HashMap<Long, Set<Tablet>>();

        partitionToTablets = new HashMap<Long, Map<Long, Map<Long, Set<Tablet>>>>();
        futurePartitionToTablets = new HashMap<Long, Map<Long, Map<Long, Set<Tablet>>>>();

        beToTabletsInTable = new HashMap<Long, Map<Long, Set<Tablet>>>();
        futureBeToTabletsInTable = new HashMap<Long, Map<Long, Set<Tablet>>>();

        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean isColocated = Env.getCurrentColocateIndex().isColocateTable(table.getId());
            for (Tablet tablet : index.getTablets()) {
                for (Replica r : tablet.getReplicas()) {
                    CloudReplica replica = (CloudReplica) r;
                    if (isColocated) {
                        long beId = -1L;
                        try {
                            beId = replica.getColocatedBeId(cluster);
                        } catch (ComputeGroupException e) {
                            continue;
                        }
                        if (allBes.contains(beId)) {
                            Set<Tablet> colocateTablets =
                                    tmpBeToColocateTabletsGlobal.computeIfAbsent(beId, k -> new HashSet<>());
                            colocateTablets.add(tablet);
                        }
                        continue;
                    }

                    Backend be = replica.getPrimaryBackend(cluster);
                    long beId = be == null ? -1L : be.getId();
                    if (!allBes.contains(beId)) {
                        continue;
                    }

                    Backend secondaryBe = replica.getSecondaryBackend(cluster);
                    long secondaryBeId = secondaryBe == null ? -1L : secondaryBe.getId();
                    if (allBes.contains(secondaryBeId)) {
                        Set<Tablet> tablets = tmpBeToTabletsGlobalInSecondary
                                .computeIfAbsent(secondaryBeId, k -> new HashSet<>());
                        tablets.add(tablet);
                    }

                    InfightTablet taskKey = new InfightTablet(tablet.getId(), cluster);
                    InfightTask task = tabletToInfightTask.get(taskKey);
                    long futureBeId = task == null ? beId : task.destBe;
                    fillBeToTablets(beId, table.getId(), partition.getId(), index.getId(), tablet,
                            tmpBeToTabletsGlobal, beToTabletsInTable, this.partitionToTablets);

                    fillBeToTablets(futureBeId, table.getId(), partition.getId(), index.getId(), tablet,
                            futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
                }
            }
        });

        beToTabletsGlobal = tmpBeToTabletsGlobal;
        beToTabletsGlobalInSecondary = tmpBeToTabletsGlobalInSecondary;
        beToColocateTabletsGlobal = tmpBeToColocateTabletsGlobal;
    }

    public void loopCloudReplica(Operator operator) {
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                table.readLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
                                String cluster = entry.getKey();
                                operator.op(db, table, partition, index, cluster);
                            }
                        } // end for indices
                    } // end for partitions
                } finally {
                    table.readUnlock();
                }
            }
        }
    }

    public void balanceInPartition(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos) {
        // balance all partition
        for (Map.Entry<Long, Map<Long, Map<Long, Set<Tablet>>>> partitionEntry : futurePartitionToTablets.entrySet()) {
            Map<Long, Map<Long, Set<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            for (Map.Entry<Long, Map<Long, Set<Tablet>>> entry : indexToTablets.entrySet()) {
                // balance a index
                balanceImpl(bes, clusterId, entry.getValue(), BalanceType.PARTITION, infos);
            }
        }
    }

    public void balanceInTable(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos) {
        // balance all tables
        for (Map.Entry<Long, Map<Long, Set<Tablet>>> entry : futureBeToTabletsInTable.entrySet()) {
            balanceImpl(bes, clusterId, entry.getValue(), BalanceType.TABLE, infos);
        }
    }

    private void sendPreHeatingRpc(Tablet pickedTablet, long srcBe, long destBe) throws Exception {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend srcBackend = cloudSystemInfoService.getBackend(srcBe);
        Backend destBackend = cloudSystemInfoService.getBackend(destBe);
        boolean ok = true;
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TWarmUpCacheAsyncRequest req = new TWarmUpCacheAsyncRequest();
            req.setHost(srcBackend.getHost());
            req.setBrpcPort(srcBackend.getBrpcPort());
            List<Long> tablets = new ArrayList<Long>();
            tablets.add(pickedTablet.getId());
            req.setTabletIds(tablets);
            TWarmUpCacheAsyncResponse result = client.warmUpCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("pre cache failed status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("send pre heating rpc error. backend[{}]", destBackend.getId(), e);
            ok = false;
            throw e;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    private Map<Long, Boolean> sendCheckWarmUpCacheAsyncRpc(List<Long> tabletIds, long be) {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend destBackend = cloudSystemInfoService.getBackend(be);
        boolean ok = true;
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TCheckWarmUpCacheAsyncRequest req = new TCheckWarmUpCacheAsyncRequest();
            req.setTablets(tabletIds);
            TCheckWarmUpCacheAsyncResponse result = client.checkWarmUpCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("check pre tablets {} cache status {} {}", tabletIds, result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            } else {
                LOG.info("check pre tablets {} cache succ status {} {}", tabletIds, result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
            return result.getTaskDone();
        } catch (Exception e) {
            LOG.warn("send check pre cache rpc error. tablets{} backend[{}]", tabletIds, destBackend.getId(), e);
            ok = false;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
        return null;
    }

    private void updateBeToTablets(Tablet pickedTablet, long srcBe, long destBe,
            Map<Long, Set<Tablet>> globalBeToTablets,
            Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTable,
            Map<Long, Map<Long, Map<Long, Set<Tablet>>>> partToTablets) {
        CloudReplica replica = (CloudReplica) pickedTablet.getReplicas().get(0);
        long tableId = replica.getTableId();
        long partId = replica.getPartitionId();
        long indexId = replica.getIndexId();

        globalBeToTablets.get(srcBe).remove(pickedTablet);
        beToTabletsInTable.get(tableId).get(srcBe).remove(pickedTablet);
        partToTablets.get(partId).get(indexId).get(srcBe).remove(pickedTablet);

        fillBeToTablets(destBe, tableId, partId, indexId, pickedTablet, globalBeToTablets, beToTabletsInTable,
                        partToTablets);
    }

    private void updateClusterToBeMap(Tablet pickedTablet, long destBe, String clusterId,
                                      List<UpdateCloudReplicaInfo> infos) {
        CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);
        Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
        if (db == null) {
            return;
        }
        OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
        if (table == null) {
            return;
        }

        table.readLock();

        try {
            if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                return;
            }

            cloudReplica.updateClusterToPrimaryBe(clusterId, destBe);
            UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                    cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                    pickedTablet.getId(), cloudReplica.getId(), clusterId, destBe);
            infos.add(info);
        } finally {
            table.readUnlock();
        }
    }

    private boolean getTransferPair(List<Long> bes, Map<Long, Set<Tablet>> beToTablets, long avgNum,
                                    TransferPairInfo pairInfo) {
        long srcBe = findSourceBackend(bes, beToTablets);
        long destBe = findDestinationBackend(bes, beToTablets, srcBe);

        if (srcBe == -1 || destBe == -1) {
            return false; // No valid backend found
        }

        long minTabletsNum = beToTablets.get(destBe) == null ? 0 : beToTablets.get(destBe).size();
        long maxTabletsNum = beToTablets.get(srcBe) == null ? 0 : beToTablets.get(srcBe).size();

        if (!isTransferValid(srcBe, minTabletsNum, maxTabletsNum, avgNum)) {
            return false; // Transfer conditions not met
        }

        pairInfo.srcBe = srcBe;
        pairInfo.destBe = destBe;
        pairInfo.minTabletsNum = minTabletsNum;
        pairInfo.maxTabletsNum = maxTabletsNum;
        return true;
    }

    private long findSourceBackend(List<Long> bes, Map<Long, Set<Tablet>> beToTablets) {
        long srcBe = -1;
        long maxTabletsNum = 0;

        for (Long be : bes) {
            long tabletNum = beToTablets.getOrDefault(be, Collections.emptySet()).size();
            Backend backend = cloudSystemInfoService.getBackend(be);

            // Check if the backend is decommissioned
            if (backend != null) {
                if (backend.isDecommissioning() && tabletNum > 0) {
                    srcBe = be; // Mark as source if decommissioned and has tablets
                    break; // Exit early if we found a decommissioned backend
                }
                if (!backend.isDecommissioning() && tabletNum > maxTabletsNum) {
                    srcBe = be;
                    maxTabletsNum = tabletNum;
                }
            } else {
                LOG.info("backend {} not found", be);
            }
        }
        return srcBe;
    }

    private long findDestinationBackend(List<Long> bes, Map<Long, Set<Tablet>> beToTablets, long srcBe) {
        long destBe = -1;
        long minTabletsNum = Long.MAX_VALUE;

        for (Long be : bes) {
            long tabletNum = beToTablets.getOrDefault(be, Collections.emptySet()).size();
            Backend backend = cloudSystemInfoService.getBackend(be);
            if (backend != null && backend.isAlive() && !backend.isDecommissioning() && !backend.isSmoothUpgradeSrc()) {
                if (tabletNum < minTabletsNum) {
                    destBe = be;
                    minTabletsNum = tabletNum;
                }
            }
        }
        return destBe;
    }

    private boolean isTransferValid(long srcBe, long minTabletsNum, long maxTabletsNum, long avgNum) {
        boolean srcDecommissioned = cloudSystemInfoService.getBackend(srcBe).isDecommissioning();

        if (!srcDecommissioned) {
            if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                    && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                    || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                return false;
            }
        }
        return true;
    }

    private boolean isConflict(long srcBe, long destBe, CloudReplica cloudReplica, BalanceType balanceType,
                           Map<Long, Map<Long, Map<Long, Set<Tablet>>>> beToTabletsInParts,
                           Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTables) {
        if (cloudSystemInfoService.getBackend(srcBe).isDecommissioning()) {
            return false; // If source BE is decommissioned, no conflict
        }

        if (balanceType == BalanceType.GLOBAL) {
            return checkGlobalBalanceConflict(srcBe, destBe, cloudReplica, beToTabletsInParts, beToTabletsInTables);
        } else if (balanceType == BalanceType.TABLE) {
            return checkTableBalanceConflict(srcBe, destBe, cloudReplica, beToTabletsInParts);
        }

        return false;
    }

    private boolean checkGlobalBalanceConflict(long srcBe, long destBe, CloudReplica cloudReplica,
                                               Map<Long, Map<Long, Map<Long, Set<Tablet>>>> beToTabletsInParts,
                                               Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTables) {
        long maxBeSize = getTabletSizeInParts(srcBe, cloudReplica, beToTabletsInParts);
        long minBeSize = getTabletSizeInParts(destBe, cloudReplica, beToTabletsInParts);

        if (minBeSize >= maxBeSize) {
            return true; // Conflict detected
        }

        maxBeSize = getTabletSizeInBes(srcBe, cloudReplica, beToTabletsInTables);
        minBeSize = getTabletSizeInBes(destBe, cloudReplica, beToTabletsInTables);

        return minBeSize >= maxBeSize; // Conflict detected
    }

    private boolean checkTableBalanceConflict(long srcBe, long destBe, CloudReplica cloudReplica,
                                              Map<Long, Map<Long, Map<Long, Set<Tablet>>>> beToTabletsInParts) {
        long maxBeSize = getTabletSizeInParts(srcBe, cloudReplica, beToTabletsInParts);
        long minBeSize = getTabletSizeInParts(destBe, cloudReplica, beToTabletsInParts);

        return minBeSize >= maxBeSize; // Conflict detected
    }

    private long getTabletSizeInParts(long beId, CloudReplica cloudReplica,
                                         Map<Long, Map<Long, Map<Long, Set<Tablet>>>> beToTabletsInParts) {
        Set<Tablet> tablets = beToTabletsInParts.get(cloudReplica.getPartitionId())
                .get(cloudReplica.getIndexId()).get(beId);
        return tablets == null ? 0 : tablets.size();
    }

    private long getTabletSizeInBes(long beId, CloudReplica cloudReplica,
                                    Map<Long, Map<Long, Set<Tablet>>> beToTabletsInTables) {
        Set<Tablet> tablets = beToTabletsInTables.get(cloudReplica.getTableId()).get(beId);
        return tablets == null ? 0 : tablets.size();
    }


    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, Set<Tablet>> beToTablets,
            BalanceType balanceType, List<UpdateCloudReplicaInfo> infos) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        long totalTabletsNum = calculateTotalTablets(bes, beToTablets);
        long beNum = countActiveBackends(bes);

        if (beNum == 0) {
            LOG.warn("zero be, but want balance, skip");
            return;
        }

        long avgNum = totalTabletsNum / beNum;
        long transferNum = calculateTransferNum(avgNum);

        for (int i = 0; i < transferNum; i++) {
            TransferPairInfo pairInfo = new TransferPairInfo();
            if (!getTransferPair(bes, beToTablets, avgNum, pairInfo)) {
                break; // no need balance
            }

            updateBalanceStatus(balanceType);

            long srcBe = pairInfo.srcBe;
            long destBe = pairInfo.destBe;

            Tablet pickedTablet = pickRandomTablet(beToTablets.get(srcBe));
            if (pickedTablet == null) {
                continue; // No tablet to pick
            }

            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);
            Backend srcBackend = Env.getCurrentSystemInfo().getBackend(srcBe);

            if (Config.enable_cloud_warm_up_for_rebalance && srcBackend != null && srcBackend.isAlive()) {
                if (isConflict(srcBe, destBe, cloudReplica, balanceType,
                        futurePartitionToTablets, futureBeToTabletsInTable)) {
                    continue;
                }
                preheatAndUpdateTablet(pickedTablet, srcBe, destBe, clusterId, balanceType, beToTablets);
            } else {
                if (isConflict(srcBe, destBe, cloudReplica, balanceType, partitionToTablets, beToTabletsInTable)) {
                    continue;
                }
                transferTablet(pickedTablet, srcBe, destBe, clusterId, balanceType, infos);
            }
        }
    }

    private long calculateTotalTablets(List<Long> bes, Map<Long, Set<Tablet>> beToTablets) {
        return bes.stream()
                .mapToLong(be -> beToTablets.getOrDefault(be, Collections.emptySet()).size())
                .sum();
    }

    private long countActiveBackends(List<Long> bes) {
        return bes.stream()
                .filter(be -> {
                    Backend backend = cloudSystemInfoService.getBackend(be);
                    return backend != null && !backend.isDecommissioning();
                })
                .count();
    }

    private long calculateTransferNum(long avgNum) {
        return Math.max(Math.round(avgNum * Config.cloud_balance_tablet_percent_per_run),
                        Config.cloud_min_balance_tablet_num_per_run);
    }

    private void updateBalanceStatus(BalanceType balanceType) {
        if (balanceType == BalanceType.PARTITION) {
            indexBalanced = false;
        } else if (balanceType == BalanceType.TABLE) {
            tableBalanced = false;
        }
    }

    private Tablet pickRandomTablet(Set<Tablet> tablets) {
        if (tablets.isEmpty()) {
            return null;
        }
        int randomIndex = rand.nextInt(tablets.size());
        return tablets.stream().skip(randomIndex).findFirst().orElse(null);
    }

    private void preheatAndUpdateTablet(Tablet pickedTablet, long srcBe, long destBe, String clusterId,
                                     BalanceType balanceType, Map<Long, Set<Tablet>> beToTablets) {
        try {
            sendPreHeatingRpc(pickedTablet, srcBe, destBe);
        } catch (Exception e) {
            LOG.warn("Failed to preheat tablet {} from {} to {}, "
                    + "help msg turn off fe config enable_cloud_warm_up_for_rebalance",
                    pickedTablet.getId(), srcBe, destBe, e);
            return;
        }

        InfightTask task = new InfightTask();
        task.pickedTablet = pickedTablet;
        task.srcBe = srcBe;
        task.destBe = destBe;
        task.balanceType = balanceType;
        task.beToTablets = beToTablets;
        task.startTimestamp = System.currentTimeMillis() / 1000;
        tabletToInfightTask.put(new InfightTablet(pickedTablet.getId(), clusterId), task);

        LOG.info("pre cache {} from {} to {}, cluster {}", pickedTablet.getId(), srcBe, destBe, clusterId);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
    }

    private void transferTablet(Tablet pickedTablet, long srcBe, long destBe, String clusterId,
                            BalanceType balanceType, List<UpdateCloudReplicaInfo> infos) {
        LOG.info("transfer {} from {} to {}, cluster {}", pickedTablet.getId(), srcBe, destBe, clusterId);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                beToTabletsGlobal, beToTabletsInTable, partitionToTablets);
        updateBeToTablets(pickedTablet, srcBe, destBe,
                futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
        updateClusterToBeMap(pickedTablet, destBe, clusterId, infos);
    }

    public void addTabletMigrationTask(Long srcBe, Long dstBe) {
        tabletsMigrateTasks.offer(Pair.of(srcBe, dstBe));
    }

    /* Migrate tablet replicas from srcBe to dstBe
     * replica location info will be updated in both master and follower FEs.
     */
    private void migrateTablets(Long srcBe, Long dstBe) {
        // get tablets
        Set<Tablet> tablets = beToTabletsGlobal.get(srcBe);
        if (tablets == null || tablets.isEmpty()) {
            LOG.info("smooth upgrade srcBe={} does not have any tablets, set inactive", srcBe);
            ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().setBeStateInactive(srcBe);
            return;
        }
        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Tablet tablet : tablets) {
            // get replica
            CloudReplica cloudReplica = (CloudReplica) tablet.getReplicas().get(0);
            Backend be = cloudSystemInfoService.getBackend(srcBe);
            if (be == null) {
                LOG.info("src backend {} not found", srcBe);
                continue;
            }
            // populate to followers
            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                long beId;
                try {
                    beId = cloudReplica.getBackendId();
                } catch (ComputeGroupException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get backend failed cloudReplica {}", cloudReplica, e);
                    }
                    beId = -1;
                }
                LOG.error("get null db from replica, tabletId={}, partitionId={}, beId={}",
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), beId);
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            String clusterId = be.getCloudClusterId();
            String clusterName = be.getCloudClusterName();

            table.readLock();
            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }
                // update replica location info
                cloudReplica.updateClusterToPrimaryBe(clusterId, dstBe);
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        tablet.getId(), cloudReplica.getId(), clusterId, dstBe);
                infos.add(info);
            } finally {
                table.readUnlock();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("cloud be migrate tablet {} from srcBe={} to dstBe={}, clusterId={}, clusterName={}",
                        tablet.getId(), srcBe, dstBe, clusterId, clusterName);
            }
        }
        long oldSize = infos.size();
        infos = batchUpdateCloudReplicaInfoEditlogs(infos);
        LOG.info("collect to editlog migrate before size={} after size={} infos", oldSize, infos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(infos);
        } catch (Exception e) {
            LOG.warn("update cloud replicas failed", e);
            // edit log failed, try next time
            throw new RuntimeException(e);
        }

        try {
            ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().registerWaterShedTxnId(srcBe);
        } catch (UserException e) {
            LOG.warn("registerWaterShedTxnId get exception", e);
            throw new RuntimeException(e);
        }
    }

    private List<UpdateCloudReplicaInfo> batchUpdateCloudReplicaInfoEditlogs(List<UpdateCloudReplicaInfo> infos) {
        long start = System.currentTimeMillis();
        List<UpdateCloudReplicaInfo> rets = new ArrayList<>();
        // clusterId, infos
        Map<String, List<UpdateCloudReplicaInfo>> clusterIdToInfos = infos.stream()
                .collect(Collectors.groupingBy(UpdateCloudReplicaInfo::getClusterId));
        for (Map.Entry<String, List<UpdateCloudReplicaInfo>> entry : clusterIdToInfos.entrySet()) {
            // same cluster
            String clusterId = entry.getKey();
            List<UpdateCloudReplicaInfo> infoList = entry.getValue();
            Map<Long, List<UpdateCloudReplicaInfo>> sameLocationInfos = infoList.stream()
                    .collect(Collectors.groupingBy(
                            info -> info.getDbId()
                            + info.getTableId() + info.getPartitionId() + info.getIndexId()));
            sameLocationInfos.forEach((location, locationInfos) -> {
                UpdateCloudReplicaInfo newInfo = new UpdateCloudReplicaInfo();
                long dbId = -1;
                long tableId = -1;
                long partitionId = -1;
                long indexId = -1;
                for (UpdateCloudReplicaInfo info : locationInfos) {
                    Preconditions.checkState(clusterId.equals(info.getClusterId()),
                            "impossible, cluster id not eq outer=" + clusterId + ", inner=" + info.getClusterId());

                    dbId = info.getDbId();
                    tableId = info.getTableId();
                    partitionId = info.getPartitionId();
                    indexId = info.getIndexId();

                    StringBuilder sb = new StringBuilder("impossible, some locations do not match location");
                    sb.append(", location=").append(location).append(", dbId=").append(dbId)
                        .append(", tableId=").append(tableId).append(", partitionId=").append(partitionId)
                        .append(", indexId=").append(indexId);
                    Preconditions.checkState(location == dbId + tableId + partitionId + indexId, sb.toString());

                    long tabletId = info.getTabletId();
                    long replicaId = info.getReplicaId();
                    long beId = info.getBeId();
                    newInfo.getTabletIds().add(tabletId);
                    newInfo.getReplicaIds().add(replicaId);
                    newInfo.getBeIds().add(beId);
                }
                newInfo.setDbId(dbId);
                newInfo.setTableId(tableId);
                newInfo.setPartitionId(partitionId);
                newInfo.setIndexId(indexId);
                newInfo.setClusterId(clusterId);
                // ATTN: in unprotectUpdateCloudReplica, use batch must set tabletId = -1
                newInfo.setTabletId(-1);
                rets.add(newInfo);
            });
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("batchUpdateCloudReplicaInfoEditlogs old size {}, cur size {} cost {} ms",
                    infos.size(), rets.size(), System.currentTimeMillis() - start);
        }
        return rets;
    }
}
