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
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class CloudTabletRebalancer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletRebalancer.class);

    private volatile ConcurrentHashMap<Long, List<Tablet>> beToTabletsGlobal =
            new ConcurrentHashMap<Long, List<Tablet>>();

    private Map<Long, List<Tablet>> futureBeToTabletsGlobal;

    private Map<String, List<Long>> clusterToBes;

    private Set<Long> allBes;

    private List<UpdateCloudReplicaInfo> replicaInfos;

    // partitionId -> indexId -> be -> tablet
    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> partitionToTablets;

    private Map<Long, Map<Long, Map<Long, List<Tablet>>>> futurePartitionToTablets;

    // tableId -> be -> tablet
    private Map<Long, Map<Long, List<Tablet>>> beToTabletsInTable;

    private Map<Long, Map<Long, List<Tablet>>> futureBeToTabletsInTable;

    private Map<Long, Long> beToDecommissionedTime = new HashMap<Long, Long>();

    private Random rand = new Random();

    private boolean indexBalanced = true;

    private boolean tableBalanced = true;

    private LinkedBlockingQueue<Pair<Long, Long>> tabletsMigrateTasks = new LinkedBlockingQueue<Pair<Long, Long>>();

    private Map<Long, InfightTask> tabletToInfightTask = new HashMap<Long, InfightTask>();

    private long assignedErrNum = 0;

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

    private class InfightTask {
        public Tablet pickedTablet;
        public long srcBe;
        public long destBe;
        public boolean isGlobal;
        public String clusterId;
        public Map<Long, List<Tablet>> beToTablets;
        public long startTimestamp;
        BalanceType balanceType;
    }

    private class TransferPairInfo {
        public long srcBe;
        public long destBe;
        public long minTabletsNum;
        public long maxTabletsNum;
        public boolean srcDecommissioned;
    }

    public Set<Long> getSnapshotTabletsByBeId(Long beId) {
        Set<Long> snapshotTablets = new HashSet<Long>();
        if (beToTabletsGlobal == null || !beToTabletsGlobal.containsKey(beId)) {
            LOG.warn("beToTabletsGlobal null or not contain beId {}", beId);
            return snapshotTablets;
        }

        beToTabletsGlobal.get(beId).forEach(tablet -> snapshotTablets.add(tablet.getId()));
        return snapshotTablets;
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

        clusterToBes = new HashMap<String, List<Long>>();
        allBes = new HashSet<Long>();
        long start = System.currentTimeMillis();

        // 1 build cluster to backend info
        for (Long beId : cloudSystemInfoService.getAllBackendIds()) {
            Backend be = cloudSystemInfoService.getBackend(beId);
            if (be == null) {
                LOG.info("backend {} not found", beId);
                continue;
            }
            clusterToBes.putIfAbsent(be.getCloudClusterId(), new ArrayList<Long>());
            clusterToBes.get(be.getCloudClusterId()).add(beId);
            allBes.add(beId);
        }
        LOG.info("cluster to backends {}", clusterToBes);

        // 2 complete route info
        replicaInfos = new ArrayList<UpdateCloudReplicaInfo>();
        completeRouteInfo();
        LOG.info("collect to editlog route {} infos", replicaInfos.size());
        try {
            Env.getCurrentEnv().getEditLog().logUpdateCloudReplicas(replicaInfos);
        } catch (Exception e) {
            LOG.warn("failed to update cloud replicas", e);
            // edit log failed, try next time
            return;
        }

        // 3 check whether the inflight preheating task has been completed
        checkInflghtWarmUpCacheAsync();

        // 4 migrate tablet for smooth upgrade
        Pair<Long, Long> pair;
        statRouteInfo();
        while (!tabletsMigrateTasks.isEmpty()) {
            try {
                pair = tabletsMigrateTasks.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOG.info("begin tablets migration from be {} to be {}", pair.first, pair.second);
            migrateTablets(pair.first, pair.second);
        }

        // 5 statistics be to tablets mapping information
        statRouteInfo();

        indexBalanced = true;
        tableBalanced = true;

        // 6 partition-level balance
        if (Config.enable_cloud_partition_balance) {
            balanceAllPartitions();
        }

        // 7 if tablets in partition-level already balanced, perform table balance
        if (Config.enable_cloud_table_balance && indexBalanced) {
            balanceAllTables();
        }

        // 8 if tablets in partition-level and table-level already balanced, perform global balance
        if (Config.enable_cloud_global_balance && indexBalanced && tableBalanced) {
            globalBalance();
        }

        // 9 check whether all tablets of decomission have been migrated
        checkDecommissionState(clusterToBes);

        LOG.info("finished to rebalancer. cost: {} ms", (System.currentTimeMillis() - start));
    }

    public void balanceAllPartitions() {
        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
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

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after partition balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void balanceAllTables() {
        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before table balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
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

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after table balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after table balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void globalBalance() {
        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("before global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
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

        for (Map.Entry<Long, List<Tablet>> entry : beToTabletsGlobal.entrySet()) {
            LOG.info("after global balance be {} tablet num {}", entry.getKey(), entry.getValue().size());
        }

        for (Map.Entry<Long, List<Tablet>> entry : futureBeToTabletsGlobal.entrySet()) {
            LOG.info("after global balance be {} tablet num(current + pre heating inflight) {}",
                    entry.getKey(), entry.getValue().size());
        }
    }

    public void checkInflghtWarmUpCacheAsync() {
        Map<Long, List<Long>> beToTabletIds = new HashMap<Long, List<Long>>();

        for (Map.Entry<Long, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToTabletIds.putIfAbsent(entry.getValue().destBe, new ArrayList<Long>());
            beToTabletIds.get(entry.getValue().destBe).add(entry.getValue().pickedTablet.getId());
        }

        List<UpdateCloudReplicaInfo> infos = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : beToTabletIds.entrySet()) {
            LOG.info("before pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
            Backend destBackend = cloudSystemInfoService.getBackend(entry.getKey());
            if (destBackend == null) {
                for (long tabletId : entry.getValue()) {
                    tabletToInfightTask.remove(tabletId);
                }
                continue;
            }

            Map<Long, Boolean> taskDone = sendCheckWarmUpCacheAsyncRpc(entry.getValue(), entry.getKey());
            if (taskDone == null) {
                LOG.warn("sendCheckWarmUpCacheAsyncRpc return null be {}, inFight tasks {}",
                        entry.getKey(), entry.getValue());
                continue;
            }

            for (Map.Entry<Long, Boolean> result : taskDone.entrySet()) {
                InfightTask task = tabletToInfightTask.get(result.getKey());
                if (result.getValue()
                        || System.currentTimeMillis() / 1000 - task.startTimestamp
                            > Config.cloud_pre_heating_time_limit_sec) {
                    if (!result.getValue()) {
                        LOG.info("{} pre cache timeout, forced to change the mapping", result.getKey());
                    }
                    updateClusterToBeMap(task.pickedTablet, task.destBe, task.clusterId, infos);
                    tabletToInfightTask.remove(result.getKey());
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
        beToTabletIds = new HashMap<Long, List<Long>>();
        for (Map.Entry<Long, InfightTask> entry : tabletToInfightTask.entrySet()) {
            beToTabletIds.putIfAbsent(entry.getValue().destBe, new ArrayList<Long>());
            beToTabletIds.get(entry.getValue().destBe).add(entry.getValue().pickedTablet.getId());
        }

        for (Map.Entry<Long, List<Long>> entry : beToTabletIds.entrySet()) {
            LOG.info("after pre cache check dest be {} inflight task num {}", entry.getKey(), entry.getValue().size());
        }
    }

    public void checkDecommissionState(Map<String, List<Long>> clusterToBes) {
        for (Map.Entry<String, List<Long>> entry : clusterToBes.entrySet()) {
            List<Long> beList = entry.getValue();
            long tabletNum = 0L;
            for (long beId : beList) {
                tabletNum = beToTabletsGlobal.get(beId) == null ? 0 : beToTabletsGlobal.get(beId).size();
                Backend backend = cloudSystemInfoService.getBackend(beId);
                if (backend == null) {
                    LOG.info("backend {} not found", beId);
                    continue;
                }
                if ((backend.isDecommissioned() && tabletNum == 0 && !backend.isActive())
                        || (backend.isDecommissioned() && beList.size() == 1)) {
                    LOG.info("check decommission be {} state {} tabletNum {} isActive {} beList {}",
                            backend.getId(), backend.isDecommissioned(), tabletNum, backend.isActive(), beList);
                    if (!beToDecommissionedTime.containsKey(beId)) {
                        LOG.info("prepare to notify meta service be {} decommissioned", backend.getId());
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
                            }
                            LOG.info("notify decommission response: {} ", response);
                        } catch (RpcException e) {
                            LOG.info("failed to notify decommission {}", e);
                            return;
                        }
                        beToDecommissionedTime.put(beId, System.currentTimeMillis() / 1000);
                    }
                }
            }
        }
    }

    private void completeRouteInfo() {
        assignedErrNum = 0L;
        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            boolean assigned = false;
            List<Long> beIds = new ArrayList<Long>();
            List<Long> tabletIds = new ArrayList<Long>();
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    if (!clusterToBackends.containsKey(cluster)) {
                        long beId = ((CloudReplica) replica).hashReplicaToBe(cluster, true);
                        if (beId <= 0) {
                            assignedErrNum++;
                            continue;
                        }
                        List<Long> bes = new ArrayList<Long>();
                        bes.add(beId);
                        clusterToBackends.put(cluster, bes);

                        assigned = true;
                        beIds.add(beId);
                        tabletIds.add(tablet.getId());
                    } else {
                        beIds.add(clusterToBackends.get(cluster).get(0));
                        tabletIds.add(tablet.getId());
                    }
                }
            }

            if (assigned) {
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(db.getId(), table.getId(),
                        partition.getId(), index.getId(), cluster, beIds, tabletIds);
                replicaInfos.add(info);
            }
        });

        if (assignedErrNum > 0) {
            LOG.warn("completeRouteInfo error num {}", assignedErrNum);
        }
    }

    public void fillBeToTablets(long be, long tableId, long partId, long indexId, Tablet tablet,
            Map<Long, List<Tablet>> globalBeToTablets,
            Map<Long, Map<Long, List<Tablet>>> beToTabletsInTable,
            Map<Long, Map<Long, Map<Long, List<Tablet>>>> partToTablets) {
        // global
        globalBeToTablets.putIfAbsent(be, new ArrayList<Tablet>());
        globalBeToTablets.get(be).add(tablet);

        // table
        beToTabletsInTable.putIfAbsent(tableId, new HashMap<Long, List<Tablet>>());
        Map<Long, List<Tablet>> beToTabletsOfTable = beToTabletsInTable.get(tableId);
        beToTabletsOfTable.putIfAbsent(be, new ArrayList<Tablet>());
        beToTabletsOfTable.get(be).add(tablet);

        // partition
        partToTablets.putIfAbsent(partId, new HashMap<Long, Map<Long, List<Tablet>>>());
        Map<Long, Map<Long, List<Tablet>>> indexToTablets = partToTablets.get(partId);
        indexToTablets.putIfAbsent(indexId, new HashMap<Long, List<Tablet>>());
        Map<Long, List<Tablet>> beToTabletsOfIndex = indexToTablets.get(indexId);
        beToTabletsOfIndex.putIfAbsent(be, new ArrayList<Tablet>());
        beToTabletsOfIndex.get(be).add(tablet);
    }

    public void statRouteInfo() {
        ConcurrentHashMap<Long, List<Tablet>> tmpBeToTabletsGlobal = new ConcurrentHashMap<Long, List<Tablet>>();
        futureBeToTabletsGlobal = new HashMap<Long, List<Tablet>>();

        partitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();
        futurePartitionToTablets = new HashMap<Long, Map<Long, Map<Long, List<Tablet>>>>();

        beToTabletsInTable = new HashMap<Long, Map<Long, List<Tablet>>>();
        futureBeToTabletsInTable = new HashMap<Long, Map<Long, List<Tablet>>>();

        loopCloudReplica((Database db, Table table, Partition partition, MaterializedIndex index, String cluster) -> {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    Map<String, List<Long>> clusterToBackends =
                            ((CloudReplica) replica).getClusterToBackends();
                    for (Map.Entry<String, List<Long>> entry : clusterToBackends.entrySet()) {
                        if (!cluster.equals(entry.getKey())) {
                            continue;
                        }

                        List<Long> bes = entry.getValue();
                        if (!allBes.contains(bes.get(0))) {
                            continue;
                        }

                        fillBeToTablets(bes.get(0), table.getId(), partition.getId(), index.getId(), tablet,
                                tmpBeToTabletsGlobal, beToTabletsInTable, this.partitionToTablets);

                        if (tabletToInfightTask.containsKey(tablet.getId())) {
                            InfightTask task = tabletToInfightTask.get(tablet.getId());
                            fillBeToTablets(task.destBe, table.getId(), partition.getId(), index.getId(), tablet,
                                    futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
                        } else {
                            fillBeToTablets(bes.get(0), table.getId(), partition.getId(), index.getId(), tablet,
                                    futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
                        }
                    }
                }
            }
        });

        beToTabletsGlobal = tmpBeToTabletsGlobal;
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
                if (!table.writeLockIfExist()) {
                    continue;
                }

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
                    table.writeUnlock();
                }
            }
        }
    }

    public void balanceInPartition(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos) {
        // balance all partition
        for (Map.Entry<Long, Map<Long, Map<Long, List<Tablet>>>> partitionEntry : futurePartitionToTablets.entrySet()) {
            Map<Long, Map<Long, List<Tablet>>> indexToTablets = partitionEntry.getValue();
            // balance all index of a partition
            for (Map.Entry<Long, Map<Long, List<Tablet>>> entry : indexToTablets.entrySet()) {
                // balance a index
                balanceImpl(bes, clusterId, entry.getValue(), BalanceType.PARTITION, infos);
            }
        }
    }

    public void balanceInTable(List<Long> bes, String clusterId, List<UpdateCloudReplicaInfo> infos) {
        // balance all tables
        for (Map.Entry<Long, Map<Long, List<Tablet>>> entry : futureBeToTabletsInTable.entrySet()) {
            balanceImpl(bes, clusterId, entry.getValue(), BalanceType.TABLE, infos);
        }
    }

    private void sendPreHeatingRpc(Tablet pickedTablet, long srcBe, long destBe) throws Exception {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend srcBackend = cloudSystemInfoService.getBackend(srcBe);
        Backend destBackend = cloudSystemInfoService.getBackend(destBe);
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
            ClientPool.backendPool.invalidateObject(address, client);
            throw e;
        } finally {
            ClientPool.backendPool.returnObject(address, client);
        }
    }

    private Map<Long, Boolean> sendCheckWarmUpCacheAsyncRpc(List<Long> tabletIds, long be) {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        Backend destBackend = cloudSystemInfoService.getBackend(be);
        try {
            address = new TNetworkAddress(destBackend.getHost(), destBackend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            TCheckWarmUpCacheAsyncRequest req = new TCheckWarmUpCacheAsyncRequest();
            req.setTablets(tabletIds);
            TCheckWarmUpCacheAsyncResponse result = client.checkWarmUpCacheAsync(req);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("check pre cache status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            } else {
                LOG.info("check pre cache succ status {} {}", result.getStatus().getStatusCode(),
                        result.getStatus().getErrorMsgs());
            }
            return result.getTaskDone();
        } catch (Exception e) {
            LOG.warn("send check pre cache rpc error. backend[{}]", destBackend.getId(), e);
            ClientPool.backendPool.invalidateObject(address, client);
        } finally {
            ClientPool.backendPool.returnObject(address, client);
        }
        return null;
    }

    private void updateBeToTablets(Tablet pickedTablet, long srcBe, long destBe, BalanceType balanceType,
            Map<Long, List<Tablet>> globalBeToTablets,
            Map<Long, Map<Long, List<Tablet>>> beToTabletsInTable,
            Map<Long, Map<Long, Map<Long, List<Tablet>>>> partToTablets) {
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
        cloudReplica.updateClusterToBe(clusterId, destBe);
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

            UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                    cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                    pickedTablet.getId(), cloudReplica.getId(), clusterId, destBe);
            infos.add(info);
        } finally {
            table.readUnlock();
        }
    }

    private boolean getTransferPair(List<Long> bes, Map<Long, List<Tablet>> beToTablets, long avgNum,
                                    TransferPairInfo pairInfo) {
        long destBe = bes.get(0);
        long srcBe = bes.get(0);

        long minTabletsNum = Long.MAX_VALUE;
        long maxTabletsNum = 0;
        boolean srcDecommissioned = false;

        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            if (tabletNum > maxTabletsNum) {
                srcBe = be;
                maxTabletsNum = tabletNum;
            }

            Backend backend = cloudSystemInfoService.getBackend(be);
            if (backend == null) {
                LOG.info("backend {} not found", be);
                continue;
            }
            if (tabletNum < minTabletsNum && backend.isAlive() && !backend.isDecommissioned()
                    && !backend.isSmoothUpgradeSrc()) {
                destBe = be;
                minTabletsNum = tabletNum;
            }
        }

        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            Backend backend = cloudSystemInfoService.getBackend(be);
            if (backend == null) {
                LOG.info("backend {} not found", be);
                continue;
            }
            if (backend.isDecommissioned() && tabletNum > 0) {
                srcBe = be;
                srcDecommissioned = true;
                break;
            }
        }

        if (!srcDecommissioned) {
            if ((maxTabletsNum < avgNum * (1 + Config.cloud_rebalance_percent_threshold)
                    && minTabletsNum > avgNum * (1 - Config.cloud_rebalance_percent_threshold))
                    || minTabletsNum > maxTabletsNum - Config.cloud_rebalance_number_threshold) {
                return false;
            }
        }

        pairInfo.srcBe = srcBe;
        pairInfo.destBe = destBe;
        pairInfo.minTabletsNum = minTabletsNum;
        pairInfo.maxTabletsNum = maxTabletsNum;
        return true;
    }

    private boolean isConflict(long srcBe, long destBe, CloudReplica cloudReplica, BalanceType balanceType,
            Map<Long, Map<Long, Map<Long, List<Tablet>>>> beToTabletsInParts,
            Map<Long, Map<Long, List<Tablet>>> beToTabletsInTables) {
        if (balanceType == balanceType.GLOBAL) {
            // check is conflict with partition balance
            long maxBeSize = beToTabletsInParts.get(cloudReplica.getPartitionId())
                    .get(cloudReplica.getIndexId()).get(srcBe).size();
            List<Tablet> destBeTablets = beToTabletsInParts.get(cloudReplica.getPartitionId())
                    .get(cloudReplica.getIndexId()).get(destBe);
            long minBeSize = destBeTablets == null ? 0 : destBeTablets.size();
            if (minBeSize >= maxBeSize) {
                return true;
            }

            // check is conflict with table balance
            maxBeSize = beToTabletsInTables.get(cloudReplica.getTableId()).get(srcBe).size();
            destBeTablets = beToTabletsInTables.get(cloudReplica.getTableId()).get(destBe);
            minBeSize = destBeTablets == null ? 0 : destBeTablets.size();
            if (minBeSize >= maxBeSize) {
                return true;
            }
        }

        if (balanceType == balanceType.TABLE) {
            // check is conflict with partition balance
            long maxBeSize = beToTabletsInParts.get(cloudReplica.getPartitionId())
                    .get(cloudReplica.getIndexId()).get(srcBe).size();
            List<Tablet> destBeTablets = beToTabletsInParts.get(cloudReplica.getPartitionId())
                    .get(cloudReplica.getIndexId()).get(destBe);
            long minBeSize = destBeTablets == null ? 0 : destBeTablets.size();
            if (minBeSize >= maxBeSize) {
                return true;
            }
        }

        return false;
    }

    private void balanceImpl(List<Long> bes, String clusterId, Map<Long, List<Tablet>> beToTablets,
            BalanceType balanceType, List<UpdateCloudReplicaInfo> infos) {
        if (bes == null || bes.isEmpty() || beToTablets == null || beToTablets.isEmpty()) {
            return;
        }

        long totalTabletsNum = 0;
        long beNum = 0;
        for (Long be : bes) {
            long tabletNum = beToTablets.get(be) == null ? 0 : beToTablets.get(be).size();
            Backend backend = cloudSystemInfoService.getBackend(be);
            if (backend != null && !backend.isDecommissioned()) {
                beNum++;
            }
            totalTabletsNum += tabletNum;
        }
        if (beNum == 0) {
            LOG.warn("zero be, but want balance, skip");
            return;
        }
        long avgNum = totalTabletsNum / beNum;
        long transferNum = Math.max(Math.round(avgNum * Config.cloud_balance_tablet_percent_per_run),
                                    Config.cloud_min_balance_tablet_num_per_run);

        for (int i = 0; i < transferNum; i++) {
            TransferPairInfo pairInfo = new TransferPairInfo();
            if (!getTransferPair(bes, beToTablets, avgNum, pairInfo)) {
                // no need balance;
                break;
            }

            if (balanceType == balanceType.PARTITION) {
                indexBalanced = false;
            }

            if (balanceType == balanceType.TABLE) {
                tableBalanced = false;
            }

            long srcBe = pairInfo.srcBe;
            long destBe = pairInfo.destBe;
            long minTabletsNum = pairInfo.minTabletsNum;
            long maxTabletsNum = pairInfo.maxTabletsNum;

            int randomIndex = rand.nextInt(beToTablets.get(srcBe).size());
            Tablet pickedTablet = beToTablets.get(srcBe).get(randomIndex);
            CloudReplica cloudReplica = (CloudReplica) pickedTablet.getReplicas().get(0);

            if (Config.enable_cloud_warm_up_for_rebalance) {
                if (isConflict(srcBe, destBe, cloudReplica, balanceType, futurePartitionToTablets,
                        futureBeToTabletsInTable)) {
                    continue;
                }

                try {
                    sendPreHeatingRpc(pickedTablet, srcBe, destBe);
                } catch (Exception e) {
                    break;
                }

                InfightTask task = new InfightTask();
                task.pickedTablet = pickedTablet;
                task.srcBe = srcBe;
                task.destBe = destBe;
                task.balanceType = balanceType;
                task.clusterId = clusterId;
                task.beToTablets = beToTablets;
                task.startTimestamp = System.currentTimeMillis() / 1000;
                tabletToInfightTask.put(pickedTablet.getId(), task);

                LOG.info("pre cache {} from {} to {}, cluster {} minNum {} maxNum {} beNum {} tabletsNum {}, part {}",
                         pickedTablet.getId(), srcBe, destBe, clusterId,
                         minTabletsNum, maxTabletsNum, beNum, totalTabletsNum, cloudReplica.getPartitionId());
                updateBeToTablets(pickedTablet, srcBe, destBe, balanceType,
                        futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
            } else {
                if (isConflict(srcBe, destBe, cloudReplica, balanceType, partitionToTablets, beToTabletsInTable)) {
                    continue;
                }

                LOG.info("transfer {} from {} to {}, cluster {} minNum {} maxNum {} beNum {} tabletsNum {}, part {}",
                        pickedTablet.getId(), srcBe, destBe, clusterId,
                        minTabletsNum, maxTabletsNum, beNum, totalTabletsNum, cloudReplica.getPartitionId());

                updateBeToTablets(pickedTablet, srcBe, destBe, balanceType, beToTabletsGlobal,
                        beToTabletsInTable, partitionToTablets);
                updateBeToTablets(pickedTablet, srcBe, destBe, balanceType,
                        futureBeToTabletsGlobal, futureBeToTabletsInTable, futurePartitionToTablets);
                updateClusterToBeMap(pickedTablet, destBe, clusterId, infos);
            }
        }
    }

    public void addTabletMigrationTask(Long srcBe, Long dstBe) {
        tabletsMigrateTasks.offer(Pair.of(srcBe, dstBe));
    }

    /* Migrate tablet replicas from srcBe to dstBe
     * replica location info will be updated in both master and follower FEs.
     */
    private void migrateTablets(Long srcBe, Long dstBe) {
        // get tablets
        List<Tablet> tablets = new ArrayList<>();
        if (!beToTabletsGlobal.containsKey(srcBe)) {
            LOG.info("smooth upgrade srcBe={} does not have any tablets, set inactive", srcBe);
            ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().setBeStateInactive(srcBe);
            return;
        }
        tablets = beToTabletsGlobal.get(srcBe);
        if (tablets.isEmpty()) {
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
                LOG.info("backend {} not found", be);
                continue;
            }
            String clusterId = be.getCloudClusterId();
            String clusterName = be.getCloudClusterName();
            // update replica location info
            cloudReplica.updateClusterToBe(clusterId, dstBe);
            LOG.info("cloud be migrate tablet {} from srcBe={} to dstBe={}, clusterId={}, clusterName={}",
                    tablet.getId(), srcBe, dstBe, clusterId, clusterName);

            // populate to followers
            Database db = Env.getCurrentInternalCatalog().getDbNullable(cloudReplica.getDbId());
            if (db == null) {
                LOG.error("get null db from replica, tabletId={}, partitionId={}, beId={}",
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getBackendId());
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(cloudReplica.getTableId());
            if (table == null) {
                continue;
            }

            table.readLock();
            try {
                if (db.getTableNullable(cloudReplica.getTableId()) == null) {
                    continue;
                }
                UpdateCloudReplicaInfo info = new UpdateCloudReplicaInfo(cloudReplica.getDbId(),
                        cloudReplica.getTableId(), cloudReplica.getPartitionId(), cloudReplica.getIndexId(),
                        tablet.getId(), cloudReplica.getId(), clusterId, dstBe);
                infos.add(info);
            } finally {
                table.readUnlock();
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
                // APPR: in unprotectUpdateCloudReplica, use batch must set tabletId = -1
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

