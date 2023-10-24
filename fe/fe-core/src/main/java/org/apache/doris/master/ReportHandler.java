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

package org.apache.doris.master;


import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.cooldown.CooldownConf;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.BackendReplicasInfo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Backend.BackendStatus;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.DropReplicaTask;
import org.apache.doris.task.MasterTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.task.PushCooldownConfTask;
import org.apache.doris.task.PushStoragePolicyTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStoragePolicy;
import org.apache.doris.thrift.TStorageResource;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class ReportHandler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(ReportHandler.class);

    private BlockingQueue<ReportTask> reportQueue = Queues.newLinkedBlockingQueue();

    private enum ReportType {
        UNKNOWN,
        TASK,
        DISK,
        TABLET
    }

    public ReportHandler() {
        GaugeMetric<Long> gauge = new GaugeMetric<Long>(
                "report_queue_size", MetricUnit.NOUNIT, "report queue size") {
            @Override
            public Long getValue() {
                return (long) reportQueue.size();
            }
        };
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    public TMasterResult handleReport(TReportRequest request) throws TException {
        TMasterResult result = new TMasterResult();
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBePort();
        Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatusCode(TStatusCode.INTERNAL_ERROR);
            List<String> errorMsgs = Lists.newArrayList();
            errorMsgs.add("backend[" + NetUtils
                    .getHostPortInAccessibleFormat(host, bePort) + "] does not exist.");
            tStatus.setErrorMsgs(errorMsgs);
            return result;
        }

        long beId = backend.getId();
        Map<TTaskType, Set<Long>> tasks = null;
        Map<String, TDisk> disks = null;
        Map<Long, TTablet> tablets = null;
        long reportVersion = -1;

        ReportType reportType = ReportType.UNKNOWN;

        if (request.isSetTasks()) {
            tasks = request.getTasks();
            reportType = ReportType.TASK;
        }

        if (request.isSetDisks()) {
            disks = request.getDisks();
            reportType = ReportType.DISK;
        }

        if (request.isSetTablets()) {
            tablets = request.getTablets();
            reportVersion = request.getReportVersion();
            reportType = ReportType.TABLET;
        } else if (request.isSetTabletList()) {
            // the 'tablets' member will be deprecated in future.
            tablets = buildTabletMap(request.getTabletList());
            reportVersion = request.getReportVersion();
            reportType = ReportType.TABLET;
        }

        if (request.isSetTabletMaxCompactionScore()) {
            backend.setTabletMaxCompactionScore(request.getTabletMaxCompactionScore());
        }

        ReportTask reportTask = new ReportTask(beId, tasks, disks, tablets, reportVersion,
                request.getStoragePolicy(), request.getResource(), request.getNumCores(),
                request.getPipelineExecutorSize());
        try {
            putToQueue(reportTask);
        } catch (Exception e) {
            tStatus.setStatusCode(TStatusCode.INTERNAL_ERROR);
            List<String> errorMsgs = Lists.newArrayList();
            errorMsgs.add("failed to put report task to queue. queue size: " + reportQueue.size());
            errorMsgs.add("err: " + e.getMessage());
            tStatus.setErrorMsgs(errorMsgs);
            return result;
        }
        LOG.info("receive report from be {}. type: {}, current queue size: {}",
                backend.getId(), reportType, reportQueue.size());
        return result;
    }

    private void putToQueue(ReportTask reportTask) throws Exception {
        int currentSize = reportQueue.size();
        if (currentSize > Config.report_queue_size) {
            LOG.warn("the report queue size exceeds the limit: {}. current: {}", Config.report_queue_size, currentSize);
            throw new Exception(
                    "the report queue size exceeds the limit: "
                            + Config.report_queue_size + ". current: " + currentSize);
        }
        reportQueue.put(reportTask);
    }

    private Map<Long, TTablet> buildTabletMap(List<TTablet> tabletList) {
        Map<Long, TTablet> tabletMap = Maps.newHashMap();
        for (TTablet tTablet : tabletList) {
            if (tTablet.getTabletInfos().isEmpty()) {
                continue;
            }

            tabletMap.put(tTablet.getTabletInfos().get(0).getTabletId(), tTablet);
        }
        return tabletMap;
    }

    private class ReportTask extends MasterTask {

        private long beId;
        private Map<TTaskType, Set<Long>> tasks;
        private Map<String, TDisk> disks;
        private Map<Long, TTablet> tablets;
        private long reportVersion;

        private List<TStoragePolicy> storagePolicies;
        private List<TStorageResource> storageResources;
        private int cpuCores;
        private int pipelineExecutorSize;

        public ReportTask(long beId, Map<TTaskType, Set<Long>> tasks,
                Map<String, TDisk> disks,
                Map<Long, TTablet> tablets, long reportVersion,
                List<TStoragePolicy> storagePolicies, List<TStorageResource> storageResources, int cpuCores,
                int pipelineExecutorSize) {
            this.beId = beId;
            this.tasks = tasks;
            this.disks = disks;
            this.tablets = tablets;
            this.reportVersion = reportVersion;
            this.storagePolicies = storagePolicies;
            this.storageResources = storageResources;
            this.cpuCores = cpuCores;
            this.pipelineExecutorSize = pipelineExecutorSize;
        }

        @Override
        protected void exec() {
            if (tasks != null) {
                ReportHandler.taskReport(beId, tasks);
            }
            if (disks != null) {
                ReportHandler.diskReport(beId, disks);
                ReportHandler.cpuReport(beId, cpuCores, pipelineExecutorSize);
            }
            if (Config.enable_storage_policy && storagePolicies != null && storageResources != null) {
                storagePolicyReport(beId, storagePolicies, storageResources);
            }

            if (tablets != null) {
                long backendReportVersion = Env.getCurrentSystemInfo().getBackendReportVersion(beId);
                if (reportVersion < backendReportVersion) {
                    LOG.warn("out of date report version {} from backend[{}]. current report version[{}]",
                            reportVersion, beId, backendReportVersion);
                } else {
                    ReportHandler.tabletReport(beId, tablets, reportVersion);
                }
            }
        }
    }

    private static void handlePushCooldownConf(long backendId, List<CooldownConf> cooldownConfToPush) {
        final int PUSH_BATCH_SIZE = 1024;
        AgentBatchTask batchTask = new AgentBatchTask();
        for (int start = 0; start < cooldownConfToPush.size(); start += PUSH_BATCH_SIZE) {
            PushCooldownConfTask task = new PushCooldownConfTask(backendId,
                    cooldownConfToPush.subList(start, Math.min(start + PUSH_BATCH_SIZE, cooldownConfToPush.size())));
            batchTask.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handlePushStoragePolicy(long backendId, List<Policy> policyToPush,
                                                List<Resource> resourceToPush, List<Long> policyToDrop) {
        AgentBatchTask batchTask = new AgentBatchTask();
        PushStoragePolicyTask pushStoragePolicyTask = new PushStoragePolicyTask(backendId, policyToPush,
                resourceToPush, policyToDrop);
        batchTask.addTask(pushStoragePolicyTask);
        AgentTaskExecutor.submit(batchTask);
    }

    private static void storagePolicyReport(long backendId,
                                            List<TStoragePolicy> storagePoliciesInBe,
                                            List<TStorageResource> storageResourcesInBe) {
        LOG.info("backend[{}] reports policies {}, report resources: {}",
                backendId, storagePoliciesInBe, storageResourcesInBe);
        // do the diff. find out (intersection) / (be - meta) / (meta - be)
        List<Policy> policiesInFe = Env.getCurrentEnv().getPolicyMgr().getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
        List<Resource> resourcesInFe = Env.getCurrentEnv().getResourceMgr().getResource(ResourceType.S3);
        resourcesInFe.addAll(Env.getCurrentEnv().getResourceMgr().getResource(ResourceType.HDFS));

        List<Resource> resourceToPush = new ArrayList<>();
        List<Policy> policyToPush = new ArrayList<>();
        List<Long> policyToDrop = new ArrayList<>();

        diffPolicy(storagePoliciesInBe, policiesInFe, policyToPush, policyToDrop);
        diffResource(storageResourcesInBe, resourcesInFe, resourceToPush);

        if (policyToPush.isEmpty() && resourceToPush.isEmpty() && policyToDrop.isEmpty()) {
            return;
        }
        LOG.info("after diff policy, policyToPush {}, policyToDrop {}, and resourceToPush {}",
                policyToPush.stream()
                        .map(p -> "StoragePolicy(name=" + p.getPolicyName() + " id=" + p.getId() + " version="
                                + p.getVersion()).collect(Collectors.toList()),
                policyToDrop, resourceToPush.stream()
                        .map(r -> "Resource(name=" + r.getName() + " id=" + r.getId() + " version="
                                + r.getVersion()).collect(Collectors.toList()));
        // send push rpc
        handlePushStoragePolicy(backendId, policyToPush, resourceToPush, policyToDrop);
    }

    private static void diffPolicy(List<TStoragePolicy> storagePoliciesInBe, List<Policy> policiesInFe,
            List<Policy> policyToPush, List<Long> policyToDrop) {
        // fe - be
        for (Policy policy : policiesInFe) {
            if (policy.getId() <= 0 || ((StoragePolicy) policy).getStorageResource() == null) {
                continue; // ignore policy with invalid id or storage resource
            }
            boolean beHasIt = false;
            for (TStoragePolicy tStoragePolicy : storagePoliciesInBe) {
                if (policy.getId() == tStoragePolicy.getId()) {
                    beHasIt = true;
                    // find id eq
                    if (policy.getVersion() == tStoragePolicy.getVersion()) {
                        // find version eq
                    } else if (policy.getVersion() > tStoragePolicy.getVersion()) {
                        // need to add
                        policyToPush.add(policy);
                    } else {
                        // impossible
                        LOG.warn("fe policy version {} litter than be {}, impossible",
                                policy.getVersion(), tStoragePolicy.getVersion());
                    }
                    break;
                }
            }
            if (!beHasIt) {
                policyToPush.add(policy);
            }
        }

        // be - fe
        for (TStoragePolicy tStoragePolicy : storagePoliciesInBe) {
            boolean feHasIt = false;
            for (Policy policy : policiesInFe) {
                if (policy.getId() == tStoragePolicy.getId()) {
                    feHasIt = true;
                    // find id eq
                    break;
                }
            }
            if (!feHasIt) {
                policyToDrop.add(tStoragePolicy.getId());
            }
        }
    }

    private static void diffResource(List<TStorageResource> storageResourcesInBe, List<Resource> resourcesInFe,
                                   List<Resource> resourceToPush) {
        // fe - be
        for (Resource resource : resourcesInFe) {
            if (resource.getId() <= 0) {
                continue; // ignore resource with invalid id
            }
            boolean beHasIt = false;
            for (TStorageResource tStorageResource : storageResourcesInBe) {
                if (resource.getId() == tStorageResource.getId()) {
                    beHasIt = true;
                    // find id eq
                    if (resource.getVersion() == tStorageResource.getVersion()) {
                        // find version eq
                    } else if (resource.getVersion() > tStorageResource.getVersion()) {
                        // need to add
                        resourceToPush.add(resource);
                    } else {
                        // impossible
                        LOG.warn("fe resource version {} litter than be {}, impossible",
                                resource.getVersion(), tStorageResource.getVersion());
                    }
                    break;
                }
            }
            if (!beHasIt) {
                resourceToPush.add(resource);
            }
        }
    }

    // public for fe ut
    public static void tabletReport(long backendId, Map<Long, TTablet> backendTablets, long backendReportVersion) {
        long start = System.currentTimeMillis();
        LOG.info("backend[{}] reports {} tablet(s). report version: {}",
                backendId, backendTablets.size(), backendReportVersion);

        // storage medium map
        HashMap<Long, TStorageMedium> storageMediumMap = Config.disable_storage_medium_check
                ? Maps.newHashMap() : Env.getCurrentEnv().getPartitionIdToStorageMediumMap();

        // db id -> tablet id
        ListMultimap<Long, Long> tabletSyncMap = LinkedListMultimap.create();
        // db id -> tablet id
        ListMultimap<Long, Long> tabletDeleteFromMeta = LinkedListMultimap.create();
        // tablet ids which both in fe and be
        Set<Long> tabletFoundInMeta = Sets.newConcurrentHashSet();
        // storage medium -> tablet id
        ListMultimap<TStorageMedium, Long> tabletMigrationMap = LinkedListMultimap.create();

        // dbid -> txn id -> [partition info]
        Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish = Maps.newHashMap();
        ListMultimap<Long, Long> transactionsToClear = LinkedListMultimap.create();

        // db id -> tablet id
        ListMultimap<Long, Long> tabletRecoveryMap = LinkedListMultimap.create();

        List<TTabletMetaInfo> tabletToUpdate = Lists.newArrayList();

        List<CooldownConf> cooldownConfToPush = new LinkedList<>();
        List<CooldownConf> cooldownConfToUpdate = new LinkedList<>();

        // 1. do the diff. find out (intersection) / (be - meta) / (meta - be)
        Env.getCurrentInvertedIndex().tabletReport(backendId, backendTablets, storageMediumMap,
                tabletSyncMap,
                tabletDeleteFromMeta,
                tabletFoundInMeta,
                tabletMigrationMap,
                transactionsToPublish,
                transactionsToClear,
                tabletRecoveryMap,
                tabletToUpdate,
                cooldownConfToPush,
                cooldownConfToUpdate);

        // 2. sync
        if (!tabletSyncMap.isEmpty()) {
            sync(backendTablets, tabletSyncMap, backendId, backendReportVersion);
        }

        // 3. delete (meta - be)
        // BE will automatically drop defective tablets. these tablets should also be dropped in catalog
        if (!tabletDeleteFromMeta.isEmpty()) {
            deleteFromMeta(tabletDeleteFromMeta, backendId, backendReportVersion);
        }

        // 4. handle (be - meta)
        if (tabletFoundInMeta.size() != backendTablets.size()) {
            deleteFromBackend(backendTablets, tabletFoundInMeta, backendId);
        }

        // 5. migration (ssd <-> hdd)
        if (!Config.disable_storage_medium_check && !tabletMigrationMap.isEmpty()) {
            handleMigration(tabletMigrationMap, backendId);
        }

        // 6. send clear transactions to be
        if (!transactionsToClear.isEmpty()) {
            handleClearTransactions(transactionsToClear, backendId);
        }

        // 7. send publish version request to be
        if (!transactionsToPublish.isEmpty()) {
            handleRepublishVersionInfo(transactionsToPublish, backendId);
        }

        // 8. send recover request to be
        if (!tabletRecoveryMap.isEmpty()) {
            handleRecoverTablet(tabletRecoveryMap, backendTablets, backendId);
        }

        // 9. send tablet meta to be for updating
        if (!tabletToUpdate.isEmpty()) {
            handleUpdateTabletMeta(backendId, tabletToUpdate);
        }

        // handle cooldown conf
        if (!cooldownConfToPush.isEmpty()) {
            handlePushCooldownConf(backendId, cooldownConfToPush);
        }
        if (!cooldownConfToUpdate.isEmpty()) {
            Env.getCurrentEnv().getCooldownConfHandler().addCooldownConfToUpdate(cooldownConfToUpdate);
        }

        final SystemInfoService currentSystemInfo = Env.getCurrentSystemInfo();
        Backend reportBackend = currentSystemInfo.getBackend(backendId);
        if (reportBackend != null) {
            BackendStatus backendStatus = reportBackend.getBackendStatus();
            backendStatus.lastSuccessReportTabletsTime = TimeUtils.longToTimeString(start);
        }

        long end = System.currentTimeMillis();
        LOG.info("finished to handle tablet report from backend[{}] cost: {} ms", backendId, (end - start));
    }

    private static void taskReport(long backendId, Map<TTaskType, Set<Long>> runningTasks) {
        LOG.debug("begin to handle task report from backend {}", backendId);
        long start = System.currentTimeMillis();

        if (LOG.isDebugEnabled()) {
            for (TTaskType type : runningTasks.keySet()) {
                Set<Long> taskSet = runningTasks.get(type);
                if (!taskSet.isEmpty()) {
                    String signatures = StringUtils.join(taskSet, ", ");
                    LOG.debug("backend task[{}]: {}", type.name(), signatures);
                }
            }
        }

        List<AgentTask> diffTasks = AgentTaskQueue.getDiffTasks(backendId, runningTasks);

        AgentBatchTask batchTask = new AgentBatchTask();
        long taskReportTime = System.currentTimeMillis();
        for (AgentTask task : diffTasks) {
            // these tasks no need to do diff
            // 1. CREATE
            // 2. SYNC DELETE
            // 3. CHECK_CONSISTENCY
            // 4. STORAGE_MDEIUM_MIGRATE
            if (task.getTaskType() == TTaskType.CREATE
                    || task.getTaskType() == TTaskType.CHECK_CONSISTENCY
                    || task.getTaskType() == TTaskType.STORAGE_MEDIUM_MIGRATE) {
                continue;
            }

            // to escape sending duplicate agent task to be
            if (task.shouldResend(taskReportTime)) {
                batchTask.addTask(task);
            }

        }

        LOG.debug("get {} diff task(s) to resend", batchTask.getTaskNum());
        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
        LOG.info("finished to handle task report from backend {}, diff task num: {}. cost: {} ms",
                backendId, batchTask.getTaskNum(), (System.currentTimeMillis() - start));
    }

    private static void diskReport(long backendId, Map<String, TDisk> backendDisks) {
        LOG.info("begin to handle disk report from backend {}", backendId);
        long start = System.currentTimeMillis();
        Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend doesn't exist. id: " + backendId);
            return;
        }
        backend.updateDisks(backendDisks);
        LOG.info("finished to handle disk report from backend {}, cost: {} ms",
                backendId, (System.currentTimeMillis() - start));
    }

    private static void cpuReport(long backendId, int cpuCores, int pipelineExecutorSize) {
        LOG.info("begin to handle cpu report from backend {}", backendId);
        long start = System.currentTimeMillis();
        Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend doesn't exist. id: " + backendId);
            return;
        }
        if (backend.updateCpuInfo(cpuCores, pipelineExecutorSize)) {
            // cpu info is changed
            LOG.info("new cpu info. backendId: {}, cpucores: {}, pipelineExecutorSize: {}", backendId, cpuCores,
                    pipelineExecutorSize);
            // log change
            Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
        }
        LOG.info("finished to handle cpu report from backend {}, cost: {} ms",
                backendId, (System.currentTimeMillis() - start));
    }

    private static void sync(Map<Long, TTablet> backendTablets, ListMultimap<Long, Long> tabletSyncMap,
                             long backendId, long backendReportVersion) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        OUTER:
        for (Long dbId : tabletSyncMap.keySet()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            int syncCounter = 0;
            List<Long> tabletIds = tabletSyncMap.get(dbId);
            LOG.info("before sync tablets in db[{}]. report num: {}. backend[{}]",
                    dbId, tabletIds.size(), backendId);
            List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                TabletMeta tabletMeta = tabletMetaList.get(i);
                if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                    continue;
                }
                long tabletId = tabletIds.get(i);
                long tableId = tabletMeta.getTableId();
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !olapTable.writeLockIfExist()) {
                    continue;
                }

                try {
                    if (backendReportVersion < Env.getCurrentSystemInfo().getBackendReportVersion(backendId)) {
                        break OUTER;
                    }

                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (replica == null) {
                        continue;
                    }

                    // yiguolei: it is very important here, if the replica is under schema change or rollup
                    // should ignore the report.
                    // eg.
                    // original replica import successfully, but the dest schema change replica failed
                    // the fe will sync the replica with the original replica, but ignore the schema change replica.
                    // if the last failed version is changed, then fe will think schema change successfully.
                    // this is an fatal error.
                    if (replica.getState() == ReplicaState.NORMAL) {
                        long metaVersion = replica.getVersion();
                        long backendVersion = -1L;
                        long rowCount = -1L;
                        long dataSize = -1L;
                        long remoteDataSize = -1L;
                        // schema change maybe successfully in fe, but not inform be,
                        // then be will report two schema hash
                        // just select the dest schema hash
                        for (TTabletInfo tabletInfo : backendTablets.get(tabletId).getTabletInfos()) {
                            if (tabletInfo.getSchemaHash() == schemaHash) {
                                backendVersion = tabletInfo.getVersion();
                                rowCount = tabletInfo.getRowCount();
                                dataSize = tabletInfo.getDataSize();
                                remoteDataSize = tabletInfo.getRemoteDataSize();
                                break;
                            }
                        }
                        if (backendVersion == -1L) {
                            continue;
                        }

                        boolean needSync = false;
                        if (metaVersion < backendVersion) {
                            needSync = true;
                        } else if (metaVersion == backendVersion) {
                            if (replica.isBad()) {
                                needSync = true;
                            }
                            if (replica.getVersion() >= partition.getCommittedVersion()
                                    && replica.getLastFailedVersion() > partition.getCommittedVersion()) {
                                LOG.info("sync replica {} of tablet {} in backend {} in db {}. replica last failed"
                                        + " version change to -1 because last failed version > replica's committed"
                                        + " version {}",
                                        replica, tabletId, backendId, dbId, partition.getCommittedVersion());
                                replica.updateLastFailedVersion(-1L);
                                needSync = true;
                            }
                        }

                        if (needSync) {
                            // happens when
                            // 1. PUSH finished in BE but failed or not yet report to FE
                            // 2. repair for VERSION_INCOMPLETE finished in BE, but failed or not yet report to FE
                            replica.updateVersionInfo(backendVersion, dataSize, remoteDataSize, rowCount);

                            if (replica.getLastFailedVersion() < 0) {
                                if (replica.setBad(false)) {
                                    LOG.info("sync replica {} of tablet {} in backend {} in db {}. "
                                            + "replica change from bad to good.",
                                            replica, tabletId, backendId, dbId);
                                }

                                // last failed version < 0 means this replica becomes health after sync,
                                // so we write an edit log to sync this operation
                                ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tableId,
                                        partitionId, indexId, tabletId, backendId, replica.getId(),
                                        replica.getVersion(), schemaHash,
                                        dataSize, remoteDataSize, rowCount,
                                        replica.getLastFailedVersion(),
                                        replica.getLastSuccessVersion());
                                Env.getCurrentEnv().getEditLog().logUpdateReplica(info);
                            }

                            ++syncCounter;
                            LOG.debug("sync replica {} of tablet {} in backend {} in db {}. report version: {}",
                                    replica.getId(), tabletId, backendId, dbId, backendReportVersion);
                        } else {
                            LOG.debug("replica {} of tablet {} in backend {} version is changed"
                                            + " between check and real sync. meta[{}]. backend[{}]",
                                    replica.getId(), tabletId, backendId, metaVersion,
                                    backendVersion);
                        }
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }
            LOG.info("sync {} tablets in db[{}]. backend[{}]", syncCounter, dbId, backendId);
        } // end for dbs
    }

    private static void deleteFromMeta(ListMultimap<Long, Long> tabletDeleteFromMeta, long backendId,
                                       long backendReportVersion) {
        AgentBatchTask createReplicaBatchTask = new AgentBatchTask();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (Long dbId : tabletDeleteFromMeta.keySet()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            int deleteCounter = 0;
            List<Long> tabletIds = tabletDeleteFromMeta.get(dbId);
            List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                TabletMeta tabletMeta = tabletMetaList.get(i);
                if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                    continue;
                }
                long tabletId = tabletIds.get(i);
                long tableId = tabletMeta.getTableId();
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !olapTable.writeLockIfExist()) {
                    continue;
                }
                try {
                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    short replicationNum = olapTable.getPartitionInfo()
                            .getReplicaAllocation(partition.getId()).getTotalReplicaNum();

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }
                    if (index.getState() == IndexState.SHADOW) {
                        // This index is under schema change or rollup, tablet may not be created on BE.
                        // ignore it.
                        continue;
                    }

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (replica == null) {
                        continue;
                    }

                    // check report version again
                    long currentBackendReportVersion = Env.getCurrentSystemInfo()
                            .getBackendReportVersion(backendId);
                    if (backendReportVersion < currentBackendReportVersion) {
                        continue;
                    }

                    BinlogConfig binlogConfig = new BinlogConfig(olapTable.getBinlogConfig());

                    ReplicaState state = replica.getState();
                    if (state == ReplicaState.NORMAL || state == ReplicaState.SCHEMA_CHANGE) {
                        // if state is PENDING / ROLLUP / CLONE
                        // it's normal that the replica is not created in BE but exists in meta.
                        // so we do not delete it.
                        List<Replica> replicas = tablet.getReplicas();
                        if (replicas.size() <= 1) {
                            LOG.error("backend [{}] invalid situation. tablet[{}] has few replica[{}], "
                                            + "replica num setting is [{}]",
                                    backendId, tabletId, replicas.size(), replicationNum);
                            // there is a replica in FE, but not in BE and there is only one replica in this tablet
                            // in this case, it means data is lost.
                            // should generate a create replica request to BE to create a replica forcibly.
                            if (replicas.size() == 1) {
                                if (Config.recover_with_empty_tablet) {
                                    // only create this task if force recovery is true
                                    LOG.warn("tablet {} has only one replica {} on backend {}"
                                                    + " and it is lost. create an empty replica to recover it",
                                            tabletId, replica.getId(), backendId);
                                    MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                                    Set<String> bfColumns = olapTable.getCopiedBfColumns();
                                    double bfFpp = olapTable.getBfFpp();
                                    CreateReplicaTask createReplicaTask = new CreateReplicaTask(backendId, dbId,
                                            tableId, partitionId, indexId, tabletId, replica.getId(),
                                            indexMeta.getShortKeyColumnCount(),
                                            indexMeta.getSchemaHash(), partition.getVisibleVersion(),
                                            indexMeta.getKeysType(),
                                            TStorageType.COLUMN,
                                            TStorageMedium.HDD, indexMeta.getSchema(), bfColumns, bfFpp, null,
                                            olapTable.getCopiedIndexes(),
                                            olapTable.isInMemory(),
                                            olapTable.getPartitionInfo().getTabletType(partitionId),
                                            null,
                                            olapTable.getCompressionType(),
                                            olapTable.getEnableUniqueKeyMergeOnWrite(), olapTable.getStoragePolicy(),
                                            olapTable.disableAutoCompaction(),
                                            olapTable.enableSingleReplicaCompaction(),
                                            olapTable.skipWriteIndexOnLoad(), olapTable.getCompactionPolicy(),
                                            olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                                            olapTable.getTimeSeriesCompactionFileCountThreshold(),
                                            olapTable.getTimeSeriesCompactionTimeThresholdSeconds(),
                                            olapTable.storeRowColumn(),
                                            binlogConfig);

                                    createReplicaTask.setIsRecoverTask(true);
                                    createReplicaBatchTask.addTask(createReplicaTask);
                                } else {
                                    // just set this replica as bad
                                    if (replica.setBad(true)) {
                                        LOG.warn("tablet {} has only one replica {} on backend {}"
                                                        + " and it is lost, set it as bad",
                                                tabletId, replica.getId(), backendId);
                                        BackendTabletsInfo tabletsInfo = new BackendTabletsInfo(backendId);
                                        tabletsInfo.setBad(true);
                                        ReplicaPersistInfo replicaPersistInfo = ReplicaPersistInfo.createForReport(dbId,
                                                tableId, partitionId, indexId, tabletId, backendId, replica.getId());
                                        tabletsInfo.addReplicaInfo(replicaPersistInfo);
                                        Env.getCurrentEnv().getEditLog().logBackendTabletsInfo(tabletsInfo);
                                    }
                                }
                            }
                            continue;
                        }

                        tablet.deleteReplicaByBackendId(backendId);
                        ++deleteCounter;

                        // remove replica related tasks
                        AgentTaskQueue.removeReplicaRelatedTasks(backendId, tabletId);

                        // write edit log
                        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(dbId, tableId, partitionId,
                                indexId, tabletId, backendId);

                        Env.getCurrentEnv().getEditLog().logDeleteReplica(info);
                        LOG.warn("delete replica[{}] in tablet[{}] from meta. backend[{}], report version: {}"
                                        + ", current report version: {}",
                                replica.getId(), tabletId, backendId, backendReportVersion,
                                currentBackendReportVersion);

                        // check for clone
                        replicas = tablet.getReplicas();
                        if (replicas.size() == 0) {
                            LOG.error("invalid situation. tablet[{}] is empty", tabletId);
                        }
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }
            LOG.info("delete {} replica(s) from catalog in db[{}]", deleteCounter, dbId);
        } // end for dbs

        if (Config.recover_with_empty_tablet && createReplicaBatchTask.getTaskNum() > 0) {
            // must add to queue, so that when task finish report, the task can be found in queue.
            // the task will be eventually removed from queue by task report, so no need to worry
            // about the residuals.
            AgentTaskQueue.addBatchTask(createReplicaBatchTask);
            AgentTaskExecutor.submit(createReplicaBatchTask);
        }
    }

    private static void deleteFromBackend(Map<Long, TTablet> backendTablets,
                                          Set<Long> tabletFoundInMeta,
                                          long backendId) {
        int deleteFromBackendCounter = 0;
        int addToMetaCounter = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (Long tabletId : backendTablets.keySet()) {
            TTablet backendTablet = backendTablets.get(tabletId);
            TTabletInfo backendTabletInfo = backendTablet.getTabletInfos().get(0);
            boolean needDelete = false;
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            LOG.debug("process tablet [{}], backend[{}]", tabletId, backendId);
            if (!tabletFoundInMeta.contains(tabletId)) {
                if (isBackendReplicaHealthy(backendTabletInfo)) {
                    // if this tablet meta is still in invertedIndex. try to add it.
                    // if add failed. delete this tablet from backend.
                    if (tabletMeta != null && addReplica(tabletId, tabletMeta, backendTabletInfo, backendId)) {
                        // update counter
                        ++addToMetaCounter;
                        LOG.debug("add to meta. tablet[{}], backend[{}]", tabletId, backendId);
                    } else {
                        LOG.info("failed add to meta. tablet[{}], backend[{}]", tabletId, backendId);
                        needDelete = true;
                    }
                } else {
                    needDelete = true;
                }
            }

            if (needDelete) {
                // drop replica
                long replicaId = backendTabletInfo.getReplicaId();
                // If no such tablet meta, this indicates that the tablet belongs to a dropped table or partition
                boolean isDropTableOrPartition = tabletMeta == null;
                DropReplicaTask task = new DropReplicaTask(backendId, tabletId, replicaId,
                        backendTabletInfo.getSchemaHash(), isDropTableOrPartition);
                batchTask.addTask(task);
                LOG.info("delete tablet[{}] from backend[{}] because not found in meta", tabletId, backendId);
                ++deleteFromBackendCounter;
            }
        } // end for backendTabletIds

        if (batchTask.getTaskNum() != 0) {
            AgentTaskExecutor.submit(batchTask);
        }

        LOG.info("delete {} tablet(s) and add {} replica(s) to meta from backend[{}]",
                deleteFromBackendCounter, addToMetaCounter, backendId);
    }

    // replica is used and no version missing
    private static boolean isBackendReplicaHealthy(TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            return false;
        }
        if (backendTabletInfo.isSetVersionMiss() && backendTabletInfo.isVersionMiss()) {
            return false;
        }
        return true;
    }

    private static void handleMigration(ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap,
                                        long backendId) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Backend be = infoService.getBackend(backendId);
        if (be == null) {
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (TStorageMedium storageMedium : tabletMetaMigrationMap.keySet()) {
            List<Long> tabletIds = tabletMetaMigrationMap.get(storageMedium);
            if (!be.hasSpecifiedStorageMedium(storageMedium)) {
                LOG.warn("no specified storage medium {} on backend {}, skip storage migration."
                        + " sample tablet id: {}", storageMedium, backendId, tabletIds.isEmpty()
                        ? "-1" : tabletIds.get(0));
                continue;
            }
            List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                long tabletId = tabletIds.get(i);
                TabletMeta tabletMeta = tabletMetaList.get(i);
                // always get old schema hash(as effective one)
                int effectiveSchemaHash = tabletMeta.getOldSchemaHash();
                StorageMediaMigrationTask task = new StorageMediaMigrationTask(backendId, tabletId,
                        effectiveSchemaHash, storageMedium);
                batchTask.addTask(task);
            }
        }

        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleRepublishVersionInfo(
            Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish, long backendId) {
        AgentBatchTask batchTask = new AgentBatchTask();
        long createPublishVersionTaskTime = System.currentTimeMillis();
        for (Long dbId : transactionsToPublish.keySet()) {
            ListMultimap<Long, TPartitionVersionInfo> map = transactionsToPublish.get(dbId);
            for (long txnId : map.keySet()) {
                PublishVersionTask task = new PublishVersionTask(backendId, txnId, dbId,
                        map.get(txnId), createPublishVersionTaskTime);
                batchTask.addTask(task);
                // add to AgentTaskQueue for handling finish report.
                AgentTaskQueue.addTask(task);
            }
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleRecoverTablet(ListMultimap<Long, Long> tabletRecoveryMap,
                                            Map<Long, TTablet> backendTablets, long backendId) {
        // print a warn log here to indicate the exceptions on the backend
        LOG.warn("find {} tablets on backend {} which is bad or misses versions that need clone or force recovery",
                tabletRecoveryMap.size(), backendId);

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        BackendReplicasInfo backendReplicasInfo = new BackendReplicasInfo(backendId);
        for (Long dbId : tabletRecoveryMap.keySet()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Long> tabletIds = tabletRecoveryMap.get(dbId);
            List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(tabletIds);
            for (int i = 0; i < tabletMetaList.size(); i++) {
                TabletMeta tabletMeta = tabletMetaList.get(i);
                if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                    continue;
                }
                long tabletId = tabletIds.get(i);
                long tableId = tabletMeta.getTableId();
                OlapTable olapTable = (OlapTable) db.getTableNullable(tableId);
                if (olapTable == null || !olapTable.writeLockIfExist()) {
                    continue;
                }
                try {
                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }

                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    Replica replica = tablet.getReplicaByBackendId(backendId);
                    if (replica == null) {
                        continue;
                    }

                    for (TTabletInfo tTabletInfo : backendTablets.get(tabletId).getTabletInfos()) {
                        if (tTabletInfo.getSchemaHash() == schemaHash) {
                            if (tTabletInfo.isSetUsed() && !tTabletInfo.isUsed()) {
                                if (replica.setBad(true)) {
                                    LOG.warn("set bad for replica {} of tablet {} on backend {}",
                                            replica.getId(), tabletId, backendId);
                                    backendReplicasInfo.addBadReplica(tabletId);
                                }
                                break;
                            }

                            if ((tTabletInfo.isSetVersionMiss() && tTabletInfo.isVersionMiss())
                                    || replica.checkVersionRegressive(tTabletInfo.getVersion())) {
                                // If the origin last failed version is larger than 0, not change it.
                                // Otherwise, we set last failed version to replica'version + 1.
                                // Because last failed version should always larger than replica's version.
                                long newLastFailedVersion = replica.getLastFailedVersion();
                                if (newLastFailedVersion < 0) {
                                    newLastFailedVersion = replica.getVersion() + 1;
                                    replica.updateLastFailedVersion(newLastFailedVersion);
                                    LOG.warn("set missing version for replica {} of tablet {} on backend {}, "
                                            + "version in fe {}, version in be {}, be missing {}",
                                            replica.getId(), tabletId, backendId, replica.getVersion(),
                                            tTabletInfo.getVersion(), tTabletInfo.isVersionMiss());
                                }
                                backendReplicasInfo.addMissingVersionReplica(tabletId, newLastFailedVersion);
                                break;
                            }

                            break;
                        }
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }
        } // end for recovery map

        if (!backendReplicasInfo.isEmpty()) {
            // need to write edit log the sync the bad info to other FEs
            Env.getCurrentEnv().getEditLog().logBackendReplicasInfo(backendReplicasInfo);
        }
    }

    private static void handleUpdateTabletMeta(long backendId, List<TTabletMetaInfo> tabletToUpdate) {
        final int updateBatchSize = 4096;
        AgentBatchTask batchTask = new AgentBatchTask();
        for (int start = 0; start < tabletToUpdate.size(); start += updateBatchSize) {
            UpdateTabletMetaInfoTask task = new UpdateTabletMetaInfoTask(backendId,
                    tabletToUpdate.subList(start, Math.min(start + updateBatchSize, tabletToUpdate.size())));
            batchTask.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleClearTransactions(ListMultimap<Long, Long> transactionsToClear, long backendId) {
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long transactionId : transactionsToClear.keySet()) {
            ClearTransactionTask clearTransactionTask = new ClearTransactionTask(backendId,
                    transactionId, transactionsToClear.get(transactionId));
            batchTask.addTask(clearTransactionTask);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    // return false if add replica failed
    private static boolean addReplica(long tabletId, TabletMeta tabletMeta, TTabletInfo backendTabletInfo,
            long backendId) {
        long dbId = tabletMeta.getDbId();
        long tableId = tabletMeta.getTableId();
        long partitionId = tabletMeta.getPartitionId();
        long indexId = tabletMeta.getIndexId();

        int schemaHash = backendTabletInfo.getSchemaHash();
        long version = backendTabletInfo.getVersion();
        long dataSize = backendTabletInfo.getDataSize();
        long remoteDataSize = backendTabletInfo.getRemoteDataSize();
        long rowCount = backendTabletInfo.getRowCount();

        Database db;
        OlapTable olapTable;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            olapTable = (OlapTable) db.getTableOrMetaException(tableId, Table.TableType.OLAP);
            olapTable.writeLockOrMetaException();
        } catch (MetaNotFoundException e) {
            LOG.warn(e);
            return false;
        }

        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                LOG.warn("partition[{}] does not exist", partitionId);
                return false;
            }
            ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId());

            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                LOG.warn("index[{}] does not exist", indexId);
                return false;
            }

            Tablet tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                LOG.warn("tablet[{}] does not exist", tabletId);
                return false;
            }

            // check replica id
            long replicaId = backendTabletInfo.getReplicaId();
            if (replicaId <= 0) {
                LOG.warn("replica id is invalid");
                return false;
            }

            long visibleVersion = partition.getVisibleVersion();

            // check replica version
            if (version < visibleVersion) {
                LOG.warn("version is invalid. tablet[{}], visible[{}]", version, visibleVersion);
                return false;
            }

            // check schema hash
            if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                LOG.warn("schema hash is diff[{}-{}]", schemaHash, olapTable.getSchemaHashByIndexId(indexId));
                return false;
            }

            // colocate table will delete Replica in meta when balance
            // but we need to rely on MetaNotFoundException to decide whether delete the tablet in backend
            // if the tablet is healthy, delete it.
            boolean isColocateBackend = false;
            ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
            if (colocateTableIndex.isColocateTable(olapTable.getId())) {
                ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
                Preconditions.checkState(groupId != null,
                        "can not get colocate group for %s", tableId);
                int tabletOrderIdx = materializedIndex.getTabletOrderIdx(tabletId);
                Preconditions.checkState(tabletOrderIdx != -1, "get tablet materializedIndex for %s fail", tabletId);
                Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, tabletOrderIdx);
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
                if (groupSchema != null) {
                    replicaAlloc = groupSchema.getReplicaAlloc();
                }
                TabletStatus status =
                        tablet.getColocateHealthStatus(visibleVersion, replicaAlloc, backendsSet);
                if (status == TabletStatus.HEALTHY) {
                    return false;
                }

                if (backendsSet.contains(backendId)) {
                    isColocateBackend = true;
                }
            }

            SystemInfoService infoService = Env.getCurrentSystemInfo();
            List<Long> aliveBeIds = infoService.getAllBackendIds(true);
            Pair<TabletStatus, TabletSchedCtx.Priority> status = tablet.getHealthStatusWithPriority(infoService,
                    visibleVersion, replicaAlloc, aliveBeIds);

            // FORCE_REDUNDANT is a specific missing case.
            // So it can add replica when it's in FORCE_REDUNDANT.
            // But must be careful to avoid: delete a replica then add it back, then repeat forever.
            // If this replica is sched available and existing another replica is sched unavailable,
            // it's safe to add this replica.
            // Because if the tablet scheduler want to delete a replica, it will choose the sched
            // unavailable replica and avoid the repeating loop as above.
            boolean canAddForceRedundant = status.first == TabletStatus.FORCE_REDUNDANT
                    && infoService.checkBackendScheduleAvailable(backendId)
                    && tablet.getReplicas().stream().anyMatch(
                            r -> !infoService.checkBackendScheduleAvailable(r.getBackendId()));

            if (isColocateBackend
                    || canAddForceRedundant
                    || status.first == TabletStatus.VERSION_INCOMPLETE
                    || status.first == TabletStatus.REPLICA_MISSING
                    || status.first == TabletStatus.UNRECOVERABLE) {
                long lastFailedVersion = -1L;

                // For some partition created by old version's Doris
                // The init partition's version in FE is (1-0), the tablet's version in BE is (2-0)
                // If the BE report version is (2-0) and partition's version is (1-0),
                // we should add the tablet to meta.
                // But old version doris is too old, we should not consider them any more,
                // just throw exception in this case
                if (version > partition.getNextVersion() - 1) {
                    // this is a fatal error
                    LOG.warn("version is invalid. tablet[{}], partition's max version [{}]", version,
                            partition.getNextVersion() - 1);
                    return false;
                } else if (version < partition.getCommittedVersion()) {
                    lastFailedVersion = partition.getCommittedVersion();
                }

                if (backendTabletInfo.isSetCooldownMetaId()) {
                    // replica has cooldowned data
                    do {
                        Pair<Long, Long> cooldownConf = tablet.getCooldownConf();
                        if (backendTabletInfo.getCooldownTerm() > cooldownConf.second) {
                            // should not be here
                            LOG.warn("report cooldownTerm({}) > cooldownTerm in TabletMeta({}), tabletId={}",
                                    backendTabletInfo.getCooldownTerm(), cooldownConf.second, tabletId);
                            return false;
                        }
                        if (backendTabletInfo.getReplicaId() == cooldownConf.first) {
                            // this replica is true cooldown replica, so replica's cooldowned data must not be deleted
                            break;
                        }
                        List<Replica> replicas = Env.getCurrentInvertedIndex().getReplicas(tabletId);
                        if (backendTabletInfo.getCooldownTerm() <= 0) {
                            if (replicas.stream().anyMatch(
                                    r -> backendTabletInfo.getCooldownMetaId().equals(r.getCooldownMetaId()))) {
                                // this backend is just restarted, and shares same cooldowned data with others replica,
                                // so replica's cooldowned data must not be deleted
                                break;
                            }
                        }
                        long minCooldownTerm = Long.MAX_VALUE;
                        for (Replica r : replicas) {
                            minCooldownTerm = Math.min(r.getCooldownTerm(), minCooldownTerm);
                        }
                        if (backendTabletInfo.getCooldownTerm() >= minCooldownTerm) {
                            if (replicas.stream().anyMatch(
                                    r -> backendTabletInfo.getCooldownMetaId().equals(r.getCooldownMetaId()))) {
                                // this replica shares same cooldowned data with others replica, and won't follow data
                                // of lower cooldown term, so replica's cooldowned data must not be deleted
                                break;
                            }
                        }
                        LOG.warn("replica's cooldowned data may have been deleted. tabletId={}, replicaId={}", tabletId,
                                replicaId);
                        return false;
                    } while (false);
                }

                // use replicaId reported by BE to maintain replica meta consistent between FE and BE
                Replica replica = new Replica(replicaId, backendId, version, schemaHash,
                        dataSize, remoteDataSize, rowCount, ReplicaState.NORMAL,
                        lastFailedVersion, version);
                tablet.addReplica(replica);

                // write edit log
                ReplicaPersistInfo info = ReplicaPersistInfo.createForAdd(dbId, tableId, partitionId, indexId,
                        tabletId, backendId, replicaId,
                        version, schemaHash, dataSize, remoteDataSize, rowCount,
                        lastFailedVersion,
                        version);

                Env.getCurrentEnv().getEditLog().logAddReplica(info);

                LOG.info("add replica[{}-{}] to catalog. backend[{}], tablet status {}, tablet size {}, "
                        + "is colocate backend {}",
                        tabletId, replicaId, backendId, status.first.name(), tablet.getReplicas().size(),
                        isColocateBackend);
                return true;
            } else {
                // replica is enough. check if this tablet is already in meta
                // (status changed between 'tabletReport()' and 'addReplica()')
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getBackendId() == backendId) {
                        // tablet is already in meta. return true
                        return true;
                    }
                }
                LOG.warn("no add replica [{}-{}] cause it is enough[{}-{}], tablet status {}",
                        tabletId, replicaId, tablet.getReplicas().size(), replicaAlloc.toCreateStmt(),
                        status.first.name());
                return false;
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    protected void runOneCycle() {
        while (true) {
            ReportTask task = null;
            try {
                task = reportQueue.take();
                task.exec();
            } catch (InterruptedException e) {
                LOG.warn("got interupted exception when executing report", e);
            }
        }
    }
}
