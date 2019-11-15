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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
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
import org.apache.doris.task.PushTask;
import org.apache.doris.task.RecoverTabletTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class ReportHandler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(ReportHandler.class);

    private BlockingQueue<ReportTask> reportQueue = Queues.newLinkedBlockingQueue();

    private GaugeMetric<Long> gaugeQueueSize;

    public ReportHandler() {
        gaugeQueueSize = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "report_queue_size", "report queue size") {
            @Override
            public Long getValue() {
                return (long) reportQueue.size();
            }
        };
        MetricRepo.addMetric(gaugeQueueSize);
    }

    public TMasterResult handleReport(TReportRequest request) throws TException {
        TMasterResult result = new TMasterResult();
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);
        
        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBe_port();
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatus_code(TStatusCode.INTERNAL_ERROR);
            List<String> errorMsgs = Lists.newArrayList();
            errorMsgs.add("backend[" + host + ":" + bePort + "] does not exist.");
            tStatus.setError_msgs(errorMsgs);
            return result;
        }
        
        long beId = backend.getId();
        Map<TTaskType, Set<Long>> tasks = null;
        Map<String, TDisk> disks = null;
        Map<Long, TTablet> tablets = null;
        boolean forceRecovery = false;
        long reportVersion = -1;

        String reportType = "";
        if (request.isSetTasks()) {
            tasks = request.getTasks();
            reportType += "task";
        }
        
        if (request.isSetDisks()) {
            disks = request.getDisks();
            reportType += "disk";
        }
        
        if (request.isSetTablets()) {
            tablets = request.getTablets();
            reportVersion = request.getReport_version();
            reportType += "tablet";
        } else if (request.isSetTablet_list()) {
            // the 'tablets' member will be deprecated in future.
            tablets = buildTabletMap(request.getTablet_list());
            reportVersion = request.getReport_version();
            reportType += "tablet";
        }
        
        if (request.isSetForce_recovery()) {
            forceRecovery = request.isForce_recovery();
        }
        
        ReportTask reportTask = new ReportTask(beId, tasks, disks, tablets, reportVersion, forceRecovery);
        try {
            putToQueue(reportTask);
        } catch (Exception e) {
            tStatus.setStatus_code(TStatusCode.INTERNAL_ERROR);
            List<String> errorMsgs = Lists.newArrayList();
            errorMsgs.add("failed to put report task to queue. queue size: " + reportQueue.size());
            errorMsgs.add("err: " + e.getMessage());
            tStatus.setError_msgs(errorMsgs);
            return result;
        }
        
        LOG.info("receive report from be {}. type: {}, current queue size: {}",
                backend.getId(), reportType, reportQueue.size());
        return result;
    }

    private void putToQueue(ReportTask reportTask) throws Exception {
        int currentSize = reportQueue.size();
        if (currentSize > Config.report_queue_size) {
            LOG.warn("the report queue size exceeds the limit: {}. current: {}",  Config.report_queue_size, currentSize);
            throw new Exception(
                    "the report queue size exceeds the limit: " +  Config.report_queue_size + ". current: " + currentSize);
        }
        reportQueue.put(reportTask);
    }

    private Map<Long, TTablet> buildTabletMap(List<TTablet> tabletList) {
        Map<Long, TTablet> tabletMap = Maps.newHashMap();
        for (TTablet tTablet : tabletList) {
            if (tTablet.getTablet_infos().isEmpty()) {
                continue;
            }

            tabletMap.put(tTablet.getTablet_infos().get(0).getTablet_id(), tTablet);
        }
        return tabletMap;
    }

    private class ReportTask extends MasterTask {

        private long beId;
        private Map<TTaskType, Set<Long>> tasks;
        private Map<String, TDisk> disks;
        private Map<Long, TTablet> tablets;
        private long reportVersion;
        private boolean forceRecovery = false;

        public ReportTask(long beId, Map<TTaskType, Set<Long>> tasks,
                Map<String, TDisk> disks,
                Map<Long, TTablet> tablets, long reportVersion, 
                boolean forceRecovery) {
            this.beId = beId;
            this.tasks = tasks;
            this.disks = disks;
            this.tablets = tablets;
            this.reportVersion = reportVersion;
            this.forceRecovery = forceRecovery;
        }

        @Override
        protected void exec() {
            if (tasks != null) {
                ReportHandler.taskReport(beId, tasks);
            }
            if (disks != null) {
                ReportHandler.diskReport(beId, disks);
            }
            if (tablets != null) {
                long backendReportVersion = Catalog.getCurrentSystemInfo().getBackendReportVersion(beId);
                if (reportVersion < backendReportVersion) {
                    LOG.warn("out of date report version {} from backend[{}]. current report version[{}]",
                             reportVersion, beId, backendReportVersion);
                } else {
                    ReportHandler.tabletReport(beId, tablets, reportVersion, forceRecovery);
                }
            }
        }
    }

    private static void tabletReport(long backendId, Map<Long, TTablet> backendTablets, long backendReportVersion, 
            boolean forceRecovery) {
        long start = System.currentTimeMillis();
        LOG.info("backend[{}] reports {} tablet(s). report version: {}",
                 backendId, backendTablets.size(), backendReportVersion);

        // storage medium map
        HashMap<Long, TStorageMedium> storageMediumMap = Catalog.getInstance().getPartitionIdToStorageMediumMap();

        // db id -> tablet id
        ListMultimap<Long, Long> tabletSyncMap = LinkedListMultimap.create();
        // db id -> tablet id
        ListMultimap<Long, Long> tabletDeleteFromMeta = LinkedListMultimap.create();
        // tablet ids which schema hash is valid
        Set<Long> foundTabletsWithValidSchema = new HashSet<Long>();
        // tablet ids which schema hash is invalid
        Map<Long, TTabletInfo> foundTabletsWithInvalidSchema = new HashMap<Long, TTabletInfo>();
        // storage medium -> tablet id
        ListMultimap<TStorageMedium, Long> tabletMigrationMap = LinkedListMultimap.create();
        
        // dbid -> txn id -> [partition info]
        Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish = Maps.newHashMap();
        ListMultimap<Long, Long> transactionsToClear = LinkedListMultimap.create();
        
        // db id -> tablet id
        ListMultimap<Long, Long> tabletRecoveryMap = LinkedListMultimap.create();
        
        SetMultimap<Long, Integer> tabletWithoutPartitionId = HashMultimap.create();

        // 1. do the diff. find out (intersection) / (be - meta) / (meta - be)
        Catalog.getCurrentInvertedIndex().tabletReport(backendId, backendTablets, storageMediumMap,
                                                       tabletSyncMap,
                                                       tabletDeleteFromMeta,
                                                       foundTabletsWithValidSchema,
                                                       foundTabletsWithInvalidSchema,
                                                       tabletMigrationMap, 
                                                       transactionsToPublish, 
                                                       transactionsToClear, 
                                                       tabletRecoveryMap, 
                                                       tabletWithoutPartitionId);
        

        // 2. sync
        sync(backendTablets, tabletSyncMap, backendId, backendReportVersion);

        // 3. delete (meta - be)
        // BE will automatically drop defective tablets. these tablets should also be dropped in catalog
        deleteFromMeta(tabletDeleteFromMeta, backendId, backendReportVersion, forceRecovery);
        
        // 4. handle (be - meta)
        deleteFromBackend(backendTablets, foundTabletsWithValidSchema, foundTabletsWithInvalidSchema, backendId);
        
        // 5. migration (ssd <-> hdd)
        handleMigration(tabletMigrationMap, backendId);
        
        // 6. send clear transactions to be
        handleClearTransactions(transactionsToClear, backendId);
        
        // 7. send publish version request to be
        handleRepublishVersionInfo(transactionsToPublish, backendId);
        
        // 8. send recover request to 
        handleRecoverTablet(tabletRecoveryMap, backendTablets, backendId, forceRecovery);
        
        // 9. send force create replica task to be
        // handleForceCreateReplica(createReplicaTasks, backendId, forceRecovery);
        
        // 10. send set tablet partition info to backend
        handleSetTabletMetaInfo(backendId, tabletWithoutPartitionId);
        
        long end = System.currentTimeMillis();
        LOG.info("tablet report from backend[{}] cost: {} ms", backendId, (end - start));
    }

    private static void taskReport(long backendId, Map<TTaskType, Set<Long>> runningTasks) {
        LOG.info("begin to handle task report from backend {}", backendId);
        long start = System.currentTimeMillis();

        for (TTaskType type : runningTasks.keySet()) {
            Set<Long> taskSet = runningTasks.get(type);
            if (!taskSet.isEmpty()) {
                String signatures = StringUtils.join(taskSet, ", ");
                LOG.debug("backend task[{}]: {}", type.name(), signatures);
            }
        }

        List<AgentTask> diffTasks = AgentTaskQueue.getDiffTasks(backendId, runningTasks);

        AgentBatchTask batchTask = new AgentBatchTask();
        for (AgentTask task : diffTasks) {
            // these tasks no need to do diff
            // 1. CREATE
            // 2. SYNC DELETE
            // 3. CHECK_CONSISTENCY
            if (task.getTaskType() == TTaskType.CREATE
                    || (task.getTaskType() == TTaskType.PUSH && ((PushTask) task).getPushType() == TPushType.DELETE
                    && ((PushTask) task).isSyncDelete())
                    || task.getTaskType() == TTaskType.CHECK_CONSISTENCY) {
                continue;
            }
            batchTask.addTask(task);
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
        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            LOG.warn("backend doesn't exist. id: " + backendId);
            return;
        }
        
        backend.updateDisks(backendDisks);
        LOG.info("finished to handle disk report from backend {}, cost: {} ms",
                 backendId, (System.currentTimeMillis() - start));
    }

    private static void sync(Map<Long, TTablet> backendTablets, ListMultimap<Long, Long> tabletSyncMap,
                             long backendId, long backendReportVersion) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (Long dbId : tabletSyncMap.keySet()) {
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                int syncCounter = 0;
                List<Long> tabletIds = tabletSyncMap.get(dbId);
                LOG.info("before sync tablets in db[{}]. report num: {}. backend[{}]",
                         dbId, tabletIds.size(), backendId);

                for (Long tabletId : tabletIds) {
                    long tableId = invertedIndex.getTableId(tabletId);
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }

                    long partitionId = invertedIndex.getPartitionId(tabletId);
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = invertedIndex.getIndexId(tabletId);
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
                        long metaVersionHash = replica.getVersionHash();
                        long backendVersion = -1L;
                        long backendVersionHash = -1L;
                        long rowCount = -1L;
                        long dataSize = -1L;
                        // schema change maybe successfully in fe, but not inform be, then be will report two schema hash
                        // just select the dest schema hash
                        for (TTabletInfo tabletInfo : backendTablets.get(tabletId).getTablet_infos()) {
                            if (tabletInfo.getSchema_hash() == schemaHash) {
                                backendVersion = tabletInfo.getVersion();
                                backendVersionHash = tabletInfo.getVersion_hash();
                                rowCount = tabletInfo.getRow_count();
                                dataSize = tabletInfo.getData_size();
                                break;
                            }
                        }
                        if (backendVersion == -1L || backendVersionHash == -1L) {
                            continue;
                        }

                        if (metaVersion < backendVersion
                                || (metaVersion == backendVersion && metaVersionHash != backendVersionHash)
                                || (metaVersion == backendVersion && metaVersionHash == backendVersionHash && replica.isBad())) {
                            
                            // This is just a optimization for the old compatibility
                            // The init version in FE is (1-0), in BE is (2-0)
                            // If the BE report version is (2-0), we just update the replica's version in Master FE,
                            // and no need to write edit log, to save some time.
                            // TODO(cmy): This will be removed later.
                            boolean isInitVersion = metaVersion == 1 && metaVersionHash == 0
                                    && backendVersion == 2 && backendVersionHash == 0;

                            if (backendReportVersion < Catalog.getCurrentSystemInfo()
                                    .getBackendReportVersion(backendId)) {
                                continue;
                            }

                            // happens when
                            // 1. PUSH finished in BE but failed or not yet report to FE
                            // 2. repair for VERSION_INCOMPLETE finished in BE, but failed or not yet report to FE
                            replica.updateVersionInfo(backendVersion, backendVersionHash, dataSize, rowCount);
                            replica.setBad(false);

                            if (replica.getLastFailedVersion() < 0 && !isInitVersion) {
                                // last failed version < 0 means this replica becomes health after sync,
                                // so we write an edit log to sync this operation
                                ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tableId,
                                        partitionId, indexId, tabletId, backendId, replica.getId(),
                                        replica.getVersion(), replica.getVersionHash(), schemaHash,
                                        dataSize, rowCount,
                                        replica.getLastFailedVersion(), replica.getLastFailedVersionHash(),
                                        replica.getLastSuccessVersion(), replica.getLastSuccessVersionHash());
                                Catalog.getInstance().getEditLog().logUpdateReplica(info);
                            }

                            ++syncCounter;
                            LOG.debug("sync replica {} of tablet {} in backend {} in db {}. report version: {}",
                                    replica.getId(), tabletId, backendId, dbId, backendReportVersion);
                        } else {
                            LOG.debug("replica {} of tablet {} in backend {} version is changed"
                                    + " between check and real sync. meta[{}-{}]. backend[{}-{}]",
                                    replica.getId(), tabletId, backendId, metaVersion, metaVersionHash,
                                      backendVersion, backendVersionHash);
                        }
                    }
                } // end for tabletMetaSyncMap
                LOG.info("sync {} tablets in db[{}]. backend[{}]", syncCounter, dbId, backendId);
            } finally {
                db.writeUnlock();
            }
        } // end for dbs
    }

    private static void deleteFromMeta(ListMultimap<Long, Long> tabletDeleteFromMeta, long backendId,
            long backendReportVersion, boolean forceRecovery) {
        AgentBatchTask createReplicaBatchTask = new AgentBatchTask();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (Long dbId : tabletDeleteFromMeta.keySet()) {
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                int deleteCounter = 0;
                List<Long> tabletIds = tabletDeleteFromMeta.get(dbId);
                for (Long tabletId : tabletIds) {
                    long tableId = invertedIndex.getTableId(tabletId);
                    OlapTable olapTable = (OlapTable) db.getTable(tableId);
                    if (olapTable == null) {
                        continue;
                    }

                    long partitionId = invertedIndex.getPartitionId(tabletId);
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

                    long indexId = invertedIndex.getIndexId(tabletId);
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
                    long currentBackendReportVersion = Catalog.getCurrentSystemInfo().getBackendReportVersion(backendId);
                    if (backendReportVersion < currentBackendReportVersion) {
                        continue;
                    }

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
                                if (forceRecovery) {
                                    // only create this task if force recovery is true
                                    LOG.warn("tablet {} has only one replica {} on backend {}"
                                            + "and it is lost. create an empty replica to recover it",
                                            tabletId, replica.getId(), backendId);
                                    short shortKeyColumnCount = olapTable.getShortKeyColumnCountByIndexId(indexId);
                                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                                    KeysType keysType = olapTable.getKeysType();
                                    List<Column> columns = olapTable.getSchemaByIndexId(indexId);
                                    Set<String> bfColumns = olapTable.getCopiedBfColumns();
                                    double bfFpp = olapTable.getBfFpp();
                                    CreateReplicaTask createReplicaTask = new CreateReplicaTask(backendId, dbId,
                                            tableId, partitionId, indexId, tabletId, shortKeyColumnCount,
                                            schemaHash, partition.getVisibleVersion(),
                                            partition.getVisibleVersionHash(), keysType,
                                            TStorageType.COLUMN,
                                            TStorageMedium.HDD, columns, bfColumns, bfFpp, null);
                                    createReplicaBatchTask.addTask(createReplicaTask);
                                } else {
                                    // just set this replica as bad
                                    if (replica.setBad(true)) {
                                        LOG.warn("tablet {} has only one replica {} on backend {}"
                                                + "and it is lost, set it as bad",
                                                tabletId, replica.getId(), backendId);
                                        BackendTabletsInfo tabletsInfo = new BackendTabletsInfo(backendId);
                                        tabletsInfo.setBad(true);
                                        tabletsInfo.addTabletWithSchemaHash(tabletId,
                                                olapTable.getSchemaHashByIndexId(indexId));
                                        Catalog.getInstance().getEditLog().logBackendTabletsInfo(tabletsInfo);
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

                        Catalog.getInstance().getEditLog().logDeleteReplica(info);
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
                } // end for tabletMetas
                LOG.info("delete {} replica(s) from catalog in db[{}]", deleteCounter, dbId);
            } finally {
                db.writeUnlock();
            }
        } // end for dbs

        if (forceRecovery && createReplicaBatchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(createReplicaBatchTask);
        }
    }

    private static void deleteFromBackend(Map<Long, TTablet> backendTablets,
                                          Set<Long> foundTabletsWithValidSchema,
                                          Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                                          long backendId) {
        int deleteFromBackendCounter = 0;
        int addToMetaCounter = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long tabletId : backendTablets.keySet()) {
            TTablet backendTablet = backendTablets.get(tabletId);
            for (TTabletInfo backendTabletInfo : backendTablet.getTablet_infos()) {
                boolean needDelete = false;
                if (!foundTabletsWithValidSchema.contains(tabletId)) {
                    if (isBackendReplicaHealthy(backendTabletInfo)) {
                        // if this tablet is not in meta. try adding it.
                        // if add failed. delete this tablet from backend.
                        try {
                            addReplica(tabletId, backendTabletInfo, backendId);
                            // update counter
                            needDelete = false;
                            ++addToMetaCounter;
                        } catch (MetaNotFoundException e) {
                            LOG.warn("failed add to meta. tablet[{}], backend[{}]. {}",
                                    tabletId, backendId, e.getMessage());
                            needDelete = true;
                        }
                    } else {
                        needDelete = true;
                    }
                }

                if (needDelete) {
                    // drop replica
                    DropReplicaTask task = new DropReplicaTask(backendId, tabletId, backendTabletInfo.getSchema_hash());
                    batchTask.addTask(task);
                    LOG.warn("delete tablet[" + tabletId + " - " + backendTabletInfo.getSchema_hash()
                            + "] from backend[" + backendId + "] because not found in meta");
                    ++deleteFromBackendCounter;
                }
            } // end for tabletInfos

            if (foundTabletsWithInvalidSchema.containsKey(tabletId)) {
                // this tablet is found in meta but with invalid schema hash.
                // delete it.
                int schemaHash = foundTabletsWithInvalidSchema.get(tabletId).getSchema_hash();
                DropReplicaTask task = new DropReplicaTask(backendId, tabletId, schemaHash);
                batchTask.addTask(task);
                LOG.warn("delete tablet[" + tabletId + " - " + schemaHash + "] from backend[" + backendId
                        + "] because invalid schema hash");
                ++deleteFromBackendCounter;
            }
        } // end for backendTabletIds
        AgentTaskExecutor.submit(batchTask);
        
        LOG.info("delete {} tablet(s) from backend[{}]", deleteFromBackendCounter, backendId);
        LOG.info("add {} replica(s) to meta. backend[{}]", addToMetaCounter, backendId);
    }

    // replica is used and no version missing
    private static boolean isBackendReplicaHealthy(TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            return false;
        }
        if (backendTabletInfo.isSetVersion_miss() && backendTabletInfo.isVersion_miss()) {
            return false;
        }
        return true;
    }

    private static void handleMigration(ListMultimap<TStorageMedium, Long> tabletMetaMigrationMap,
                                        long backendId) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        AgentBatchTask batchTask = new AgentBatchTask();
        for (TStorageMedium storageMedium : tabletMetaMigrationMap.keySet()) {
            List<Long> tabletIds = tabletMetaMigrationMap.get(storageMedium);
            for (Long tabletId : tabletIds) {
                StorageMediaMigrationTask task = new StorageMediaMigrationTask(backendId, tabletId,
                                                      invertedIndex.getEffectiveSchemaHash(tabletId),
                                                      storageMedium);
                batchTask.addTask(task);
            }
        }

        AgentTaskExecutor.submit(batchTask);
    }

    private static void handleRepublishVersionInfo(Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish, 
            long backendId) {
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long dbId : transactionsToPublish.keySet()) {
            ListMultimap<Long, TPartitionVersionInfo> map = transactionsToPublish.get(dbId);
            for (long txnId : map.keySet()) {
                PublishVersionTask task = new PublishVersionTask(backendId, txnId, dbId, map.get(txnId));
                batchTask.addTask(task);
                // add to AgentTaskQueue for handling finish report.
                AgentTaskQueue.addTask(task);
            }
        }
        AgentTaskExecutor.submit(batchTask);
    }
    
    private static void handleRecoverTablet(ListMultimap<Long, Long> tabletRecoveryMap,
            Map<Long, TTablet> backendTablets, long backendId, boolean forceRecovery) {
        if (tabletRecoveryMap.isEmpty()) {
            return;
        }

        // print a warn log here to indicate the exceptions on the backend
        LOG.warn("find {} tablets with report version less than version in meta, or is set bad, on backend {}"
                + ", they need clone or force recovery",
                tabletRecoveryMap.size(), backendId);

        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        if (!forceRecovery) {
            LOG.warn("force recovery is disable. try reset the tablets' version"
                    + " or set it as bad, and waiting clone");

            BackendTabletsInfo backendTabletsInfo = new BackendTabletsInfo(backendId);
            backendTabletsInfo.setBad(true);
            for (Long dbId : tabletRecoveryMap.keySet()) {
                Database db = Catalog.getInstance().getDb(dbId);
                if (db == null) {
                    continue;
                }
                db.writeLock();
                try {
                    for (Long tabletId : tabletRecoveryMap.get(dbId)) {
                        long tableId = invertedIndex.getTableId(tabletId);
                        OlapTable olapTable = (OlapTable) db.getTable(tableId);
                        if (olapTable == null) {
                            continue;
                        }

                        long partitionId = invertedIndex.getPartitionId(tabletId);
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }

                        long indexId = invertedIndex.getIndexId(tabletId);
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

                        for (TTabletInfo tTabletInfo : backendTablets.get(tabletId).getTablet_infos()) {
                            if (tTabletInfo.getSchema_hash() == schemaHash) {
                                if (tTabletInfo.isSetUsed() && !tTabletInfo.isUsed()) {
                                    if (replica.setBad(true)) {
                                        LOG.warn("set bad for replica {} of tablet {} on backend {}",
                                                replica.getId(), tabletId, backendId);
                                        backendTabletsInfo.addTabletWithSchemaHash(tabletId, schemaHash);
                                    }
                                    break;
                                }

                                if (replica.getVersion() > tTabletInfo.getVersion()) {
                                    LOG.warn("recover for replica {} of tablet {} on backend {}",
                                            replica.getId(), tabletId, backendId);
                                    if (replica.getVersion() == tTabletInfo.getVersion() + 1) {
                                        // this missing version is the last version of this replica
                                        replica.updateVersionInfoForRecovery(
                                                tTabletInfo.getVersion(), /* set version to BE report version */
                                                -1, /* BE report version hash is meaningless here */
                                                replica.getVersion(), /* set LFV to current FE version */
                                                replica.getVersionHash(), /* set LFV hash to current FE version hash */
                                                tTabletInfo.getVersion(), /* set LSV to BE report version */
                                                -1 /* LSV hash is unknown */);
                                    } else {
                                        // this missing version is a hole
                                        replica.updateVersionInfoForRecovery(
                                                tTabletInfo.getVersion(), /* set version to BE report version */
                                                -1, /* BE report version hash is meaningless here */
                                                tTabletInfo.getVersion() + 1, /* LFV */
                                                -1, /* LFV hash is unknown */
                                                /* remain LSV unchanged, which should be equal to replica.version */
                                                replica.getLastSuccessVersion(),
                                                replica.getLastSuccessVersionHash());
                                    }
                                    // no need to write edit log, if FE crashed, this will be recovered again
                                    break;
                                }
                            }
                        }
                    }
                } finally {
                    db.writeUnlock();
                }
            } // end for recovery map

            if (!backendTabletsInfo.isEmpty()) {
                // need to write edit log the sync the bad info to other FEs
                Catalog.getInstance().getEditLog().logBackendTabletsInfo(backendTabletsInfo);
            }

            return;
        } else {
            LOG.warn("force recovery is enable. use recovery tablet task to recover");
            AgentBatchTask batchTask = new AgentBatchTask();
            for (long tabletId : tabletRecoveryMap.values()) {
                Replica replica = invertedIndex.getReplica(tabletId, backendId);

                RecoverTabletTask recoverTask = new RecoverTabletTask(backendId,
                        tabletId, replica.getVersion(), replica.getVersionHash(),
                        backendTablets.get(tabletId).getTablet_infos().get(0).getSchema_hash());
                LOG.warn("recover replica {} of tablet {} on backend {}, schema hash: {}"
                        + ", version: {}-{}",
                        replica.getId(), tabletId, backendId,
                        backendTablets.get(tabletId).getTablet_infos().get(0).getSchema_hash(),
                        replica.getVersion(), replica.getVersionHash());

                batchTask.addTask(recoverTask);
                AgentTaskQueue.addTask(recoverTask);
            }

            AgentTaskExecutor.submit(batchTask);
        }
    }
    
    private static void handleForceCreateReplica(List<CreateReplicaTask> createReplicaTasks, 
            long backendId, boolean forceRecovery) {
        // print this warn info to indicate admin the fatal state
        if (createReplicaTasks.size() > 0) {
            // print a warn log here to indicate the exceptions on the backend
            LOG.warn("find {} tablets with only on replica and it is on this backend {}" 
                        +  " admin need create the tablet on this backend forcibly, " 
                        + " force recovery is [{}]", 
                        createReplicaTasks.size(), backendId, forceRecovery);
        }
        if (!forceRecovery) {
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (CreateReplicaTask recoverTask : createReplicaTasks) {
            batchTask.addTask(recoverTask);
            AgentTaskQueue.addTask(recoverTask);
        }
        
        AgentTaskExecutor.submit(batchTask);
    }
    

    private static void handleSetTabletMetaInfo(long backendId, SetMultimap<Long, Integer> tabletWithoutPartitionId) {
        LOG.info("find [{}] tablets without partition id, try to set them", tabletWithoutPartitionId.size());
        if (tabletWithoutPartitionId.size() < 1) {
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        UpdateTabletMetaInfoTask task = new UpdateTabletMetaInfoTask(backendId, tabletWithoutPartitionId);
        batchTask.addTask(task);
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

    private static void addReplica(long tabletId, TTabletInfo backendTabletInfo, long backendId)
            throws MetaNotFoundException {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();

        long dbId = invertedIndex.getDbId(tabletId);
        long tableId = invertedIndex.getTableId(tabletId);
        long partitionId = invertedIndex.getPartitionId(tabletId);
        long indexId = invertedIndex.getIndexId(tabletId);
        
        int schemaHash = backendTabletInfo.getSchema_hash();
        long version = backendTabletInfo.getVersion();
        long versionHash = backendTabletInfo.getVersion_hash();
        long dataSize = backendTabletInfo.getData_size();
        long rowCount = backendTabletInfo.getRow_count();

        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db[" + dbId + "] does not exist");
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                throw new MetaNotFoundException("table[" + tableId + "] does not exist");
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("partition[" + partitionId + "] does not exist");
            }
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new MetaNotFoundException("index[" + indexId + "] does not exist");
            }

            Tablet tablet = materializedIndex.getTablet(tabletId);
            if (tablet == null) {
                throw new MetaNotFoundException("tablet[" + tabletId + "] does not exist");
            }

            long visibleVersion = partition.getVisibleVersion();
            long visibleVersionHash = partition.getVisibleVersionHash();

            // check replica version
            if (version < visibleVersion || (version == visibleVersion && versionHash != visibleVersionHash)) {
                throw new MetaNotFoundException("version is invalid. tablet[" + version + "-" + versionHash + "]"
                        + ", visible[" + visibleVersion + "-" + visibleVersionHash + "]");
            }
            
            // check schema hash
            if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                throw new MetaNotFoundException("schema hash is diff[" + schemaHash + "-"
                        + olapTable.getSchemaHashByIndexId(indexId) + "]");
            }

            // colocate table will delete Replica in meta when balance
            // but we need to rely on MetaNotFoundException to decide whether delete the tablet in backend
            if (Catalog.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
                return;
            }

            int availableBackendsNum = infoService.getClusterBackendIds(db.getClusterName(), true).size();
            Pair<TabletStatus, TabletSchedCtx.Priority> status = tablet.getHealthStatusWithPriority(infoService,
                    db.getClusterName(), visibleVersion, visibleVersionHash,
                    replicationNum, availableBackendsNum);
            
            if (status.first == TabletStatus.VERSION_INCOMPLETE || status.first == TabletStatus.REPLICA_MISSING) {
                long lastFailedVersion = -1L;
                long lastFailedVersionHash = 0L;
                if (version > partition.getNextVersion() - 1) {
                    // this is a fatal error
                    throw new MetaNotFoundException("version is invalid. tablet[" + version + "-" + versionHash + "]"
                            + ", partition's max version [" + (partition.getNextVersion() - 1) + "]");
                } else if (version < partition.getCommittedVersion() 
                        || version == partition.getCommittedVersion() 
                            && versionHash != partition.getCommittedVersionHash()) {
                    lastFailedVersion = partition.getCommittedVersion();
                    lastFailedVersionHash = partition.getCommittedVersionHash();
                }

                long replicaId = Catalog.getInstance().getNextId();
                Replica replica = new Replica(replicaId, backendId, version, versionHash, schemaHash,
                                              dataSize, rowCount, ReplicaState.NORMAL, 
                                              lastFailedVersion, lastFailedVersionHash, version, versionHash);
                tablet.addReplica(replica);
                
                // write edit log
                ReplicaPersistInfo info = ReplicaPersistInfo.createForAdd(dbId, tableId, partitionId, indexId,
                        tabletId, backendId, replicaId,
                        version, versionHash, schemaHash, dataSize, rowCount,
                        lastFailedVersion, lastFailedVersionHash,
                        version, versionHash);

                Catalog.getInstance().getEditLog().logAddReplica(info);

                LOG.info("add replica[{}-{}] to catalog. backend[{}]", tabletId, replicaId, backendId);
            } else {
                // replica is enough. check if this tablet is already in meta
                // (status changed between 'tabletReport()' and 'addReplica()')
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getBackendId() == backendId) {
                        // tablet is already in meta. return true
                        return;
                    }
                }
                throw new MetaNotFoundException(
                        "replica is enough[" + tablet.getReplicas().size() + "-" + replicationNum + "]");
            }
        } finally {
            db.writeUnlock();
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
                continue;
            }
        }
    }
}
