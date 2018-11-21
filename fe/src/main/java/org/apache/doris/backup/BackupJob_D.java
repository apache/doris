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

package org.apache.doris.backup;

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LoadBalancer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Deprecated
public class BackupJob_D extends AbstractBackupJob_D {
    private static final Logger LOG = LogManager.getLogger(BackupJob_D.class);

    private static final long SNAPSHOT_TIMEOUT_MS = 2000; // 1s for one tablet

    public enum BackupJobState {
        PENDING,
        SNAPSHOT,
        UPLOAD,
        UPLOADING,
        FINISHING,
        FINISHED,
        CANCELLED
    }

    private BackupJobState state;

    private String lastestLoadLabel;
    private DeleteInfo lastestDeleteInfo;

    // all partitions need to be backuped
    private Map<Long, Set<Long>> tableIdToPartitionIds;
    private Multimap<Long, Long> tableIdToIndexIds;
    // partition id -> (version, version hash)
    private Map<Long, Pair<Long, Long>> partitionIdToVersionInfo;
    
    private Map<Long, Pair<Long, String>> tabletIdToSnapshotPath;

    private long metaSavedTime;
    private long snapshotFinishedTime;
    private long uploadFinishedTime;

    private long phasedTimeoutMs;

    private String readableManifestPath;

    public BackupJob_D() {
        super();
        tableIdToPartitionIds = Maps.newHashMap();
        tableIdToIndexIds = HashMultimap.create();
        partitionIdToVersionInfo = Maps.newHashMap();
        tabletIdToSnapshotPath = Maps.newHashMap();
    }

    public BackupJob_D(long jobId, long dbId, LabelName labelName, String backupPath,
                     Map<String, String> remoteProperties) {
        super(jobId, dbId, labelName, backupPath, remoteProperties);
        this.state = BackupJobState.PENDING;

        tableIdToPartitionIds = Maps.newHashMap();
        tableIdToIndexIds = HashMultimap.create();
        partitionIdToVersionInfo = Maps.newHashMap();

        tabletIdToSnapshotPath = Maps.newHashMap();

        metaSavedTime = -1;
        snapshotFinishedTime = -1;
        uploadFinishedTime = -1;
        phasedTimeoutMs = -1;

        lastestLoadLabel = "N/A";
        readableManifestPath = "";
    }

    public void setState(BackupJobState state) {
        this.state = state;
    }

    public BackupJobState getState() {
        return state;
    }

    public String getLatestLoadLabel() {
        return lastestLoadLabel;
    }

    public DeleteInfo getLastestDeleteInfo() {
        return lastestDeleteInfo;
    }

    public PathBuilder getPathBuilder() {
        return pathBuilder;
    }

    public long getMetaSavedTimeMs() {
        return metaSavedTime;
    }

    public long getSnapshotFinishedTimeMs() {
        return snapshotFinishedTime;
    }

    public long getUploadFinishedTimeMs() {
        return uploadFinishedTime;
    }

    public String getReadableManifestPath() {
        return readableManifestPath;
    }

    public Map<Long, Set<Long>> getTableIdToPartitionIds() {
        return tableIdToPartitionIds;
    }

    public void addPartitionId(long tableId, long partitionId) {
        Set<Long> partitionIds = tableIdToPartitionIds.get(tableId);
        if (partitionIds == null) {
            partitionIds = Sets.newHashSet();
            tableIdToPartitionIds.put(tableId, partitionIds);
        }
        if (partitionId != -1L) {
            partitionIds.add(partitionId);
        }

        LOG.debug("add partition[{}] from table[{}], job[{}]", partitionId, tableId, jobId);
    }

    public void addIndexId(long tableId, long indexId) {
        tableIdToIndexIds.put(tableId, indexId);
        LOG.debug("add index[{}] from table[{}], job[{}]", indexId, tableId, jobId);
    }

    public void handleFinishedSnapshot(long tabletId, long backendId, String snapshotPath) {
        synchronized (unfinishedTabletIds) {
            if (!unfinishedTabletIds.containsKey(tabletId)) {
                LOG.warn("backup job[{}] does not contains tablet[{}]", jobId, tabletId);
                return;
            }

            if (unfinishedTabletIds.get(tabletId) == null
                    || !unfinishedTabletIds.get(tabletId).contains(backendId)) {
                LOG.warn("backup job[{}] does not contains tablet[{}]'s snapshot from backend[{}]. "
                        + "it should from backend[{}]",
                         jobId, tabletId, backendId, unfinishedTabletIds.get(tabletId));
                return;
            }
            unfinishedTabletIds.remove(tabletId, backendId);
        }

        synchronized (tabletIdToSnapshotPath) {
            tabletIdToSnapshotPath.put(tabletId, new Pair<Long, String>(backendId, snapshotPath));
        }
        LOG.debug("finished add tablet[{}] from backend[{}]. snapshot path: {}", tabletId, backendId, snapshotPath);
    }

    public void handleFinishedUpload(long tabletId, long backendId) {
        synchronized (unfinishedTabletIds) {
            if (unfinishedTabletIds.remove(tabletId, backendId)) {
                LOG.debug("finished upload tablet[{}] snapshot, backend[{}]", tabletId, backendId);
            }
        }
    }

    @Override
    public List<Comparable> getJobInfo() {
        List<Comparable> jobInfo = Lists.newArrayList();
        jobInfo.add(jobId);
        jobInfo.add(getLabel());
        jobInfo.add(state.name());
        jobInfo.add(TimeUtils.longToTimeString(createTime));
        jobInfo.add(TimeUtils.longToTimeString(metaSavedTime));
        jobInfo.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        jobInfo.add(TimeUtils.longToTimeString(uploadFinishedTime));
        jobInfo.add(TimeUtils.longToTimeString(finishedTime));
        jobInfo.add(errMsg);
        jobInfo.add(PathBuilder.createPath(remotePath, getLabel()));
        jobInfo.add(getReadableManifestPath());
        jobInfo.add(getLeftTasksNum());
        jobInfo.add(getLatestLoadLabel());
        return jobInfo;
    }

    @Override
    public void runOnce() {
        LOG.debug("begin to run backup job: {}, state: {}", jobId, state.name());
        try {
            switch (state) {
                case PENDING:
                    saveMetaAndMakeSnapshot();
                    break;
                case SNAPSHOT:
                    waitSnapshot();
                    break;
                case UPLOAD:
                    upload();
                    break;
                case UPLOADING:
                    waitUpload();
                    break;
                case FINISHING:
                    finishing();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            errMsg = e.getMessage() == null ? "Unknown Exception" : e.getMessage();
            LOG.warn("failed to backup: " + errMsg + ", job[" + jobId + "]", e);
            state = BackupJobState.CANCELLED;
        }
        
        if (state == BackupJobState.FINISHED || state == BackupJobState.CANCELLED) {
            end(Catalog.getInstance(), false);
        }
    }

    private void saveMetaAndMakeSnapshot() throws DdlException, IOException {
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new DdlException("[" + getDbName() + "] does not exist");
        }

        try {
            pathBuilder = PathBuilder.createPathBuilder(getLocalDirName());
        } catch (IOException e) {
            pathBuilder = null;
            throw e;
        }

        // file path -> writable objs
        Map<String, List<? extends Writable>> pathToWritables = Maps.newHashMap();
        // 1. get meta
        getMeta(db, pathToWritables);

        // 2. write meta
        // IO ops should be done outside db.lock
        try {
            writeMeta(pathToWritables);
        } catch (IOException e) {
            errMsg = e.getMessage();
            state = BackupJobState.CANCELLED;
            return;
        }

        metaSavedTime = System.currentTimeMillis();
        LOG.info("save meta finished. path: {}, job: {}", pathBuilder.getRoot().getFullPath(), jobId);

        // 3. send snapshot tasks
        snapshot(db);
    }

    private void getMeta(Database db, Map<String, List<? extends Writable>> pathToWritables) throws DdlException {
        db.readLock();
        try {
            for (long tableId : tableIdToPartitionIds.keySet()) {
                Table table = db.getTable(tableId);
                if (table == null) {
                    throw new DdlException("table[" + tableId + "] does not exist");
                }

                // 1. get table meta
                getTableMeta(db.getFullName(), table, pathToWritables);

                if (table.getType() != TableType.OLAP) {
                    // this is not a OLAP table. just save table meta
                    continue;
                }
                
                OlapTable olapTable = (OlapTable) table;

                // 2. get rollup meta
                // 2.1 check all indices exist
                for (Long indexId : tableIdToIndexIds.get(tableId)) {
                    if (olapTable.getIndexNameById(indexId) == null) {
                        errMsg = "Index[" + indexId + "] does not exist";
                        state = BackupJobState.CANCELLED;
                        return;
                    }
                }
                getRollupMeta(db.getFullName(), olapTable, pathToWritables);

                // 3. save partition meta
                Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    List<Map.Entry<Long, Range<PartitionKey>>> rangeMap = rangePartitionInfo.getSortedRangeMap();
                    for (Map.Entry<Long, Range<PartitionKey>> entry : rangeMap) {
                        long partitionId = entry.getKey();
                        if (!partitionIds.contains(partitionId)) {
                            continue;
                        }

                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            throw new DdlException("partition[" + partitionId + "] does not exist");
                        }
                        getPartitionMeta(db.getFullName(), olapTable, partition.getName(), pathToWritables);

                        // save version info
                        partitionIdToVersionInfo.put(partitionId,
                                                     new Pair<Long, Long>(partition.getVisibleVersion(),
                                                                          partition.getVisibleVersionHash()));
                    }
                } else {
                    Preconditions.checkState(partitionIds.size() == 1);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        // save version info
                        partitionIdToVersionInfo.put(partitionId,
                                                     new Pair<Long, Long>(partition.getVisibleVersion(),
                                                                          partition.getVisibleVersionHash()));
                    }
                }
            } // end for tables

            // get last finished load job and delele job label
            Load load = Catalog.getInstance().getLoadInstance();
            LoadJob lastestLoadJob = load.getLastestFinishedLoadJob(dbId);
            if (lastestLoadJob == null) {
                // there is no load job, or job info has been removed
                lastestLoadLabel = "N/A";
            } else {
                lastestLoadLabel = lastestLoadJob.getLabel();
            }
            LOG.info("get lastest load job label: {}, job: {}", lastestLoadJob, jobId);

            lastestDeleteInfo = load.getLastestFinishedDeleteInfo(dbId);
            LOG.info("get lastest delete info: {}, job: {}", lastestDeleteInfo, jobId);

            LOG.info("get meta finished. job[{}]", jobId);
        } finally {
            db.readUnlock();
        }
    }

    private void getTableMeta(String dbName, Table table, Map<String, List<? extends Writable>> pathToWritables) {
        CreateTableStmt stmt = table.toCreateTableStmt(dbName);
        int tableSignature = table.getSignature(BackupVersion.VERSION_1);
        stmt.setTableSignature(tableSignature);
        List<CreateTableStmt> stmts = Lists.newArrayList(stmt);
        String filePath = pathBuilder.createTableStmt(dbName, table.getName());

        Preconditions.checkState(!pathToWritables.containsKey(filePath));
        pathToWritables.put(filePath, stmts);
    }

    private void getRollupMeta(String dbName, OlapTable olapTable,
                                Map<String, List<? extends Writable>> pathToWritables) {
        Set<Long> indexIds = Sets.newHashSet(tableIdToIndexIds.get(olapTable.getId()));
        if (indexIds.size() == 1) {
            // only contains base index. do nothing
            return;
        } else {
            // remove base index id
            Preconditions.checkState(indexIds.size() > 1);
            indexIds.remove(olapTable.getId());
        }
        AlterTableStmt stmt = olapTable.toAddRollupStmt(dbName, indexIds);
        String filePath = pathBuilder.addRollupStmt(dbName, olapTable.getName());
        List<AlterTableStmt> stmts = Lists.newArrayList(stmt);

        Preconditions.checkState(!pathToWritables.containsKey(filePath));
        pathToWritables.put(filePath, stmts);
    }

    private void getPartitionMeta(String dbName, OlapTable olapTable, String partitionName,
                                  Map<String, List<? extends Writable>> pathToWritables) {
        AlterTableStmt stmt = olapTable.toAddPartitionStmt(dbName, partitionName);
        String filePath = pathBuilder.addPartitionStmt(dbName, olapTable.getName(), partitionName);
        List<AlterTableStmt> stmts = Lists.newArrayList(stmt);

        Preconditions.checkState(!pathToWritables.containsKey(filePath));
        pathToWritables.put(filePath, stmts);
    }

    private void writeMeta(Map<String, List<? extends Writable>> pathToWritables) throws IOException {
        // 1. write meta
        for (Map.Entry<String, List<? extends Writable>> entry : pathToWritables.entrySet()) {
            String filePath = entry.getKey();
            List<? extends Writable> writables = entry.getValue();
            ObjectWriter.write(filePath, writables);
        }
    }

    private void snapshot(Database db) throws DdlException {
        AgentBatchTask batchTask = new AgentBatchTask();
        LoadBalancer<Long> loadBalancer = new LoadBalancer<Long>(1L);
        long dbId = db.getId();
        db.readLock();
        try {
            for (Map.Entry<Long, Set<Long>> entry : tableIdToPartitionIds.entrySet()) {
                long tableId = entry.getKey();
                Set<Long> partitionIds = entry.getValue();

                Table table = db.getTable(tableId);
                if (table == null) {
                    throw new DdlException("table[" + tableId + "] does not exist");
                }

                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;

                for (Long partitionId : partitionIds) {
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        throw new DdlException("partition[" + partitionId + "] does not exist");
                    }

                    Pair<Long, Long> versionInfo = partitionIdToVersionInfo.get(partitionId);
                    for (Long indexId : tableIdToIndexIds.get(tableId)) {
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index == null) {
                            throw new DdlException("index[" + indexId + "] does not exist");
                        }

                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            List<Long> backendIds = Lists.newArrayList();
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.checkVersionCatchUp(versionInfo.first, versionInfo.second)) {
                                    backendIds.add(replica.getBackendId());
                                }
                            }

                            if (backendIds.isEmpty()) {
                                String msg = "tablet[" + tabletId + "] does not check up with version: "
                                        + versionInfo.first + "-" + versionInfo.second;
                                // this should not happen
                                LOG.error(msg);
                                throw new DdlException(msg);
                            }

                            long chosenBackendId = loadBalancer.chooseKey(backendIds);
                            SnapshotTask task = new SnapshotTask(null, chosenBackendId, tabletId, jobId, dbId, tableId,
                                                                 partitionId, indexId, tabletId,
                                                                 versionInfo.first, versionInfo.second,
                                    schemaHash, -1L, false);
                            LOG.debug("choose backend[{}] to make snapshot for tablet[{}]", chosenBackendId, tabletId);
                            batchTask.addTask(task);
                            unfinishedTabletIds.put(tabletId, chosenBackendId);
                        } // end for tablet
                    } // end for indices
                } // end for partitions
            } // end for tables

        } finally {
            db.readUnlock();
        }

        phasedTimeoutMs = unfinishedTabletIds.size() * SNAPSHOT_TIMEOUT_MS;
        LOG.debug("estimate snapshot timeout: {}, tablet size: {}", phasedTimeoutMs, unfinishedTabletIds.size());

        // send task
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = BackupJobState.SNAPSHOT;
        LOG.info("finish send snapshot task. job: {}", jobId);
    }

    private synchronized void waitSnapshot() throws DdlException {
        if (unfinishedTabletIds.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = BackupJobState.UPLOAD;

            Catalog.getInstance().getEditLog().logBackupFinishSnapshot(this);
            LOG.info("backup job[{}] is finished making snapshot", jobId);

            return;
        } else if (System.currentTimeMillis() - metaSavedTime > phasedTimeoutMs) {
            // remove task in AgentTaskQueue
            for (Map.Entry<Long, Long> entry : unfinishedTabletIds.entries()) {
                AgentTaskQueue.removeTask(entry.getValue(), TTaskType.MAKE_SNAPSHOT, entry.getKey());
            }

            // check timeout
            String msg = "snapshot timeout. " + phasedTimeoutMs + "s.";
            LOG.warn("{}. job[{}]", msg, jobId);
            throw new DdlException(msg);
        } else {
            LOG.debug("waiting {} tablets to make snapshot", unfinishedTabletIds.size());
        }
    }

    private void upload() throws IOException, DdlException, InterruptedException, ExecutionException {
        LOG.debug("start upload. job[{}]", jobId);

        if (commandBuilder == null) {
            String remotePropFilePath = pathBuilder.remoteProperties();
            commandBuilder = CommandBuilder.create(remotePropFilePath, remoteProperties);
        }
        Preconditions.checkNotNull(commandBuilder);

        // 1. send meta to remote source
        if (!uploadMetaObjs()) {
            return;
        }

        // 2. send upload task to be
        sendUploadTasks();
    }

    private boolean uploadMetaObjs() throws IOException, InterruptedException, ExecutionException {
        if (future == null) {
            LOG.info("begin to submit upload meta objs. job: {}", jobId);
            String dest = PathBuilder.createPath(remotePath, getLabel());
            String uploadCmd = commandBuilder.uploadCmd(getLabel(), pathBuilder.getRoot().getFullPath(), dest);

            MetaUploadTask uploadTask = new MetaUploadTask(uploadCmd);
            // future = Catalog.getInstance().getBackupHandler().getAsynchronousCmdExecutor().submit(uploadTask);
            return false;
        } else {
            return checkFuture("upload meta objs");
        }
    }

    private synchronized void sendUploadTasks() throws DdlException {
        Preconditions.checkState(unfinishedTabletIds.isEmpty());

        AgentBatchTask batchTask = new AgentBatchTask();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new DdlException("database[" + getDbName() + "] does not exist");
        }
        db.readLock();
        try {
            String dbName = db.getFullName();
            for (Map.Entry<Long, Set<Long>> entry : tableIdToPartitionIds.entrySet()) {
                long tableId = entry.getKey();
                Set<Long> partitionIds = entry.getValue();

                Table table = db.getTable(tableId);
                if (table == null) {
                    throw new DdlException("table[" + tableId + "] does not exist");
                }

                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                String tableName = olapTable.getName();
                for (Long partitionId : partitionIds) {
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        throw new DdlException("partition[" + partitionId + "] does not exist");
                    }

                    String partitionName = partition.getName();
                    for (Long indexId : tableIdToIndexIds.get(tableId)) {
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index == null) {
                            throw new DdlException("index[" + index + "] does not exist");
                        }

                        String indexName = olapTable.getIndexNameById(indexId);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            if (!tabletIdToSnapshotPath.containsKey(tabletId)) {
                                // this should not happend
                                String msg = "tablet[" + tabletId + "]'s snapshot is missing";
                                LOG.error(msg);
                                throw new DdlException(msg);
                            }

                            Pair<Long, String> snapshotInfo = tabletIdToSnapshotPath.get(tabletId);
                            String dest = pathBuilder.tabletRemotePath(dbName, tableName, partitionName,
                                                                       indexName, tabletId, remotePath, getLabel());

                            unfinishedTabletIds.put(tabletId, snapshotInfo.first);
                        } // end for tablet
                    } // end for indices
                } // end for partitions
            } // end for tables

        } finally {
            db.readUnlock();
        }

        // send task
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        state = BackupJobState.UPLOADING;
        LOG.info("finish send upload task. job: {}", jobId);
    }

    private synchronized void waitUpload() throws DdlException {
        if (unfinishedTabletIds.isEmpty()) {
            LOG.info("backup job[{}] is finished upload snapshot", jobId);
            uploadFinishedTime = System.currentTimeMillis();
            state = BackupJobState.FINISHING;
            return;
        } else {
            LOG.debug("waiting {} tablets to upload snapshot", unfinishedTabletIds.size());
        }
    }

    private void finishing() throws DdlException, InterruptedException, ExecutionException, IOException {
        // save manifest and upload
        // manifest contain all file under {label}/

        if (future == null) {
            LOG.info("begin to submit save and upload manifest. job: {}", jobId);
            String deleteInfo = lastestDeleteInfo == null ? "" : lastestDeleteInfo.toString();
            SaveManifestTask task = new SaveManifestTask(jobId, getLabel(), remotePath, getLocalDirName(),
                                                         lastestLoadLabel, deleteInfo, pathBuilder, commandBuilder);
            // future = Catalog.getInstance().getBackupHandler().getAsynchronousCmdExecutor().submit(task);
        } else {
            boolean finished = checkFuture("save and upload manifest");
            if (finished) {
                // reset future
                readableManifestPath =
                        PathBuilder.createPath(remotePath, getLabel(), PathBuilder.READABLE_MANIFEST_NAME);
                future = null;
                state = BackupJobState.FINISHED;
            }
        }
    }

    public void restoreTableState(Catalog catalog) {
        Database db = catalog.getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                for (long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = db.getTable(tableId);
                    if (table != null && table.getType() == TableType.OLAP) {
                        if (((OlapTable) table).getState() == OlapTableState.BACKUP) {
                            ((OlapTable) table).setState(OlapTableState.NORMAL);
                            LOG.debug("set table[{}] state to NORMAL", table.getName());
                        }
                    }
                }
            } finally {
                db.writeUnlock();
            }
        }
    }

    private void removeLeftTasks() {
        for (Map.Entry<Long, Long> entry : unfinishedTabletIds.entries()) {
            AgentTaskQueue.removeTask(entry.getValue(), TTaskType.MAKE_SNAPSHOT, entry.getKey());
            AgentTaskQueue.removeTask(entry.getValue(), TTaskType.UPLOAD, entry.getKey());
        }
    }

    @Override
    public void end(Catalog catalog, boolean isReplay) {
        // 1. set table state
        restoreTableState(catalog);

        if (!isReplay) {
            // 2. remove agent tasks if left
            removeLeftTasks();

            if (pathBuilder == null) {
                finishedTime = System.currentTimeMillis();
                Catalog.getInstance().getEditLog().logBackupFinish(this);
                LOG.info("finished end job[{}]. state: {}", jobId, state.name());
                return;
            }

            // 3. remove local file
            String labelDir = pathBuilder.getRoot().getFullPath();
            Util.deleteDirectory(new File(labelDir));
            LOG.debug("delete local dir: {}", labelDir);

            // 4. release snapshot
            synchronized (tabletIdToSnapshotPath) {
                AgentBatchTask batchTask = new AgentBatchTask();
                for (Long tabletId : tabletIdToSnapshotPath.keySet()) {
                    long backendId = tabletIdToSnapshotPath.get(tabletId).first;
                    String snapshotPath = tabletIdToSnapshotPath.get(tabletId).second;
                    ReleaseSnapshotTask task = new ReleaseSnapshotTask(null, backendId, dbId, tabletId, snapshotPath);
                    batchTask.addTask(task);
                }
                // no need to add to AgentTaskQueue
                AgentTaskExecutor.submit(batchTask);
            }

            finishedTime = System.currentTimeMillis();

            Catalog.getInstance().getEditLog().logBackupFinish(this);
        }

        clearJob();

        LOG.info("finished end job[{}]. state: {}, replay: {}", jobId, state.name(), isReplay);
    }

    @Override
    protected void clearJob() {
        tableIdToPartitionIds = null;
        tableIdToIndexIds = null;
        partitionIdToVersionInfo = null;
        tabletIdToSnapshotPath = null;

        unfinishedTabletIds = null;
        remoteProperties = null;
        pathBuilder = null;
        commandBuilder = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, state.name());
        Text.writeString(out, lastestLoadLabel);

        if (lastestDeleteInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            lastestDeleteInfo.write(out);
        }

        if (tableIdToPartitionIds == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableIdToPartitionIds.size();
            out.writeInt(size);
            for (Map.Entry<Long, Set<Long>> entry : tableIdToPartitionIds.entrySet()) {
                out.writeLong(entry.getKey());
                size = entry.getValue().size();
                out.writeInt(size);
                for (Long partitionId : entry.getValue()) {
                    out.writeLong(partitionId);
                }
            }
        }

        if (tableIdToIndexIds == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Collection<Map.Entry<Long, Long>> entries = tableIdToIndexIds.entries();
            int size = entries.size();
            out.writeInt(size);
            for (Map.Entry<Long, Long> entry : entries) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }
        }

        if (partitionIdToVersionInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = partitionIdToVersionInfo.size();
            out.writeInt(size);
            for (Map.Entry<Long, Pair<Long, Long>> entry : partitionIdToVersionInfo.entrySet()) {
                out.writeLong(entry.getKey());
                Pair<Long, Long> pair = entry.getValue();
                out.writeLong(pair.first);
                out.writeLong(pair.second);
            }
        }

        if (tabletIdToSnapshotPath == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tabletIdToSnapshotPath.size();
            out.writeInt(size);
            for (Map.Entry<Long, Pair<Long, String>> entry : tabletIdToSnapshotPath.entrySet()) {
                out.writeLong(entry.getKey());
                Pair<Long, String> pair = entry.getValue();
                out.writeLong(pair.first);
                Text.writeString(out, pair.second);
            }
        }

        out.writeLong(metaSavedTime);
        out.writeLong(snapshotFinishedTime);
        out.writeLong(uploadFinishedTime);
        out.writeLong(phasedTimeoutMs);

        Text.writeString(out, readableManifestPath);
        
        if (pathBuilder == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            pathBuilder.write(out);
        }

        if (commandBuilder == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            commandBuilder.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        state = BackupJobState.valueOf(Text.readString(in));
        lastestLoadLabel = Text.readString(in);

        if (in.readBoolean()) {
            lastestDeleteInfo = new DeleteInfo();
            lastestDeleteInfo.readFields(in);
        }

        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long tableId = in.readLong();
                Set<Long> partitionIds = Sets.newHashSet();
                tableIdToPartitionIds.put(tableId, partitionIds);
                int count = in.readInt();
                for (int j = 0; j < count; j++) {
                    long partitionId = in.readLong();
                    partitionIds.add(partitionId);
                }
            }
        }

        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long tableId = in.readLong();
                long indexId = in.readLong();
                tableIdToIndexIds.put(tableId, indexId);
            }
        }

        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long partitionId = in.readLong();
                long version = in.readLong();
                long versionHash = in.readLong();
                partitionIdToVersionInfo.put(partitionId, new Pair<Long, Long>(version, versionHash));
            }
        }

        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long tabletId = in.readLong();
                long backendId = in.readLong();
                String path = Text.readString(in);
                tabletIdToSnapshotPath.put(tabletId, new Pair<Long, String>(backendId, path));
            }
        }

        metaSavedTime = in.readLong();
        snapshotFinishedTime = in.readLong();
        uploadFinishedTime = in.readLong();
        phasedTimeoutMs = in.readLong();

        readableManifestPath = Text.readString(in);

        if (in.readBoolean()) {
            pathBuilder = new PathBuilder();
            pathBuilder.readFields(in);
        }

        if (in.readBoolean()) {
            commandBuilder = new CommandBuilder();
            commandBuilder.readFields(in);
        }

    }
}
