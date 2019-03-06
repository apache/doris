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

import org.apache.doris.alter.RollupHandler;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
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
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
public class RestoreJob_D extends AbstractBackupJob_D {
    private static final Logger LOG = LogManager.getLogger(RestoreJob_D.class);

    public enum RestoreJobState {
        PENDING,
        RESTORE_META,
        DOWNLOAD,
        DOWNLOADING,
        FINISHED,
        CANCELLED
    }

    private RestoreJobState state;

    private Map<String, Set<String>> tableToPartitionNames;
    private Map<String, String> tableRenameMap;

    private Map<String, CreateTableStmt> tableToCreateTableStmt;
    private Map<String, AlterTableStmt> tableToRollupStmt;
    private com.google.common.collect.Table<String, String, AlterTableStmt> tableToPartitionStmts;

    private Map<String, Boolean> tableToReplace;

    private Map<String, Table> restoredTables;
    // tableid - partition name - partition
    private com.google.common.collect.Table<Long, String, Partition> restoredPartitions;

    private long metaRestoredTime;
    private long downloadFinishedTime;

    public RestoreJob_D() {
        super();
    }

    public RestoreJob_D(long jobId, long dbId, LabelName labelName, String restorePath,
                      Map<String, String> remoteProperties, Map<String, Set<String>> tableToPartitionNames,
                      Map<String, String> tableRenameMap) {
        super(jobId, dbId, labelName, restorePath, remoteProperties);
        state = RestoreJobState.PENDING;

        this.tableToPartitionNames = tableToPartitionNames;
        this.tableRenameMap = tableRenameMap;

        this.tableToCreateTableStmt = Maps.newHashMap();
        this.tableToRollupStmt = Maps.newHashMap();
        this.tableToPartitionStmts = HashBasedTable.create();

        this.tableToReplace = Maps.newHashMap();
        this.restoredTables = Maps.newHashMap();
        this.restoredPartitions = HashBasedTable.create();

        this.metaRestoredTime = -1L;
        this.downloadFinishedTime = -1L;
    }

    public void setState(RestoreJobState state) {
        this.state = state;
    }

    public RestoreJobState getState() {
        return state;
    }

    public long getMetaRestoredTime() {
        return metaRestoredTime;
    }

    public long getDownloadFinishedTime() {
        return downloadFinishedTime;
    }

    public Map<String, Set<String>> getTableToPartitionNames() {
        return tableToPartitionNames;
    }

    @Override
    public List<Comparable> getJobInfo() {
        List<Comparable> jobInfo = Lists.newArrayList();
        jobInfo.add(jobId);
        jobInfo.add(getLabel());
        jobInfo.add(state.name());
        jobInfo.add(TimeUtils.longToTimeString(createTime));
        jobInfo.add(TimeUtils.longToTimeString(metaRestoredTime));
        jobInfo.add(TimeUtils.longToTimeString(downloadFinishedTime));
        jobInfo.add(TimeUtils.longToTimeString(finishedTime));
        jobInfo.add(errMsg);
        jobInfo.add(remotePath);
        jobInfo.add(getLeftTasksNum());
        return jobInfo;
    }

    @Override
    public void runOnce() {
        LOG.debug("begin to run restore job: {}, state: {}", jobId, state.name());
        try {
            switch (state) {
                case PENDING:
                    downloadBackupMeta();
                    break;
                case RESTORE_META:
                    restoreMeta();
                    break;
                case DOWNLOAD:
                    download();
                    break;
                case DOWNLOADING:
                    waitDownload();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            errMsg = Strings.nullToEmpty(e.getMessage());
            LOG.warn("failed to restore: [" + errMsg + "], job[" + jobId + "]", e);
            state = RestoreJobState.CANCELLED;
        }

        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            end(Catalog.getInstance(), false);
        }
    }

    private void downloadBackupMeta() throws DdlException, IOException, AnalysisException, InterruptedException,
            ExecutionException {
        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(dbId);
        if (db == null) {
            throw new DdlException("Database[" + getDbName() + "] does not exist");
        }

        if (pathBuilder == null) {
            pathBuilder = PathBuilder.createPathBuilder(getLocalDirName());
        }

        if (commandBuilder == null) {
            String remotePropFilePath = pathBuilder.remoteProperties();
            commandBuilder = CommandBuilder.create(remotePropFilePath, remoteProperties);
        }

        if (future == null) {
            // 1. download manifest
            LOG.info("begin to submit download backup meta. job: {}", jobId);
            MetaDownloadTask task = new MetaDownloadTask(jobId, getDbName(), getLabel(), getLocalDirName(), remotePath,
                                                         pathBuilder, commandBuilder,
                                                         tableToPartitionNames, tableToCreateTableStmt,
                                                         tableToRollupStmt, tableToPartitionStmts, tableToReplace,
                                                         tableRenameMap);
            // future = Catalog.getInstance().getBackupHandler().getAsynchronousCmdExecutor().submit(task);
        } else {
            boolean finished = checkFuture("download backup meta");
            if (!finished) {
                return;
            }

            future = null;
            state = RestoreJobState.RESTORE_META;
        }
    }

    private void restoreMeta() throws DdlException {
        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(dbId);
        if (db == null) {
            throw new DdlException("Database[" + getDbName() + "] does not exist");
        }
        for (Map.Entry<String, CreateTableStmt> entry : tableToCreateTableStmt.entrySet()) {
            String newTableName = entry.getKey();
            CreateTableStmt createTableStmt = entry.getValue();
            Boolean replace = tableToReplace.get(newTableName);
            if (replace) {
                // 1. create table
                Table restoredTable = catalog.createTable(createTableStmt, true);
                restoredTables.put(newTableName, restoredTable);

                if (restoredTable.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable restoredOlapTable = (OlapTable) restoredTable;

                // 2. create rollup
                RollupHandler rollupHandler = catalog.getRollupHandler();
                AlterTableStmt rollupStmt = tableToRollupStmt.get(newTableName);
                if (rollupStmt != null) {
                    // check if new table name conflicts with rollup index name
                    for (AlterClause clause : rollupStmt.getOps()) {
                        Preconditions.checkState(clause instanceof AddRollupClause);
                        String rollupName = ((AddRollupClause) clause).getRollupName();
                        if (rollupName.equals(newTableName)) {
                            throw new DdlException("New table name[" + newTableName
                                    + "] conflicts with rollup index name");
                        }
                    }

                    rollupHandler.process(rollupStmt.getOps(), db, restoredOlapTable, true);
                }

                // 3. create partition
                Map<String, AlterTableStmt> partitionStmts = tableToPartitionStmts.row(newTableName);
                if (partitionStmts.isEmpty()) {
                    continue;
                }

                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) restoredOlapTable.getPartitionInfo();
                for (Map.Entry<String, AlterTableStmt> entry2 : partitionStmts.entrySet()) {
                    AlterTableStmt stmt = entry2.getValue();
                    AddPartitionClause clause = (AddPartitionClause) stmt.getOps().get(0);
                    Pair<Long, Partition> res = catalog.addPartition(db, newTableName, restoredOlapTable, clause, true);
                    Partition partition = res.second;
                    rangePartitionInfo.handleNewSinglePartitionDesc(clause.getSingeRangePartitionDesc(),
                                                                    partition.getId());
                    restoredOlapTable.addPartition(partition);
                }
            } else {
                Map<String, AlterTableStmt> partitionStmts = tableToPartitionStmts.row(newTableName);
                for (Map.Entry<String, AlterTableStmt> entry2 : partitionStmts.entrySet()) {
                    AlterTableStmt stmt = entry2.getValue();
                    Pair<Long, Partition> res = catalog.addPartition(db, newTableName, null,
                                                                     (AddPartitionClause) stmt.getOps().get(0), true);
                    long tableId = res.first;
                    Partition partition = res.second;
                    restoredPartitions.put(tableId, partition.getName(), partition);
                }
            }
        }

        metaRestoredTime = System.currentTimeMillis();
        state = RestoreJobState.DOWNLOAD;
        LOG.info("finished restore tables. job[{}]", jobId);
    }

    private void download() {
        for (Map.Entry<String, Table> entry : restoredTables.entrySet()) {
            String newTableName = entry.getKey();
            String tableName = tableRenameMap.get(newTableName);
            Table table = entry.getValue();
            if (table.getType() != TableType.OLAP) {
                continue;
            }

            AgentBatchTask batchTask = new AgentBatchTask();
            OlapTable olapTable = (OlapTable) table;
            long tableId = olapTable.getId();
            for (Partition partition : olapTable.getPartitions()) {
                String partitionName = partition.getName();
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    // single partition table
                    partitionName = tableName;
                }
                long partitionId = partition.getId();
                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                    long indexId = index.getId();
                    String indexName = olapTable.getIndexNameById(index.getId());
                    if (indexName.equals(newTableName)) {
                        // base index
                        indexName = tableName;
                    }

                    List<Long> orderedBackupedTabletIdList = getRestoredTabletInfo(tableName, partitionName, indexName);

                    int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                    List<Tablet> tablets = index.getTablets();
                    for (int i = 0; i < tablets.size(); i++) {
                        Tablet tablet = tablets.get(i);
                        Long backupedTabletId = orderedBackupedTabletIdList.get(i);
                        String remoteFilePath = PathBuilder.createPath(remotePath, getDbName(), tableName,
                                                                       partitionName, indexName,
                                                                       backupedTabletId.toString());
                        for (Replica replica : tablet.getReplicas()) {

                        }
                    } // end for tablets
                } // end for indices
            } // end for partitions

            synchronized (unfinishedTabletIds) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                    unfinishedTabletIds.put(task.getTabletId(), task.getBackendId());
                }
            }
            AgentTaskExecutor.submit(batchTask);

            LOG.info("finished send restore tasks for table: {}, job: {}", tableName, jobId);
        } // end for tables

        state = RestoreJobState.DOWNLOADING;
        LOG.info("finished send all restore tasks. job: {}", jobId);
    }

    private List<Long> getRestoredTabletInfo(String tableName, String partitionName, String indexName) {
        // pathBuilder.getRoot().print("\t");
        DirSaver indexDir = (DirSaver) pathBuilder.getRoot().getChild(getDbName()).getChild(tableName)
                .getChild(partitionName).getChild(indexName);
        Collection<String> tabletNames = indexDir.getChildrenName();
        Set<Long> orderedBackupedTabletIds = Sets.newTreeSet();
        for (String tabletName : tabletNames) {
            orderedBackupedTabletIds.add(Long.valueOf(tabletName));
        }

        List<Long> orderedBackupedTabletIdList = Lists.newArrayList(orderedBackupedTabletIds);
        return orderedBackupedTabletIdList;
    }

    private void waitDownload() throws DdlException {
        synchronized (unfinishedTabletIds) {
            if (!unfinishedTabletIds.isEmpty()) {
                LOG.debug("waiting for unfinished download task. size: {}", unfinishedTabletIds.size());
                return;
            }
        }

        downloadFinishedTime = System.currentTimeMillis();
        LOG.info("all tablets restore finished. job: {}", jobId);
        
        finishing(Catalog.getInstance(), false);

        state = RestoreJobState.FINISHED;
    }

    public void finishing(Catalog catalog, boolean isReplay) throws DdlException {
        Database db = catalog.getDb(dbId);
        if (db == null && !isReplay) {
            throw new DdlException("Database[{}] does not exist");
        }

        db.writeLock();
        try {
            // check again if table or partition already exist
            for (Map.Entry<String, Table> entry : restoredTables.entrySet()) {
                String tableName = entry.getKey();

                Table currentTable = db.getTable(tableName);
                if (currentTable != null) {
                    throw new DdlException("Table[" + tableName + "]' already exist. "
                            + "Drop table first or restore to another table");
                }
            }

            for (long tableId : restoredPartitions.rowKeySet()) {
                Table table = db.getTable(tableId);
                if (table == null || table.getType() != TableType.OLAP) {
                    throw new DdlException("Table[" + tableId + "]' does not exist.");
                }

                Map<String, Partition> partitions = restoredPartitions.row(tableId);
                OlapTable olapTable = (OlapTable) table;
                for (Map.Entry<String, Partition> entry : partitions.entrySet()) {
                    String partitionName = entry.getKey();
                    Partition currentPartition = olapTable.getPartition(partitionName);
                    if (currentPartition != null) {
                        throw new DdlException("Partition[" + partitionName + "]' already exist in table["
                                + tableId + "]. Drop partition first or restore to another table");
                    }
                }
            }

            // add tables
            for (Map.Entry<String, Table> entry : restoredTables.entrySet()) {
                String tableName = entry.getKey();
                Table restoredTable = entry.getValue();

                if (restoredTable.getType() == TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) restoredTable;
                    olapTable.setState(OlapTableState.NORMAL);
                    if (isReplay) {
                        // add inverted index
                        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                        long tableId = olapTable.getId();
                        for (Partition partition : olapTable.getPartitions()) {
                            long partitionId = partition.getId();
                            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                                long indexId = index.getId();
                                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                                for (Tablet tablet : index.getTablets()) {
                                    long tabletId = tablet.getId();
                                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId,
                                            schemaHash, TStorageMedium.HDD);
                                    invertedIndex.addTablet(tabletId, tabletMeta);
                                    for (Replica replica : tablet.getReplicas()) {
                                        invertedIndex.addReplica(tabletId, replica);
                                    }
                                }
                            }
                        }
                    }
                }
                db.createTable(restoredTable);
                LOG.info("finished add table: {}, job: {}, replay: {}", tableName, jobId, isReplay);
            }

            // add partitions
            for (long tableId : restoredPartitions.rowKeySet()) {
                Table table = db.getTable(tableId);
                String tableName = table.getName();
                Preconditions.checkState(table != null, tableName);
                Preconditions.checkState(table.getType() == TableType.OLAP, tableName);
                OlapTable olapTable = (OlapTable) table;

                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE);
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;

                Map<String, Partition> partitions = restoredPartitions.row(tableId);

                for (Map.Entry<String, Partition> entry : partitions.entrySet()) {
                    String partitionName = entry.getKey();
                    Partition partition = entry.getValue();
                    long partitionId = partition.getId();

                    // add restored partition
                    AlterTableStmt stmt = tableToPartitionStmts.get(tableName, partitionName);
                    AddPartitionClause clause = (AddPartitionClause) stmt.getOps().get(0);
                    rangePartitionInfo.handleNewSinglePartitionDesc(clause.getSingeRangePartitionDesc(), partitionId);
                    olapTable.addPartition(partition);

                    // add inverted index
                    if (isReplay) {
                        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                        for (MaterializedIndex index : partition.getMaterializedIndices()) {
                            long indexId = index.getId();
                            int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                for (Replica replica : tablet.getReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                }
                                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId,
                                        schemaHash, TStorageMedium.HDD);
                                invertedIndex.addTablet(tabletId, tabletMeta);
                            }
                        }
                    }

                    LOG.info("finished add partition: {}, table: {}, job: {}, replay: {}",
                             partitionName, tableName, jobId, isReplay);
                } // end for partitions

                olapTable.setState(OlapTableState.NORMAL);
            } // end for tables
        } finally {
            db.writeUnlock();
        }
    }

    public void handleFinishedRestore(long tabletId, long backendId) {
        synchronized (unfinishedTabletIds) {
            if (unfinishedTabletIds.remove(tabletId, backendId)) {
                LOG.debug("finished restore tablet[{}], backend[{}]", tabletId, backendId);
            }
        }
    }

    @Override
    public void end(Catalog catalog, boolean isReplay) {
        if (state == RestoreJobState.CANCELLED) {
            rollback(catalog);
        }

        // 2. set table state
        // restoreTableState(catalog);

        if (!isReplay) {
            // 3. remove agent tasks if left
            removeLeftTasks();

            // 4. remove local file
            String labelDir = pathBuilder.getRoot().getFullPath();
            Util.deleteDirectory(new File(labelDir));
            LOG.debug("delete local dir: {}", labelDir);

            // 5. remove unused tablet in tablet inverted index
            clearInvertedIndex();

            finishedTime = System.currentTimeMillis();
            // log
            Catalog.getInstance().getEditLog().logRestoreFinish(this);
        }

        // clear for saving memory
        clearJob();

        LOG.info("finished end job[{}]. state: {}, replay: {}", jobId, state.name(), isReplay);
    }

    private void clearInvertedIndex() {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        if (state == RestoreJobState.CANCELLED) {
            // clear restored table tablets
            for (Table restoredTable : restoredTables.values()) {
                if (restoredTable.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) restoredTable;
                for (Partition partition : olapTable.getPartitions()) {
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        for (Tablet tablet : index.getTablets()) {
                            invertedIndex.deleteTablet(tablet.getId());
                        }
                    }
                }
            }

            // partition
            for (Partition partition : restoredPartitions.values()) {
                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    @Override
    protected void clearJob() {
        tableRenameMap = null;
        
        tableToCreateTableStmt = null;
        tableToRollupStmt = null;
        tableToPartitionStmts = null;

        tableToReplace = null;
        restoredTables = null;
        restoredPartitions = null;

        unfinishedTabletIds = null;
        remoteProperties = null;
        pathBuilder = null;
        commandBuilder = null;
        LOG.info("job[{}] cleared for saving memory", jobId);
    }

    private void rollback(Catalog catalog) {
        Database db = catalog.getDb(dbId);
        if (db == null) {
            errMsg = "Database does not exist[" + getDbName() + "]";
            LOG.info("{}. finished restore old meta. job: {}", errMsg, jobId);
            return;
        }

        db.writeLock();
        try {
            // tables
            for (Table restoredTable : restoredTables.values()) {
                String tableName = restoredTable.getName();
                // use table id rather than table name.
                // because table with same name may be created when doing restore.
                // find table by name may get unexpected one.
                Table currentTable = db.getTable(restoredTable.getId());
                // drop restored table
                if (currentTable != null) {
                    db.dropTable(tableName);
                    LOG.info("drop restored table[{}] in db[{}]", tableName, dbId);
                }
            }

            // partitions
            for (long tableId : restoredPartitions.rowKeySet()) {
                OlapTable currentTable = (OlapTable) db.getTable(tableId);
                if (currentTable == null) {
                    // table may be dropped during FINISHING phase
                    continue;
                }

                // drop restored partitions
                for (String partitionName : restoredPartitions.row(tableId).keySet()) {
                    Partition currentPartition = currentTable.getPartition(partitionName);
                    if (currentPartition != null) {
                        currentTable.dropPartition(dbId, partitionName, true);
                        LOG.info("drop restored partition[{}] in table[{}] in db[{}]",
                                 partitionName, tableId, dbId);
                    }

                    currentTable.setState(OlapTableState.NORMAL);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    private void removeLeftTasks() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, state.name());

        if (tableToPartitionNames == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableToPartitionNames.size();
            out.writeInt(size);
            for (Map.Entry<String, Set<String>> entry : tableToPartitionNames.entrySet()) {
                Text.writeString(out, entry.getKey());
                Set<String> partitionNames = entry.getValue();
                size = partitionNames.size();
                out.writeInt(size);
                for (String partitionName : partitionNames) {
                    Text.writeString(out, partitionName);
                }
            }
        }

        if (tableRenameMap == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableRenameMap.size();
            out.writeInt(size);
            for (Map.Entry<String, String> entry : tableRenameMap.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }

        if (tableToCreateTableStmt == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableToCreateTableStmt.size();
            out.writeInt(size);
            for (Map.Entry<String, CreateTableStmt> entry : tableToCreateTableStmt.entrySet()) {
                throw new RuntimeException("Don't support CreateTableStmt serialization anymore");
            }
        }

        if (tableToRollupStmt == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableToRollupStmt.size();
            out.writeInt(size);
            for (Map.Entry<String, AlterTableStmt> entry : tableToRollupStmt.entrySet()) {
                Text.writeString(out, entry.getKey());
                entry.getValue().write(out);
            }
        }

        if (tableToPartitionStmts == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableToPartitionStmts.rowKeySet().size();
            out.writeInt(size);
            for (String tableName : tableToPartitionStmts.rowKeySet()) {
                Text.writeString(out, tableName);
                Map<String, AlterTableStmt> row = tableToPartitionStmts.row(tableName);
                size = row.size();
                out.writeInt(size);
                for (Map.Entry<String, AlterTableStmt> entry : row.entrySet()) {
                    Text.writeString(out, entry.getKey());
                    entry.getValue().write(out);
                }
            }
        }

        if (tableToReplace == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = tableToReplace.size();
            out.writeInt(size);
            for (Map.Entry<String, Boolean> entry : tableToReplace.entrySet()) {
                Text.writeString(out, entry.getKey());
                out.writeBoolean(entry.getValue());
            }
        }

        if (restoredTables == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = restoredTables.size();
            out.writeInt(size);
            for (Map.Entry<String, Table> entry : restoredTables.entrySet()) {
                Text.writeString(out, entry.getKey());
                entry.getValue().write(out);
            }
        }

        if (restoredPartitions == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = restoredPartitions.size();
            out.writeInt(size);
            for (long tableId : restoredPartitions.rowKeySet()) {
                out.writeLong(tableId);
                Map<String, Partition> row = restoredPartitions.row(tableId);
                size = row.size();
                out.writeInt(size);
                for (Map.Entry<String, Partition> entry : row.entrySet()) {
                    Text.writeString(out, entry.getKey());
                    entry.getValue().write(out);
                }
            }
        }

        out.writeLong(metaRestoredTime);
        out.writeLong(downloadFinishedTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        state = RestoreJobState.valueOf(Text.readString(in));

        if (in.readBoolean()) {
            tableToPartitionNames = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                int count = in.readInt();
                Set<String> partitionNames = Sets.newHashSet();
                for (int j = 0; j < count; j++) {
                    String partitionName = Text.readString(in);
                    partitionNames.add(partitionName);
                }
                tableToPartitionNames.put(tableName, partitionNames);
            }
        }

        if (in.readBoolean()) {
            tableRenameMap = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String newTableName = Text.readString(in);
                String tableName = Text.readString(in);
                tableRenameMap.put(newTableName, tableName);
            }
        }

        if (in.readBoolean()) {
            tableToCreateTableStmt = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                CreateTableStmt stmt = CreateTableStmt.read(in);
                tableToCreateTableStmt.put(tableName, stmt);
            }
        }

        if (in.readBoolean()) {
            tableToRollupStmt = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                AlterTableStmt stmt = new AlterTableStmt();
                stmt.readFields(in);
                tableToRollupStmt.put(tableName, stmt);
            }
        }

        if (in.readBoolean()) {
            tableToPartitionStmts = HashBasedTable.create();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                int count = in.readInt();
                for (int j = 0; j < count; j++) {
                    String partitionName = Text.readString(in);
                    AlterTableStmt stmt = new AlterTableStmt();
                    stmt.readFields(in);
                    tableToPartitionStmts.put(tableName, partitionName, stmt);
                }
            }
        }

        if (in.readBoolean()) {
            tableToReplace = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                Boolean replace = in.readBoolean();
                tableToReplace.put(tableName, replace);
            }
        }

        if (in.readBoolean()) {
            restoredTables = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String tableName = Text.readString(in);
                Table table = Table.read(in);
                restoredTables.put(tableName, table);
            }
        }

        if (in.readBoolean()) {
            restoredPartitions = HashBasedTable.create();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long tableId = in.readLong();
                int count = in.readInt();
                for (int j = 0; j < count; j++) {
                    String partitionName = Text.readString(in);
                    Partition partition = Partition.read(in);
                    restoredPartitions.put(tableId, partitionName, partition);
                }
            }
        }

        metaRestoredTime = in.readLong();
        downloadFinishedTime = in.readLong();
    }
}
