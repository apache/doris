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

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaDownloadTask extends ResultfulTask {
    private static final Logger LOG = LogManager.getLogger(MetaDownloadTask.class);

    private final long jobId;
    private final String dbName;
    private final String label;

    private final String localDirName;
    private final String remotePath;

    private final CommandBuilder commandBuilder;
    private final PathBuilder pathBuilder;

    private final Map<String, Set<String>> tableToPartitionNames;
    private final Map<String, CreateTableStmt> tableToCreateTableStmt;
    private final Map<String, AlterTableStmt> tableToRollupStmt;
    private final com.google.common.collect.Table<String, String, AlterTableStmt> tableToPartitionStmts;
    private final Map<String, Boolean> tableToReplace;

    private final Map<String, String> tableRenameMap;

    public MetaDownloadTask(long jobId, String dbName, String label, String localDirName, String remotePath,
                            PathBuilder pathBuilder, CommandBuilder commandBuilder,
                            Map<String, Set<String>> tableToPartitionNames,
                            Map<String, CreateTableStmt> tableToCreateTableStmt,
                            Map<String, AlterTableStmt> tableToRollupStmt,
                            com.google.common.collect.Table<String, String, AlterTableStmt> tableToPartitionStmts,
                            Map<String, Boolean> tableToReplace, Map<String, String> tableRenameMap) {
        this.jobId = jobId;
        this.dbName = dbName;
        this.label = label;
        this.localDirName = localDirName;
        this.remotePath = remotePath;

        this.pathBuilder = pathBuilder;
        this.commandBuilder = commandBuilder;
        this.tableToPartitionNames = tableToPartitionNames;
        this.tableToCreateTableStmt = tableToCreateTableStmt;
        this.tableToRollupStmt = tableToRollupStmt;
        this.tableToPartitionStmts = tableToPartitionStmts;
        this.tableToReplace = tableToReplace;

        this.tableRenameMap = tableRenameMap;
    }

    @Override
    public String call() {
        try {
            String local = pathBuilder.getRoot().getFullPath() + PathBuilder.MANIFEST_NAME;
            String remote = PathBuilder.createPath(remotePath, PathBuilder.MANIFEST_NAME);
            downloadFile(local, remote);

            // read manifest
            DirSaver rootDir;

            rootDir = ObjectWriter.readManifest(local);

            LOG.info("get backup label: {}", rootDir.getName());

            // rename rootDir to restore label so that it map to the current dir
            rootDir.setName(localDirName);
            pathBuilder.setRoot(rootDir);

            // check if all restore obj include
            checkRestoreObjs();

        } catch (Exception e) {
            setErrMsg(e);
            LOG.warn("get exception", e);
            return errMsg;
        }

        return null;
    }

    private void checkRestoreObjs() throws UserException {
        try {
            Database db = Catalog.getInstance().getDb(dbName);
            if (db == null) {
                throw new UserException("Database[" + dbName + "] does not exist");
            }
            
            // 1.1 check restored objs exist
            for (Map.Entry<String, Set<String>> entry : tableToPartitionNames.entrySet()) {
                String newTableName = entry.getKey();
                String tableName = tableRenameMap.get(newTableName);
                checkContains(PathBuilder.createPath(dbName, tableName));
                
                Set<String> partitionNames = entry.getValue();
                if (!partitionNames.isEmpty()) {
                    for (String partitionName : partitionNames) {
                        checkContains(PathBuilder.createPath(dbName, tableName, partitionName));
                    }
                }
            }
            
            // 1.2 add all metas
            if (tableToPartitionNames.isEmpty()) {
                Preconditions.checkState(tableRenameMap.isEmpty());
                DirSaver dbDir = (DirSaver) pathBuilder.getRoot().getChild(dbName);
                if (dbDir == null) {
                    throw new UserException("Backup path does not contains database[" + dbName + "]");
                }
                Collection<FileSaverI> tableDirs = dbDir.getChildren();
                for (FileSaverI child : tableDirs) {
                    if (child instanceof DirSaver) {
                        Set<String> partitionNames = Sets.newHashSet();
                        tableToPartitionNames.put(child.getName(), partitionNames);
                    }
                    tableRenameMap.put(child.getName(), child.getName());
                }
            }
            
            // 2. download all ddl file
            String local = null;
            String remote = null;
            for (Map.Entry<String, Set<String>> entry : tableToPartitionNames.entrySet()) {
                // create table stmt
                String newTableName = entry.getKey();
                String tableName = tableRenameMap.get(newTableName);
                local = PathBuilder.createPath(pathBuilder.getRoot().getFullPath(), dbName, newTableName,
                                               PathBuilder.CREATE_TABLE_STMT_FILE);
                remote = PathBuilder.createPath(remotePath, dbName, tableName, PathBuilder.CREATE_TABLE_STMT_FILE);
                downloadFile(local, remote);
                CreateTableStmt createTableStmt = ObjectWriter.readCreateTableStmt(local);
                createTableStmt.setTableName(newTableName);
                tableToCreateTableStmt.put(newTableName, createTableStmt);
                
                // rollup stmt if exists
                String rollupStmtPath = PathBuilder.createPath(dbName, tableName, PathBuilder.ADD_ROLLUP_STMT_FILE);
                if (pathBuilder.getRoot().contains(rollupStmtPath)) {
                    local = PathBuilder.createPath(pathBuilder.getRoot().getFullPath(), dbName, newTableName,
                                                   PathBuilder.ADD_ROLLUP_STMT_FILE);
                    remote = PathBuilder.createPath(remotePath, dbName, tableName, PathBuilder.ADD_ROLLUP_STMT_FILE);
                    downloadFile(local, remote);
                    List<AlterTableStmt> rollupStmts = ObjectWriter.readAlterTableStmt(local);
                    Preconditions.checkState(rollupStmts.size() == 1);
                    AlterTableStmt rollupStmt = rollupStmts.get(0);
                    rollupStmt.setTableName(newTableName);
                    tableToRollupStmt.put(newTableName, rollupStmt);
                }
                
                // add partition stmt if exists
                Set<String> partitionNames = entry.getValue();
                if (!partitionNames.isEmpty()) {
                    List<AlterTableStmt> partitionStmts = Lists.newArrayList();
                    for (String partitionName : partitionNames) {
                        local = PathBuilder.createPath(pathBuilder.getRoot().getFullPath(), dbName, newTableName,
                                                       partitionName, PathBuilder.ADD_PARTITION_STMT_FILE);
                        remote = PathBuilder.createPath(remotePath, dbName, tableName, partitionName,
                                                        PathBuilder.ADD_PARTITION_STMT_FILE);
                        downloadFile(local, remote);
                        partitionStmts.addAll(ObjectWriter.readAlterTableStmt(local));
                    }
                    
                    for (AlterTableStmt partitionStmt : partitionStmts) {
                        Preconditions.checkState(partitionStmt.getOps().size() == 1);
                        AddPartitionClause clause = (AddPartitionClause) partitionStmt.getOps().get(0);
                        String partitionName = clause.getSingeRangePartitionDesc().getPartitionName();
                        partitionStmt.setTableName(newTableName);
                        tableToPartitionStmts.put(newTableName, partitionName, partitionStmt);
                    }
                } else {
                    // get all existed partitions
                    FileSaverI tableSaver = pathBuilder.getRoot().getChild(dbName).getChild(tableName);
                    if (!(tableSaver instanceof DirSaver)) {
                        throw new UserException("Table[" + tableName + "] dir does not exist");
                    }
                    
                    List<AlterTableStmt> partitionStmts = Lists.newArrayList();
                    DirSaver tableDir = (DirSaver) tableSaver;
                    Collection<FileSaverI> children = tableDir.getChildren();
                    for (FileSaverI child : children) {
                        if (!(child instanceof DirSaver)) {
                            continue;
                        }
                        
                        DirSaver partitionDir = (DirSaver) child;
                        if (partitionDir.hasChild(PathBuilder.ADD_PARTITION_STMT_FILE)) {
                            local = PathBuilder.createPath(pathBuilder.getRoot().getFullPath(), dbName, newTableName,
                                                           partitionDir.getName(), PathBuilder.ADD_PARTITION_STMT_FILE);
                            remote = PathBuilder.createPath(remotePath, dbName, tableName, partitionDir.getName(),
                                                            PathBuilder.ADD_PARTITION_STMT_FILE);
                            downloadFile(local, remote);
                            partitionStmts.addAll(ObjectWriter.readAlterTableStmt(local));
                        }
                    }
                    
                    for (AlterTableStmt partitionStmt : partitionStmts) {
                        Preconditions.checkState(partitionStmt.getOps().size() == 1);
                        AddPartitionClause clause = (AddPartitionClause) partitionStmt.getOps().get(0);
                        String partitionName = clause.getSingeRangePartitionDesc().getPartitionName();
                        partitionStmt.setTableName(newTableName);
                        tableToPartitionStmts.put(newTableName, partitionName, partitionStmt);
                    }
                }
            } // end for tableToPartitions
            
            // 3. check validation
            db.readLock();
            try {
                for (Map.Entry<String, Set<String>> entry : tableToPartitionNames.entrySet()) {
                    String newTableName = entry.getKey();
                    Set<String> partitionNames = entry.getValue();
                    Table table = db.getTable(newTableName);
                    if (table == null) {
                        tableToReplace.put(newTableName, true);
                        continue;
                    } else {
                        if (partitionNames.isEmpty()) {
                            throw new UserException("Table[" + newTableName + "]' already exist. "
                                    + "Drop table first or restore to another table");
                        }
                    }
                    
                    // table
                    CreateTableStmt stmt = tableToCreateTableStmt.get(newTableName);
                    if (table.getSignature(BackupVersion.VERSION_1) != stmt.getTableSignature()) {
                        throw new UserException("Table[" + newTableName + "]'s struct is not same");
                    }
                    
                    // partition
                    Preconditions.checkState(!partitionNames.isEmpty());
                    Preconditions.checkState(table.getType() == TableType.OLAP);
                    OlapTable olapTable = (OlapTable) table;
                    for (String partitionName : partitionNames) {
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition == null) {
                            // check range valid
                            checkRangeValid(olapTable, partitionName);
                        } else {
                            // do not allow overwrite a partition
                            throw new UserException("Partition[" + partitionName + "]' already exist in table["
                                    + newTableName + "]. Drop partition first or restore to another table");
                        }
                    }
                    tableToReplace.put(newTableName, false);
                }
            } finally {
                db.readUnlock();
            }
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    private void checkRangeValid(OlapTable olapTable, String partitionName) throws AnalysisException, DdlException {
        AlterTableStmt stmt = tableToPartitionStmts.get(olapTable.getName(), partitionName);
        RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        AddPartitionClause clause = (AddPartitionClause) stmt.getOps().get(0);
        SingleRangePartitionDesc desc = clause.getSingeRangePartitionDesc();
        desc.analyze(partitionInfo.getPartitionColumns().size(), null);
        partitionInfo.checkAndCreateRange(desc);
    }

    private void checkContains(String path) throws DdlException {
        if (!pathBuilder.getRoot().contains(path)) {
            throw new DdlException("path[" + path + "] is not backuped");
        }
    }

    private void downloadFile(String local, String remote) throws IOException {
        String downloadCmd = null;
        boolean succeed = false;
        String msg = null;
        for (int i = 0; i < MAX_RETRY_TIME; i++) {
            try {
                downloadCmd = commandBuilder.downloadCmd(label, local, remote);
            } catch (IOException e) {
                msg = e.getMessage();
                LOG.warn(msg + ". job[{}]. retry: {}", jobId, i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                    continue;
                } catch (InterruptedException e1) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }

            String[] envp = { "LC_ALL=" + Config.locale };
            CommandResult result = Util.executeCommand(downloadCmd, envp);
            if (result.getReturnCode() != 0) {
                msg = "failed to download file[" + result + "]. job[" + jobId + "]";
                LOG.warn("{}. job[{}]. retry: {}", msg, jobId, i);
                try {
                    Thread.sleep(MAX_RETRY_INTERVAL_MS);
                    continue;
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                    break;
                }
            }

            // check file exist
            File file = new File(local);
            if (!file.exists()) {
                msg = "can not find downloaded file: " + local;
                LOG.warn(msg);
                succeed = false;
                break;
            }

            succeed = true;
        }

        if (!succeed) {
            throw new IOException(msg);
        }

        LOG.info("finished download file: {}. job[{}]", local, jobId);
    }


}
