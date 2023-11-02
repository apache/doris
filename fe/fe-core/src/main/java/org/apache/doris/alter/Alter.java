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

package org.apache.doris.alter;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropPartitionFromIndexClause;
import org.apache.doris.analysis.ModifyColumnCommentClause;
import org.apache.doris.analysis.ModifyDistributionClause;
import org.apache.doris.analysis.ModifyEngineClause;
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTableCommentClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.ReplaceTableClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.ModifyCommentOperationLog;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.ModifyTableEngineOperationLog;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Alter {
    private static final Logger LOG = LogManager.getLogger(Alter.class);

    private AlterHandler schemaChangeHandler;
    private AlterHandler materializedViewHandler;
    private SystemHandler clusterHandler;

    public Alter() {
        schemaChangeHandler = new SchemaChangeHandler();
        materializedViewHandler = new MaterializedViewHandler();
        clusterHandler = new SystemHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        clusterHandler.start();
    }

    public void processCreateMaterializedView(CreateMaterializedViewStmt stmt)
            throws DdlException, AnalysisException, MetaNotFoundException {
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        // check cluster capacity
        Env.getCurrentSystemInfo().checkAvailableCapacity();
        // check db quota
        db.checkQuota();

        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP);
        ((MaterializedViewHandler) materializedViewHandler).processCreateMaterializedView(stmt, db, olapTable);
    }

    public void processDropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        TableName tableName = stmt.getTableName();
        // check db
        String dbName = tableName.getDb();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        String name = tableName.getTbl();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(name, TableType.OLAP);
        ((MaterializedViewHandler) materializedViewHandler).processDropMaterializedView(stmt, db, olapTable);
    }

    private boolean processAlterOlapTable(AlterTableStmt stmt, OlapTable olapTable, List<AlterClause> alterClauses,
            final String clusterName, Database db) throws UserException {
        if (olapTable.getDataSortInfo() != null
                && olapTable.getDataSortInfo().getSortType() == TSortType.ZORDER) {
            throw new UserException("z-order table can not support schema change!");
        }
        stmt.rewriteAlterClause(olapTable);

        // check conflict alter ops first
        alterClauses.addAll(stmt.getOps());
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);

        // check cluster capacity and db quota, only need to check once.
        if (currentAlterOps.needCheckCapacity()) {
            Env.getCurrentSystemInfo().checkAvailableCapacity();
            db.checkQuota();
        }

        olapTable.checkNormalStateForAlter();
        boolean needProcessOutsideTableLock = false;
        if (currentAlterOps.checkTableStoragePolicy(alterClauses)) {
            String tableStoragePolicy = olapTable.getStoragePolicy();
            String currentStoragePolicy = currentAlterOps.getTableStoragePolicy(alterClauses);

            // If the two policy has one same resource, then it's safe for the table to change policy
            // There would only be the cooldown ttl or cooldown time would be affected
            if (!Env.getCurrentEnv().getPolicyMgr()
                    .checkStoragePolicyIfSameResource(tableStoragePolicy, currentStoragePolicy)
                    && !tableStoragePolicy.isEmpty()) {
                for (Partition partition : olapTable.getAllPartitions()) {
                    if (Partition.PARTITION_INIT_VERSION < partition.getVisibleVersion()) {
                        throw new DdlException("Do not support alter table's storage policy , this table ["
                                + olapTable.getName() + "] has storage policy " + tableStoragePolicy
                                + ", the table need to be empty.");
                    }
                }
            }
            // check currentStoragePolicy resource exist.
            Env.getCurrentEnv().getPolicyMgr().checkStoragePolicyExist(currentStoragePolicy);

            olapTable.setStoragePolicy(currentStoragePolicy);
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.checkIsBeingSynced(alterClauses)) {
            olapTable.setIsBeingSynced(currentAlterOps.isBeingSynced(alterClauses));
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.checkMinLoadReplicaNum(alterClauses)) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            processModifyMinLoadReplicaNum(db, olapTable, alterClause);
        } else if (currentAlterOps.checkBinlogConfigChange(alterClauses)) {
            if (!Config.enable_feature_binlog) {
                throw new DdlException("Binlog feature is not enabled");
            }
            // TODO(Drogon): check error
            ((SchemaChangeHandler) schemaChangeHandler).updateBinlogConfig(db, olapTable, alterClauses);
        } else if (currentAlterOps.hasSchemaChangeOp()) {
            // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
            schemaChangeHandler.process(stmt.toSql(), alterClauses, clusterName, db, olapTable);
            // if base table schemaChanged, need change mtmv status
            Env.getCurrentEnv().getMtmvService().alterTable(olapTable);
        } else if (currentAlterOps.hasRollupOp()) {
            materializedViewHandler.process(alterClauses, clusterName, db, olapTable);
        } else if (currentAlterOps.hasPartitionOp()) {
            Preconditions.checkState(!alterClauses.isEmpty());
            for (AlterClause alterClause : alterClauses) {
                olapTable.writeLockOrDdlException();
                try {
                    if (alterClause instanceof DropPartitionClause) {
                        if (!((DropPartitionClause) alterClause).isTempPartition()) {
                            DynamicPartitionUtil.checkAlterAllowed(olapTable);
                        }
                        Env.getCurrentEnv().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                    } else if (alterClause instanceof ReplacePartitionClause) {
                        Env.getCurrentEnv().replaceTempPartition(db, olapTable, (ReplacePartitionClause) alterClause);
                    } else if (alterClause instanceof ModifyPartitionClause) {
                        ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                        // expand the partition names if it is 'Modify Partition(*)'
                        if (clause.isNeedExpand()) {
                            List<String> partitionNames = clause.getPartitionNames();
                            partitionNames.clear();
                            for (Partition partition : olapTable.getPartitions()) {
                                partitionNames.add(partition.getName());
                            }
                        }
                        Map<String, String> properties = clause.getProperties();
                        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                            boolean isInMemory =
                                    Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
                            if (isInMemory) {
                                throw new UserException("Not support set 'in_memory'='true' now!");
                            }
                            needProcessOutsideTableLock = true;
                        } else {
                            List<String> partitionNames = clause.getPartitionNames();
                            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)) {
                                modifyPartitionsProperty(db, olapTable, partitionNames, properties,
                                        clause.isTempPartition());
                            } else {
                                needProcessOutsideTableLock = true;
                            }
                        }
                    } else if (alterClause instanceof DropPartitionFromIndexClause) {
                        // do nothing
                    } else if (alterClause instanceof AddPartitionClause
                            || alterClause instanceof AddPartitionLikeClause) {
                        needProcessOutsideTableLock = true;
                    } else {
                        throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }
        } else if (currentAlterOps.hasRenameOp()) {
            processRename(db, olapTable, alterClauses);
            Env.getCurrentEnv().getMtmvService().alterTable(olapTable);
        } else if (currentAlterOps.hasReplaceTableOp()) {
            processReplaceTable(db, olapTable, alterClauses);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_DISTRIBUTION)) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            Env.getCurrentEnv()
                    .modifyDefaultDistributionBucketNum(db, olapTable, (ModifyDistributionClause) alterClause);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_COLUMN_COMMENT)) {
            processModifyColumnComment(db, olapTable, alterClauses);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_COMMENT)) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            processModifyTableComment(db, olapTable, alterClause);
        } else {
            throw new DdlException("Invalid alter operations: " + currentAlterOps);
        }

        return needProcessOutsideTableLock;
    }

    private void processModifyTableComment(Database db, OlapTable tbl, AlterClause alterClause)
            throws DdlException {
        tbl.writeLockOrDdlException();
        try {
            ModifyTableCommentClause clause = (ModifyTableCommentClause) alterClause;
            tbl.setComment(clause.getComment());
            // log
            ModifyCommentOperationLog op = ModifyCommentOperationLog
                    .forTable(db.getId(), tbl.getId(), clause.getComment());
            Env.getCurrentEnv().getEditLog().logModifyComment(op);
        } finally {
            tbl.writeUnlock();
        }
    }

    private void processModifyColumnComment(Database db, OlapTable tbl, List<AlterClause> alterClauses)
            throws DdlException {
        tbl.writeLockOrDdlException();
        try {
            // check first
            Map<String, String> colToComment = Maps.newHashMap();
            for (AlterClause alterClause : alterClauses) {
                Preconditions.checkState(alterClause instanceof ModifyColumnCommentClause);
                ModifyColumnCommentClause clause = (ModifyColumnCommentClause) alterClause;
                String colName = clause.getColName();
                if (tbl.getColumn(colName) == null) {
                    throw new DdlException("Unknown column: " + colName);
                }
                if (colToComment.containsKey(colName)) {
                    throw new DdlException("Duplicate column: " + colName);
                }
                colToComment.put(colName, clause.getComment());
            }

            // modify comment
            for (Map.Entry<String, String> entry : colToComment.entrySet()) {
                Column col = tbl.getColumn(entry.getKey());
                col.setComment(entry.getValue());
            }

            // log
            ModifyCommentOperationLog op = ModifyCommentOperationLog.forColumn(db.getId(), tbl.getId(), colToComment);
            Env.getCurrentEnv().getEditLog().logModifyComment(op);
        } finally {
            tbl.writeUnlock();
        }
    }

    public void replayModifyComment(ModifyCommentOperationLog operation) throws MetaNotFoundException {
        long dbId = operation.getDbId();
        long tblId = operation.getTblId();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        Table tbl = db.getTableOrMetaException(tblId);
        tbl.writeLock();
        try {
            ModifyCommentOperationLog.Type type = operation.getType();
            switch (type) {
                case TABLE:
                    tbl.setComment(operation.getTblComment());
                    break;
                case COLUMN:
                    for (Map.Entry<String, String> entry : operation.getColToComment().entrySet()) {
                        tbl.getColumn(entry.getKey()).setComment(entry.getValue());
                    }
                    break;
                default:
                    break;
            }
        } finally {
            tbl.writeUnlock();
        }
    }

    private void processAlterExternalTable(AlterTableStmt stmt, Table externalTable, Database db) throws UserException {
        stmt.checkExternalTableOperationAllow(externalTable);
        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);
        if (currentAlterOps.hasRenameOp()) {
            processRename(db, externalTable, alterClauses);
        } else if (currentAlterOps.hasSchemaChangeOp()) {
            schemaChangeHandler.processExternalTable(alterClauses, db, externalTable);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_ENGINE)) {
            ModifyEngineClause modifyEngineClause = (ModifyEngineClause) alterClauses.get(0);
            processModifyEngine(db, externalTable, modifyEngineClause);
        }
    }

    public void processModifyEngine(Database db, Table externalTable, ModifyEngineClause clause) throws DdlException {
        externalTable.writeLockOrDdlException();
        try {
            if (externalTable.getType() != TableType.MYSQL) {
                throw new DdlException("Only support modify table engine from MySQL to ODBC");
            }
            processModifyEngineInternal(db, externalTable, clause.getProperties(), false);
        } finally {
            externalTable.writeUnlock();
        }
        LOG.info("modify table {}'s engine from MySQL to ODBC", externalTable.getName());
    }

    public void replayProcessModifyEngine(ModifyTableEngineOperationLog log) {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(log.getDbId());
        if (db == null) {
            return;
        }
        MysqlTable mysqlTable = (MysqlTable) db.getTableNullable(log.getTableId());
        if (mysqlTable == null) {
            return;
        }
        mysqlTable.writeLock();
        try {
            processModifyEngineInternal(db, mysqlTable, log.getProperties(), true);
        } finally {
            mysqlTable.writeUnlock();
        }
    }

    private void processModifyEngineInternal(Database db, Table externalTable,
            Map<String, String> prop, boolean isReplay) {
        MysqlTable mysqlTable = (MysqlTable) externalTable;
        Map<String, String> newProp = Maps.newHashMap(prop);
        newProp.put(OdbcTable.ODBC_HOST, mysqlTable.getHost());
        newProp.put(OdbcTable.ODBC_PORT, mysqlTable.getPort());
        newProp.put(OdbcTable.ODBC_USER, mysqlTable.getUserName());
        newProp.put(OdbcTable.ODBC_PASSWORD, mysqlTable.getPasswd());
        newProp.put(OdbcTable.ODBC_DATABASE, mysqlTable.getMysqlDatabaseName());
        newProp.put(OdbcTable.ODBC_TABLE, mysqlTable.getMysqlTableName());
        newProp.put(OdbcTable.ODBC_TYPE, TOdbcTableType.MYSQL.name());

        // create a new odbc table with same id and name
        OdbcTable odbcTable = null;
        try {
            odbcTable = new OdbcTable(mysqlTable.getId(), mysqlTable.getName(), mysqlTable.getBaseSchema(), newProp);
        } catch (DdlException e) {
            LOG.warn("Should not happen", e);
            return;
        }
        odbcTable.writeLock();
        try {
            db.dropTable(mysqlTable.getName());
            db.createTable(odbcTable);
            if (!isReplay) {
                ModifyTableEngineOperationLog log = new ModifyTableEngineOperationLog(db.getId(),
                        externalTable.getId(), prop);
                Env.getCurrentEnv().getEditLog().logModifyTableEngine(log);
            }
        } finally {
            odbcTable.writeUnlock();
        }
    }

    public void processAlterTable(AlterTableStmt stmt) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();
        final String clusterName = stmt.getClusterName();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        Table table = db.getTableOrDdlException(tableName);
        List<AlterClause> alterClauses = Lists.newArrayList();
        // some operations will take long time to process, need to be done outside the table lock
        boolean needProcessOutsideTableLock = false;
        switch (table.getType()) {
            case MATERIALIZED_VIEW:
            case OLAP:
                OlapTable olapTable = (OlapTable) table;
                needProcessOutsideTableLock = processAlterOlapTable(stmt, olapTable, alterClauses, clusterName, db);
                break;
            case ODBC:
            case JDBC:
            case HIVE:
            case MYSQL:
            case ELASTICSEARCH:
                processAlterExternalTable(stmt, table, db);
                return;
            default:
                throw new DdlException("Do not support alter "
                        + table.getType().toString() + " table[" + tableName + "]");
        }

        // the following ops should done outside table lock. because it contain synchronized create operation
        if (needProcessOutsideTableLock) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                if (!((AddPartitionClause) alterClause).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed(
                            (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP));
                }
                Env.getCurrentEnv().addPartition(db, tableName, (AddPartitionClause) alterClause);
            } else if (alterClause instanceof AddPartitionLikeClause) {
                if (!((AddPartitionLikeClause) alterClause).getIsTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed(
                            (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP));
                }
                Env.getCurrentEnv().addPartitionLike(db, tableName, (AddPartitionLikeClause) alterClause);
            } else if (alterClause instanceof ModifyPartitionClause) {
                ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                Map<String, String> properties = clause.getProperties();
                List<String> partitionNames = clause.getPartitionNames();
                // currently, only in memory and storage policy property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY));
                ((SchemaChangeHandler) schemaChangeHandler).updatePartitionsProperties(
                        db, tableName, partitionNames, properties);
                OlapTable olapTable = (OlapTable) table;
                olapTable.writeLockOrDdlException();
                try {
                    modifyPartitionsProperty(db, olapTable, partitionNames, properties, clause.isTempPartition());
                } finally {
                    olapTable.writeUnlock();
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                // currently, only in memory and storage policy property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)
                        || properties
                        .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)
                        || properties
                        .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)
                        || properties
                        .containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION)
                        || properties
                        .containsKey(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD));
                ((SchemaChangeHandler) schemaChangeHandler).updateTableProperties(db, tableName, properties);
            } else {
                throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
            }
        }
    }

    // entry of processing replace table
    private void processReplaceTable(Database db, OlapTable origTable, List<AlterClause> alterClauses)
            throws UserException {
        ReplaceTableClause clause = (ReplaceTableClause) alterClauses.get(0);
        String newTblName = clause.getTblName();
        boolean swapTable = clause.isSwapTable();
        db.writeLockOrDdlException();
        try {
            List<TableType> tableTypes = Lists.newArrayList(TableType.OLAP, TableType.MATERIALIZED_VIEW);
            Table newTbl = db.getTableOrMetaException(newTblName, tableTypes);
            OlapTable olapNewTbl = (OlapTable) newTbl;
            List<Table> tableList = Lists.newArrayList(origTable, newTbl);
            tableList.sort((Comparator.comparing(Table::getId)));
            MetaLockUtils.writeLockTablesOrMetaException(tableList);
            try {
                String oldTblName = origTable.getName();
                // First, we need to check whether the table to be operated on can be renamed
                olapNewTbl.checkAndSetName(oldTblName, true);
                if (swapTable) {
                    origTable.checkAndSetName(newTblName, true);
                }
                replaceTableInternal(db, origTable, olapNewTbl, swapTable, false);
                // write edit log
                ReplaceTableOperationLog log = new ReplaceTableOperationLog(db.getId(),
                        origTable.getId(), olapNewTbl.getId(), swapTable);
                Env.getCurrentEnv().getEditLog().logReplaceTable(log);
                LOG.info("finish replacing table {} with table {}, is swap: {}", oldTblName, newTblName, swapTable);
            } finally {
                MetaLockUtils.writeUnlockTables(tableList);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayReplaceTable(ReplaceTableOperationLog log) throws MetaNotFoundException {
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        List<TableType> tableTypes = Lists.newArrayList(TableType.OLAP, TableType.MATERIALIZED_VIEW);
        OlapTable origTable = (OlapTable) db.getTableOrMetaException(origTblId, tableTypes);
        OlapTable newTbl = (OlapTable) db.getTableOrMetaException(newTblId, tableTypes);
        List<Table> tableList = Lists.newArrayList(origTable, newTbl);
        tableList.sort((Comparator.comparing(Table::getId)));
        MetaLockUtils.writeLockTablesOrMetaException(tableList);
        try {
            replaceTableInternal(db, origTable, newTbl, log.isSwapTable(), true);
        } catch (DdlException e) {
            LOG.warn("should not happen", e);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        LOG.info("finish replay replacing table {} with table {}, is swap: {}", origTblId, newTblId, log.isSwapTable());
    }

    /**
     * The replace table operation works as follow:
     * For example, REPLACE TABLE A WITH TABLE B.
     * <p>
     * 1. If "swapTable" is true, A will be renamed to B, and B will be renamed to A
     * 1.1 check if A can be renamed to B (checking name conflict, etc...)
     * 1.2 check if B can be renamed to A (checking name conflict, etc...)
     * 1.3 rename B to A, drop old A, and add new A to database.
     * 1.4 rename A to B, drop old B, and add new B to database.
     * <p>
     * 2. If "swapTable" is false, A will be dropped, and B will be renamed to A
     * 1.1 check if B can be renamed to A (checking name conflict, etc...)
     * 1.2 rename B to A, drop old A, and add new A to database.
     */
    private void replaceTableInternal(Database db, OlapTable origTable, OlapTable newTbl, boolean swapTable,
            boolean isReplay)
            throws DdlException {
        String oldTblName = origTable.getName();
        String newTblName = newTbl.getName();

        // drop origin table and new table
        db.dropTable(oldTblName);
        db.dropTable(newTblName);

        // rename new table name to origin table name and add it to database
        newTbl.checkAndSetName(oldTblName, false);
        db.createTable(newTbl);

        if (swapTable) {
            // rename origin table name to new table name and add it to database
            origTable.checkAndSetName(newTblName, false);
            db.createTable(origTable);
        } else {
            // not swap, the origin table is not used anymore, need to drop all its tablets.
            Env.getCurrentEnv().onEraseOlapTable(origTable, isReplay);
        }
    }

    public void processAlterView(AlterViewStmt stmt, ConnectContext ctx) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        String tableName = dbTableName.getTbl();
        View view = (View) db.getTableOrMetaException(tableName, TableType.VIEW);
        modifyViewDef(db, view, stmt.getInlineViewDef(), ctx.getSessionVariable().getSqlMode(), stmt.getColumns());
    }

    private void modifyViewDef(Database db, View view, String inlineViewDef, long sqlMode,
            List<Column> newFullSchema) throws DdlException {
        db.writeLockOrDdlException();
        try {
            view.writeLockOrDdlException();
            try {
                view.setInlineViewDefWithSqlMode(inlineViewDef, sqlMode);
                try {
                    view.init();
                } catch (UserException e) {
                    throw new DdlException("failed to init view stmt, reason=" + e.getMessage());
                }
                view.setNewFullSchema(newFullSchema);
                String viewName = view.getName();
                db.dropTable(viewName);
                db.createTable(view);

                AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(),
                        inlineViewDef, newFullSchema, sqlMode);
                Env.getCurrentEnv().getEditLog().logModifyViewDef(alterViewInfo);
                LOG.info("modify view[{}] definition to {}", viewName, inlineViewDef);
            } finally {
                view.writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayModifyViewDef(AlterViewInfo alterViewInfo) throws MetaNotFoundException, DdlException {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        View view = (View) db.getTableOrMetaException(tableId, TableType.VIEW);

        db.writeLock();
        view.writeLock();
        try {
            String viewName = view.getName();
            view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt, reason=" + e.getMessage());
            }
            view.setNewFullSchema(newFullSchema);

            db.dropTable(viewName);
            db.createTable(view);

            LOG.info("replay modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            view.writeUnlock();
            db.writeUnlock();
        }
    }

    public void processAlterCluster(AlterSystemStmt stmt) throws UserException {
        clusterHandler.process(Collections.singletonList(stmt.getAlterClause()), stmt.getClusterName(), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                Env.getCurrentEnv().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else {
                if (alterClause instanceof RollupRenameClause) {
                    Env.getCurrentEnv().renameRollup(db, table, (RollupRenameClause) alterClause);
                    break;
                } else if (alterClause instanceof PartitionRenameClause) {
                    Env.getCurrentEnv().renamePartition(db, table, (PartitionRenameClause) alterClause);
                    break;
                } else if (alterClause instanceof ColumnRenameClause) {
                    Env.getCurrentEnv().renameColumn(db, table, (ColumnRenameClause) alterClause);
                    break;
                } else {
                    Preconditions.checkState(false);
                }
            }
        }
    }

    private void processRename(Database db, Table table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                Env.getCurrentEnv().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    /**
     * Batch update partitions' properties
     * caller should hold the table lock
     */
    public void modifyPartitionsProperty(Database db,
            OlapTable olapTable,
            List<String> partitionNames,
            Map<String, String> properties,
            boolean isTempPartition)
            throws DdlException, AnalysisException {
        Preconditions.checkArgument(olapTable.isWriteLockHeldByCurrentThread());
        List<ModifyPartitionInfo> modifyPartitionInfos = Lists.newArrayList();
        olapTable.checkNormalStateForAlter();
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName, isTempPartition);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }
        }

        boolean hasInMemory = false;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            hasInMemory = true;
        }

        // get value from properties here
        // 1. replica allocation
        ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        if (!replicaAlloc.isNotSet()) {
            olapTable.checkChangeReplicaAllocation();
        }
        Env.getCurrentSystemInfo().checkReplicaAllocation(replicaAlloc);
        // 2. in memory
        boolean newInMemory = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        // 3. tablet type
        TTabletType tTabletType =
                PropertyAnalyzer.analyzeTabletType(properties);

        // modify meta here
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName, isTempPartition);
            // 4. data property
            // 4.1 get old data property from partition
            DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
            Map<String, String> modifiedProperties = Maps.newHashMap();
            modifiedProperties.putAll(properties);

            // 4.3 modify partition storage policy
            // can set multi times storage policy
            String currentStoragePolicy = PropertyAnalyzer.analyzeStoragePolicy(properties);
            if (!currentStoragePolicy.equals("")) {
                // check currentStoragePolicy resource exist.
                Env.getCurrentEnv().getPolicyMgr().checkStoragePolicyExist(currentStoragePolicy);
                partitionInfo.setStoragePolicy(partition.getId(), currentStoragePolicy);
            }

            // 4.4 analyze new properties
            DataProperty newDataProperty = PropertyAnalyzer.analyzeDataProperty(modifiedProperties, dataProperty);

            // 1. date property
            if (newDataProperty != null) {
                partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            }
            // 2. replica allocation
            if (!replicaAlloc.isNotSet()) {
                partitionInfo.setReplicaAllocation(partition.getId(), replicaAlloc);
            }
            // 3. in memory
            boolean oldInMemory = partitionInfo.getIsInMemory(partition.getId());
            if (hasInMemory && (newInMemory != oldInMemory)) {
                partitionInfo.setIsInMemory(partition.getId(), newInMemory);
            }
            // 4. tablet type
            if (tTabletType != partitionInfo.getTabletType(partition.getId())) {
                partitionInfo.setTabletType(partition.getId(), tTabletType);
            }
            ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(), partition.getId(),
                    newDataProperty, replicaAlloc, hasInMemory ? newInMemory : oldInMemory, currentStoragePolicy,
                    Maps.newHashMap());
            modifyPartitionInfos.add(info);
        }

        // log here
        BatchModifyPartitionsInfo info = new BatchModifyPartitionsInfo(modifyPartitionInfos);
        Env.getCurrentEnv().getEditLog().logBatchModifyPartition(info);
    }

    public void replayModifyPartition(ModifyPartitionInfo info) throws MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.getDataProperty() != null) {
                partitionInfo.setDataProperty(info.getPartitionId(), info.getDataProperty());
            }
            if (!info.getReplicaAlloc().isNotSet()) {
                partitionInfo.setReplicaAllocation(info.getPartitionId(), info.getReplicaAlloc());
            }
            Optional.ofNullable(info.getStoragePolicy()).filter(p -> !p.isEmpty())
                    .ifPresent(p -> partitionInfo.setStoragePolicy(info.getPartitionId(), p));
            partitionInfo.setIsInMemory(info.getPartitionId(), info.isInMemory());

            Map<String, String> tblProperties = info.getTblProperties();
            if (tblProperties != null && !tblProperties.isEmpty()) {
                olapTable.setReplicaAllocation(tblProperties);
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void processModifyMinLoadReplicaNum(Database db, OlapTable olapTable, AlterClause alterClause)
            throws DdlException {
        Map<String, String> properties = alterClause.getProperties();
        short minLoadReplicaNum = -1;
        try {
            minLoadReplicaNum = PropertyAnalyzer.analyzeMinLoadReplicaNum(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        ReplicaAllocation replicaAlloc = olapTable.getDefaultReplicaAllocation();
        if (minLoadReplicaNum > replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("Failed to check min load replica num [" + minLoadReplicaNum + "]  <= "
                    + "default replica num [" + replicaAlloc.getTotalReplicaNum() + "]");
        }
        if (olapTable.dynamicPartitionExists()) {
            replicaAlloc = olapTable.getTableProperty().getDynamicPartitionProperty().getReplicaAllocation();
            if (!replicaAlloc.isNotSet() && minLoadReplicaNum > replicaAlloc.getTotalReplicaNum()) {
                throw new DdlException("Failed to check min load replica num [" + minLoadReplicaNum + "]  <= "
                        + "dynamic partition replica num [" + replicaAlloc.getTotalReplicaNum() + "]");
            }
        }
        properties.put(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM, Short.toString(minLoadReplicaNum));
        olapTable.setMinLoadReplicaNum(minLoadReplicaNum);
        olapTable.writeLockOrDdlException();
        try {
            Env.getCurrentEnv().modifyTableProperties(db, olapTable, properties);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public AlterHandler getSchemaChangeHandler() {
        return schemaChangeHandler;
    }

    public AlterHandler getMaterializedViewHandler() {
        return materializedViewHandler;
    }

    public AlterHandler getClusterHandler() {
        return clusterHandler;
    }

    public void processAlterMTMV(AlterMTMV alterMTMV, boolean isReplay)
            throws UserException {
        TableNameInfo tbl = alterMTMV.getMvName();
        MTMV mtmv = null;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tbl.getDb());
            mtmv = (MTMV) db.getTableOrMetaException(tbl.getTbl(), TableType.MATERIALIZED_VIEW);

            mtmv.writeLock();
            if (alterMTMV.getRefreshInfo() != null) {
                mtmv.alterRefreshInfo(alterMTMV.getRefreshInfo());
            } else if (alterMTMV.getStatus() != null) {
                mtmv.alterStatus(alterMTMV.getStatus());
            } else if (alterMTMV.getMvProperties() != null) {
                mtmv.alterMvProperties(alterMTMV.getMvProperties());
            } else if (alterMTMV.getTaskResult() != null) {
                mtmv.alterTaskResult(alterMTMV.getTaskResult());
            }
            // 4. log it and replay it in the follower
            if (!isReplay) {
                Env.getCurrentEnv().getMtmvService().alterMTMV(mtmv, alterMTMV);
                Env.getCurrentEnv().getEditLog().logAlterMTMV(alterMTMV);
            }
        } finally {
            if (mtmv != null) {
                mtmv.writeUnlock();
            }
        }
    }
}
