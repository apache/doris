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
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.ModifyDistributionClause;
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.ReplaceTableClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkClusterCapacity(stmt.getClusterName());
        // check db quota
        db.checkQuota();

        OlapTable olapTable = (OlapTable) db.getTableOrThrowException(tableName, TableType.OLAP);
        ((MaterializedViewHandler)materializedViewHandler).processCreateMaterializedView(stmt, db,
                    olapTable);
    }

    public void processDropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        // check db
        String dbName = stmt.getTableName().getDb();
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = stmt.getTableName().getTbl();
        OlapTable olapTable = (OlapTable) db.getTableOrThrowException(tableName, TableType.OLAP);
        // drop materialized view
        ((MaterializedViewHandler)materializedViewHandler).processDropMaterializedView(stmt, db, olapTable);
    }

    private boolean processAlterOlapTable(AlterTableStmt stmt, OlapTable olapTable, List<AlterClause> alterClauses,
                                         final String clusterName, Database db) throws UserException {
        stmt.rewriteAlterClause(olapTable);

        // check conflict alter ops first
        alterClauses.addAll(stmt.getOps());
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);

        // check cluster capacity and db quota, only need to check once.
        if (currentAlterOps.needCheckCapacity()) {
            Catalog.getCurrentSystemInfo().checkClusterCapacity(clusterName);
            db.checkQuota();
        }

        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException(
                    "Table[" + olapTable.getName() + "]'s state is not NORMAL. Do not allow doing ALTER ops");
        }

        boolean needProcessOutsideTableLock = false;
        if (currentAlterOps.hasSchemaChangeOp()) {
            // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
            schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
        } else if (currentAlterOps.hasRollupOp()) {
            materializedViewHandler.process(alterClauses, clusterName, db, olapTable);
        } else if (currentAlterOps.hasPartitionOp()) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            olapTable.writeLock();
            try {
                if (alterClause instanceof DropPartitionClause) {
                    if (!((DropPartitionClause) alterClause).isTempPartition()) {
                        DynamicPartitionUtil.checkAlterAllowed(olapTable);
                    }
                    Catalog.getCurrentCatalog().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                } else if (alterClause instanceof ReplacePartitionClause) {
                    Catalog.getCurrentCatalog().replaceTempPartition(db, olapTable, (ReplacePartitionClause) alterClause);
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
                        needProcessOutsideTableLock = true;
                    } else {
                        List<String> partitionNames = clause.getPartitionNames();
                        modifyPartitionsProperty(db, olapTable, partitionNames, properties);
                    }
                } else if (alterClause instanceof AddPartitionClause) {
                    needProcessOutsideTableLock = true;
                } else {
                    throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
                }
            } finally {
                olapTable.writeUnlock();
            }
        } else if (currentAlterOps.hasRenameOp()) {
            processRename(db, olapTable, alterClauses);
        } else if (currentAlterOps.hasReplaceTableOp()) {
            processReplaceTable(db, olapTable, alterClauses);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_DISTRIBUTION)) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            Catalog.getCurrentCatalog().modifyDefaultDistributionBucketNum(db, olapTable, (ModifyDistributionClause) alterClause);
        } else {
            throw new DdlException("Invalid alter operations: " + currentAlterOps);
        }

        return needProcessOutsideTableLock;
    }

    private void processAlterExternalTable(AlterTableStmt stmt, Table externalTable, Database db) throws UserException {
        stmt.rewriteAlterClause(externalTable);

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);
        if (currentAlterOps.hasRenameOp()) {
            processRename(db, externalTable, alterClauses);
        } else if (currentAlterOps.hasSchemaChangeOp()) {
            schemaChangeHandler.processExternalTable(alterClauses, db, externalTable);
        }
    }

    public void processAlterTable(AlterTableStmt stmt) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();
        final String clusterName = stmt.getClusterName();

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = db.getTable(tableName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }
        List<AlterClause> alterClauses = Lists.newArrayList();
        // some operations will take long time to process, need to be done outside the table lock
        boolean needProcessOutsideTableLock = false;
        switch (table.getType()) {
            case OLAP:
                OlapTable olapTable = (OlapTable) table;
                needProcessOutsideTableLock = processAlterOlapTable(stmt, olapTable, alterClauses, clusterName, db);
                break;
            case ODBC:
            case MYSQL:
            case ELASTICSEARCH:
                processAlterExternalTable(stmt, table, db);
                return;
            default:
                throw new DdlException("Do not support alter " + table.getType().toString() + " table[" + tableName + "]");
        }

        // the following ops should done outside table lock. because it contain synchronized create operation
        if (needProcessOutsideTableLock) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                if (!((AddPartitionClause) alterClause).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                }
                Catalog.getCurrentCatalog().addPartition(db, tableName, (AddPartitionClause) alterClause);
            } else if (alterClause instanceof ModifyPartitionClause) {
                ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                Map<String, String> properties = clause.getProperties();
                List<String> partitionNames = clause.getPartitionNames();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                ((SchemaChangeHandler) schemaChangeHandler).updatePartitionsInMemoryMeta(
                        db, tableName, partitionNames, properties);
                OlapTable olapTable = (OlapTable) table;
                olapTable.writeLock();
                try {
                    modifyPartitionsProperty(db, olapTable, partitionNames, properties);
                } finally {
                    olapTable.writeUnlock();
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                ((SchemaChangeHandler) schemaChangeHandler).updateTableInMemoryMeta(db, tableName, properties);
            } else {
                throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
            }
        }
    }

    // entry of processing replace table
    private void processReplaceTable(Database db, OlapTable origTable, List<AlterClause> alterClauses) throws UserException {
        ReplaceTableClause clause = (ReplaceTableClause) alterClauses.get(0);
        String newTblName = clause.getTblName();
        boolean swapTable = clause.isSwapTable();
        Table newTbl = db.getTableOrThrowException(newTblName, TableType.OLAP);
        OlapTable olapNewTbl = (OlapTable) newTbl;
        db.writeLock();
        origTable.writeLock();
        try {
            String oldTblName = origTable.getName();
            // First, we need to check whether the table to be operated on can be renamed
            olapNewTbl.checkAndSetName(oldTblName, true);
            if (swapTable) {
                origTable.checkAndSetName(newTblName, true);
            }
            replaceTableInternal(db, origTable, olapNewTbl, swapTable, false);
            // write edit log
            ReplaceTableOperationLog log = new ReplaceTableOperationLog(db.getId(), origTable.getId(), olapNewTbl.getId(), swapTable);
            Catalog.getCurrentCatalog().getEditLog().logReplaceTable(log);
            LOG.info("finish replacing table {} with table {}, is swap: {}", oldTblName, newTblName, swapTable);
        } finally {
            origTable.writeUnlock();
            db.writeUnlock();
        }

    }

    public void replayReplaceTable(ReplaceTableOperationLog log) {
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        OlapTable origTable = (OlapTable) db.getTable(origTblId);
        OlapTable newTbl = (OlapTable) db.getTable(newTblId);

        try {
            replaceTableInternal(db, origTable, newTbl, log.isSwapTable(), true);
        } catch (DdlException e) {
            LOG.warn("should not happen", e);
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
            Catalog.getCurrentCatalog().onEraseOlapTable(origTable, isReplay);
        }
    }

    public void processAlterView(AlterViewStmt stmt, ConnectContext ctx) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = dbTableName.getTbl();
        View view = (View) db.getTableOrThrowException(tableName, TableType.VIEW);
        modifyViewDef(db, view, stmt.getInlineViewDef(), ctx.getSessionVariable().getSqlMode(), stmt.getColumns());
    }

    private void modifyViewDef(Database db, View view, String inlineViewDef, long sqlMode, List<Column> newFullSchema) throws DdlException {
        db.writeLock();
        view.writeLock();
        try {
            view.setInlineViewDefWithSqlMode(inlineViewDef, sqlMode);
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
            }
            view.setNewFullSchema(newFullSchema);
            String viewName = view.getName();
            db.dropTable(viewName);
            db.createTable(view);

            AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), inlineViewDef, newFullSchema, sqlMode);
            Catalog.getCurrentCatalog().getEditLog().logModifyViewDef(alterViewInfo);
            LOG.info("modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            view.writeUnlock();
            db.writeUnlock();
        }
    }

    public void replayModifyViewDef(AlterViewInfo alterViewInfo) throws DdlException {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        View view = (View) db.getTable(tableId);
        db.writeLock();
        view.writeLock();
        try {
            String viewName = view.getName();
            view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
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
        clusterHandler.process(Arrays.asList(stmt.getAlterClause()), stmt.getClusterName(), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                Catalog.getCurrentCatalog().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else {
                if (alterClause instanceof RollupRenameClause) {
                    Catalog.getCurrentCatalog().renameRollup(db, table, (RollupRenameClause) alterClause);
                    break;
                } else if (alterClause instanceof PartitionRenameClause) {
                    Catalog.getCurrentCatalog().renamePartition(db, table, (PartitionRenameClause) alterClause);
                    break;
                } else if (alterClause instanceof ColumnRenameClause) {
                    Catalog.getCurrentCatalog().renameColumn(db, table, (ColumnRenameClause) alterClause);
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
                Catalog.getCurrentCatalog().renameTable(db, table, (TableRenameClause) alterClause);
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
                                         Map<String, String> properties)
            throws DdlException, AnalysisException {
        Preconditions.checkArgument(olapTable.isWriteLockHeldByCurrentThread());
        List<ModifyPartitionInfo> modifyPartitionInfos = Lists.newArrayList();
        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
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
        // 1. data property
        DataProperty newDataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties, null);
        // 2. replication num
        short newReplicationNum =
                PropertyAnalyzer.analyzeReplicationNum(properties, (short) -1);
        // 3. in memory
        boolean newInMemory = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        // 4. tablet type
        TTabletType tTabletType =
                PropertyAnalyzer.analyzeTabletType(properties);

        // modify meta here
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            // 1. date property
            if (newDataProperty != null) {
                partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            }
            // 2. replication num
            if (newReplicationNum != (short) -1) {
                partitionInfo.setReplicationNum(partition.getId(), newReplicationNum);
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
                    newDataProperty, newReplicationNum, hasInMemory ? newInMemory : oldInMemory);
            modifyPartitionInfos.add(info);
        }

        // log here
        BatchModifyPartitionsInfo info = new BatchModifyPartitionsInfo(modifyPartitionInfos);
        Catalog.getCurrentCatalog().getEditLog().logBatchModifyPartition(info);
    }

    public void replayModifyPartition(ModifyPartitionInfo info) {
        Database db = Catalog.getCurrentCatalog().getDb(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
        if (olapTable == null) {
            LOG.warn("table {} does not eixst when replaying modify partition. db: {}", info.getTableId(), info.getDbId());
            return;
        }
        olapTable.writeLock();
        try {
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.getDataProperty() != null) {
                partitionInfo.setDataProperty(info.getPartitionId(), info.getDataProperty());
            }
            if (info.getReplicationNum() != (short) -1) {
                partitionInfo.setReplicationNum(info.getPartitionId(), info.getReplicationNum());
            }
            partitionInfo.setIsInMemory(info.getPartitionId(), info.isInMemory());
        } finally {
            olapTable.writeUnlock();
        }
    }

    public AlterHandler getSchemaChangeHandler() {
        return this.schemaChangeHandler;
    }

    public AlterHandler getMaterializedViewHandler() {
        return this.materializedViewHandler;
    }

    public AlterHandler getClusterHandler() {
        return this.clusterHandler;
    }
}
