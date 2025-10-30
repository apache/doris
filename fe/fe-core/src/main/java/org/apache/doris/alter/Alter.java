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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.cloud.alter.CloudSchemaChangeHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.PropertyAnalyzer.RewriteProperty;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionLikeOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMultiPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFromIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyDistributionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyEngineOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTableCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenamePartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplaceTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.TableRenameOp;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.ModifyCommentOperationLog;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.ModifyTableEngineOperationLog;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Alter {
    private static final Logger LOG = LogManager.getLogger(Alter.class);

    private AlterHandler schemaChangeHandler;
    private AlterHandler materializedViewHandler;
    private SystemHandler systemHandler;

    public Alter() {
        schemaChangeHandler = Config.isCloudMode() ? new CloudSchemaChangeHandler() : new SchemaChangeHandler();
        materializedViewHandler = new MaterializedViewHandler();
        systemHandler = new SystemHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        systemHandler.start();
    }

    public void processCreateMaterializedView(CreateMaterializedViewCommand command)
            throws DdlException, AnalysisException, MetaNotFoundException {
        String tableName = command.getBaseIndexName();
        // check db
        String dbName = command.getDBName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        Env.getCurrentInternalCatalog().checkAvailableCapacity(db);

        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP);
        ((MaterializedViewHandler) materializedViewHandler).processCreateMaterializedView(command, db, olapTable);
    }

    public void processDropMaterializedView(DropMaterializedViewCommand command)
            throws DdlException, MetaNotFoundException {
        TableNameInfo tableName = command.getTableName();
        String dbName = tableName.getDb();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        String name = tableName.getTbl();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(name, TableType.OLAP);
        ((MaterializedViewHandler) materializedViewHandler).processDropMaterializedView(command, db, olapTable);
    }

    private boolean processAlterOlapTable(AlterTableCommand command, OlapTable olapTable,
            List<AlterOp> alterOps,
            Database db) throws UserException {
        if (olapTable.getDataSortInfo() != null
                && olapTable.getDataSortInfo().getSortType() == TSortType.ZORDER) {
            throw new UserException("z-order table can not support schema change!");
        }

        // check conflict alter ops first
        alterOps.addAll(command.getOps());
        return processAlterOlapTableInternal(alterOps, olapTable, db, command.toSql());
    }

    private boolean processAlterOlapTableInternal(List<AlterOp> alterOps, OlapTable olapTable,
                                                  Database db, String sql) throws UserException {
        if (olapTable.getDataSortInfo() != null
                && olapTable.getDataSortInfo().getSortType() == TSortType.ZORDER) {
            throw new UserException("z-order table can not support schema change!");
        }

        // check conflict alter ops first
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterOps);

        for (AlterOp alterOp : alterOps) {
            Map<String, String> properties = null;
            try {
                properties = alterOp.getProperties();
            } catch (Exception e) {
                continue;
            }

            if (properties != null && !properties.isEmpty()) {
                checkNoForceProperty(properties);
            }
        }

        if (olapTable instanceof MTMV) {
            currentAlterOps.checkMTMVAllow(alterOps);
        }

        // check cluster capacity and db quota, only need to check once.
        if (currentAlterOps.needCheckCapacity()) {
            Env.getCurrentInternalCatalog().checkAvailableCapacity(db);
        }

        olapTable.checkNormalStateForAlter();
        boolean needProcessOutsideTableLock = false;
        BaseTableInfo oldBaseTableInfo = new BaseTableInfo(olapTable);
        Optional<BaseTableInfo> newBaseTableInfo = Optional.empty();
        if (currentAlterOps.checkTableStoragePolicy(alterOps)) {
            String tableStoragePolicy = olapTable.getStoragePolicy();
            String currentStoragePolicy = currentAlterOps.getTableStoragePolicy(alterOps);

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
            boolean enableUniqueKeyMergeOnWrite;
            olapTable.readLock();
            try {
                enableUniqueKeyMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
            } finally {
                olapTable.readUnlock();
            }
            // must check here whether you can set the policy, otherwise there will be inconsistent metadata
            if (enableUniqueKeyMergeOnWrite && !Strings.isNullOrEmpty(currentStoragePolicy)) {
                throw new UserException(
                    "Can not set UNIQUE KEY table that enables Merge-On-write"
                        + " with storage policy(" + currentStoragePolicy + ")");
            }
            olapTable.setStoragePolicy(currentStoragePolicy);
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.checkIsBeingSynced(alterOps)) {
            olapTable.setIsBeingSynced(currentAlterOps.isBeingSynced(alterOps));
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.checkMinLoadReplicaNum(alterOps)) {
            Preconditions.checkState(alterOps.size() == 1);
            AlterOp alterOp = alterOps.get(0);
            processModifyMinLoadReplicaNum(db, olapTable, alterOp);
        } else if (currentAlterOps.checkBinlogConfigChange(alterOps)) {
            if (!Config.enable_feature_binlog) {
                throw new DdlException("Binlog feature is not enabled");
            }
            // TODO(Drogon): check error
            ((SchemaChangeHandler) schemaChangeHandler).updateBinlogConfig(db, olapTable, alterOps);
        } else if (currentAlterOps.hasSchemaChangeOp()) {
            // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
            schemaChangeHandler.process(sql, alterOps, db, olapTable);
        } else if (currentAlterOps.hasRollupOp()) {
            materializedViewHandler.process(alterOps, db, olapTable);
        } else if (currentAlterOps.hasPartitionOp()) {
            Preconditions.checkState(!alterOps.isEmpty());
            for (AlterOp alterOp : alterOps) {
                olapTable.writeLockOrDdlException();
                try {
                    if (alterOp instanceof DropPartitionOp) {
                        if (!((DropPartitionOp) alterOp).isTempPartition()) {
                            DynamicPartitionUtil.checkAlterAllowed(olapTable);
                        }
                        Env.getCurrentEnv().dropPartition(db, olapTable, ((DropPartitionOp) alterOp));
                    } else if (alterOp instanceof ReplacePartitionOp) {
                        Env.getCurrentEnv().replaceTempPartition(db, olapTable, (ReplacePartitionOp) alterOp);
                    } else if (alterOp instanceof ModifyPartitionOp) {
                        ModifyPartitionOp op = ((ModifyPartitionOp) alterOp);
                        // expand the partition names if it is 'Modify Partition(*)'
                        if (op.isNeedExpand()) {
                            List<String> partitionNames = op.getPartitionNames();
                            partitionNames.clear();
                            for (Partition partition : olapTable.getPartitions()) {
                                partitionNames.add(partition.getName());
                            }
                        }
                        Map<String, String> properties = op.getProperties();
                        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                            boolean isInMemory =
                                    Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
                            if (isInMemory) {
                                throw new UserException("Not support set 'in_memory'='true' now!");
                            }
                            needProcessOutsideTableLock = true;
                        } else {
                            List<String> partitionNames = op.getPartitionNames();
                            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)) {
                                modifyPartitionsProperty(db, olapTable, partitionNames, properties,
                                        op.isTempPartition());
                            } else {
                                needProcessOutsideTableLock = true;
                            }
                        }
                    } else if (alterOp instanceof DropPartitionFromIndexOp) {
                        // do nothing
                    } else if (alterOp instanceof AddPartitionOp
                            || alterOp instanceof AddPartitionLikeOp
                            || alterOp instanceof AlterMultiPartitionOp) {
                        needProcessOutsideTableLock = true;
                    } else {
                        throw new DdlException("Invalid alter operation: " + alterOp.getOpType());
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }
        } else if (currentAlterOps.hasRenameOp()) {
            processRename(db, olapTable, alterOps);
            newBaseTableInfo = Optional.of(new BaseTableInfo(olapTable));
        } else if (currentAlterOps.hasReplaceTableOp()) {
            processReplaceTable(db, olapTable, alterOps);
            // after replace table, olapTable may still be old name, so need set it to new name
            ReplaceTableOp op = (ReplaceTableOp) alterOps.get(0);
            String newTblName = op.getTblName();
            newBaseTableInfo = Optional.of(new BaseTableInfo(olapTable));
            newBaseTableInfo.get().setTableName(newTblName);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
            needProcessOutsideTableLock = true;
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_DISTRIBUTION)) {
            Preconditions.checkState(alterOps.size() == 1);
            AlterOp alterOp = alterOps.get(0);
            Env.getCurrentEnv()
                    .modifyDefaultDistributionBucketNum(db, olapTable, (ModifyDistributionOp) alterOp);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_COLUMN_COMMENT)) {
            processModifyColumnComment(db, olapTable, alterOps);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_COMMENT)) {
            Preconditions.checkState(alterOps.size() == 1);
            AlterOp alterOp = alterOps.get(0);
            processModifyTableComment(db, olapTable, alterOp);
        } else {
            throw new DdlException("Invalid alter operations: " + currentAlterOps);
        }
        if (needChangeMTMVState(alterOps)) {
            Env.getCurrentEnv().getMtmvService()
                .alterTable(oldBaseTableInfo, newBaseTableInfo, currentAlterOps.hasReplaceTableOp());
        }

        olapTable.writeLock();
        try {
            NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
            sqlCacheManager.invalidateAboutTable(olapTable);
        } finally {
            olapTable.writeUnlock();
        }
        return needProcessOutsideTableLock;
    }

    private void setExternalTableAutoAnalyzePolicy(ExternalTable table, List<AlterOp> alterOps) {
        Preconditions.checkState(alterOps.size() == 1);
        AlterOp alterOp = alterOps.get(0);
        Preconditions.checkState(alterOp instanceof ModifyTablePropertiesOp);
        Map<String, String> properties = alterOp.getProperties();
        Preconditions.checkState(properties.size() == 1);
        Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY));
        String value = properties.get(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY);
        Preconditions.checkState(PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value)
                || PropertyAnalyzer.DISABLE_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value)
                || PropertyAnalyzer.USE_CATALOG_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value));
        value = value.equalsIgnoreCase(PropertyAnalyzer.USE_CATALOG_AUTO_ANALYZE_POLICY) ? null : value;
        table.getCatalog().setAutoAnalyzePolicy(table.getDatabase().getFullName(), table.getName(), value);
        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(table.getCatalog().getName(),
                table.getDatabase().getFullName(), table.getName(), properties);
        Env.getCurrentEnv().getEditLog().logModifyTableProperties(info);
    }

    private void processAlterTableForExternalTable(
            ExternalTable table, List<AlterOp> alterOps) throws UserException {
        for (AlterOp alterOp : alterOps) {
            if (alterOp instanceof ModifyTablePropertiesOp) {
                setExternalTableAutoAnalyzePolicy(table, alterOps);
            } else if (alterOp instanceof CreateOrReplaceBranchOp) {
                table.getCatalog().createOrReplaceBranch(
                        table, ((CreateOrReplaceBranchOp) alterOp).getBranchInfo());
            } else if (alterOp instanceof CreateOrReplaceTagOp) {
                table.getCatalog().createOrReplaceTag(
                        table, ((CreateOrReplaceTagOp) alterOp).getTagInfo());
            } else if (alterOp instanceof DropBranchOp) {
                table.getCatalog().dropBranch(
                        table, ((DropBranchOp) alterOp).getDropBranchInfo());
            } else if (alterOp instanceof DropTagOp) {
                table.getCatalog().dropTag(
                        table, ((DropTagOp) alterOp).getDropTagInfo());
            } else if (alterOp instanceof TableRenameOp) {
                TableRenameOp tableRename = (TableRenameOp) alterOp;
                table.getCatalog().renameTable(
                        table.getDbName(), table.getName(), tableRename.getNewTableName());
            } else if (alterOp instanceof AddColumnOp) {
                AddColumnOp addColumn = (AddColumnOp) alterOp;
                table.getCatalog().addColumn(table, addColumn.getColumn(), addColumn.getColPos());
            } else if (alterOp instanceof AddColumnsOp) {
                AddColumnsOp addColumns = (AddColumnsOp) alterOp;
                table.getCatalog().addColumns(table, addColumns.getColumns());
            } else if (alterOp instanceof DropColumnOp) {
                DropColumnOp dropColumn = (DropColumnOp) alterOp;
                table.getCatalog().dropColumn(table, dropColumn.getColName());
            } else if (alterOp instanceof RenameColumnOp) {
                RenameColumnOp columnRename = (RenameColumnOp) alterOp;
                table.getCatalog().renameColumn(
                        table, columnRename.getColName(), columnRename.getNewColName());
            } else if (alterOp instanceof ModifyColumnOp) {
                ModifyColumnOp modifyColumn = (ModifyColumnOp) alterOp;
                table.getCatalog().modifyColumn(table, modifyColumn.getColumn(), modifyColumn.getColPos());
            } else if (alterOp instanceof ReorderColumnsOp) {
                ReorderColumnsOp reorderColumns = (ReorderColumnsOp) alterOp;
                table.getCatalog().reorderColumns(table, reorderColumns.getColumnsByPos());
            } else {
                throw new UserException("Invalid alter operations for external table: " + alterOps);
            }
        }
    }

    private boolean needChangeMTMVState(List<AlterOp> alterOps) {
        for (AlterOp alterOp : alterOps) {
            if (alterOp.needChangeMTMVState()) {
                return true;
            }
        }
        return false;
    }

    private void processModifyTableComment(Database db, OlapTable tbl, AlterOp alterOp)
            throws DdlException {
        tbl.writeLockOrDdlException();
        try {
            ModifyTableCommentOp clause = (ModifyTableCommentOp) alterOp;
            tbl.setComment(clause.getComment());
            // log
            ModifyCommentOperationLog op = ModifyCommentOperationLog
                    .forTable(db.getId(), tbl.getId(), clause.getComment());
            Env.getCurrentEnv().getEditLog().logModifyComment(op);
        } finally {
            tbl.writeUnlock();
        }
    }

    private void processModifyColumnComment(Database db, OlapTable tbl, List<AlterOp> alterOps)
            throws DdlException {
        tbl.writeLockOrDdlException();
        try {
            // check first
            Map<String, String> colToComment = Maps.newHashMap();
            for (AlterOp alterOp : alterOps) {
                Preconditions.checkState(alterOp instanceof ModifyColumnCommentOp);
                ModifyColumnCommentOp op = (ModifyColumnCommentOp) alterOp;
                String colName = op.getColName();
                if (tbl.getColumn(colName) == null) {
                    throw new DdlException("Unknown column: " + colName);
                }
                if (colToComment.containsKey(colName)) {
                    throw new DdlException("Duplicate column: " + colName);
                }
                colToComment.put(colName, op.getComment());
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

    private void processAlterExternalTable(AlterTableCommand command, Table externalTable, Database db)
            throws UserException {
        List<AlterOp> alterOps = new ArrayList<>(command.getOps());
        processAlterExternalTableInternal(alterOps, externalTable, db);
    }

    private void processAlterExternalTableInternal(List<AlterOp> alterOps, Table externalTable, Database db)
            throws UserException {
        // check conflict alter ops first
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterOps);
        if (currentAlterOps.hasRenameOp()) {
            processRename(db, externalTable, alterOps);
        } else if (currentAlterOps.hasSchemaChangeOp()) {
            schemaChangeHandler.processExternalTable(alterOps, db, externalTable);
        } else if (currentAlterOps.contains(AlterOpType.MODIFY_ENGINE)) {
            ModifyEngineOp modifyEngineOp = (ModifyEngineOp) alterOps.get(0);
            processModifyEngine(db, externalTable, modifyEngineOp);
        }
    }

    public void processModifyEngine(Database db, Table externalTable, ModifyEngineOp op) throws DdlException {
        externalTable.writeLockOrDdlException();
        try {
            if (externalTable.getType() != TableType.MYSQL) {
                throw new DdlException("Only support modify table engine from MySQL to ODBC");
            }
            processModifyEngineInternal(db, externalTable, op.getProperties(), false);
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
            db.unregisterTable(mysqlTable.getName());
            db.registerTable(odbcTable);
            if (!isReplay) {
                ModifyTableEngineOperationLog log = new ModifyTableEngineOperationLog(db.getId(),
                        externalTable.getId(), prop);
                Env.getCurrentEnv().getEditLog().logModifyTableEngine(log);
            }
        } finally {
            odbcTable.writeUnlock();
        }
    }

    public void processAlterTable(AlterTableCommand command) throws UserException {
        TableNameInfo dbTableName = command.getTbl();
        String ctlName = dbTableName.getCtl();
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();
        DatabaseIf dbIf = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(ctlName)
                .getDbOrDdlException(dbName);
        TableIf tableIf = dbIf.getTableOrDdlException(tableName);
        List<AlterOp> alterOps = Lists.newArrayList();
        // some operations will take long time to process, need to be done outside the table lock
        boolean needProcessOutsideTableLock = false;
        switch (tableIf.getType()) {
            case MATERIALIZED_VIEW:
            case OLAP:
                OlapTable olapTable = (OlapTable) tableIf;
                needProcessOutsideTableLock = processAlterOlapTable(command, olapTable, alterOps, (Database) dbIf);
                break;
            case ODBC:
            case JDBC:
            case HIVE:
            case MYSQL:
            case ELASTICSEARCH:
                processAlterExternalTable(command, (Table) tableIf, (Database) dbIf);
                return;
            case HMS_EXTERNAL_TABLE:
            case JDBC_EXTERNAL_TABLE:
            case ICEBERG_EXTERNAL_TABLE:
            case PAIMON_EXTERNAL_TABLE:
            case MAX_COMPUTE_EXTERNAL_TABLE:
            case HUDI_EXTERNAL_TABLE:
            case TRINO_CONNECTOR_EXTERNAL_TABLE:
                alterOps.addAll(command.getOps());
                processAlterTableForExternalTable((ExternalTable) tableIf, alterOps);
                return;
            default:
                throw new DdlException("Do not support alter "
                        + tableIf.getType().toString() + " table[" + tableName + "]");
        }

        Database db = (Database) dbIf;
        // the following ops should done outside table lock. because it contain synchronized create operation
        if (needProcessOutsideTableLock) {
            Preconditions.checkState(alterOps.size() == 1);
            AlterOp alterOp = alterOps.get(0);
            if (alterOp instanceof AddPartitionOp) {
                if (!((AddPartitionOp) alterOp).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed(
                            (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP));
                }
                Env.getCurrentEnv().addPartition(db, tableName, (AddPartitionOp) alterOp, false, 0, true);
            } else if (alterOp instanceof AddPartitionLikeOp) {
                if (!((AddPartitionLikeOp) alterOp).getTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed(
                            (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP));
                }
                Env.getCurrentEnv().addPartitionLike(db, tableName, (AddPartitionLikeOp) alterOp);
            } else if (alterOp instanceof ModifyPartitionOp) {
                ModifyPartitionOp clause = ((ModifyPartitionOp) alterOp);
                Map<String, String> properties = clause.getProperties();
                List<String> partitionNames = clause.getPartitionNames();
                ((SchemaChangeHandler) schemaChangeHandler).updatePartitionsProperties(
                        db, tableName, partitionNames, properties);
                OlapTable olapTable = (OlapTable) tableIf;
                olapTable.writeLockOrDdlException();
                try {
                    modifyPartitionsProperty(db, olapTable, partitionNames, properties, clause.isTempPartition());
                } finally {
                    olapTable.writeUnlock();
                }
            } else if (alterOp instanceof ModifyTablePropertiesOp) {
                Map<String, String> properties = alterOp.getProperties();
                ((SchemaChangeHandler) schemaChangeHandler).updateTableProperties(db, tableName, properties);
            } else if (alterOp instanceof AlterMultiPartitionOp) {
                if (!((AlterMultiPartitionOp) alterOp).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed(
                             (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP));
                }
                Env.getCurrentEnv().addMultiPartitions(db, tableName, (AlterMultiPartitionOp) alterOp);
            } else {
                throw new DdlException("Invalid alter operation: " + alterOp.getOpType());
            }
        }
    }

    // entry of processing replace table
    private void processReplaceTable(Database db, OlapTable origTable, List<AlterOp> alterOps)
            throws UserException {
        ReplaceTableOp clause = (ReplaceTableOp) alterOps.get(0);
        String newTblName = clause.getTblName();
        Table newTable = db.getTableOrMetaException(newTblName);
        if (newTable.getType() == TableType.MATERIALIZED_VIEW) {
            throw new DdlException("replace table[" + newTblName + "] cannot be a materialized view");
        }
        boolean swapTable = clause.isSwapTable();
        boolean isForce = clause.isForce();
        processReplaceTable(db, origTable, newTblName, swapTable, isForce);
    }

    public void processReplaceTable(Database db, OlapTable origTable, String newTblName,
                                    boolean swapTable, boolean isForce)
            throws UserException {
        db.writeLockOrDdlException();
        try {
            List<TableType> tableTypes = Lists.newArrayList(TableType.OLAP, TableType.MATERIALIZED_VIEW);
            Table newTbl = db.getTableOrMetaException(newTblName, tableTypes);
            if (newTbl.isTemporary()) {
                throw new UserException("Do not support replace with temporary table");
            }
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
                replaceTableInternal(db, origTable, olapNewTbl, swapTable, false, isForce);
                // write edit log
                ReplaceTableOperationLog log = new ReplaceTableOperationLog(db.getId(),
                        origTable.getId(), oldTblName, olapNewTbl.getId(), newTblName, swapTable, isForce);
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
            replaceTableInternal(db, origTable, newTbl, log.isSwapTable(), true, log.isForce());
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
                                      boolean isReplay, boolean isForce)
            throws DdlException {
        String oldTblName = origTable.getName();
        String newTblName = newTbl.getName();
        // drop origin table and new table
        db.unregisterTable(oldTblName);
        db.unregisterTable(newTblName);
        // rename new table name to origin table name and add it to database
        newTbl.checkAndSetName(oldTblName, false);
        db.registerTable(newTbl);
        if (swapTable) {
            // rename origin table name to new table name and add it to database
            origTable.checkAndSetName(newTblName, false);
            db.registerTable(origTable);
        } else {

            // not swap, the origin table is not used anymore, need to drop all its tablets.
            // put original table to recycle bin.
            if (isForce) {
                Env.getCurrentEnv().onEraseOlapTable(origTable, isReplay);
            } else {
                Env.getCurrentRecycleBin().recycleTable(db.getId(), origTable, isReplay, isForce, 0);
            }
            Env.getCurrentEnv().getAnalysisManager().removeTableStats(origTable.getId());
            if (origTable instanceof MTMV) {
                Env.getCurrentEnv().getMtmvService().dropJob((MTMV) origTable, isReplay);
            }
        }
    }

    public void processAlterView(AlterViewCommand command, ConnectContext ctx) throws UserException {
        org.apache.doris.nereids.trees.plans.commands.info.AlterViewInfo alterViewInfo = command.getAlterViewInfo();
        TableNameInfo tableNameInfo = alterViewInfo.getViewName();
        String dbName = tableNameInfo.getDb();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        String tableName = tableNameInfo.getTbl();
        View view = (View) db.getTableOrMetaException(tableName, TableType.VIEW);
        modifyViewDef(db, view, alterViewInfo.getInlineViewDef(), ctx.getSessionVariable().getSqlMode(),
                alterViewInfo.getColumns(), alterViewInfo.getComment());
    }

    private void modifyViewDef(Database db, View view, String inlineViewDef, long sqlMode,
                               List<Column> newFullSchema, String comment) throws DdlException {
        db.writeLockOrDdlException();
        try {
            view.writeLockOrDdlException();
            try {
                if (comment != null) {
                    view.setComment(comment);
                }
                // when do alter view modify comment, inlineViewDef and newFullSchema will be empty.
                if (!Strings.isNullOrEmpty(inlineViewDef)) {
                    view.setInlineViewDefWithSqlMode(inlineViewDef, sqlMode);
                    view.setNewFullSchema(newFullSchema);
                }
                String viewName = view.getName();
                db.unregisterTable(viewName);
                db.registerTable(view);
                AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(),
                        inlineViewDef, newFullSchema, sqlMode, comment);
                Env.getCurrentEnv().getMtmvService().alterView(new BaseTableInfo(view));
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
        String comment = alterViewInfo.getComment();

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        View view = (View) db.getTableOrMetaException(tableId, TableType.VIEW);

        db.writeLock();
        view.writeLock();
        try {
            String viewName = view.getName();
            if (comment != null) {
                view.setComment(comment);
            } else {
                view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
                view.setNewFullSchema(newFullSchema);
            }

            // We do not need to init view here.
            // During the `init` phase, some `Alter-View` statements will access the remote file system,
            // but they should not access it during the metadata replay phase.

            db.unregisterTable(viewName);
            db.registerTable(view);
            Env.getCurrentEnv().getMtmvService().alterView(new BaseTableInfo(view));
            LOG.info("replay modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            view.writeUnlock();
            db.writeUnlock();
        }
    }

    public void processAlterSystem(AlterSystemCommand command) throws UserException {
        systemHandler.processForNereids(Collections.singletonList(command), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterOp> alterOps) throws DdlException {
        for (AlterOp alterOp : alterOps) {
            if (alterOp instanceof TableRenameOp) {
                Env.getCurrentEnv().renameTable(db, table, (TableRenameOp) alterOp);
                break;
            } else {
                if (alterOp instanceof RenameRollupOp) {
                    Env.getCurrentEnv().renameRollup(db, table, (RenameRollupOp) alterOp);
                    break;
                } else if (alterOp instanceof RenamePartitionOp) {
                    Env.getCurrentEnv().renamePartition(db, table, (RenamePartitionOp) alterOp);
                    break;
                } else if (alterOp instanceof RenameColumnOp) {
                    Env.getCurrentEnv().renameColumn(db, table, (RenameColumnOp) alterOp);
                    break;
                } else {
                    Preconditions.checkState(false);
                }
            }
        }
    }

    private void processRename(Database db, Table table, List<AlterOp> alterOps) throws DdlException {
        for (AlterOp alterOp : alterOps) {
            if (alterOp instanceof TableRenameOp) {
                Env.getCurrentEnv().renameTable(db, table, (TableRenameOp) alterOp);
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
        checkNoForceProperty(properties);
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
        Map<Long, Long> tableBeToReplicaNumMap = Maps.newHashMap();
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName, isTempPartition);
            Map<Long, Long> partitionBeToReplicaNumMap = getReplicaCountByBackend(partition);

            for (Map.Entry<Long, Long> entry : partitionBeToReplicaNumMap.entrySet()) {
                tableBeToReplicaNumMap.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }
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
            } else if (PropertyAnalyzer.hasStoragePolicy(properties)) {
                // only set "storage_policy" = "", means cancel storage policy
                // if current partition is already in remote storage
                if (partition.getRemoteDataSize() > 0) {
                    throw new AnalysisException(
                        "Cannot cancel storage policy for partition which is already on cold storage.");
                }

                // if current partition will be cooldown in 20s later
                StoragePolicy checkedPolicyCondition = StoragePolicy.ofCheck(dataProperty.getStoragePolicy());
                StoragePolicy policy = (StoragePolicy) Env.getCurrentEnv().getPolicyMgr()
                        .getPolicy(checkedPolicyCondition);
                if (policy != null) {
                    long latestTime = policy.getCooldownTimestampMs() > 0 ? policy.getCooldownTimestampMs()
                            : Long.MAX_VALUE;
                    if (policy.getCooldownTtl() > 0) {
                        latestTime = Math.min(latestTime,
                            partition.getVisibleVersionTime() + policy.getCooldownTtl() * 1000);
                    }
                    if (latestTime < System.currentTimeMillis() + 20 * 1000) {
                        throw new AnalysisException(
                            "Cannot cancel storage policy for partition which already be cooldown"
                                + " or will be cooldown soon later");
                    }
                }

                partitionInfo.setStoragePolicy(partition.getId(), "");
            }

            // 4.4 analyze new properties
            DataProperty newDataProperty = PropertyAnalyzer.analyzeDataProperty(modifiedProperties, dataProperty);

            // 1. date property
            if (newDataProperty != null) {
                partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            }
            // 2. replica allocation
            if (!replicaAlloc.isNotSet()) {
                if (Config.isNotCloudMode() && !olapTable.isColocateTable()) {
                    setReplicasToDrop(partition, partitionInfo.getReplicaAllocation(partition.getId()),
                            replicaAlloc, tableBeToReplicaNumMap);
                }
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

    public void setReplicasToDrop(Partition partition,
                                 ReplicaAllocation oldReplicaAlloc,
                                 ReplicaAllocation newReplicaAlloc,
                                 Map<Long, Long> tableBeToReplicaNumMap) {
        if (newReplicaAlloc.getAllocMap().entrySet().stream().noneMatch(
                entry -> entry.getValue() < oldReplicaAlloc.getReplicaNumByTag(entry.getKey()))) {
            return;
        }

        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        List<Long> aliveBes = systemInfoService.getAllBackendIds(true);

        processReplicasInPartition(partition,
                tableBeToReplicaNumMap, systemInfoService, oldReplicaAlloc, newReplicaAlloc, aliveBes);
    }

    private void processReplicasInPartition(Partition partition,
                                            Map<Long, Long> tableBeToReplicaNumMap, SystemInfoService systemInfoService,
                                            ReplicaAllocation oldReplicaAlloc, ReplicaAllocation newReplicaAlloc,
                                            List<Long> aliveBes) {
        List<Tag> changeTags = newReplicaAlloc.getAllocMap().entrySet().stream()
                .filter(entry -> entry.getValue() < oldReplicaAlloc.getReplicaNumByTag(entry.getKey()))
                .map(Map.Entry::getKey).collect(Collectors.toList());
        Map<Long, Long> partitionBeToReplicaNumMap = getReplicaCountByBackend(partition);
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                if (!isTabletHealthy(tablet, systemInfoService, partition, oldReplicaAlloc, aliveBes)) {
                    continue;
                }
                Map<Tag, List<Replica>> tagToReplicaMap = getReplicasWithTag(tablet);
                for (Tag tag : changeTags) {
                    List<Replica> toDealReplicas = tagToReplicaMap.get(tag);
                    if (toDealReplicas == null || toDealReplicas.isEmpty()) {
                        continue;
                    }
                    sortReplicasByBackendCount(toDealReplicas, tableBeToReplicaNumMap, partitionBeToReplicaNumMap);
                    int replicasToDrop = oldReplicaAlloc.getReplicaNumByTag(tag)
                            - newReplicaAlloc.getReplicaNumByTag(tag);
                    markReplicasForDropping(toDealReplicas, replicasToDrop,
                            tableBeToReplicaNumMap, partitionBeToReplicaNumMap);
                }
            }
        }
    }

    private boolean isTabletHealthy(Tablet tablet, SystemInfoService systemInfoService,
                                    Partition partition, ReplicaAllocation oldReplicaAlloc,
                                    List<Long> aliveBes) {
        return tablet.getHealth(systemInfoService, partition.getVisibleVersion(), oldReplicaAlloc, aliveBes)
                     .status == Tablet.TabletStatus.HEALTHY;
    }

    private Map<Tag, List<Replica>> getReplicasWithTag(Tablet tablet) {
        return tablet.getReplicas().stream()
                .collect(Collectors.groupingBy(replica -> Env.getCurrentSystemInfo()
                .getBackend(replica.getBackendIdWithoutException()).getLocationTag()));
    }

    private void sortReplicasByBackendCount(List<Replica> replicas,
                                            Map<Long, Long> tableBeToReplicaNumMap,
                                            Map<Long, Long> partitionBeToReplicaNumMap) {
        replicas.sort((Replica r1, Replica r2) -> {
            long countPartition1 = partitionBeToReplicaNumMap.getOrDefault(r1.getBackendIdWithoutException(), 0L);
            long countPartition2 = partitionBeToReplicaNumMap.getOrDefault(r2.getBackendIdWithoutException(), 0L);
            if (countPartition1 != countPartition2) {
                return Long.compare(countPartition2, countPartition1);
            }
            long countTable1 = tableBeToReplicaNumMap.getOrDefault(r1.getBackendIdWithoutException(), 0L);
            long countTable2 = tableBeToReplicaNumMap.getOrDefault(r2.getBackendIdWithoutException(), 0L);
            return Long.compare(countTable2, countTable1); // desc sort
        });
    }

    private void markReplicasForDropping(List<Replica> replicas, int replicasToDrop,
                                  Map<Long, Long> tableBeToReplicaNumMap,
                                  Map<Long, Long> partitionBeToReplicaNumMap) {
        for (int i = 0; i < replicas.size(); i++) {
            Replica r = replicas.get(i);
            long beId = r.getBackendIdWithoutException();
            if (i >= replicasToDrop) {
                r.setScaleInDropTimeStamp(-1); // Mark for not dropping
            } else {
                r.setScaleInDropTimeStamp(System.currentTimeMillis()); // Mark for dropping
                tableBeToReplicaNumMap.put(beId, tableBeToReplicaNumMap.get(beId) - 1);
                partitionBeToReplicaNumMap.put(beId, partitionBeToReplicaNumMap.get(beId) - 1);
            }
        }
    }

    public static Map<Long, Long> getReplicaCountByBackend(Partition partition) {
        return partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).stream()
                .flatMap(index -> index.getTablets().stream())
                .flatMap(tablet -> tablet.getBackendIds().stream())
                .collect(Collectors.groupingBy(id -> id, Collectors.counting()));
    }

    public void checkNoForceProperty(Map<String, String> properties) throws DdlException {
        for (RewriteProperty property : PropertyAnalyzer.getInstance().getForceProperties()) {
            if (properties.containsKey(property.key())) {
                throw new DdlException("Cann't modify property '" + property.key() + "'"
                    + (Config.isCloudMode() ? " in cloud mode" : "") + ".");
            }
        }
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

    private void processModifyMinLoadReplicaNum(Database db, OlapTable olapTable, AlterOp alterOp)
            throws DdlException {
        Map<String, String> properties = alterOp.getProperties();
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

    public Set<Long> getUnfinishedAlterTableIds() {
        Set<Long> unfinishedTableIds = Sets.newHashSet();
        for (AlterJobV2 job : schemaChangeHandler.getAlterJobsV2().values()) {
            if (!job.isDone()) {
                unfinishedTableIds.add(job.getTableId());
            }
        }
        for (IndexChangeJob job : ((SchemaChangeHandler) schemaChangeHandler).getIndexChangeJobs().values()) {
            if (!job.isDone()) {
                unfinishedTableIds.add(job.getTableId());
            }
        }
        for (AlterJobV2 job : materializedViewHandler.getAlterJobsV2().values()) {
            if (!job.isDone()) {
                unfinishedTableIds.add(job.getTableId());
            }
        }

        return unfinishedTableIds;
    }

    public AlterHandler getSchemaChangeHandler() {
        return schemaChangeHandler;
    }

    public AlterHandler getMaterializedViewHandler() {
        return materializedViewHandler;
    }

    public AlterHandler getSystemHandler() {
        return systemHandler;
    }

    public void processAlterMTMV(AlterMTMV alterMTMV, boolean isReplay) {
        TableNameInfo tbl = alterMTMV.getMvName();
        MTMV mtmv = null;
        boolean alterSuccess = true;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tbl.getDb());
            mtmv = (MTMV) db.getTableOrMetaException(tbl.getTbl(), TableType.MATERIALIZED_VIEW);
            switch (alterMTMV.getOpType()) {
                case ALTER_REFRESH_INFO:
                    mtmv.alterRefreshInfo(alterMTMV.getRefreshInfo());
                    break;
                case ALTER_STATUS:
                    mtmv.alterStatus(alterMTMV.getStatus());
                    break;
                case ALTER_PROPERTY:
                    mtmv.alterMvProperties(alterMTMV.getMvProperties());
                    break;
                case ADD_TASK:
                    alterSuccess = mtmv.addTaskResult(alterMTMV.getTask(), alterMTMV.getRelation(),
                            alterMTMV.getPartitionSnapshots(),
                            isReplay);
                    // If it is not a replay thread, it means that the current service is already a new version
                    // and does not require compatibility
                    if (isReplay) {
                        mtmv.compatible(Env.getCurrentEnv().getCatalogMgr());
                    }
                    break;
                default:
                    throw new RuntimeException("Unknown type value: " + alterMTMV.getOpType());
            }
            if (alterMTMV.isNeedRebuildJob()) {
                Env.getCurrentEnv().getMtmvService().alterJob(mtmv, isReplay);
            }
            // 4. log it and replay it in the follower
            if (!isReplay && alterSuccess) {
                Env.getCurrentEnv().getEditLog().logAlterMTMV(alterMTMV);
            }
        } catch (UserException e) {
            // if MTMV has been dropped, ignore this exception
            LOG.warn(e);
        }
    }
}
