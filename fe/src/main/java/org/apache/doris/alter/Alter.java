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
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
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
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

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
            throws DdlException, AnalysisException {
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkClusterCapacity(stmt.getClusterName());
        // check db quota
        db.checkQuota();

        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Do not support alter non-OLAP table[" + tableName + "]");
            }
            OlapTable olapTable = (OlapTable) table;
            olapTable.checkStableAndNormal(db.getClusterName());

            ((MaterializedViewHandler)materializedViewHandler).processCreateMaterializedView(stmt, db,
                    olapTable);
        } finally {
            db.writeUnlock();
        }
    }

    public void processDropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        // check db
        String dbName = stmt.getTableName().getDb();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        db.writeLock();
        try {
            String tableName = stmt.getTableName().getTbl();
            Table table = db.getTable(tableName);
            // if table exists
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
            // check table type
            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Do not support non-OLAP table [" + tableName + "] when drop materialized view");
            }
            // check table state
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table[" + table.getName() + "]'s state is not NORMAL. "
                        + "Do not allow doing DROP ops");
            }

            // drop materialized view
            ((MaterializedViewHandler)materializedViewHandler).processDropMaterializedView(stmt, db, olapTable);

        } finally {
            db.writeUnlock();
        }
    }

    public void processAlterTable(AlterTableStmt stmt) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();
        final String clusterName = stmt.getClusterName();

        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);

        // check cluster capacity and db quota, only need to check once.
        if (currentAlterOps.needCheckCapacity()) {
            Catalog.getCurrentSystemInfo().checkClusterCapacity(clusterName);
            db.checkQuota();
        }

        // some operations will take long time to process, need to be done outside the databse lock
        boolean needProcessOutsideDatabaseLock = false;
        String tableName = dbTableName.getTbl();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Do not support alter non-OLAP table[" + tableName + "]");
            }
            OlapTable olapTable = (OlapTable) table;

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException(
                        "Table[" + table.getName() + "]'s state is not NORMAL. Do not allow doing ALTER ops");
            }

            if (currentAlterOps.hasSchemaChangeOp()) {
                // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
                schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (currentAlterOps.hasRollupOp()) {
                materializedViewHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (currentAlterOps.hasPartitionOp()) {
                Preconditions.checkState(alterClauses.size() == 1);
                AlterClause alterClause = alterClauses.get(0);
                if (alterClause instanceof DropPartitionClause) {
                    if (!((DropPartitionClause) alterClause).isTempPartition()) {
                        DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                    }
                    Catalog.getInstance().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                } else if (alterClause instanceof ReplacePartitionClause) {
                    Catalog.getCurrentCatalog().replaceTempPartition(db, tableName, (ReplacePartitionClause) alterClause);
                } else if (alterClause instanceof ModifyPartitionClause) {
                    ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                    Map<String, String> properties = clause.getProperties();
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                        needProcessOutsideDatabaseLock = true;
                    } else {
                        String partitionName = clause.getPartitionName();
                        Catalog.getInstance().modifyPartitionProperty(db, olapTable, partitionName, properties);
                    }
                } else if (alterClause instanceof AddPartitionClause) {
                    needProcessOutsideDatabaseLock = true;
                } else {
                    throw new DdlException("Invalid alter opertion: " + alterClause.getOpType());
                }
            } else if (currentAlterOps.hasRenameOp()) {
                processRename(db, olapTable, alterClauses);
            } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
                needProcessOutsideDatabaseLock = true;
            } else {
                throw new DdlException("Invalid alter operations: " + currentAlterOps);
            }
        } finally {
            db.writeUnlock();
        }

        // the following ops should done outside db lock. because it contain synchronized create operation
        if (needProcessOutsideDatabaseLock) {
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
                String partitionName = clause.getPartitionName();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                boolean isInMemory = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
                ((SchemaChangeHandler) schemaChangeHandler).updatePartitionInMemoryMeta(
                        db, tableName, partitionName, isInMemory);

                db.writeLock();
                try {
                    OlapTable olapTable = (OlapTable) db.getTable(tableName);
                    Catalog.getCurrentCatalog().modifyPartitionProperty(db, olapTable, partitionName, properties);
                } finally {
                    db.writeUnlock();
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                ((SchemaChangeHandler) schemaChangeHandler).updateTableInMemoryMeta(db, tableName, properties);
            } else {
                throw new DdlException("Invalid alter opertion: " + alterClause.getOpType());
            }
        }
    }

    public void processAlterView(AlterViewStmt stmt, ConnectContext ctx) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();

        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = dbTableName.getTbl();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.VIEW) {
                throw new DdlException("The specified table [" + tableName + "] is not a view");
            }

            View view = (View) table;
            modifyViewDef(db, view, stmt.getInlineViewDef(), ctx.getSessionVariable().getSqlMode(), stmt.getColumns());
        } finally {
            db.writeUnlock();
        }
    }

    private void modifyViewDef(Database db, View view, String inlineViewDef, long sqlMode, List<Column> newFullSchema) throws DdlException {
        String viewName = view.getName();

        view.setInlineViewDefWithSqlMode(inlineViewDef, sqlMode);
        try {
            view.init();
        } catch (UserException e) {
            throw new DdlException("failed to init view stmt", e);
        }
        view.setNewFullSchema(newFullSchema);

        db.dropTable(viewName);
        db.createTable(view);

        AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), inlineViewDef, newFullSchema, sqlMode);
        Catalog.getInstance().getEditLog().logModifyViewDef(alterViewInfo);
        LOG.info("modify view[{}] definition to {}", viewName, inlineViewDef);
    }

    public void replayModifyViewDef(AlterViewInfo alterViewInfo) throws DdlException {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();

        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        db.writeLock();
        try {
            View view = (View) db.getTable(tableId);
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
            db.writeUnlock();
        }
    }

    public void processAlterCluster(AlterSystemStmt stmt) throws UserException {
        clusterHandler.process(Arrays.asList(stmt.getAlterClause()), stmt.getClusterName(), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                Catalog.getInstance().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else if (alterClause instanceof RollupRenameClause) {
                Catalog.getInstance().renameRollup(db, table, (RollupRenameClause) alterClause);
                break;
            } else if (alterClause instanceof PartitionRenameClause) {
                Catalog.getInstance().renamePartition(db, table, (PartitionRenameClause) alterClause);
                break;
            } else if (alterClause instanceof ColumnRenameClause) {
                Catalog.getInstance().renameColumn(db, table, (ColumnRenameClause) alterClause);
                break;
            } else {
                Preconditions.checkState(false);
            }
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
