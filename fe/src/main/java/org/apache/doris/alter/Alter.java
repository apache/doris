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

import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DropColumnClause;
import org.apache.doris.analysis.DropIndexClause;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropRollupClause;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReorderColumnsClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
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
import java.util.Set;
import java.util.TreeSet;

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

    public void processCreateMaterializedView(CreateMaterializedViewStmt stmt) throws DdlException, AnalysisException {
        String tableName = stmt.getBaseIndexName();
        Database db = Catalog.getInstance().getDb(stmt.getDBName());
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

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table[" + table.getName() + "]'s state is not NORMAL. "
                                               + "Do not allow doing materialized view");
            }
            // check if all tablets are healthy, and no tablet is in tablet scheduler
            boolean isStable = olapTable.isStable(Catalog.getCurrentSystemInfo(),
                                                  Catalog.getCurrentCatalog().getTabletScheduler(),
                                                  db.getClusterName());
            if (!isStable) {
                throw new DdlException("table [" + olapTable.getName() + "] is not stable."
                                               + " Some tablets of this table may not be healthy or are being "
                                               + "scheduled."
                                               + " You need to repair the table first"
                                               + " or stop cluster balance. See 'help admin;'.");
            }

            ((MaterializedViewHandler)materializedViewHandler).processCreateMaterializedView(stmt, db, olapTable);
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

        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkClusterCapacity(clusterName);

        // schema change ops can appear several in one alter stmt without other alter ops entry
        boolean hasSchemaChange = false;
        // materialized view ops (include rollup), if has, should appear one and only one add or drop mv entry
        boolean hasAddMaterializedView = false;
        boolean hasDropRollup = false;
        // partition ops, if has, should appear one and only one entry
        boolean hasPartition = false;
        // rename ops, if has, should appear one and only one entry
        boolean hasRename = false;
        // modify properties ops, if has, should appear one and only one entry
        boolean hasModifyProp = false;

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        // check conflict alter ops first

        // if all alter clauses are DropPartitionClause or DropRollupClause, no need to check quota.
        boolean allIsDropOps = true;
        for (AlterClause alterClause : alterClauses) {
            if (!(alterClause instanceof DropPartitionClause)
                    && !(alterClause instanceof DropRollupClause)) {
                allIsDropOps = false;
                break;
            }
        }

        if (!allIsDropOps) {
            // check db quota
            db.checkQuota();
        }

        // synchronized operation must handle outside db write lock
        boolean needSynchronized = false;
        boolean needTableStable = false;
        for (AlterClause alterClause : alterClauses) {
            if (!needTableStable) {
                needTableStable = ((AlterTableClause) alterClause).isNeedTableStable();
            }
            if ((alterClause instanceof AddColumnClause
                    || alterClause instanceof AddColumnsClause
                    || alterClause instanceof DropColumnClause
                    || alterClause instanceof ModifyColumnClause
                    || alterClause instanceof ReorderColumnsClause
                    || alterClause instanceof CreateIndexClause
                    || alterClause instanceof DropIndexClause)
                    && !hasAddMaterializedView && !hasDropRollup && !hasPartition && !hasRename) {
                hasSchemaChange = true;
                if (alterClause instanceof CreateIndexClause) {
                    Table table = db.getTable(dbTableName.getTbl());
                    if (!(table instanceof OlapTable)) {
                        throw new AnalysisException("create index only support in olap table at current version.");
                    }
                    List<Index> indexes = ((OlapTable) table).getIndexes();
                    IndexDef indexDef = ((CreateIndexClause) alterClause).getIndexDef();
                    Set<String> newColset = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    newColset.addAll(indexDef.getColumns());
                    for (Index idx : indexes) {
                        if (idx.getIndexName().equalsIgnoreCase(indexDef.getIndexName())) {
                            throw new AnalysisException("index `" + indexDef.getIndexName() + "` already exist.");
                        }
                        Set<String> idxSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                        idxSet.addAll(idx.getColumns());
                        if (newColset.equals(idxSet)) {
                            throw new AnalysisException("index for columns (" + String
                                    .join(",", indexDef.getColumns()) + " ) already exist.");
                        }
                    }
                    OlapTable olapTable = (OlapTable) table;
                    for (String col : indexDef.getColumns()) {
                        Column column = olapTable.getColumn(col);
                        if (column != null) {
                            indexDef.checkColumn(column, olapTable.getKeysType());
                        } else {
                            throw new AnalysisException("BITMAP column does not exist in table. invalid column: "
                                    + col);
                        }
                    }
                } else if (alterClause instanceof DropIndexClause) {
                    Table table = db.getTable(dbTableName.getTbl());
                    if (!(table instanceof OlapTable)) {
                        throw new AnalysisException("drop index only support in olap table at current version.");
                    }
                    String indexName = ((DropIndexClause) alterClause).getIndexName();
                    List<Index> indexes = ((OlapTable) table).getIndexes();
                    Index found = null;
                    for (Index idx : indexes) {
                        if (idx.getIndexName().equalsIgnoreCase(indexName)) {
                            found = idx;
                            break;
                        }
                    }
                    if (found == null) {
                            throw new AnalysisException("index " + indexName + " does not exist");
                        }
                }
            } else if ((alterClause instanceof AddRollupClause)
                    && !hasSchemaChange && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasAddMaterializedView = true;
            } else if (alterClause instanceof DropRollupClause && !hasSchemaChange && !hasAddMaterializedView
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasDropRollup = true;
            } else if (alterClause instanceof AddPartitionClause && !hasSchemaChange && !hasAddMaterializedView && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if (alterClause instanceof DropPartitionClause && !hasSchemaChange && !hasAddMaterializedView && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if (alterClause instanceof ModifyPartitionClause && !hasSchemaChange && !hasAddMaterializedView
                    && !hasDropRollup && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if ((alterClause instanceof TableRenameClause || alterClause instanceof RollupRenameClause
                    || alterClause instanceof PartitionRenameClause || alterClause instanceof ColumnRenameClause)
                    && !hasSchemaChange && !hasAddMaterializedView && !hasDropRollup && !hasPartition && !hasRename
                    && !hasModifyProp) {
                hasRename = true;
            } else if (alterClause instanceof ModifyTablePropertiesClause && !hasSchemaChange && !hasAddMaterializedView
                    && !hasDropRollup && !hasPartition && !hasRename && !hasModifyProp) {
                Map<String, String> properties = alterClause.getProperties();
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                    needSynchronized = true;
                } else  {
                    hasModifyProp = true;
                }
            } else {
                throw new DdlException("Conflicting alter clauses. see help for more information");
            }
        } // end for alter clauses

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

            if (olapTable.getPartitions().size() == 0 && !hasPartition) {
                throw new DdlException("table with empty parition cannot do schema change. [" + tableName + "]");
            }

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table[" + table.getName() + "]'s state is not NORMAL. Do not allow doing ALTER ops");
            }

            // schema change job will wait until table become stable
            if (needTableStable && !hasSchemaChange && !hasAddMaterializedView) {
                // check if all tablets are healthy, and no tablet is in tablet scheduler
                boolean isStable = olapTable.isStable(Catalog.getCurrentSystemInfo(),
                        Catalog.getCurrentCatalog().getTabletScheduler(),
                        db.getClusterName());
                if (!isStable) {
                    throw new DdlException("table [" + olapTable.getName() + "] is not stable."
                            + " Some tablets of this table may not be healthy or are being scheduled."
                            + " You need to repair the table first"
                            + " or stop cluster balance. See 'help admin;'.");
                }
            }

            if (hasSchemaChange || hasModifyProp) {
                // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
                schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasAddMaterializedView || hasDropRollup) {
                materializedViewHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasPartition) {
                Preconditions.checkState(alterClauses.size() == 1);
                AlterClause alterClause = alterClauses.get(0);
                if (alterClause instanceof DropPartitionClause) {
                    DynamicPartitionUtil.checkAlterAllowed(olapTable);
                    Catalog.getInstance().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                } else if (alterClause instanceof ModifyPartitionClause) {
                    DynamicPartitionUtil.checkAlterAllowed(olapTable);
                    ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                    Map<String, String> properties = clause.getProperties();
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                        needSynchronized = true;
                    } else {
                        String partitionName = clause.getPartitionName();
                        Catalog.getInstance().modifyPartition(db, olapTable, partitionName, properties);
                    }
                } else {
                    needSynchronized = true;
                }
            } else if (hasRename) {
                processRename(db, olapTable, alterClauses);
            }
        } finally {
            db.writeUnlock();
        }

        // the following ops should done outside db lock. because it contain synchronized create operation
        if (needSynchronized) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                Catalog.getInstance().addPartition(db, tableName, (AddPartitionClause) alterClause);
            } else if (alterClause instanceof ModifyPartitionClause) {
                ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                Map<String, String> properties = clause.getProperties();
                String partitionName = clause.getPartitionName();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                    boolean isInMemory = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
                    ((SchemaChangeHandler)schemaChangeHandler).updatePartitionInMemoryMeta(
                            db, tableName, partitionName, isInMemory);
                }

                db.writeLock();
                try {
                    OlapTable olapTable = (OlapTable)db.getTable(tableName);
                    Catalog.getInstance().modifyPartition(db, olapTable, partitionName, properties);
                } finally {
                    db.writeUnlock();
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                ((SchemaChangeHandler)schemaChangeHandler).updateTableInMemoryMeta(db, tableName, properties);
            } else {
                Preconditions.checkState(false);
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
