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
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.DropColumnClause;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropRollupClause;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReorderColumnsClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class Alter {
    private static final Logger LOG = LogManager.getLogger(Alter.class);

    private AlterHandler schemaChangeHandler;
    private AlterHandler rollupHandler;
    private SystemHandler clusterHandler;

    public Alter() {
        schemaChangeHandler = new SchemaChangeHandler();
        rollupHandler = new RollupHandler();
        clusterHandler = new SystemHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        rollupHandler.start();
        clusterHandler.start();
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
        // rollup ops, if has, should appear one and only one add or drop rollup entry
        boolean hasAddRollup = false;
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

        for (AlterClause alterClause : alterClauses) {
            if ((alterClause instanceof AddColumnClause
                    || alterClause instanceof AddColumnsClause
                    || alterClause instanceof DropColumnClause
                    || alterClause instanceof ModifyColumnClause
                    || alterClause instanceof ReorderColumnsClause)
                    && !hasAddRollup && !hasDropRollup && !hasPartition && !hasRename) {
                hasSchemaChange = true;
            } else if (alterClause instanceof AddRollupClause && !hasSchemaChange && !hasAddRollup && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasAddRollup = true;
            } else if (alterClause instanceof DropRollupClause && !hasSchemaChange && !hasAddRollup && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasDropRollup = true;
            } else if (alterClause instanceof AddPartitionClause && !hasSchemaChange && !hasAddRollup && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if (alterClause instanceof DropPartitionClause && !hasSchemaChange && !hasAddRollup && !hasDropRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if (alterClause instanceof ModifyPartitionClause && !hasSchemaChange && !hasAddRollup
                    && !hasDropRollup && !hasPartition && !hasRename && !hasModifyProp) {
                hasPartition = true;
            } else if ((alterClause instanceof TableRenameClause || alterClause instanceof RollupRenameClause
                    || alterClause instanceof PartitionRenameClause || alterClause instanceof ColumnRenameClause)
                    && !hasSchemaChange && !hasAddRollup && !hasDropRollup && !hasPartition && !hasRename
                    && !hasModifyProp) {
                hasRename = true;
            } else if (alterClause instanceof ModifyTablePropertiesClause && !hasSchemaChange && !hasAddRollup
                    && !hasDropRollup && !hasPartition && !hasRename && !hasModifyProp) {
                hasModifyProp = true;
            } else {
                throw new DdlException("Conflicting alter clauses. see help for more information");
            }
        } // end for alter clauses

        boolean hasAddPartition = false;
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
            
            if (hasSchemaChange || hasModifyProp || hasAddRollup) {
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
                schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasAddRollup || hasDropRollup) {
                rollupHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasPartition) {
                Preconditions.checkState(alterClauses.size() == 1);
                AlterClause alterClause = alterClauses.get(0);
                if (alterClause instanceof DropPartitionClause) {
                    Catalog.getInstance().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                } else if (alterClause instanceof ModifyPartitionClause) {
                    Catalog.getInstance().modifyPartition(db, olapTable, ((ModifyPartitionClause) alterClause));
                } else {
                    hasAddPartition = true;
                }
            } else if (hasRename) {
                processRename(db, olapTable, alterClauses);
            }
        } finally {
            db.writeUnlock();
        }

        // add partition op should done outside db lock. cause it contain synchronized create operation
        if (hasAddPartition) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                Catalog.getInstance().addPartition(db, tableName, (AddPartitionClause) alterClause);
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    public void processAlterCluster(AlterSystemStmt stmt) throws DdlException {
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

    public AlterHandler getRollupHandler() {
        return this.rollupHandler;
    }

    public AlterHandler getClusterHandler() {
        return this.clusterHandler;
    }
}
