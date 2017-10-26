// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.alter;

import com.baidu.palo.analysis.AddColumnClause;
import com.baidu.palo.analysis.AddColumnsClause;
import com.baidu.palo.analysis.AddPartitionClause;
import com.baidu.palo.analysis.AddRollupClause;
import com.baidu.palo.analysis.AlterClause;
import com.baidu.palo.analysis.AlterSystemStmt;
import com.baidu.palo.analysis.AlterTableStmt;
import com.baidu.palo.analysis.ColumnRenameClause;
import com.baidu.palo.analysis.DropColumnClause;
import com.baidu.palo.analysis.DropPartitionClause;
import com.baidu.palo.analysis.DropRollupClause;
import com.baidu.palo.analysis.ModifyColumnClause;
import com.baidu.palo.analysis.ModifyPartitionClause;
import com.baidu.palo.analysis.ModifyTablePropertiesClause;
import com.baidu.palo.analysis.PartitionRenameClause;
import com.baidu.palo.analysis.ReorderColumnsClause;
import com.baidu.palo.analysis.RollupRenameClause;
import com.baidu.palo.analysis.TableName;
import com.baidu.palo.analysis.TableRenameClause;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.load.Load;

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

    public void processAlterTable(AlterTableStmt stmt) throws DdlException {
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
        // rollup ops, if has, should appear one and only one entry
        boolean hasRollup = false;
        // partition ops, if has, should appear one and only one entry
        boolean hasPartition = false;
        // rename ops, if has, should appear one and only one entry
        boolean hasRename = false;

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        // check conflict alter ops first                 
        // if all alterclause is DropPartitionClause, no call checkQuota.
        boolean allDropPartitionClause = true;
        
        for (AlterClause alterClause : alterClauses) {
            if (!(alterClause instanceof DropPartitionClause)) {
                allDropPartitionClause = false;
                break;
            }
        }

        if (!allDropPartitionClause) {
            // check db quota
            db.checkQuota();
        }
        for (AlterClause alterClause : alterClauses) {
            if ((alterClause instanceof AddColumnClause
                    || alterClause instanceof AddColumnsClause
                    || alterClause instanceof DropColumnClause
                    || alterClause instanceof ModifyColumnClause
                    || alterClause instanceof ReorderColumnsClause
                    || alterClause instanceof ModifyTablePropertiesClause)
                    && !hasRollup && !hasPartition && !hasRename) {
                hasSchemaChange = true;
            } else if (alterClause instanceof AddRollupClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename) {
                hasRollup = true;
            } else if (alterClause instanceof DropRollupClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename) {
                hasRollup = true;
            } else if (alterClause instanceof AddPartitionClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename) {
                hasPartition = true;
            } else if (alterClause instanceof DropPartitionClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename) {
                hasPartition = true;
            } else if (alterClause instanceof ModifyPartitionClause && !hasSchemaChange && !hasRollup
                    && !hasPartition && !hasRename) {
                hasPartition = true;
            } else if ((alterClause instanceof TableRenameClause || alterClause instanceof RollupRenameClause
                    || alterClause instanceof PartitionRenameClause || alterClause instanceof ColumnRenameClause)
                    && !hasSchemaChange && !hasRollup && !hasPartition && !hasRename) {
                hasRename = true;
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
                throw new DdlException("Donot support alter non-OLAP table[" + tableName + "]");
            }

            OlapTable olapTable = (OlapTable) table;

            if (olapTable.getPartitions().size() == 0) {
                throw new DdlException("table with empty parition cannot do schema change. [" + tableName + "]");
            }

            if (olapTable.getState() == OlapTableState.SCHEMA_CHANGE
                    || olapTable.getState() == OlapTableState.BACKUP
                    || olapTable.getState() == OlapTableState.RESTORE) {
                throw new DdlException("Table[" + table.getName() + "]'s state[" + olapTable.getState()
                        + "] does not allow doing ALTER ops");
                // here we pass NORMAL and ROLLUP
                // NORMAL: ok to do any alter ops
                // ROLLUP: we allow user DROP a rollup index when it's under ROLLUP
            }
            
            if (!hasPartition) {
                // partition op include add/drop/modify partition. these ops do not required no loading jobs.
                // NOTICE: if adding other partition op, may change code path here.
                Load load = Catalog.getInstance().getLoadInstance();
                for (Partition partition : olapTable.getPartitions()) {
                    load.checkHashRunningDeleteJob(partition.getId(), partition.getName());
                }
            }

            if (hasSchemaChange) {
                schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasRollup) {
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
