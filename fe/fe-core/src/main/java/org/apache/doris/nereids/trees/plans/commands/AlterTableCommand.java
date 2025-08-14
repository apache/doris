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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyEngineOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AlterTableCommand
 */
public class AlterTableCommand extends Command implements ForwardWithSync {
    private TableNameInfo tbl;
    private List<AlterTableOp> ops;
    private List<AlterTableOp> originOps;

    public AlterTableCommand(TableNameInfo tbl, List<AlterTableOp> ops) {
        super(PlanType.ALTER_TABLE_COMMAND);
        this.tbl = tbl;
        this.ops = ops;
        this.originOps = ops;
    }

    public TableNameInfo getTbl() {
        return tbl;
    }

    /**
     * getOps
     */
    public List<AlterTableClause> getOps() {
        List<AlterTableClause> alterTableClauses = new ArrayList<>(ops.size());
        for (AlterTableOp op : ops) {
            AlterTableClause alter = op.translateToLegacyAlterClause();
            alter.setTableName(tbl.transferToTableName());
            alterTableClauses.add(alter);
        }
        return alterTableClauses;
    }

    /**
     * validate
     */
    private void validate(ConnectContext ctx) throws UserException {
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(ctx);
        InternalDatabaseUtil.checkDatabase(tbl.getDb(), ConnectContext.get());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tbl.getCtl(), tbl.getDb(), tbl.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tbl.getDb() + ": " + tbl.getTbl());
        }
        if (ops == null || ops.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        for (AlterTableOp op : ops) {
            op.setTableName(tbl);
            op.validate(ctx);
        }
        String ctlName = tbl.getCtl();
        String dbName = tbl.getDb();
        String tableName = tbl.getTbl();
        DatabaseIf dbIf = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(ctlName, catalog -> new DdlException("Unknown catalog " + catalog))
                .getDbOrDdlException(dbName);
        TableIf tableIf = dbIf.getTableOrDdlException(tableName);
        if (tableIf.isTemporary()) {
            throw new AnalysisException("Do not support alter temporary table[" + tableName + "]");
        }
        if (tableIf instanceof OlapTable) {
            rewriteAlterOpForOlapTable(ctx, (OlapTable) tableIf);
        } else {
            checkExternalTableOperationAllow(tableIf);
        }
    }

    private void rewriteAlterOpForOlapTable(ConnectContext ctx, OlapTable table) throws UserException {
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        for (AlterTableOp alterClause : ops) {
            if (alterClause instanceof EnableFeatureOp) {
                EnableFeatureOp.Features alterFeature = ((EnableFeatureOp) alterClause).getFeature();
                if (alterFeature == null || alterFeature == EnableFeatureOp.Features.UNKNOWN) {
                    throw new AnalysisException("unknown feature for alter clause");
                }
                if (table.getKeysType() != KeysType.UNIQUE_KEYS
                        && alterFeature == EnableFeatureOp.Features.BATCH_DELETE) {
                    throw new AnalysisException("Batch delete only supported in unique tables.");
                }
                if (table.getKeysType() != KeysType.UNIQUE_KEYS
                        && alterFeature == EnableFeatureOp.Features.SEQUENCE_LOAD) {
                    throw new AnalysisException("Sequence load only supported in unique tables.");
                }
                if (alterFeature == EnableFeatureOp.Features.UPDATE_FLEXIBLE_COLUMNS) {
                    if (!(table.getKeysType() == KeysType.UNIQUE_KEYS && table.getEnableUniqueKeyMergeOnWrite())) {
                        throw new AnalysisException("Update flexible columns feature is only supported"
                                + " on merge-on-write unique tables.");
                    }
                    if (table.hasSkipBitmapColumn()) {
                        throw new AnalysisException("table " + table.getName()
                                + " has enabled update flexible columns feature already.");
                    }
                }
                // analyse sequence column
                Type sequenceColType = null;
                if (alterFeature == EnableFeatureOp.Features.SEQUENCE_LOAD) {
                    Map<String, String> propertyMap = new HashMap<>(alterClause.getProperties());
                    try {
                        sequenceColType = PropertyAnalyzer.analyzeSequenceType(propertyMap, table.getKeysType());
                        if (sequenceColType == null) {
                            throw new AnalysisException("unknown sequence column type");
                        }
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage());
                    }
                }

                // has rollup table
                if (table.getVisibleIndex().size() > 1) {
                    for (MaterializedIndex idx : table.getVisibleIndex()) {
                        // add a column to rollup index it will add to base table automatically,
                        // if add a column here it will duplicated
                        if (idx.getId() == table.getBaseIndexId()) {
                            continue;
                        }
                        AddColumnOp addColumnOp = null;
                        if (alterFeature == EnableFeatureOp.Features.BATCH_DELETE) {
                            addColumnOp = new AddColumnOp(ColumnDefinition.newDeleteSignColumnDefinition(), null,
                                    table.getIndexNameById(idx.getId()), null);
                        } else if (alterFeature == EnableFeatureOp.Features.SEQUENCE_LOAD) {
                            addColumnOp = new AddColumnOp(
                                    ColumnDefinition.newSequenceColumnDefinition(
                                            DataType.fromCatalogType(sequenceColType)),
                                    null,
                                    table.getIndexNameById(idx.getId()), null);
                        } else {
                            throw new AnalysisException("unknown feature : " + alterFeature);
                        }
                        addColumnOp.setTableName(tbl);
                        addColumnOp.validate(ctx);
                        alterTableOps.add(addColumnOp);
                    }
                } else {
                    // no rollup tables
                    AddColumnOp addColumnOp = null;
                    if (alterFeature == EnableFeatureOp.Features.BATCH_DELETE) {
                        addColumnOp = new AddColumnOp(ColumnDefinition.newDeleteSignColumnDefinition(), null,
                                null, null);
                    } else if (alterFeature == EnableFeatureOp.Features.SEQUENCE_LOAD) {
                        addColumnOp = new AddColumnOp(
                                ColumnDefinition.newSequenceColumnDefinition(DataType.fromCatalogType(sequenceColType)),
                                null,
                                null, null);
                    } else if (alterFeature == EnableFeatureOp.Features.UPDATE_FLEXIBLE_COLUMNS) {
                        ColumnDefinition skipBItmapCol = ColumnDefinition.newSkipBitmapColumnDef(AggregateType.NONE);
                        List<Column> fullSchema = table.getBaseSchema(true);
                        String lastCol = fullSchema.get(fullSchema.size() - 1).getName();
                        addColumnOp = new AddColumnOp(skipBItmapCol, new ColumnPosition(lastCol), null, null);
                    }
                    addColumnOp.setTableName(tbl);
                    addColumnOp.validate(ctx);
                    alterTableOps.add(addColumnOp);
                }
                // add hidden column to rollup table
            } else {
                alterTableOps.add(alterClause);
            }
        }
        ops = alterTableOps;
    }

    /**
     * checkExternalTableOperationAllow
     */
    private void checkExternalTableOperationAllow(TableIf table) throws UserException {
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        for (AlterTableOp alterClause : ops) {
            if (alterClause instanceof RenameTableOp
                    || alterClause instanceof AddColumnOp
                    || alterClause instanceof AddColumnsOp
                    || alterClause instanceof DropColumnOp
                    || alterClause instanceof RenameColumnOp
                    || alterClause instanceof ModifyColumnOp
                    || alterClause instanceof ReorderColumnsOp
                    || alterClause instanceof ModifyEngineOp
                    || alterClause instanceof ModifyTablePropertiesOp
                    || alterClause instanceof CreateOrReplaceBranchOp
                    || alterClause instanceof CreateOrReplaceTagOp
                    || alterClause instanceof DropBranchOp
                    || alterClause instanceof DropTagOp) {
                alterTableOps.add(alterClause);
            } else {
                throw new AnalysisException(table.getType().toString() + " [" + table.getName() + "] "
                        + "do not support " + alterClause.getOpType().toString() + " clause now");
            }
        }
        ops = alterTableOps;
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(tbl.toSql()).append(" ");
        int idx = 0;
        for (AlterTableOp op : originOps) {
            if (idx != 0) {
                sb.append(", \n");
            }
            if (op instanceof AddRollupOp) {
                if (idx == 0) {
                    sb.append("ADD ROLLUP");
                }
                sb.append(op.toSql().replace("ADD ROLLUP", ""));
            } else if (op instanceof DropRollupOp) {
                if (idx == 0) {
                    sb.append("DROP ROLLUP ");
                }
                sb.append(((DropRollupOp) op).getRollupName());
            } else {
                sb.append(op.toSql());
            }
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().alterTable(this);
    }
}
