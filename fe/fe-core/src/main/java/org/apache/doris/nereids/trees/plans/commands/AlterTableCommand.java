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

import org.apache.doris.analysis.ColumnPath;
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
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyEngineOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;
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
    public List<AlterTableOp> getOps() {
        return ops;
    }

    /**
     * validate
     */
    private void validate(ConnectContext ctx) throws UserException {
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(ctx.getNameSpaceContext());
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
        checkColumnOperationsSupported(tableIf, ops);
        for (AlterTableOp op : ops) {
            op.setTableName(tbl);
            op.validate(ctx);
        }
        if (tableIf instanceof OlapTable) {
            rewriteAlterOpForOlapTable(ctx, (OlapTable) tableIf);
        } else {
            checkExternalTableOperationAllow(tableIf);
        }
    }

    static void checkColumnOperationsSupported(TableIf table, List<AlterTableOp> alterTableOps)
            throws AnalysisException {
        if (table instanceof IcebergExternalTable) {
            checkIcebergCompoundColumnOperations(alterTableOps);
            for (AlterTableOp alterTableOp : alterTableOps) {
                ColumnDefinition columnDefinition = getColumnDefinition(alterTableOp);
                ColumnPath nestedColumnPath = getNestedColumnPath(alterTableOp);
                // Keep this before AddColumnOp.validate(), whose generic NOT NULL check would mask
                // the Iceberg nested-field invariant. Metadata validation retains the same guard for other callers.
                if (alterTableOp instanceof AddColumnOp && columnDefinition != null && nestedColumnPath != null
                        && !columnDefinition.isNullable()) {
                    throw new AnalysisException("New nested field '" + nestedColumnPath.getFullPath()
                            + "' must be nullable");
                }
                if (!isIcebergColumnSchemaOperation(alterTableOp)) {
                    continue;
                }
                if (getRollupName(alterTableOp) != null) {
                    throw new AnalysisException("Rollup is not supported for Iceberg column operations");
                }
                Map<String, String> properties = alterTableOp.getProperties();
                if (properties != null && !properties.isEmpty()) {
                    throw new AnalysisException("PROPERTIES are not supported for Iceberg column operations");
                }
                checkIcebergColumnDefinition(alterTableOp, columnDefinition);
                if (alterTableOp instanceof AddColumnsOp) {
                    for (ColumnDefinition definition : ((AddColumnsOp) alterTableOp).getColumnDefinitions()) {
                        checkIcebergColumnDefinition(alterTableOp, definition);
                    }
                }
            }
            return;
        }
        for (AlterTableOp alterTableOp : alterTableOps) {
            ColumnPath columnPath = getNestedColumnPath(alterTableOp);
            if (columnPath != null) {
                throw new AnalysisException("Nested column path is only supported for Iceberg tables: "
                        + columnPath.getFullPath());
            }
        }
    }

    private static void checkIcebergCompoundColumnOperations(List<AlterTableOp> alterTableOps)
            throws AnalysisException {
        if (alterTableOps.size() <= 1) {
            return;
        }
        for (AlterTableOp alterTableOp : alterTableOps) {
            if (getNestedColumnPath(alterTableOp) != null
                    || alterTableOp instanceof ModifyColumnCommentOp) {
                throw new AnalysisException("Multiple Iceberg column operations are not supported when a statement "
                        + "contains a nested column path or MODIFY COLUMN COMMENT");
            }
        }
    }

    private static ColumnPath getNestedColumnPath(AlterTableOp alterTableOp) {
        ColumnPath columnPath = null;
        if (alterTableOp instanceof AddColumnOp) {
            columnPath = ((AddColumnOp) alterTableOp).getColumnPath();
        } else if (alterTableOp instanceof DropColumnOp) {
            columnPath = ((DropColumnOp) alterTableOp).getColumnPath();
        } else if (alterTableOp instanceof RenameColumnOp) {
            columnPath = ((RenameColumnOp) alterTableOp).getColumnPath();
        } else if (alterTableOp instanceof ModifyColumnOp) {
            columnPath = ((ModifyColumnOp) alterTableOp).getColumnPath();
        } else if (alterTableOp instanceof ModifyColumnCommentOp) {
            columnPath = ((ModifyColumnCommentOp) alterTableOp).getColumnPath();
        }
        return columnPath != null && columnPath.isNested() ? columnPath : null;
    }

    private static ColumnDefinition getColumnDefinition(AlterTableOp alterTableOp) {
        if (alterTableOp instanceof AddColumnOp) {
            return ((AddColumnOp) alterTableOp).getColumnDef();
        }
        if (alterTableOp instanceof ModifyColumnOp) {
            return ((ModifyColumnOp) alterTableOp).getColumnDef();
        }
        return null;
    }

    private static boolean isIcebergColumnSchemaOperation(AlterTableOp alterTableOp) {
        return alterTableOp instanceof AddColumnOp
                || alterTableOp instanceof AddColumnsOp
                || alterTableOp instanceof DropColumnOp
                || alterTableOp instanceof ModifyColumnOp
                || alterTableOp instanceof ReorderColumnsOp;
    }

    private static void checkIcebergColumnDefinition(AlterTableOp alterTableOp, ColumnDefinition columnDefinition)
            throws AnalysisException {
        if (columnDefinition == null) {
            return;
        }
        if (columnDefinition.isKey()) {
            throw new AnalysisException("KEY is not supported for Iceberg ADD/MODIFY COLUMN");
        }
        if (columnDefinition.getGeneratedColumnDesc().isPresent()) {
            throw new AnalysisException("Generated columns are not supported for Iceberg ADD/MODIFY COLUMN");
        }
        if (alterTableOp instanceof ModifyColumnOp
                && (columnDefinition.hasDefaultValue() || columnDefinition.hasOnUpdateDefaultValue())) {
            throw new AnalysisException("Modifying default values is not supported for Iceberg columns: "
                    + ((ModifyColumnOp) alterTableOp).getColumnPath().getFullPath());
        }
        if ((alterTableOp instanceof AddColumnOp || alterTableOp instanceof AddColumnsOp)
                && columnDefinition.hasOnUpdateDefaultValue()) {
            throw new AnalysisException("ON UPDATE is not supported for Iceberg ADD COLUMN: "
                    + columnDefinition.getName());
        }
    }

    private static String getRollupName(AlterTableOp alterTableOp) {
        if (alterTableOp instanceof AddColumnOp) {
            return ((AddColumnOp) alterTableOp).getRollupName();
        }
        if (alterTableOp instanceof AddColumnsOp) {
            return ((AddColumnsOp) alterTableOp).getRollupName();
        }
        if (alterTableOp instanceof DropColumnOp) {
            return ((DropColumnOp) alterTableOp).getRollupName();
        }
        if (alterTableOp instanceof ModifyColumnOp) {
            return ((ModifyColumnOp) alterTableOp).getRollupName();
        }
        if (alterTableOp instanceof ReorderColumnsOp) {
            return ((ReorderColumnsOp) alterTableOp).getRollupName();
        }
        return null;
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
        for (AlterTableOp alterTableOp : ops) {
            if (alterTableOp instanceof RenameTableOp
                    || alterTableOp instanceof AddColumnOp
                    || alterTableOp instanceof AddColumnsOp
                    || alterTableOp instanceof DropColumnOp
                    || alterTableOp instanceof RenameColumnOp
                    || alterTableOp instanceof ModifyColumnOp
                    || (alterTableOp instanceof ModifyColumnCommentOp && table instanceof IcebergExternalTable)
                    || alterTableOp instanceof ReorderColumnsOp
                    || alterTableOp instanceof ModifyEngineOp
                    || alterTableOp instanceof ModifyTablePropertiesOp
                    || alterTableOp instanceof CreateOrReplaceBranchOp
                    || alterTableOp instanceof CreateOrReplaceTagOp
                    || alterTableOp instanceof DropBranchOp
                    || alterTableOp instanceof DropTagOp
                    || alterTableOp instanceof AddPartitionFieldOp
                    || alterTableOp instanceof DropPartitionFieldOp
                    || alterTableOp instanceof ReplacePartitionFieldOp) {
                alterTableOps.add(alterTableOp);
            } else {
                throw new AnalysisException(table.getType().toString() + " [" + table.getName() + "] "
                        + "do not support " + alterTableOp.getOpType().toString() + " clause now");
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
