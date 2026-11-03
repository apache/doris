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
import org.apache.doris.analysis.ColumnPath;
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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceIndexAdmission;
import org.apache.doris.datasource.lance.LanceIndexMutationOutcome;
import org.apache.doris.datasource.lance.LanceIndexMutationPlan;
import org.apache.doris.datasource.lance.LanceIndexMutationValidator;
import org.apache.doris.datasource.lance.index.LanceIndexMutationExecutor;
import org.apache.doris.datasource.lance.index.LanceIndexMutationRefresher;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AlterTableCommand
 */
public class AlterTableCommand extends Command implements ForwardWithSync {
    private TableNameInfo tbl;
    private List<AlterTableOp> ops;
    private List<AlterTableOp> originOps;
    /**
     * Top-level CREATE/DROP INDEX ops targeting a Lance catalog table, collected by validate()
     * and admitted by run(). ForwardWithSync commands are forwarded before run() on a follower
     * and re-parsed into a fresh command object on the master, so this collection can only be
     * populated once, on the master, on this object; validate() pins that single-call invariant.
     */
    private final List<AlterTableOp> lanceIndexOps = new ArrayList<>();
    // Resolved during validate() and reused by the Lance admission branch of run(); both run
    // exactly once on the same master-side object, so there is no stale-state hazard.
    private CatalogIf catalog;
    private DatabaseIf dbIf;
    private TableIf tableIf;
    /**
     * The Lance mutation cancel flag, set by {@link #cancel()} (the StmtExecutor cancel hook,
     * same contract as InsertOverwriteTableCommand) and observed by the synchronous executor
     * at its phase boundaries before the dispatch. Cancelling never rolls back a possible
     * commit; the executor owns that semantics.
     */
    private final AtomicBoolean lanceMutationCancelled = new AtomicBoolean(false);

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
     * Return the path-aware operations before branch-4.1 translates them to legacy clauses.
     */
    public List<AlterTableOp> getNereidsOps() {
        return ops;
    }

    /**
     * validate
     */
    private void validate(ConnectContext ctx) throws UserException {
        Preconditions.checkState(lanceIndexOps.isEmpty(),
                "AlterTableCommand.validate() must run exactly once per command object");
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
        String ctlName = tbl.getCtl();
        String dbName = tbl.getDb();
        String tableName = tbl.getTbl();
        catalog = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(ctlName, catalogName -> new DdlException("Unknown catalog " + catalogName));
        validateLanceIndexOperationsBeforeResolution(catalog);
        dbIf = catalog.getDbOrDdlException(dbName);
        tableIf = dbIf.getTableOrDdlException(tableName);
        if (tableIf.isTemporary()) {
            throw new AnalysisException("Do not support alter temporary table[" + tableName + "]");
        }
        checkColumnOperationsSupported(tableIf, ops);
        if (tableIf instanceof LanceExternalTable) {
            // Top-level CREATE/DROP INDEX on Lance catalog tables: static-validate, then either
            // reject with the mutation-disabled error while the admission gate stays off (the
            // default; no metadata read, no id allocation), or hand the op to run() for
            // admission. ALTER TABLE ADD/DROP INDEX (alter = true) falls through to the existing
            // generic external-table rejection below.
            for (AlterTableOp op : ops) {
                if (op instanceof CreateIndexOp && !((CreateIndexOp) op).isAlter()) {
                    IndexDefinition indexDef = ((CreateIndexOp) op).getIndexDef();
                    LanceIndexMutationValidator.validateCreateIndex((LanceExternalCatalog) catalog,
                            (LanceExternalTable) tableIf, indexDef);
                    if (!Config.enable_lance_index_mutation) {
                        LanceIndexMutationValidator.rejectMutationDisabled(
                                indexDef.isOrReplace() ? "CREATE OR REPLACE INDEX" : "CREATE INDEX");
                    }
                    lanceIndexOps.add(op);
                } else if (op instanceof DropIndexOp && !((DropIndexOp) op).isAlter()) {
                    // The DROP static checks (REST fail-fast and the name bounds) already ran in
                    // validateLanceIndexOperationsBeforeResolution; only the gate forks here.
                    if (!Config.enable_lance_index_mutation) {
                        LanceIndexMutationValidator.rejectMutationDisabled("DROP INDEX");
                    }
                    lanceIndexOps.add(op);
                }
            }
            if (!lanceIndexOps.isEmpty()) {
                // Admission fully takes over a top-level index DDL: skip the generic op loop and
                // the alterTable call below. CreateIndexOp.validate() would otherwise hit the
                // internal Lance guards and, for ANN (null internal IndexType), even allocate an
                // internal index id before the generic external-table rejection.
                return;
            }
            // Non-index ops (rename etc.) on a Lance table continue on the generic path exactly
            // as before.
        }
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

    /**
     * Lance index checks that do not need the resolved table: REST catalogs fail fast for
     * top-level CREATE/DROP INDEX before database/table metadata resolution, and DROP INDEX on
     * Directory catalogs gets the shared index-name bounds here because the op-level
     * DropIndexOp.validate() is never reached on the typed-rejection path below.
     */
    private void validateLanceIndexOperationsBeforeResolution(CatalogIf catalog) throws AnalysisException {
        if (!(catalog instanceof LanceExternalCatalog)) {
            return;
        }
        for (AlterTableOp op : ops) {
            if (op instanceof CreateIndexOp && !((CreateIndexOp) op).isAlter()) {
                LanceIndexMutationValidator.validateCreateIndexCatalog((LanceExternalCatalog) catalog,
                        ((CreateIndexOp) op).getIndexDef());
            } else if (op instanceof DropIndexOp && !((DropIndexOp) op).isAlter()) {
                LanceIndexMutationValidator.validateDropIndex((LanceExternalCatalog) catalog,
                        ((DropIndexOp) op).getIndexName());
            }
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
            if (isIcebergColumnSchemaOperation(alterTableOp)) {
                throw new AnalysisException("Multiple Iceberg ALTER clauses are not supported when a statement "
                        + "contains a column operation");
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
                || alterTableOp instanceof RenameColumnOp
                || alterTableOp instanceof ModifyColumnOp
                || alterTableOp instanceof ModifyColumnCommentOp
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
            columnDefinition.validateComplexTypeDefaultValue();
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
        for (AlterTableOp alterClause : ops) {
            if (alterClause instanceof RenameTableOp
                    || alterClause instanceof AddColumnOp
                    || alterClause instanceof AddColumnsOp
                    || alterClause instanceof DropColumnOp
                    || alterClause instanceof RenameColumnOp
                    || alterClause instanceof ModifyColumnOp
                    || (alterClause instanceof ModifyColumnCommentOp && table instanceof IcebergExternalTable)
                    || alterClause instanceof ReorderColumnsOp
                    || alterClause instanceof ModifyEngineOp
                    || alterClause instanceof ModifyTablePropertiesOp
                    || alterClause instanceof CreateOrReplaceBranchOp
                    || alterClause instanceof CreateOrReplaceTagOp
                    || alterClause instanceof DropBranchOp
                    || alterClause instanceof DropTagOp
                    || alterClause instanceof AddPartitionFieldOp
                    || alterClause instanceof DropPartitionFieldOp
                    || alterClause instanceof ReplacePartitionFieldOp) {
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
        // The Lance mutation budget starts at statement entry and is shared by admission
        // (the snapshot read below), the executor's admission queue, the dispatch and the wait.
        long statementStartMs = System.currentTimeMillis();
        validate(ctx);
        if (!lanceIndexOps.isEmpty()) {
            // A top-level CREATE/DROP INDEX statement carries exactly one operation.
            Preconditions.checkState(lanceIndexOps.size() == 1,
                    "a top-level Lance index statement must carry exactly one operation");
            AlterTableOp op = lanceIndexOps.get(0);
            LanceIndexMutationPlan plan;
            if (op instanceof CreateIndexOp) {
                IndexDefinition indexDef = ((CreateIndexOp) op).getIndexDef();
                plan = LanceIndexAdmission.admitCreate((LanceExternalCatalog) catalog,
                        (LanceExternalDatabase) dbIf, (LanceExternalTable) tableIf, indexDef,
                        indexDef.isIfNotExists());
            } else {
                DropIndexOp dropIndexOp = (DropIndexOp) op;
                plan = LanceIndexAdmission.admitDrop((LanceExternalCatalog) catalog,
                        (LanceExternalDatabase) dbIf, (LanceExternalTable) tableIf,
                        dropIndexOp.getIndexName(), dropIndexOp.isSetIfExists());
            }
            // An IF preflight no-op completes with the default OK packet, exactly as before
            // the synchronous execution path existed - but it still owes the same bounded
            // local cache invalidation the entry-internal no-op completes (design v6
            // section 2.3): the two no-op paths must behave identically. A residual failure
            // is the typed no-op refresh error: nothing was mutated, so it is neither a
            // build failure nor an ambiguity.
            if (plan == null) {
                String refreshFailure = LanceIndexMutationRefresher
                        .forTable((LanceExternalTable) tableIf).invalidateTableMetadata();
                if (refreshFailure != null) {
                    throw new AnalysisException(
                            ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED.formatErrorMsg(
                                    "the admission no-op completed, but " + refreshFailure),
                            ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED);
                }
                return;
            }
            // Execute outside the admission critical section (capture -> bounded execution ->
            // classified outcome): the executor re-validates the authoritative verdict inside
            // its entry, dispatches once, and derives the refresh obligation from the table.
            // Its typed busy/preflight rejections propagate unchanged; any non-success
            // outcome surfaces as its own client-facing class.
            LanceIndexMutationOutcome outcome = LanceIndexMutationExecutor.execute(
                    plan.populate(LanceIndexMutationExecutor.MutationRequest.newBuilder(
                            plan.getCatalog(), plan.getMutationType(), plan.getDatasetUri(),
                            plan.getAdmittedDatasetVersion(), plan.getNormalizedIndexName(),
                            LanceIndexMutationExecutor.budgetDeadlineMs(statementStartMs)))
                            .setCancelSignal(lanceMutationCancelled::get)
                            .setTable((LanceExternalTable) tableIf)
                            .build());
            if (outcome.getKind() != LanceIndexMutationOutcome.Kind.SUCCESS) {
                throw outcome.toUserException();
            }
            // A success (a committed mutation or an authoritative entry no-op) also completes
            // with the default OK packet.
            return;
        }
        ctx.getEnv().alterTable(this);
    }

    /**
     * cancel lance index mutation, called by the StmtExecutor cancel hook. Best effort: the
     * flag stops the synchronous executor at its phase boundaries before the dispatch; after
     * the dispatch started the executor's budget-bounded wait is the only way back.
     */
    public void cancel() {
        this.lanceMutationCancelled.set(true);
    }
}
