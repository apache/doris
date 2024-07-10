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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * delete from unique key table.
 */
public class DeleteFromCommand extends Command implements ForwardWithSync, Explainable {

    protected final List<String> nameParts;
    protected final String tableAlias;
    protected final boolean isTempPart;
    protected final List<String> partitions;
    protected final LogicalPlan logicalQuery;

    /**
     * constructor
     */
    public DeleteFromCommand(List<String> nameParts, String tableAlias,
            boolean isTempPart, List<String> partitions, LogicalPlan logicalQuery) {
        super(PlanType.DELETE_COMMAND);
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.tableAlias = tableAlias;
        this.isTempPart = isTempPart;
        this.partitions = Utils.copyRequiredList(partitions);
        this.logicalQuery = logicalQuery;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        updateSessionVariableForDelete(ctx.getSessionVariable());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.setPlanner(planner);
        executor.checkBlockRules();
        // if fe could do fold constant to get delete will do nothing for table, just return.
        if (planner.getPhysicalPlan() instanceof PhysicalEmptyRelation) {
            Env.getCurrentEnv()
                    .getDeleteHandler().processEmptyRelation(ctx.getState());
            return;
        }
        Optional<PhysicalFilter<?>> optFilter = (planner.getPhysicalPlan()
                .<PhysicalFilter<?>>collect(PhysicalFilter.class::isInstance)).stream()
                .findAny();
        Optional<PhysicalOlapScan> optScan = (planner.getPhysicalPlan()
                .<PhysicalOlapScan>collect(PhysicalOlapScan.class::isInstance)).stream()
                .findAny();
        Optional<UnboundRelation> optRelation = (logicalQuery
                .<UnboundRelation>collect(UnboundRelation.class::isInstance)).stream()
                .findAny();
        Preconditions.checkArgument(optFilter.isPresent(), "delete command must contain filter");
        Preconditions.checkArgument(optScan.isPresent(), "delete command could be only used on olap table");
        Preconditions.checkArgument(optRelation.isPresent(), "delete command could be only used on olap table");
        PhysicalOlapScan scan = optScan.get();
        UnboundRelation relation = optRelation.get();
        PhysicalFilter<?> filter = optFilter.get();

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), scan.getDatabase().getCatalog().getName(),
                        scan.getDatabase().getFullName(),
                        scan.getTable().getName(), PrivPredicate.LOAD)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    scan.getDatabase().getFullName() + ": " + scan.getTable().getName());
            throw new AnalysisException(message);
        }

        // predicate check
        OlapTable olapTable = scan.getTable();
        Set<String> columns = olapTable.getFullSchema().stream().map(Column::getName).collect(Collectors.toSet());
        try {
            Plan plan = planner.getPhysicalPlan();
            checkSubQuery(plan);
            for (Expression conjunct : filter.getConjuncts()) {
                conjunct.<SlotReference>collect(SlotReference.class::isInstance)
                        .forEach(s -> checkColumn(columns, s, olapTable));
                checkPredicate(conjunct);
            }
        } catch (Exception e) {
            try {
                new DeleteFromUsingCommand(nameParts, tableAlias, isTempPart, partitions,
                        logicalQuery, Optional.empty()).run(ctx, executor);
                return;
            } catch (Exception e2) {
                throw e;
            }
        }

        if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite()
                && !olapTable.getEnableMowLightDelete()) {
            new DeleteFromUsingCommand(nameParts, tableAlias, isTempPart, partitions,
                    logicalQuery, Optional.empty()).run(ctx, executor);
            return;
        }

        // call delete handler to process
        List<Predicate> predicates = planner.getScanNodes().get(0).getConjuncts().stream()
                .filter(c -> {
                    // filter predicate __DORIS_DELETE_SIGN__ = 0
                    List<Expr> slotRefs = Lists.newArrayList();
                    c.collect(SlotRef.class::isInstance, slotRefs);
                    return slotRefs.stream().map(SlotRef.class::cast)
                            .noneMatch(s -> Column.DELETE_SIGN.equalsIgnoreCase(s.getColumnName()));
                })
                .map(c -> {
                    if (c instanceof Predicate) {
                        return (Predicate) c;
                    } else {
                        throw new AnalysisException("non predicate in filter: " + c.toSql());
                    }
                }).collect(Collectors.toList());
        if (predicates.isEmpty()) {
            // TODO this will delete all rows, however storage layer do not support true predicate now
            //  just throw exception to fallback until storage support true predicate.
            throw new AnalysisException("delete all rows is forbidden temporary.");
        }
        Env.getCurrentEnv()
                .getDeleteHandler()
                .process((Database) scan.getDatabase(), scan.getTable(),
                        Lists.newArrayList(relation.getPartNames()), predicates, ctx.getState());
    }

    private void updateSessionVariableForDelete(SessionVariable sessionVariable) {
        sessionVariable.setIsSingleSetVar(true);
        try {
            // turn off forbid unknown col stats
            VariableMgr.setVar(sessionVariable,
                    new SetVar(SessionVariable.FORBID_UNKNOWN_COLUMN_STATS, new StringLiteral("false")));
            // disable eliminate not null rule
            List<String> disableRules = Lists.newArrayList(
                    RuleType.ELIMINATE_NOT_NULL.name(), RuleType.INFER_FILTER_NOT_NULL.name());
            disableRules.addAll(sessionVariable.getDisableNereidsRuleNames());
            VariableMgr.setVar(sessionVariable,
                    new SetVar(SessionVariable.DISABLE_NEREIDS_RULES,
                            new StringLiteral(StringUtils.join(disableRules, ","))));
        } catch (Exception e) {
            throw new AnalysisException("set session variable by delete from command failed", e);
        }
    }

    private void checkColumn(Set<String> tableColumns, SlotReference slotReference, OlapTable table) {
        // 0. must slot from table
        if (!slotReference.getColumn().isPresent()) {
            throw new AnalysisException("");
        }
        Column column = slotReference.getColumn().get();

        if (Column.DELETE_SIGN.equalsIgnoreCase(column.getName())) {
            return;
        }
        // 1. shadow column
        if (Column.isShadowColumn(column.getName())) {
            throw new AnalysisException("Can not apply delete condition to shadow column " + column.getName());
        }
        // 2. table has shadow column on table related to column in predicates
        String shadowName = Column.getShadowName(column.getName());
        if (tableColumns.contains(shadowName)) {
            throw new AnalysisException(String.format("Column '%s' is under"
                    + " schema change operation. Do not allow delete operation", shadowName));
        }
        // 3. check column is primitive type
        // TODO(Now we can not push down non-scala type like array/map/struct to storage layer because of
        //  predict_column in be not support non-scala type, so we just should ban this type in delete predict, when
        //  we delete predict_column in be we should delete this ban)
        if (!column.getType().isScalarType()
                || (column.getType().isOnlyMetricType() && !column.getType().isJsonbType())) {
            throw new AnalysisException(String.format("Can not apply delete condition to column type: "
                    + column.getType()));
        }
        // 4. column should not float or double
        if (slotReference.getDataType().isFloatLikeType()) {
            throw new AnalysisException("Column[" + column.getName() + "] type is float or double.");
        }
        // 5. only contains key column if agg or mor
        if (!column.isKey()) {
            if (table.getKeysType() == KeysType.AGG_KEYS) {
                throw new AnalysisException("delete predicate on value column only supports Unique table with"
                        + " merge-on-write enabled and Duplicate table, but " + "Table[" + table.getName()
                        + "] is an Aggregate table.");
            } else if (table.getKeysType() == KeysType.UNIQUE_KEYS && !table.getEnableUniqueKeyMergeOnWrite()) {
                throw new AnalysisException("delete predicate on value column only supports Unique table with"
                        + " merge-on-write enabled and Duplicate table, but " + "Table[" + table.getName()
                        + "] is an unique table without merge-on-write.");
            }
        }

        for (String indexName : table.getIndexNameToId().keySet()) {
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(table.getIndexIdByName(indexName));
            Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            meta.getSchema().stream()
                    .map(col -> org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(col.getName()))
                    .forEach(name -> columns.add(name));
            if (!columns.contains(column.getName())) {
                throw new AnalysisException("Column[" + column.getName() + "] not exist in index " + indexName
                        + ". maybe you need drop the corresponding materialized-view.");
            }
        }
    }

    private void checkSubQuery(Plan plan) {
        while (true) {
            if (!(plan instanceof PhysicalDistribute
                    || plan instanceof PhysicalOlapScan
                    || plan instanceof PhysicalProject
                    || plan instanceof PhysicalFilter)) {
                throw new AnalysisException("Where clause only supports compound predicate,"
                        + " binary predicate, is_null predicate or in predicate.");
            }
            if (plan instanceof PhysicalOlapScan) {
                break;
            }
            plan = ((PhysicalUnary<?>) plan).child();
        }
    }

    private void checkComparisonPredicate(ComparisonPredicate cp) {
        if (!(cp.left() instanceof SlotReference)) {
            throw new AnalysisException(
                    "Left expr of binary predicate should be column name, predicate: " + cp.toSql()
                            + ", left expr type:" + cp.left().getDataType());
        }
        if (!(cp.right() instanceof Literal)) {
            throw new AnalysisException(
                    "Right expr of binary predicate should be value, predicate: " + cp.toSql()
                            + ", right expr type:" + cp.right().getDataType());
        }
    }

    private void checkIsNull(IsNull isNull) {
        if (!(isNull.child() instanceof SlotReference)) {
            throw new AnalysisException(
                    "Child expr of is_null predicate should be column name, predicate: " + isNull.toSql());
        }
    }

    private void checkInPredicate(InPredicate in) {
        if (!(in.getCompareExpr() instanceof SlotReference)) {
            throw new AnalysisException(
                    "Left expr of in predicate should be column name, predicate: " + in.toSql()
                            + ", left expr type:" + in.getCompareExpr().getDataType());
        }
        int maxAllowedInElementNumOfDelete = Config.max_allowed_in_element_num_of_delete;
        if (in.getOptions().size() > maxAllowedInElementNumOfDelete) {
            throw new AnalysisException("Element num of in predicate should not be more than "
                    + maxAllowedInElementNumOfDelete);
        }
        for (Expression option : in.getOptions()) {
            if (!(option instanceof Literal)) {
                throw new AnalysisException("Child of in predicate should be value, but get " + option);
            }
        }
    }

    private void checkPredicate(Expression predicate) {
        if (predicate instanceof And) {
            checkPredicate(((And) predicate).left());
            checkPredicate(((And) predicate).right());
        } else if (predicate instanceof ComparisonPredicate) {
            checkComparisonPredicate((ComparisonPredicate) predicate);
        } else if (predicate instanceof IsNull) {
            checkIsNull((IsNull) predicate);
        } else if (predicate instanceof Not) {
            Expression child = ((Not) predicate).child();
            if (child instanceof IsNull) {
                checkIsNull((IsNull) child);
            } else if (child instanceof ComparisonPredicate) {
                checkComparisonPredicate((ComparisonPredicate) child);
            } else if (child instanceof InPredicate) {
                checkInPredicate((InPredicate) child);
            } else {
                throw new AnalysisException("Where clause only supports compound predicate,"
                        + " binary predicate, is_null predicate or in predicate. But we meet "
                        + child.toSql());
            }
        } else if (predicate instanceof InPredicate) {
            checkInPredicate((InPredicate) predicate);
        } else {
            throw new AnalysisException("Where clause only supports compound predicate,"
                    + " binary predicate, is_null predicate or in predicate. But we meet "
                    + predicate.toSql());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDeleteFromCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        return completeQueryPlan(ctx, logicalQuery);
    }

    private OlapTable getTargetTable(ConnectContext ctx) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("table must be olapTable in delete command");
        }
        return ((OlapTable) table);
    }

    /**
     * for explain command
     */
    public LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery) {
        OlapTable targetTable = getTargetTable(ctx);
        checkTargetTable(targetTable);
        // add select and insert node.
        List<NamedExpression> selectLists = Lists.newArrayList();
        List<String> cols = Lists.newArrayList();
        boolean isMow = targetTable.getEnableUniqueKeyMergeOnWrite();
        String tableName = tableAlias != null ? tableAlias : targetTable.getName();
        for (Column column : targetTable.getFullSchema()) {
            if (column.getName().equalsIgnoreCase(Column.DELETE_SIGN)) {
                selectLists.add(new UnboundAlias(new TinyIntLiteral(((byte) 1)), Column.DELETE_SIGN));
            } else if (column.getName().equalsIgnoreCase(Column.SEQUENCE_COL)
                    && targetTable.getSequenceMapCol() != null) {
                selectLists.add(new UnboundSlot(tableName, targetTable.getSequenceMapCol()));
            } else if (column.isKey()) {
                selectLists.add(new UnboundSlot(tableName, column.getName()));
            } else if (!isMow && (!column.isVisible() || (!column.isAllowNull() && !column.hasDefaultValue()))) {
                selectLists.add(new UnboundSlot(tableName, column.getName()));
            } else {
                selectLists.add(new UnboundSlot(tableName, column.getName()));
            }
            cols.add(column.getName());
        }

        logicalQuery = new LogicalProject<>(selectLists, logicalQuery);

        boolean isPartialUpdate = targetTable.getEnableUniqueKeyMergeOnWrite()
                && cols.size() < targetTable.getColumns().size();
        logicalQuery = handleCte(logicalQuery);
        // make UnboundTableSink
        return UnboundTableSinkCreator.createUnboundTableSink(nameParts, cols, ImmutableList.of(),
                isTempPart, partitions, isPartialUpdate, DMLCommandType.DELETE, logicalQuery);
    }

    protected LogicalPlan handleCte(LogicalPlan logicalPlan) {
        return logicalPlan;
    }

    protected void checkTargetTable(OlapTable targetTable) {
        if (targetTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("delete command on aggregate/duplicate table is not explainable");
        }
    }
}
