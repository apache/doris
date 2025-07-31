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
import org.apache.doris.analysis.MVColumnItem;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.CheckPrivileges;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmapWithCheck;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * create synchronized materialized view
 */
public class CreateMaterializedViewCommand extends Command implements ForwardWithSync {
    private static final String SYNC_MV_PLANER_DISABLE_RULES = "HAVING_TO_FILTER";
    private final TableNameInfo name;

    private final LogicalPlan logicalPlan;
    private Map<String, String> properties;
    private List<MVColumnItem> mvColumnItemList;
    private MVColumnItem whereClauseItem;
    private String dbName;
    private String baseIndexName;
    private KeysType mvKeysType;
    private OriginStatement originStatement;

    public CreateMaterializedViewCommand(TableNameInfo name, LogicalPlan logicalPlan,
                                         Map<String, String> properties) {
        super(PlanType.CREATE_MATERIALIZED_VIEW_COMMAND);
        this.name = name;
        this.logicalPlan = logicalPlan;
        this.properties = properties;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedViewCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        originStatement = executor.getOriginStmt();
        validate(ctx);
        ctx.getEnv().createMaterializedView(this);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public String getMVName() {
        return name.getTbl();
    }

    public List<MVColumnItem> getMVColumnItemList() {
        return mvColumnItemList;
    }

    public String getBaseIndexName() {
        return baseIndexName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getDBName() {
        return dbName;
    }

    public KeysType getMVKeysType() {
        return mvKeysType;
    }

    public OriginStatement getOriginStatement() {
        return originStatement;
    }

    public Column getWhereClauseItemColumn(OlapTable olapTable) throws DdlException {
        if (whereClauseItem == null) {
            return null;
        }
        return whereClauseItem.toMVColumn(olapTable);
    }

    public MVColumnItem getWhereClauseItem() {
        return whereClauseItem;
    }

    /**
     * validate
     *
     * @param ctx ConnectContext
     * @throws Exception auth denied
     */
    public void validate(ConnectContext ctx) throws Exception {
        Pair<LogicalPlan, CascadesContext> result = analyzeLogicalPlan(logicalPlan, ctx);
        CheckPrivileges checkPrivileges = new CheckPrivileges();
        checkPrivileges.rewriteRoot(result.first, result.second.getCurrentJobContext());
        PlanValidator planValidator = new PlanValidator();
        planValidator.validate(result.first, result.second);
        mvColumnItemList = planValidator.context.selectItems;
        whereClauseItem = planValidator.context.filterItem;
        mvKeysType = planValidator.context.keysType;
        dbName = planValidator.context.dbName;
        baseIndexName = planValidator.context.baseIndexName;
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, baseIndexName,
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
        }
    }

    private Pair<LogicalPlan, CascadesContext> analyzeLogicalPlan(LogicalPlan unboundPlan,
                                                                  ConnectContext ctx) {
        StatementContext statementContext = ctx.getStatementContext();
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        Set<String> tempDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
        ctx.getSessionVariable().setDisableNereidsRules(SYNC_MV_PLANER_DISABLE_RULES);
        ctx.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        LogicalPlan plan;
        try {
            // disable constant fold
            ctx.getSessionVariable().setVarOnce(SessionVariable.DEBUG_SKIP_FOLD_CONSTANT, "true");
            plan = (LogicalPlan) planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                    ExplainCommand.ExplainLevel.ANALYZED_PLAN);
        } finally {
            // after operate, roll back the disable rules
            ctx.getSessionVariable().setDisableNereidsRules(String.join(",", tempDisableRules));
            ctx.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        }
        return Pair.of(plan, planner.getCascadesContext());
    }

    private class ValidateContext {
        public List<MVColumnItem> selectItems;
        public MVColumnItem filterItem;
        public String dbName;
        public String baseIndexName;
        public KeysType keysType;
        private final PlanTranslatorContext planTranslatorContext;
        private Map<ExprId, Expression> groupByExprs;
        private List<NamedExpression> orderByExprs;
        private Map<Slot, Expression> exprReplaceMap = Maps.newHashMap();
        private Set<String> allColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        public ValidateContext(CascadesContext cascadesContext) {
            this.planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        }
    }

    private class PlanValidator extends DefaultPlanRewriter<ValidateContext> {
        public ValidateContext context;

        public Plan validate(LogicalPlan plan, CascadesContext cascadesContext) {
            context = new ValidateContext(cascadesContext);
            return plan.accept(this, context);
        }

        @Override
        public Plan visit(Plan plan, ValidateContext context) {
            throw new AnalysisException(String.format("%s is not supported in sync materialized view",
                    plan.getClass().getSimpleName()));
        }

        @Override
        public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias plan, ValidateContext context) {
            // do nothing
            return super.visit(plan, context);
        }

        @Override
        public Plan visitLogicalApply(LogicalApply plan, ValidateContext context) {
            throw new AnalysisException("subquery or join is not supported");
        }

        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, ValidateContext validateContext) {
            OlapTable olapTable = olapScan.getTable();
            if (olapTable.isTemporary()) {
                throw new AnalysisException("do not support create materialized view on temporary table");
            }
            for (Column column : olapTable.getFullSchema()) {
                // we don't check the duplicate name of historic mv for backwards compatibility
                validateContext.allColumnNames.add(column.getName());
            }
            validateContext.baseIndexName = olapTable.getName();
            validateContext.dbName = olapTable.getDBName();
            validateContext.keysType = olapTable.getKeysType();
            PlanTranslatorContext translatorContext = validateContext.planTranslatorContext;
            TupleDescriptor tupleDescriptor = validateContext.planTranslatorContext.generateTupleDesc();
            tupleDescriptor.setTable(olapTable);
            for (Slot slot : olapScan.getOutput()) {
                translatorContext.createSlotDesc(tupleDescriptor, (SlotReference) slot, olapTable);
                SlotRef slotRef = translatorContext.findSlotRef(slot.getExprId());
                slotRef.setLabel("`" + slot.getName() + "`");
                slotRef.disableTableName();
            }
            return olapScan;
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, ValidateContext context) {
            super.visit(filter, context);
            if (context.filterItem != null) {
                throw new AnalysisException(
                        String.format("Only support one filter node, the second is %s", filter.getPredicate()));
            }
            checkNoNondeterministicFunction(filter);
            Set<Expression> conjuncts = filter.getConjuncts().stream().filter(expr -> {
                Set<Slot> slots = expr.getInputSlots();
                for (Slot slot : slots) {
                    if (slot instanceof SlotReference) {
                        Column column = ((SlotReference) slot).getOriginalColumn().orElse(null);
                        if (column != null) {
                            if (column.isVisible()) {
                                AggregateType aggregateType = column.getAggregationType();
                                if (aggregateType != null && aggregateType != AggregateType.NONE) {
                                    throw new AnalysisException(String.format(
                                            "The where clause contained aggregate column is not supported, expr is %s",
                                            expr));
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                }
                return true;
            }).collect(Collectors.toSet());

            if (!conjuncts.isEmpty()) {
                Expression predicate = ExpressionUtils.and(conjuncts);
                if (!context.exprReplaceMap.isEmpty()) {
                    predicate = ExpressionUtils.replace(predicate, context.exprReplaceMap);
                }
                try {
                    Expr defineExpr = translateToLegacyExpr(predicate, context.planTranslatorContext);
                    context.filterItem = new MVColumnItem(defineExpr.toSqlWithoutTbl(), defineExpr);
                } catch (Exception ex) {
                    throw new AnalysisException(ex.getMessage());
                }
            }
            return filter.withConjuncts(conjuncts);
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, ValidateContext context) {
            super.visit(aggregate, context);
            if (context.groupByExprs != null) {
                throw new AnalysisException(String.format("Only support one agg node, the second is %s", aggregate));
            }
            context.keysType = KeysType.AGG_KEYS;
            checkNoNondeterministicFunction(aggregate);
            for (AggregateFunction aggregateFunction : aggregate.getAggregateFunctions()) {
                validateAggFunnction(aggregateFunction);
            }
            List<NamedExpression> outputs = aggregate.getOutputs();
            if (!context.exprReplaceMap.isEmpty()) {
                outputs = ExpressionUtils.replaceNamedExpressions(outputs, context.exprReplaceMap);
            }
            int groupByExprCount = aggregate.getGroupByExpressions().size();
            context.groupByExprs = Maps.newHashMap();
            for (int i = 0; i < groupByExprCount; ++i) {
                if (outputs.get(i).getDataType().isObjectOrVariantType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
                context.groupByExprs.put(outputs.get(i).getExprId(), outputs.get(i));
            }
            context.exprReplaceMap.putAll(ExpressionUtils.generateReplaceMap(outputs));
            return aggregate;
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, ValidateContext context) {
            super.visit(sort, context);
            if (context.orderByExprs != null) {
                throw new AnalysisException(String.format("Only support one sort node, the second is %s", sort));
            }
            checkNoNondeterministicFunction(sort);
            if (sort.getOrderKeys().stream().anyMatch((
                    orderKey -> orderKey.getExpr().getDataType().isObjectOrVariantType()))) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
            context.orderByExprs = (List<NamedExpression>) sort.getExpressions();
            if (!context.exprReplaceMap.isEmpty()) {
                context.orderByExprs = ExpressionUtils.replaceNamedExpressions(context.orderByExprs,
                        context.exprReplaceMap);
                context.orderByExprs = context.orderByExprs.stream()
                        .map(expr -> expr instanceof Alias && ((Alias) expr).child() instanceof SlotReference
                                ? (SlotReference) ((Alias) expr).child()
                                : expr)
                        .collect(Collectors.toList());
            }
            return sort;
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, ValidateContext context) {
            super.visit(project, context);
            checkNoNondeterministicFunction(project);
            List<NamedExpression> outputs = project.getOutputs();
            if (!context.exprReplaceMap.isEmpty()) {
                outputs = ExpressionUtils.replaceNamedExpressions(outputs, context.exprReplaceMap);
            }
            context.exprReplaceMap.putAll(ExpressionUtils.generateReplaceMap(outputs));
            return project;
        }

        @Override
        public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> resultSink, ValidateContext context) {
            super.visit(resultSink, context);
            List<NamedExpression> outputs = resultSink.getOutputExprs();
            List<String> outputNames = new ArrayList<>(outputs.size());
            for (NamedExpression expr : outputs) {
                String colName = expr.getName();
                if (context.allColumnNames.add(colName)) {
                    outputNames.add(colName);
                } else {
                    throw new AnalysisException(String.format("duplicate column name %s in full schema, "
                            + "please use a new unique name xxx, like %s as xxx in select list", colName, colName));
                }
            }
            if (!context.exprReplaceMap.isEmpty()) {
                outputs = ExpressionUtils.replaceNamedExpressions(outputs, context.exprReplaceMap);
                outputs = outputs.stream()
                        .map(expr -> expr instanceof Alias && ((Alias) expr).child() instanceof SlotReference
                                ? (SlotReference) ((Alias) expr).child()
                                : expr)
                        .collect(Collectors.toList());
            }

            Set<ExprId> outputExprIds = outputs.stream().map(NamedExpression::getExprId).collect(Collectors.toSet());
            if (outputExprIds.size() != outputs.size()) {
                throw new AnalysisException("The select expr is duplicated.");
            }
            if (context.groupByExprs != null) {
                for (ExprId exprId : context.groupByExprs.keySet()) {
                    if (!outputExprIds.contains(exprId)) {
                        throw new AnalysisException(String.format("The group expr %s not in select list",
                                context.groupByExprs.get(exprId)));
                    }
                }
            } else {
                if (context.keysType == KeysType.AGG_KEYS) {
                    throw new AnalysisException("agg mv must has group by clause");
                }
            }

            if (context.orderByExprs != null) {
                int orderByExprCount = context.orderByExprs.size();
                if (outputs.size() < orderByExprCount) {
                    throw new AnalysisException("The number of columns in order clause must be less than "
                            + "the number of columns in select clause");
                }
                if (context.groupByExprs != null && context.groupByExprs.size() != orderByExprCount) {
                    throw new AnalysisException("The key of columns in mv must be all of group by columns");
                }
                for (int i = 0; i < orderByExprCount; ++i) {
                    if (outputs.get(i).getExprId() != context.orderByExprs.get(i).getExprId()) {
                        throw new AnalysisException(String.format(
                                "The order of columns in order by clause must be same as the order of columns"
                                        + "in select list, %s vs %s", outputs.get(i), context.orderByExprs.get(i)));
                    }
                }
            }

            outputs = ExpressionUtils.rewriteDownShortCircuit(outputs, e -> {
                if (e instanceof ToBitmap) {
                    return new ToBitmapWithCheck(e.child(0));
                } else {
                    return e;
                }
            });
            context.selectItems = new ArrayList<>(outputs.size());
            boolean meetAggFunction = false;
            boolean meetNoneAggExpr = false;
            for (int i = 0; i < outputs.size(); ++i) {
                NamedExpression output = outputs.get(i);
                String colName = outputNames.get(i);
                Expression expr = output;
                if (output instanceof Alias) {
                    expr = ((Alias) output).child();
                }
                if (expr.isConstant()) {
                    throw new AnalysisException(String.format(
                            "The materialized view contain constant expr is disallowed, expr: %s", expr));
                }
                Expression ignoreCastExpr = expr instanceof Cast ? ((Cast) expr).child() : expr;
                if (!(ignoreCastExpr instanceof SlotReference || ignoreCastExpr instanceof BinaryArithmetic
                        || ignoreCastExpr instanceof BoundFunction)) {
                    throw new AnalysisException(
                            String.format(
                                    "The materialized view only support the single column or function expr. "
                                            + "Error column: %s", ignoreCastExpr));
                }
                List<SlotReference> usedSlots = expr.collectToList(SlotReference.class::isInstance);
                for (SlotReference slot : usedSlots) {
                    if (slot.hasAutoInc()) {
                        throw new AnalysisException("The materialized view can not involved auto increment column");
                    }
                }
                if (expr.containsType(AggregateFunction.class)) {
                    meetAggFunction = true;
                    if (expr instanceof AggregateFunction) {
                        context.selectItems.add(buildMVColumnItem(colName, (AggregateFunction) expr, context));
                    } else {
                        throw new AnalysisException(String.format(
                                "The materialized view's expr calculations cannot be included outside"
                                        + " aggregate functions, expr: %s", expr));
                    }
                } else {
                    if (meetAggFunction) {
                        throw new AnalysisException("The aggregate column should be after none agg column");
                    }
                    meetNoneAggExpr = true;
                    try {
                        context.selectItems
                                .add(new MVColumnItem(colName,
                                        translateToLegacyExpr(expr, context.planTranslatorContext)));
                    } catch (Exception ex) {
                        throw new AnalysisException(ex.getMessage());
                    }
                }
            }
            if (!meetNoneAggExpr) {
                throw new AnalysisException("The materialized view must contain at least one key column");
            }
            setKeyForSelectItems(context.selectItems, context);
            return resultSink;
        }

        private void setKeyForSelectItems(List<MVColumnItem> selectItems, ValidateContext ctx) {
            if (ctx.orderByExprs != null) {
                int size = ctx.orderByExprs.size();
                for (int i = 0; i < size; ++i) {
                    MVColumnItem mvColumnItem = selectItems.get(i);
                    Preconditions.checkState(mvColumnItem.getAggregationType() == null, String.format(
                            "key column's agg type should be null, but it's %s", mvColumnItem.getAggregationType()));
                    selectItems.get(i).setIsKey(true);
                }
                for (int i = size; i < selectItems.size(); ++i) {
                    MVColumnItem mvColumnItem = selectItems.get(i);
                    if (mvColumnItem.getAggregationType() != null) {
                        break;
                    }
                    mvColumnItem.setAggregationType(AggregateType.NONE, true);
                }
            } else {
                /*
                  The keys type of Materialized view is aggregation.
                  All of group by columns are keys of materialized view.
                 */
                if (context.keysType == KeysType.DUP_KEYS) {
                    /*
                      There is no aggregation function in materialized view.
                      Supplement key of MV columns
                      The key is same as the short key in duplicate table
                      For example: select k1, k2 ... kn from t1
                      The default key columns are first 36 bytes of the columns in define order.
                      If the number of columns in the first 36 is more than 3, the first 3 columns will be used.
                      column: k1, k2, k3. The key is true.
                      Supplement non-key of MV columns
                      column: k4... kn. The key is false, aggregation type is none, isAggregationTypeImplicit is true.
                     */
                    int theBeginIndexOfValue = 0;
                    // supply key
                    int keySizeByte = 0;
                    for (; theBeginIndexOfValue < selectItems.size(); theBeginIndexOfValue++) {
                        MVColumnItem column = selectItems.get(theBeginIndexOfValue);
                        keySizeByte += column.getType().getIndexSize();
                        if (theBeginIndexOfValue + 1 > FeConstants.shortkey_max_column_count
                                || keySizeByte > FeConstants.shortkey_maxsize_bytes) {
                            if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                                column.setIsKey(true);
                                theBeginIndexOfValue++;
                            }
                            break;
                        }
                        if (!column.getType().couldBeShortKey()) {
                            break;
                        }
                        if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                            column.setIsKey(true);
                            theBeginIndexOfValue++;
                            break;
                        }
                        column.setIsKey(true);
                    }
                    if (theBeginIndexOfValue == 0) {
                        throw new AnalysisException("The first column could not be float, double or complex type "
                                + "like array, struct, map, json, variant.");
                    }
                    // supply value
                    for (; theBeginIndexOfValue < selectItems.size(); theBeginIndexOfValue++) {
                        MVColumnItem mvColumnItem = selectItems.get(theBeginIndexOfValue);
                        mvColumnItem.setAggregationType(AggregateType.NONE, true);
                    }
                } else {
                    for (MVColumnItem mvColumnItem : selectItems) {
                        if (mvColumnItem.getAggregationType() != null) {
                            break;
                        }
                        mvColumnItem.setIsKey(true);
                    }
                }
            }
        }

        private MVColumnItem buildMVColumnItem(String name, AggregateFunction aggregateFunction, ValidateContext ctx)
                throws AnalysisException {
            Expression defineExpr = getAggFunctionFirstParam(aggregateFunction);
            DataType paramDataType = defineExpr.getDataType();
            DataType mvDataType = aggregateFunction.getDataType();
            AggregateType mvAggType;
            if (aggregateFunction instanceof Sum) {
                mvAggType = AggregateType.SUM;
                if (mvDataType != paramDataType) {
                    defineExpr = new Cast(defineExpr, mvDataType, true);
                }
            } else if (aggregateFunction instanceof Min) {
                mvAggType = AggregateType.MIN;
            } else if (aggregateFunction instanceof Max) {
                mvAggType = AggregateType.MAX;
            } else if (aggregateFunction instanceof Count) {
                mvAggType = AggregateType.SUM;
                List<WhenClause> whenClauses = new ArrayList<>(1);
                whenClauses.add(new WhenClause(new IsNull(defineExpr), new BigIntLiteral(0)));
                defineExpr = new CaseWhen(whenClauses, new BigIntLiteral(1));
            } else if (aggregateFunction instanceof BitmapUnion) {
                mvAggType = AggregateType.BITMAP_UNION;
            } else if (aggregateFunction instanceof HllUnion) {
                mvAggType = AggregateType.HLL_UNION;
            } else {
                mvAggType = AggregateType.GENERIC;
                defineExpr = StateCombinator.create(aggregateFunction);
                mvDataType = defineExpr.getDataType();
            }
            Expr expr = translateToLegacyExpr(defineExpr, ctx.planTranslatorContext);
            return new MVColumnItem(name, mvDataType.toCatalogDataType(), mvAggType, expr);
        }

        private Expr translateToLegacyExpr(Expression expression, PlanTranslatorContext context) {
            Expr expr = ExpressionTranslator.translate(expression, context);
            expr.disableTableName();
            return expr;
        }

        private Expression getAggFunctionFirstParam(AggregateFunction aggregateFunction) {
            if (aggregateFunction instanceof Count && ((Count) aggregateFunction).isStar()) {
                return new BigIntLiteral(1);
            }
            if (aggregateFunction.children().isEmpty()) {
                throw new AnalysisException(String.format("%s must have a param", aggregateFunction.getName()));
            }
            return aggregateFunction.child(0);
        }

        private void checkNoNondeterministicFunction(Plan plan) {
            for (Expression expression : plan.getExpressions()) {
                Set<Expression> nondeterministicFunctions = expression
                        .collect(expr -> !((ExpressionTrait) expr).isDeterministic()
                                && expr instanceof FunctionTrait);
                if (!nondeterministicFunctions.isEmpty()) {
                    throw new AnalysisException(String.format(
                            "can not contain nonDeterministic expression, the expression is %s ", expression));
                }
            }
        }

        private void validateAggFunnction(AggregateFunction aggregateFunction) {
            // if aggregate function use a value column of agg table,
            // the value columns' agg type must be consistent with aggregate function
            // we do it in two steps:
            //   1. if aggregate function use a value column param, find the value column's agg type or else get null
            //   2. check the value column's agg type is consistent with aggregate function.
            // if no value column used in aggregate function, we check the column type is valid for aggregate functions
            Set<Slot> inputSlots = aggregateFunction.getInputSlots();
            AggregateType aggregateType = null;
            Slot aggParamSlot = null;
            // try to find a value column
            for (Slot slot : inputSlots) {
                aggregateType = getAggTypeFromSlot(slot);
                if (aggregateType != null && aggregateType != AggregateType.NONE) {
                    aggParamSlot = slot;
                    break;
                }
            }
            if (aggParamSlot != null) {
                // if aggregate function use a value column param, the value column must be the one and only param
                if (aggregateFunction.children().size() != 1 || aggregateFunction.child(0) != aggParamSlot) {
                    throw new AnalysisException(
                            String.format("only allow single column as %s's param, but meet %s",
                                    aggregateFunction.getName(), aggregateFunction.child(0)));
                }
                // check the value columns' agg type is consistent with aggregate function
                if (aggregateFunction instanceof Sum) {
                    if (aggregateType != AggregateType.SUM) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, required: SUM",
                                aggregateType));
                    }
                } else if (aggregateFunction instanceof Min) {
                    if (aggregateType != AggregateType.MIN) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, required: MIN",
                                aggregateType));
                    }
                } else if (aggregateFunction instanceof Max) {
                    if (aggregateType != AggregateType.MAX) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, required: MAX",
                                aggregateType));
                    }
                } else if (aggregateFunction instanceof Count) {
                    if (aggregateType != AggregateType.SUM) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, required: SUM",
                                aggregateType));
                    }
                } else if (aggregateFunction instanceof BitmapUnion) {
                    if (aggregateType != AggregateType.BITMAP_UNION) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, "
                                        + "required: BITMAP_UNION", aggregateType));
                    }
                } else if (aggregateFunction instanceof HllUnion) {
                    if (aggregateType != AggregateType.HLL_UNION) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, "
                                        + "required: HLL_UNION", aggregateType));
                    }
                } else {
                    if (aggregateType != AggregateType.GENERIC) {
                        throw new AnalysisException(String.format(
                                "Aggregate function require same with slot aggregate type, input: %s, "
                                        + "required: GENERIC", aggregateType));
                    }
                }
            } else {
                // no value column used in aggregate function, we check the param's column type is valid
                DataType paramDataType = getAggFunctionFirstParam(aggregateFunction).getDataType();
                if (aggregateFunction instanceof BitmapUnion) {
                    if (!paramDataType.isBitmapType()) {
                        throw new AnalysisException(String.format(
                                "BITMAP_UNION need input a bitmap column, but input %s", paramDataType));
                    }
                } else if (aggregateFunction instanceof HllUnion) {
                    if (!paramDataType.isHllType()) {
                        throw new AnalysisException(String.format(
                                "HLL_UNION need input a hll column, but input %s", paramDataType));
                    }
                } else if (aggregateFunction instanceof Sum || aggregateFunction instanceof Max
                        || aggregateFunction instanceof Min || aggregateFunction instanceof Count) {
                    if (paramDataType.isAggStateType()) {
                        throw new AnalysisException(String.format(
                                "% can not use agg_state as its param", aggregateFunction.getName()));
                    }
                }
            }
        }

        private AggregateType getAggTypeFromSlot(Slot slot) {
            if (slot instanceof SlotReference) {
                Column column = ((SlotReference) slot).getOriginalColumn().orElse(null);
                if (column != null && column.isVisible()) {
                    return column.getAggregationType();
                }
            }
            return null;
        }
    }
}
