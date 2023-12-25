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

package org.apache.doris.nereids.rules.rewrite.mv;

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.PlanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for selecting materialized index rules.
 */
public abstract class AbstractSelectMaterializedIndexRule {
    protected boolean shouldSelectIndexWithAgg(LogicalOlapScan scan) {
        switch (scan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
            case DUP_KEYS:
                // SelectMaterializedIndexWithAggregate(R1) run before SelectMaterializedIndexWithoutAggregate(R2)
                // if R1 selects baseIndex and preAggStatus is off
                // we should give a chance to R2 to check if some prefix-index can be selected
                // so if R1 selects baseIndex and preAggStatus is off, we keep scan's index unselected in order to
                // let R2 to get a chance to do its work
                // at last, after R1, the scan may be the 4 status
                // 1. preAggStatus is ON and baseIndex is selected, it means select baseIndex is correct.
                // 2. preAggStatus is ON and some other Index is selected, this is correct, too.
                // 3. preAggStatus is OFF, no index is selected, it means R2 could get a chance to run
                // so we check the preAggStatus and if some index is selected to make sure R1 can be run only once
                return scan.getPreAggStatus().isOn() && !scan.isIndexSelected();
            default:
                return false;
        }
    }

    protected boolean shouldSelectIndexWithoutAgg(LogicalOlapScan scan) {
        switch (scan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
            case DUP_KEYS:
                return !scan.isIndexSelected();
            default:
                return false;
        }
    }

    protected static boolean containAllRequiredColumns(MaterializedIndex index, LogicalOlapScan scan,
            Set<Slot> requiredScanOutput, Set<? extends Expression> requiredExpr, Set<Expression> predicateExpr) {
        OlapTable table = scan.getTable();
        MaterializedIndexMeta meta = table.getIndexMetaByIndexId(index.getId());

        Set<String> predicateExprSql = predicateExpr.stream().map(ExpressionTrait::toSql).collect(Collectors.toSet());

        // Here we use toSqlWithoutTbl because the output of toSql() is slot#[0] in Nereids
        Set<String> indexConjuncts = PlanNode.splitAndCompoundPredicateToConjuncts(meta.getWhereClause()).stream()
                .map(e -> {
                    e.setDisableTableName(true);
                    return e;
                })
                .map(e -> new NereidsParser().parseExpression(e.toSql()).toSql()).collect(Collectors.toSet());
        Set<String> commonConjuncts = indexConjuncts.stream().filter(predicateExprSql::contains)
                .collect(Collectors.toSet());
        if (commonConjuncts.size() != indexConjuncts.size()) {
            return false;
        }

        Set<String> requiredMvColumnNames = requiredScanOutput.stream()
                .map(s -> normalizeName(Column.getNameWithoutMvPrefix(s.getName())))
                .collect(Collectors.toCollection(() -> new TreeSet<String>(String.CASE_INSENSITIVE_ORDER)));

        Set<String> mvColNames = meta.getSchema().stream()
                .map(c -> normalizeName(parseMvColumnToSql(c.getNameWithoutMvPrefix())))
                .collect(Collectors.toCollection(() -> new TreeSet<String>(String.CASE_INSENSITIVE_ORDER)));
        mvColNames.addAll(indexConjuncts);

        return mvColNames.containsAll(requiredMvColumnNames)
                && (indexConjuncts.isEmpty() || commonConjuncts.size() == predicateExprSql.size())
                || requiredExpr.stream().filter(e -> !containsAllColumn(e, mvColNames)).collect(Collectors.toSet())
                        .isEmpty();
    }

    public static String parseMvColumnToSql(String mvName) {
        return new NereidsParser().parseExpression(
                    org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(mvName)).toSql();
    }

    public static String parseMvColumnToMvName(String mvName, Optional<String> aggTypeName) {
        return CreateMaterializedViewStmt.mvColumnBuilder(aggTypeName,
                new NereidsParser().parseExpression(
                    org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(mvName)).toSql());
    }

    protected static boolean containsAllColumn(Expression expression, Set<String> mvColumnNames) {
        if (mvColumnNames.contains(expression.toSql()) || mvColumnNames
                .contains(org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(expression.toSql()))) {
            return true;
        }
        if (expression.children().isEmpty()) {
            return false;
        }
        for (Expression child : expression.children()) {
            if (child instanceof Literal) {
                continue;
            }
            if (!containsAllColumn(child, mvColumnNames)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 1. find matching key prefix most.
     * 2. sort by row count, column count and index id.
     */
    protected static long selectBestIndex(
            List<MaterializedIndex> candidates,
            LogicalOlapScan scan,
            Set<Expression> predicates) {
        if (candidates.isEmpty()) {
            return scan.getTable().getBaseIndexId();
        }

        OlapTable table = scan.getTable();
        // Scan slot exprId -> slot name
        Map<ExprId, String> exprIdToName = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(NamedExpression::getExprId, NamedExpression::getName));

        // find matching key prefix most.
        List<MaterializedIndex> matchingKeyPrefixMost = matchPrefixMost(scan, candidates, predicates, exprIdToName);

        List<Long> partitionIds = scan.getSelectedPartitionIds();
        // sort by row count, column count and index id.
        List<Long> sortedIndexIds = matchingKeyPrefixMost.stream()
                .map(MaterializedIndex::getId)
                .sorted(Comparator
                        // compare by row count
                        .comparing(rid -> partitionIds.stream()
                                .mapToLong(pid -> table.getPartition(pid).getIndex((Long) rid).getRowCount())
                                .sum())
                        // compare by column count
                        .thenComparing(rid -> table.getSchemaByIndexId((Long) rid).size())
                        // compare by index id
                        .thenComparing(rid -> (Long) rid))
                .collect(Collectors.toList());

        return CollectionUtils.isEmpty(sortedIndexIds) ? scan.getTable().getBaseIndexId() : sortedIndexIds.get(0);
    }

    protected static List<MaterializedIndex> matchPrefixMost(
            LogicalOlapScan scan,
            List<MaterializedIndex> candidate,
            Set<Expression> predicates,
            Map<ExprId, String> exprIdToName) {
        Map<Boolean, Set<String>> split = filterCanUsePrefixIndexAndSplitByEquality(predicates, exprIdToName);
        Set<String> equalColNames = split.getOrDefault(true, ImmutableSet.of());
        Set<String> nonEqualColNames = split.getOrDefault(false, ImmutableSet.of());

        if (!(equalColNames.isEmpty() && nonEqualColNames.isEmpty())) {
            List<MaterializedIndex> matchingResult = matchKeyPrefixMost(scan.getTable(), candidate,
                    equalColNames, nonEqualColNames);
            return matchingResult.isEmpty() ? candidate : matchingResult;
        } else {
            return candidate;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Split conjuncts into equal-to and non-equal-to.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Filter the input conjuncts those can use prefix and split into 2 groups: is equal-to or non-equal-to predicate
     * when comparing the key column.
     */
    private static Map<Boolean, Set<String>> filterCanUsePrefixIndexAndSplitByEquality(
            Set<Expression> conjuncts, Map<ExprId, String> exprIdToColName) {
        return conjuncts.stream()
                .map(expr -> PredicateChecker.canUsePrefixIndex(expr, exprIdToColName))
                .filter(result -> !result.equals(PrefixIndexCheckResult.FAILURE))
                .collect(Collectors.groupingBy(
                        result -> result.type == ResultType.SUCCESS_EQUAL,
                        Collectors.mapping(result -> result.colName, Collectors.toSet())
                ));
    }

    private enum ResultType {
        FAILURE,
        SUCCESS_EQUAL,
        SUCCESS_NON_EQUAL,
    }

    private static class PrefixIndexCheckResult {
        public static final PrefixIndexCheckResult FAILURE = new PrefixIndexCheckResult(null, ResultType.FAILURE);
        private final String colName;
        private final ResultType type;

        private PrefixIndexCheckResult(String colName, ResultType result) {
            this.colName = colName;
            this.type = result;
        }

        public static PrefixIndexCheckResult createEqual(String name) {
            return new PrefixIndexCheckResult(name, ResultType.SUCCESS_EQUAL);
        }

        public static PrefixIndexCheckResult createNonEqual(String name) {
            return new PrefixIndexCheckResult(name, ResultType.SUCCESS_NON_EQUAL);
        }
    }

    /**
     * Check if an expression could prefix key index.
     */
    private static class PredicateChecker extends ExpressionVisitor<PrefixIndexCheckResult, Map<ExprId, String>> {
        private static final PredicateChecker INSTANCE = new PredicateChecker();

        private PredicateChecker() {
        }

        public static PrefixIndexCheckResult canUsePrefixIndex(Expression expression,
                Map<ExprId, String> exprIdToName) {
            return expression.accept(INSTANCE, exprIdToName);
        }

        @Override
        public PrefixIndexCheckResult visit(Expression expr, Map<ExprId, String> context) {
            return PrefixIndexCheckResult.FAILURE;
        }

        @Override
        public PrefixIndexCheckResult visitInPredicate(InPredicate in, Map<ExprId, String> context) {
            Optional<ExprId> slotOrCastOnSlot = ExpressionUtils.isSlotOrCastOnSlot(in.getCompareExpr());
            if (slotOrCastOnSlot.isPresent() && in.getOptions().stream().allMatch(Literal.class::isInstance)) {
                return PrefixIndexCheckResult.createEqual(context.get(slotOrCastOnSlot.get()));
            } else {
                return PrefixIndexCheckResult.FAILURE;
            }
        }

        @Override
        public PrefixIndexCheckResult visitComparisonPredicate(ComparisonPredicate cp, Map<ExprId, String> context) {
            if (cp instanceof EqualPredicate) {
                return check(cp, context, PrefixIndexCheckResult::createEqual);
            } else {
                return check(cp, context, PrefixIndexCheckResult::createNonEqual);
            }
        }

        private PrefixIndexCheckResult check(ComparisonPredicate cp, Map<ExprId, String> exprIdToColumnName,
                Function<String, PrefixIndexCheckResult> resultMapper) {
            return check(cp).map(exprId -> resultMapper.apply(exprIdToColumnName.get(exprId)))
                    .orElse(PrefixIndexCheckResult.FAILURE);
        }

        private Optional<ExprId> check(ComparisonPredicate cp) {
            Optional<ExprId> exprId = check(cp.left(), cp.right());
            return exprId.isPresent() ? exprId : check(cp.right(), cp.left());
        }

        private Optional<ExprId> check(Expression maybeSlot, Expression maybeConst) {
            Optional<ExprId> exprIdOpt = ExpressionUtils.isSlotOrCastOnSlot(maybeSlot);
            return exprIdOpt.isPresent() && maybeConst.isConstant() ? exprIdOpt : Optional.empty();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Matching key prefix
    ///////////////////////////////////////////////////////////////////////////
    private static List<MaterializedIndex> matchKeyPrefixMost(
            OlapTable table,
            List<MaterializedIndex> indexes,
            Set<String> equalColumns,
            Set<String> nonEqualColumns) {
        TreeMap<Integer, List<MaterializedIndex>> collect = indexes.stream()
                .collect(Collectors.toMap(
                        index -> indexKeyPrefixMatchCount(table, index, equalColumns, nonEqualColumns),
                        Lists::newArrayList,
                        (l1, l2) -> {
                            l1.addAll(l2);
                            return l1;
                        },
                        TreeMap::new)
                );
        return collect.descendingMap().firstEntry().getValue();
    }

    private static int indexKeyPrefixMatchCount(
            OlapTable table,
            MaterializedIndex index,
            Set<String> equalColNames,
            Set<String> nonEqualColNames) {
        int matchCount = 0;
        for (Column column : table.getSchemaByIndexId(index.getId())) {
            if (equalColNames.contains(column.getName())) {
                matchCount++;
            } else if (nonEqualColNames.contains(column.getName())) {
                // un-equivalence predicate's columns can match only first column in index.
                matchCount++;
                break;
            } else {
                break;
            }
        }
        return matchCount;
    }

    protected static boolean preAggEnabledByHint(LogicalOlapScan olapScan) {
        return olapScan.getHints().stream().anyMatch("PREAGGOPEN"::equalsIgnoreCase);
    }

    public static String normalizeName(String name) {
        return name.replace("`", "").toLowerCase();
    }

    public static Expression slotToCaseWhen(Expression expression) {
        return new CaseWhen(ImmutableList.of(new WhenClause(new IsNull(expression), new TinyIntLiteral((byte) 0))),
                new TinyIntLiteral((byte) 1));
    }

    protected SlotContext generateBaseScanExprToMvExpr(LogicalOlapScan mvPlan) {
        Map<Slot, Slot> baseSlotToMvSlot = new HashMap<>();
        Map<String, Slot> mvNameToMvSlot = new HashMap<>();
        if (mvPlan.getSelectedIndexId() == mvPlan.getTable().getBaseIndexId()) {
            return new SlotContext(baseSlotToMvSlot, mvNameToMvSlot, new TreeSet<Expression>());
        }
        for (Slot mvSlot : mvPlan.getOutputByIndex(mvPlan.getSelectedIndexId())) {
            boolean isPushed = false;
            for (Slot baseSlot : mvPlan.getOutput()) {
                if (org.apache.doris.analysis.CreateMaterializedViewStmt.isMVColumn(mvSlot.getName())) {
                    continue;
                }
                if (baseSlot.toSql().equalsIgnoreCase(
                        org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(
                            normalizeName(mvSlot.getName())))) {
                    baseSlotToMvSlot.put(baseSlot, mvSlot);
                    isPushed = true;
                    break;
                }
            }
            if (!isPushed) {
                if (org.apache.doris.analysis.CreateMaterializedViewStmt.isMVColumn(mvSlot.getName())) {
                    mvNameToMvSlot.put(normalizeName(
                            org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBreaker(mvSlot.getName())),
                            mvSlot);
                }
                mvNameToMvSlot.put(normalizeName(mvSlot.getName()), mvSlot);
            }
        }
        OlapTable table = mvPlan.getTable();
        MaterializedIndexMeta meta = table.getIndexMetaByIndexId(mvPlan.getSelectedIndexId());

        return new SlotContext(baseSlotToMvSlot, mvNameToMvSlot,
                PlanNode.splitAndCompoundPredicateToConjuncts(meta.getWhereClause()).stream()
                        .map(e -> {
                            e.setDisableTableName(true);
                            return e;
                        })
                        .map(e -> new NereidsParser().parseExpression(e.toSql()))
                        .collect(Collectors.toSet()));
    }

    /** SlotContext */
    protected static class SlotContext {

        // base index Slot to selected mv Slot
        public final Map<Slot, Slot> baseSlotToMvSlot;
        // selected mv Slot name to mv Slot, we must use ImmutableSortedMap because column name could be uppercase
        public final ImmutableSortedMap<String, Slot> mvNameToMvSlot;

        public final ImmutableSet<Expression> trueExprs;

        public SlotContext(Map<Slot, Slot> baseSlotToMvSlot, Map<String, Slot> mvNameToMvSlot,
                Set<Expression> trueExprs) {
            this.baseSlotToMvSlot = ImmutableMap.copyOf(baseSlotToMvSlot);
            this.mvNameToMvSlot = ImmutableSortedMap.copyOf(mvNameToMvSlot, String.CASE_INSENSITIVE_ORDER);
            this.trueExprs = ImmutableSet.copyOf(trueExprs);
        }
    }

    /**
     * ReplaceExpressions
     * Notes: For the sum type, the original column and the mv column may have inconsistent types,
     *        but the be side will not check the column type, so it can work normally
     */
    protected static class ReplaceExpressions extends DefaultPlanVisitor<Plan, Void> {
        private final SlotContext slotContext;

        public ReplaceExpressions(SlotContext slotContext) {
            this.slotContext = slotContext;
        }

        public Plan replace(Plan plan, LogicalOlapScan scan) {
            if (scan.getSelectedIndexId() == scan.getTable().getBaseIndexId()) {
                return plan;
            }
            return plan.accept(this, null);
        }

        @Override
        public LogicalAggregate<Plan> visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, Void ctx) {
            Plan child = agg.child(0).accept(this, ctx);
            List<Expression> groupByExprs = agg.getGroupByExpressions();
            List<Expression> newGroupByExprs = groupByExprs.stream()
                    .map(expr -> new ReplaceExpressionWithMvColumn(slotContext).replace(expr))
                    .collect(ImmutableList.toImmutableList());

            List<NamedExpression> outputExpressions = agg.getOutputExpressions();
            List<NamedExpression> newOutputExpressions = outputExpressions.stream()
                    .map(expr -> (NamedExpression) new ReplaceExpressionWithMvColumn(slotContext).replace(expr))
                    .collect(ImmutableList.toImmutableList());

            return agg.withNormalized(newGroupByExprs, newOutputExpressions, child);
        }

        @Override
        public LogicalRepeat<Plan> visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void ctx) {
            Plan child = repeat.child(0).accept(this, ctx);
            List<List<Expression>> groupingSets = repeat.getGroupingSets();
            ImmutableList.Builder<List<Expression>> newGroupingExprs = ImmutableList.builder();
            for (List<Expression> expressions : groupingSets) {
                newGroupingExprs.add(expressions.stream()
                        .map(expr -> new ReplaceExpressionWithMvColumn(slotContext).replace(expr))
                        .collect(ImmutableList.toImmutableList())
                );
            }

            List<NamedExpression> outputExpressions = repeat.getOutputExpressions();
            List<NamedExpression> newOutputExpressions = outputExpressions.stream()
                    .map(expr -> (NamedExpression) new ReplaceExpressionWithMvColumn(slotContext).replace(expr))
                    .collect(ImmutableList.toImmutableList());

            return repeat.withNormalizedExpr(newGroupingExprs.build(), newOutputExpressions, child);
        }

        @Override
        public LogicalFilter<Plan> visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void ctx) {
            Plan child = filter.child(0).accept(this, ctx);
            Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(
                    new ReplaceExpressionWithMvColumn(slotContext).replace(filter.getPredicate())));

            return filter.withConjunctsAndChild(newConjuncts, child);
        }

        @Override
        public LogicalProject<Plan> visitLogicalProject(LogicalProject<? extends Plan> project, Void ctx) {
            Plan child = project.child(0).accept(this, ctx);
            List<NamedExpression> projects = project.getProjects();
            List<NamedExpression> newProjects = projects.stream()
                    .map(expr -> (NamedExpression) new ReplaceExpressionWithMvColumn(slotContext).replace(expr))
                    .collect(ImmutableList.toImmutableList());

            return project.withProjectsAndChild(newProjects, child);
        }

        @Override
        public LogicalOlapScan visitLogicalOlapScan(LogicalOlapScan scan, Void ctx) {
            return (LogicalOlapScan) scan.withGroupExprLogicalPropChildren(scan.getGroupExpression(), Optional.empty(),
                    ImmutableList.of());
        }
    }

    /**
     * ReplaceExpressionWithMvColumn
     */
    protected static class ReplaceExpressionWithMvColumn extends DefaultExpressionRewriter<Void> {

        // base index Slot to selected mv Slot
        private final Map<Slot, Slot> baseSlotToMvSlot;
        // selected mv Slot name to mv Slot,  we must use ImmutableSortedMap because column name could be uppercase
        private final ImmutableSortedMap<String, Slot> mvNameToMvSlot;

        private final ImmutableSet<String> trueExprs;

        public ReplaceExpressionWithMvColumn(SlotContext slotContext) {
            this.baseSlotToMvSlot = ImmutableMap.copyOf(slotContext.baseSlotToMvSlot);
            this.mvNameToMvSlot = ImmutableSortedMap.copyOf(slotContext.mvNameToMvSlot, String.CASE_INSENSITIVE_ORDER);
            this.trueExprs = slotContext.trueExprs.stream().map(e -> e.toSql()).collect(ImmutableSet.toImmutableSet());
        }

        public Expression replace(Expression expression) {
            return expression.accept(this, null);
        }

        @Override
        public Expression visit(Expression expr, Void context) {
            if (notUseMv() || org.apache.doris.analysis.CreateMaterializedViewStmt.isMVColumn(expr.toSql())) {
                return expr;
            } else if (trueExprs.contains(expr.toSql())) {
                return BooleanLiteral.TRUE;
            } else if (checkExprIsMvColumn(expr)) {
                return mvNameToMvSlot
                        .get(org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBuilder(expr.toSql()));
            } else {
                expr = super.visit(expr, context);
                return expr;
            }
        }

        @Override
        public Expression visitSlotReference(SlotReference slotReference, Void context) {
            if (baseSlotToMvSlot.containsKey(slotReference)) {
                return baseSlotToMvSlot.get(slotReference);
            }
            if (mvNameToMvSlot.containsKey(slotReference.toSql())) {
                return mvNameToMvSlot.get(slotReference.toSql());
            }
            return slotReference;
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction, Void context) {
            String childrenName = aggregateFunction.children()
                    .stream()
                    .map(Expression::toSql)
                    .collect(Collectors.joining(", "));
            String mvName = org.apache.doris.analysis.CreateMaterializedViewStmt.mvAggregateColumnBuilder(
                    aggregateFunction.getName(), childrenName);
            if (mvNameToMvSlot.containsKey(mvName)) {
                return aggregateFunction.withChildren(mvNameToMvSlot.get(mvName));
            } else if (mvNameToMvSlot.containsKey(childrenName)) {
                // aggRewrite eg: bitmap_union_count -> bitmap_union
                return aggregateFunction.withChildren(mvNameToMvSlot.get(childrenName));
            }
            return visit(aggregateFunction, context);
        }

        @Override
        public Expression visitScalarFunction(ScalarFunction scalarFunction, Void context) {
            List<Expression> newChildrenWithoutCast = scalarFunction.children().stream()
                    .map(child -> {
                        if (child instanceof Cast) {
                            return ((Cast) child).child();
                        }
                        return child;
                    }).collect(ImmutableList.toImmutableList());
            Expression newScalarFunction = scalarFunction.withChildren(newChildrenWithoutCast);
            if (checkExprIsMvColumn(newScalarFunction)) {
                return mvNameToMvSlot.get(
                    org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBuilder(newScalarFunction.toSql()));
            }
            return visit(scalarFunction, context);
        }

        private boolean notUseMv() {
            return baseSlotToMvSlot.isEmpty() && mvNameToMvSlot.isEmpty();
        }

        private boolean checkExprIsMvColumn(Expression expr) {
            return mvNameToMvSlot.containsKey(
                org.apache.doris.analysis.CreateMaterializedViewStmt.mvColumnBuilder(expr.toSql()));
        }
    }

    protected List<NamedExpression> generateProjectsAlias(List<? extends NamedExpression> oldProjects,
            SlotContext slotContext) {
        return oldProjects.stream().map(e -> {
            Expression real = e;
            if (real instanceof Alias) {
                real = real.child(0);
            }
            if (slotContext.baseSlotToMvSlot.containsKey(e.toSlot())) {
                return new Alias(e.getExprId(), slotContext.baseSlotToMvSlot.get(e.toSlot()), e.getName());
            }
            if (slotContext.mvNameToMvSlot.containsKey(real.toSql())) {
                return new Alias(e.getExprId(), slotContext.mvNameToMvSlot.get(real.toSql()), e.getName());
            }
            return e.toSlot();
        }).collect(ImmutableList.toImmutableList());
    }
}
