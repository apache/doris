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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Partition Increment Maintainer, this is used to check whether the materialized view can be partition level
 * increment maintained
 */
public class PartitionIncrementMaintainer {

    /**
     * Partition Increment Checker
     */
    public static class PartitionIncrementChecker extends
            DefaultPlanVisitor<Void, PartitionIncrementCheckContext> {
        public static final PartitionIncrementChecker INSTANCE = new PartitionIncrementChecker();
        public static final Set<Class<? extends Expression>> SUPPORT_EXPRESSION_TYPES =
                ImmutableSet.of(DateTrunc.class, SlotReference.class, Literal.class);

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project,
                PartitionIncrementCheckContext context) {
            List<Slot> output = project.getOutput();
            boolean isValid = checkPartition(output, project, context);
            if (!isValid) {
                return null;
            }
            return visit(project, context);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, PartitionIncrementCheckContext context) {
            return visit(filter, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                PartitionIncrementCheckContext context) {
            if (join.isMarkJoin()) {
                context.addFailReason("partition track doesn't support mark join");
                return null;
            }
            Plan left = join.child(0);
            Set<Column> leftColumnSet = left.getOutputSet().stream()
                    .filter(slot -> slot instanceof SlotReference && slot.isColumnFromTable())
                    .map(slot -> ((SlotReference) slot).getOriginalColumn().get())
                    .collect(Collectors.toSet());
            Set<SlotReference> partitionColumnSetToCheck = new HashSet<>(getPartitionColumnsToCheck(context));
            if (partitionColumnSetToCheck.isEmpty()) {
                return null;
            }
            for (SlotReference partitionSlotToCheck : partitionColumnSetToCheck) {
                Pair<Set<Slot>, Set<Slot>> partitionEqualSlotPair = calEqualSet(partitionSlotToCheck, join);
                if (!partitionEqualSlotPair.value().isEmpty()) {
                    context.getShuttledEqualSlotSet().add(partitionEqualSlotPair.value());
                }
                for (Slot partitionEqualSlot : partitionEqualSlotPair.key()) {
                    // If found equal set, add the slot and ref expression to checker context
                    Optional<Expression> replacedPartitionExpression = replace(partitionEqualSlot, partitionSlotToCheck,
                            context.getPartitionAndRefExpressionMap().get(partitionSlotToCheck)
                                    .getPartitionExpression());
                    context.getPartitionAndRefExpressionMap().put(partitionEqualSlot,
                            RelatedTableColumnInfo.of(partitionEqualSlot, replacedPartitionExpression.orElse(null),
                                    false, false));
                }
                boolean useLeft = leftColumnSet.contains(partitionSlotToCheck.getOriginalColumn().orElse(null));
                JoinType joinType = join.getJoinType();
                if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
                    return visit(join, context);
                } else if ((joinType.isLeftJoin()
                        || joinType.isLeftSemiJoin()
                        || joinType.isLeftAntiJoin()) && useLeft) {
                    context.collectInvalidTableSet(join.right());
                    return visit(join, context);
                } else if ((joinType.isRightJoin()
                        || joinType.isRightAntiJoin()
                        || joinType.isRightSemiJoin()) && !useLeft) {
                    context.collectInvalidTableSet(join.left());
                    return visit(join, context);
                } else {
                    context.addFailReason(String.format("partition column is in un supported join null generate side, "
                            + "current join type is %s, partitionSlot is %s", joinType, partitionSlotToCheck));
                }
            }
            return null;
        }

        @Override
        public Void visitLogicalRelation(LogicalRelation relation, PartitionIncrementCheckContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                context.addFailReason(String.format("relation should be LogicalCatalogRelation, "
                        + "but now is %s", relation.getClass().getSimpleName()));
                return null;
            }
            Set<SlotReference> partitionColumnsToCheck = getPartitionColumnsToCheck(context);
            if (partitionColumnsToCheck.isEmpty()) {
                context.addFailReason("mv partition column is not from table when relation check, "
                        + "Or the partition column is in the invalid table");
                return null;
            }
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            if (!(table instanceof MTMVRelatedTableIf)) {
                context.addFailReason(String.format("relation base table is not MTMVRelatedTableIf, the table is %s",
                        table.getName()));
                return null;
            }
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            PartitionType type = relatedTable.getPartitionType(MvccUtil.getSnapshotFromContext(relatedTable));
            if (PartitionType.UNPARTITIONED.equals(type)) {
                context.addFailReason(String.format("related base table is not partition table, the table is %s",
                        table.getName()));
                return null;
            }
            Set<Column> relatedTablePartitionColumnSet = new HashSet<>(relatedTable.getPartitionColumns(
                    MvccUtil.getSnapshotFromContext(relatedTable)));
            for (Map.Entry<NamedExpression, RelatedTableColumnInfo> partitionColumnEntry
                    : context.getPartitionAndRefExpressionMap().entrySet()) {
                NamedExpression partitionNamedExpression = partitionColumnEntry.getKey();
                if (!(partitionNamedExpression instanceof SlotReference)) {
                    continue;
                }
                SlotReference partitionColumn = (SlotReference) partitionNamedExpression;
                if (!partitionColumn.getOriginalTable().map(TableIf::getFullQualifiers).orElse(ImmutableList.of())
                        .equals(((LogicalCatalogRelation) relation).getTable().getFullQualifiers())) {
                    // mv partition column name is not belonged to current table, continue check
                    continue;
                }
                Column mvReferenceColumnToCheck = partitionColumnEntry.getValue().getColumn();
                if (relatedTablePartitionColumnSet.contains(mvReferenceColumnToCheck)
                        && (!mvReferenceColumnToCheck.isAllowNull() || relatedTable.isPartitionColumnAllowNull())) {
                    SlotReference currentPartitionSlot = null;
                    for (Slot catalogSlot : logicalCatalogRelation.getOutputSet()) {
                        if (catalogSlot instanceof SlotReference
                                && Objects.equals(((SlotReference) catalogSlot).getOriginalColumn().orElse(null),
                                mvReferenceColumnToCheck)) {
                            currentPartitionSlot = (SlotReference) catalogSlot;
                        }
                    }
                    // If self join such as inner join or partition is in invalid side such as
                    // null generate side(outer join),
                    // should also check the partition column is in the shuttled equal set
                    boolean catalogChecked = context.getPartitionAndRefExpressionMap().values().stream()
                            .anyMatch(tableColumnInfo ->
                                    tableColumnInfo.isFromTablePartitionColumn()
                                            && new BaseTableInfo(((LogicalCatalogRelation) relation).getTable())
                                            .equals(tableColumnInfo.getTableInfo())
                            );
                    if (catalogChecked || context.getInvalidCatalogRelationToCheck().contains(relation)) {
                        boolean checkSuccess = false;
                        for (Set<Slot> equalSlotSet : context.getShuttledEqualSlotSet()) {
                            checkSuccess = equalSlotSet.contains(partitionColumn)
                                    && equalSlotSet.contains(currentPartitionSlot);
                            if (checkSuccess) {
                                break;
                            }
                        }
                        if (!checkSuccess) {
                            context.addFailReason(String.format("partition column is in join invalid side, "
                                            + "but is not in join condition, the mvReferenceColumnToCheck is %s",
                                    mvReferenceColumnToCheck));
                            context.getPartitionAndRefExpressionMap().remove(partitionColumn);
                            continue;
                        }
                    }
                    RelatedTableColumnInfo tableColumnInfo =
                            context.getPartitionAndRefExpressionMap().get(partitionColumn);
                    tableColumnInfo.setFromTablePartitionColumn(true);
                } else {
                    context.addFailReason(String.format("related base table partition column doesn't contain the mv"
                                    + " partition or partition nullable check fail, the mvReferenceColumnToCheck is %s",
                            mvReferenceColumnToCheck));
                }
            }
            return visit(relation, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                PartitionIncrementCheckContext context) {
            Set<Expression> groupByExprSet = new HashSet<>(aggregate.getGroupByExpressions());
            if (groupByExprSet.isEmpty()) {
                context.addFailReason("group by sets is empty, doesn't contain the target partition");
                return null;
            }
            boolean isValid = checkPartition(groupByExprSet, aggregate, context);
            if (!isValid) {
                return null;
            }
            return visit(aggregate, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, PartitionIncrementCheckContext context) {
            List<NamedExpression> windowExpressions = window.getWindowExpressions();
            if (windowExpressions.isEmpty()) {
                return visit(window, context);
            }
            for (NamedExpression namedExpression : windowExpressions) {
                if (!checkWindowPartition(namedExpression, context)) {
                    context.addFailReason("window partition sets doesn't contain the target partition");
                    return null;
                }
            }
            return super.visitLogicalWindow(window, context);
        }

        @Override
        public Void visit(Plan plan, PartitionIncrementCheckContext context) {
            if (plan instanceof LogicalProject
                    || plan instanceof LogicalLimit
                    || plan instanceof LogicalFilter
                    || plan instanceof LogicalJoin
                    || plan instanceof LogicalAggregate
                    || plan instanceof LogicalCatalogRelation
                    || plan instanceof LogicalResultSink
                    || plan instanceof LogicalWindow) {
                return super.visit(plan, context);
            }
            context.addFailReason(String.format("Unsupported plan operate in track partition, "
                    + "the invalid plan node is %s", plan.getClass().getSimpleName()));
            return null;
        }

        private boolean checkWindowPartition(Expression expression, PartitionIncrementCheckContext context) {
            List<Object> windowExpressions =
                    expression.collectToList(expressionTreeNode -> expressionTreeNode instanceof WindowExpression);
            for (Object windowExpressionObj : windowExpressions) {
                WindowExpression windowExpression = (WindowExpression) windowExpressionObj;
                List<Expression> partitionKeys = windowExpression.getPartitionKeys();
                Set<Column> originalPartitionbyExprSet = new HashSet<>();
                partitionKeys.forEach(groupExpr -> {
                    if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                        originalPartitionbyExprSet.add(((SlotReference) groupExpr).getOriginalColumn().get());
                    }
                });
                Set<SlotReference> contextPartitionColumnSet = getPartitionColumnsToCheck(context);
                if (contextPartitionColumnSet.isEmpty()) {
                    return false;
                }
                if (contextPartitionColumnSet.stream().noneMatch(
                        partition -> originalPartitionbyExprSet.contains(partition.getOriginalColumn().get()))) {
                    return false;
                }
            }
            return true;
        }

        private Set<SlotReference> getPartitionColumnsToCheck(PartitionIncrementCheckContext context) {
            Set<NamedExpression> partitionExpressionSet = context.getPartitionAndRefExpressionMap().keySet();
            Set<SlotReference> partitionSlotSet = new HashSet<>();
            for (NamedExpression namedExpression : partitionExpressionSet) {
                if (!namedExpression.isColumnFromTable()) {
                    context.addFailReason(String.format("context partition column should be slot from column, "
                            + "context column is %s", namedExpression));
                    continue;
                }
                partitionSlotSet.add((SlotReference) namedExpression);
            }
            return partitionSlotSet;
        }

        /**
         * Given a partition named expression and expressionsToCheck, check the partition is valid
         * example 1:
         * partition expression is date_trunc(date_alias#25, 'hour') AS `date_trunc(date_alias, 'hour')`#30
         * expressionsToCheck is date_trunc(date_alias, 'hour')#30
         * expressionsToCheck is the slot to partition expression, but they are expression
         * example 2:
         * partition expression is L_SHIPDATE#10
         * expressionsToCheck isL_SHIPDATE#10
         * both of them are slot
         * example 3:
         * partition expression is date_trunc(L_SHIPDATE#10, 'hour')#30
         * expressionsToCheck is L_SHIPDATE#10
         * all above should check successfully
         */
        private static boolean checkPartition(Collection<? extends Expression> expressionsToCheck, Plan plan,
                PartitionIncrementCheckContext context) {
            Set<Entry<NamedExpression, RelatedTableColumnInfo>> partitionAndExprEntrySet
                    = new HashSet<>(context.getPartitionAndRefExpressionMap().entrySet());
            for (Map.Entry<NamedExpression, RelatedTableColumnInfo> partitionExpressionEntry
                    : partitionAndExprEntrySet) {
                NamedExpression partitionNamedExpression = partitionExpressionEntry.getKey();
                RelatedTableColumnInfo partitionTableColumnInfo = partitionExpressionEntry.getValue();
                Optional<Expression> partitionExpressionOpt = partitionTableColumnInfo.getPartitionExpression();

                OUTER_CHECK:
                for (Expression projectSlotToCheck : expressionsToCheck) {
                    if (partitionNamedExpression.isColumnFromTable() && projectSlotToCheck.isColumnFromTable()
                            && projectSlotToCheck.equals(partitionNamedExpression.toSlot())) {
                        continue;
                    }
                    // check the expression which use partition column
                    Expression expressionShuttledToCheck =
                            ExpressionUtils.shuttleExpressionWithLineage(projectSlotToCheck, plan, new BitSet());
                    // merge date_trunc
                    expressionShuttledToCheck = new ExpressionNormalization().rewrite(expressionShuttledToCheck,
                            new ExpressionRewriteContext(context.getCascadesContext()));

                    Expression partitionExpressionActual = partitionExpressionOpt.orElseGet(
                            () -> ExpressionUtils.shuttleExpressionWithLineage(partitionNamedExpression, plan,
                                    new BitSet()));
                    // merge date_trunc
                    partitionExpressionActual = new ExpressionNormalization().rewrite(partitionExpressionActual,
                            new ExpressionRewriteContext(context.getCascadesContext()));

                    Set<SlotReference> expressionToCheckSlots =
                            expressionShuttledToCheck.collectToSet(SlotReference.class::isInstance);
                    Set<SlotReference> partitionColumnSlots =
                            partitionExpressionActual.collectToSet(SlotReference.class::isInstance);
                    if (Sets.intersection(expressionToCheckSlots, partitionColumnSlots).isEmpty()
                            || expressionToCheckSlots.isEmpty() || partitionColumnSlots.isEmpty()) {
                        // this expression doesn't use partition column
                        continue;
                    }
                    if (expressionToCheckSlots.size() != 1 || partitionColumnSlots.size() != 1) {
                        context.addFailReason(
                                String.format("partition expression use more than one slot reference, invalid "
                                                + "expressionToCheckSlots is %s, partitionColumnDateColumns is %s",
                                        expressionToCheckSlots, partitionColumnSlots));
                        continue;
                    }
                    List<Expression> expressionsToCheckList = expressionShuttledToCheck.collectToList(
                            Expression.class::isInstance);
                    for (Expression expression : expressionsToCheckList) {
                        if (SUPPORT_EXPRESSION_TYPES.stream().noneMatch(
                                supportExpression -> supportExpression.isAssignableFrom(expression.getClass()))) {
                            context.addFailReason(
                                    String.format("column to check use invalid implicit expression, invalid "
                                            + "expression is %s", expression));
                            continue OUTER_CHECK;
                        }
                    }
                    List<Expression> partitionExpressionList = partitionExpressionActual.collectToList(
                            Expression.class::isInstance);
                    for (Expression expression : partitionExpressionList) {
                        if (SUPPORT_EXPRESSION_TYPES.stream().noneMatch(
                                supportExpression -> supportExpression.isAssignableFrom(expression.getClass()))) {
                            context.addFailReason(
                                    String.format("partition column use invalid implicit expression, invalid "
                                            + "expression is %s", expression));
                            continue OUTER_CHECK;
                        }
                    }
                    List<DateTrunc> expressionToCheckDataTruncList =
                            expressionShuttledToCheck.collectToList(DateTrunc.class::isInstance);
                    List<DateTrunc> partitionExpressionDateTrucList =
                            partitionExpressionActual.collectToList(DateTrunc.class::isInstance);
                    if (expressionToCheckDataTruncList.size() > 1 || partitionExpressionDateTrucList.size() > 1) {
                        // mv time unit level is little then query
                        context.addFailReason("partition column time unit level should be "
                                + "greater than sql select column");
                        continue;
                    }
                    SlotReference checkedPartitionSlot = partitionColumnSlots.iterator().next();
                    if (!partitionNamedExpression.isColumnFromTable()
                            || !context.getPartitionAndRefExpressionMap().get(partitionNamedExpression)
                            .getPartitionExpression().isPresent()) {
                        context.getPartitionAndRefExpressionMap().put(checkedPartitionSlot,
                                RelatedTableColumnInfo.of(checkedPartitionSlot, partitionExpressionActual,
                                        partitionTableColumnInfo.isOriginalPartition(),
                                        partitionTableColumnInfo.isFromTablePartitionColumn()));
                    }
                }
            }
            return context.getPartitionAndRefExpressionMap().keySet().stream().anyMatch(
                    Expression::isColumnFromTable);
        }
    }

    /**
     * The context used in IncrementChecker
     */
    public static final class PartitionIncrementCheckContext {
        // This is used to record partition slot, and the value of map is ref date expression and bool value which
        // identify it's original partition or not, the key of map is the namedExpression to check
        private final Map<NamedExpression, RelatedTableColumnInfo> partitionAndRefExpressionMap
                = new HashMap<>();
        private final Set<String> failReasons = new HashSet<>();
        private final CascadesContext cascadesContext;
        // This record the invalid table, such as the right side of left join, the partition column
        // is invalid if is form the table when partition column is not in shuttledEqualSlotSet
        private final Set<LogicalCatalogRelation> invalidCatalogRelationToCheck = new HashSet<>();
        // This is used to record the equal slot set shuttled from children which are equals to partition column
        // to check, this expends the partition slot to check
        private final Set<Set<Slot>> shuttledEqualSlotSet = new HashSet<>();

        public PartitionIncrementCheckContext(NamedExpression mvPartitionColumn,
                CascadesContext cascadesContext) {
            this.partitionAndRefExpressionMap.put(mvPartitionColumn, RelatedTableColumnInfo.of(
                    mvPartitionColumn, null, true, false));
            this.cascadesContext = cascadesContext;
        }

        public Set<String> getFailReasons() {
            return failReasons;
        }

        public void addFailReason(String failReason) {
            this.failReasons.add(failReason);
        }

        public Set<LogicalCatalogRelation> getInvalidCatalogRelationToCheck() {
            return invalidCatalogRelationToCheck;
        }

        public CascadesContext getCascadesContext() {
            return cascadesContext;
        }

        public Set<Set<Slot>> getShuttledEqualSlotSet() {
            return shuttledEqualSlotSet;
        }

        public Map<NamedExpression, RelatedTableColumnInfo> getPartitionAndRefExpressionMap() {
            return partitionAndRefExpressionMap;
        }

        /**
         * collect invalid table set to check self join
         */
        public void collectInvalidTableSet(Plan plan) {
            plan.accept(new DefaultPlanVisitor<Void, Set<LogicalCatalogRelation>>() {
                @Override
                public Void visitLogicalRelation(LogicalRelation relation,
                        Set<LogicalCatalogRelation> invalidTableSet) {
                    if (relation instanceof LogicalCatalogRelation) {
                        invalidTableSet.add((LogicalCatalogRelation) relation);
                    }
                    return null;
                }
            }, this.invalidCatalogRelationToCheck);
        }
    }

    /**
     * Add partitionEqualSlot to partitionAndRefExpressionToCheck if partitionExpression use the partitionSlot
     */
    private static Optional<Expression> replace(NamedExpression partitionEqualSlot,
            NamedExpression partitionSlot,
            Optional<Expression> partitionExpression) {
        if (Objects.equals(partitionSlot, partitionEqualSlot)) {
            return partitionExpression;
        }
        if (!partitionExpression.isPresent()) {
            return Optional.empty();
        }
        // Replace partitionSlot in partitionExpression with partitionEqualSlot
        Expression replacedExpression = partitionExpression.map(
                partitionExpr -> partitionExpr.accept(new DefaultExpressionRewriter<Void>() {
                    @Override
                    public Expression visitNamedExpression(NamedExpression namedExpression, Void context) {
                        if (namedExpression.equals(partitionSlot)) {
                            return partitionEqualSlot;
                        }
                        return namedExpression;
                    }
                }, null)).orElse(null);
        // if replacedExpression doesn't contain partitionSlot which means replace successfully, then add
        if (replacedExpression == null) {
            return Optional.empty();
        }
        Set<NamedExpression> partitionSlotSet =
                replacedExpression.collectToSet(expr -> expr.equals(partitionSlot));
        if (partitionSlotSet.isEmpty()) {
            // If replaced successfully, then add partitionEqualSlot to partition and ref
            // expression map to check
            return Optional.of(replacedExpression);
        }
        return Optional.empty();
    }

    /**
     * the key of result is the equal slot set to slot, which are not shuttled
     * the value of result is the equal slot set to slot, which are shuttled from children
     * the key equal set should not contain the slot itself
     * the value equal set contain the slot itself
     */
    private static Pair<Set<Slot>, Set<Slot>> calEqualSet(Slot slot,
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        Set<Slot> partitionEqualSlotSet = new HashSet<>();
        JoinType joinType = join.getJoinType();
        if (joinType.isInnerJoin() || joinType.isSemiJoin()) {
            partitionEqualSlotSet.addAll(join.getLogicalProperties().getTrait().calEqualSet(slot));
        }
        if (joinType.isOuterJoin()) {
            ImmutableEqualSet.Builder<Slot> builder = new ImmutableEqualSet.Builder<>();
            join.getHashJoinConjuncts().stream()
                    .filter(e -> e instanceof EqualPredicate
                            && e.child(0) instanceof Slot
                            && e.child(1) instanceof Slot)
                    .forEach(e ->
                            builder.addEqualPair((Slot) e.child(0), (Slot) e.child(1)));
            ImmutableEqualSet<Slot> equalSet = builder.build();
            partitionEqualSlotSet.addAll(equalSet.calEqualSet(slot));
        }
        // construct shuttled partitionEqualSlotSet
        Set<Slot> shuttledPartitionEqualSlotSet = new HashSet<>();
        if (partitionEqualSlotSet.isEmpty()) {
            return Pair.of(partitionEqualSlotSet, shuttledPartitionEqualSlotSet);
        }
        List<Expression> extendedPartitionEqualSlotSet = new ArrayList<>(partitionEqualSlotSet);
        extendedPartitionEqualSlotSet.add(slot);
        List<? extends Expression> shuttledEqualExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                extendedPartitionEqualSlotSet, join, new BitSet());
        for (Expression shuttledEqualExpression : shuttledEqualExpressions) {
            Set<Slot> objects = shuttledEqualExpression.collectToSet(expr -> expr instanceof SlotReference);
            if (objects.size() != 1 || !(shuttledEqualExpression instanceof SlotReference)) {
                continue;
            }
            shuttledPartitionEqualSlotSet.add((Slot) shuttledEqualExpression);
        }
        return Pair.of(partitionEqualSlotSet, shuttledPartitionEqualSlotSet);
    }

    /**
     * Remove the sink node from materialized view plan
     */
    public static Plan removeSink(Plan materializedViewPlan) {
        return materializedViewPlan.accept(new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, Void context) {
                return new LogicalProject<>(logicalSink.getOutputExprs(), logicalSink.child());
            }
        }, null);
    }
}
