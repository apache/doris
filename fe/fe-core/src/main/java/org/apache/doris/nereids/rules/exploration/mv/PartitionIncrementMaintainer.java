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
import org.apache.doris.nereids.trees.expressions.CTEId;
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
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
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
                context.collectFailedTableSet(project);
            }
            return visit(project, context);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, PartitionIncrementCheckContext context) {
            return visit(filter, context);
        }

        @Override
        public Void visitLogicalUnion(LogicalUnion union, PartitionIncrementCheckContext context) {
            Set<NamedExpression> checkingNamedExpressionSet = new HashSet<>(
                    context.getPartitionAndRefExpressionMap().keySet());
            int index = -1;
            List<Slot> output = union.getOutput();
            for (int j = 0; j < output.size(); j++) {
                if (checkingNamedExpressionSet.contains(output.get(j))) {
                    index = j;
                    break;
                }
            }
            if (index == -1) {
                context.addFailReason("union all output doesn't contain the target partition");
                context.collectFailedTableSet(union);
                return null;
            }
            List<Plan> children = union.children();
            List<PartitionIncrementCheckContext> childrenContextList = new ArrayList<>();
            Slot unionSlotToCheck = output.get(index);
            for (int i = 0; i < children.size(); i++) {
                List<SlotReference> regularChildOutput = union.getRegularChildOutput(i);
                SlotReference childMvPartitionSlot = regularChildOutput.get(index);
                Optional<Expression> childPartitionExpression = replace(childMvPartitionSlot, unionSlotToCheck,
                        context.getPartitionAndRefExpressionMap().get(unionSlotToCheck)
                                .getPartitionExpression());
                PartitionIncrementCheckContext childContext = new PartitionIncrementCheckContext(childMvPartitionSlot,
                        childPartitionExpression.orElse(null), context.getProducerCteIdToPlanMap(),
                        children.get(i), context.getCascadesContext());
                children.get(i).accept(this, childContext);
                childrenContextList.add(childContext);
            }
            boolean allReachRelationCheck = true;
            boolean allIsFromTablePartitionColumn = true;
            for (PartitionIncrementCheckContext childContext : childrenContextList) {
                boolean childAnyIsFromTablePartitionColumn = false;
                boolean childAnyReachRelationCheck = false;
                for (RelatedTableColumnInfo tableColumnInfo : childContext.getPartitionAndRefExpressionMap().values()) {
                    if (tableColumnInfo.isReachRelationCheck()) {
                        childAnyReachRelationCheck = true;
                    }
                    childAnyIsFromTablePartitionColumn
                            = childAnyIsFromTablePartitionColumn || tableColumnInfo.isFromTablePartitionColumn();
                }
                if (!childAnyReachRelationCheck) {
                    context.addFailReason(String.format(
                            "union all output doesn't match the partition increment check, fail reason is %s",
                            childContext.getFailReasons()));
                    allReachRelationCheck = false;
                    break;
                }
                allIsFromTablePartitionColumn = allIsFromTablePartitionColumn && childAnyIsFromTablePartitionColumn;
            }
            if (allReachRelationCheck && allIsFromTablePartitionColumn) {
                childrenContextList.forEach(
                        childContext -> context.getPartitionAndRefExpressionMap().putAll(
                                childContext.getPartitionAndRefExpressionMap()));
            } else {
                context.collectFailedTableSet(union);
                context.addFailReason("not union all output pass partition increment check");
            }
            return super.visit(union, context);
        }

        @Override
        public Void visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, PartitionIncrementCheckContext context) {
            Plan producerPlan = context.getProducerCteIdToPlanMap().get(cteConsumer.getCteId());
            if (producerPlan == null) {
                context.addFailReason(String.format("can't find cte producer producerPlan by cte id %s",
                        cteConsumer.getCteId()));
                return null;
            }
            Map<Slot, Slot> consumerToProducerOutputMap = cteConsumer.getConsumerToProducerOutputMap();
            Map<NamedExpression, RelatedTableColumnInfo> needAddMap = new HashMap<>();
            for (Map.Entry<NamedExpression, RelatedTableColumnInfo> entry
                    : context.getPartitionAndRefExpressionMap().entrySet()) {
                NamedExpression consumerSlot = entry.getKey();
                Slot producerSlot = consumerToProducerOutputMap.get(consumerSlot);
                if (producerSlot == null) {
                    continue;
                }
                needAddMap.put(producerSlot, RelatedTableColumnInfo.of(producerSlot,
                        replace(producerSlot, consumerSlot, entry.getValue().getPartitionExpression()).orElse(null),
                        entry.getValue().isOriginalPartition(),
                        entry.getValue().isFromTablePartitionColumn()));
                // clac the equal set in context
                Set<Set<Slot>> shuttledEqualSlotSet = context.getShuttledEqualSlotSet();
                for (Set<Slot> equalSlotSet : shuttledEqualSlotSet) {
                    if (equalSlotSet.contains(consumerSlot)) {
                        Expression shuttledSlot = ExpressionUtils.shuttleExpressionWithLineage(
                                producerSlot, producerPlan);
                        if (shuttledSlot instanceof Slot) {
                            equalSlotSet.add((Slot) shuttledSlot);
                        }
                    }
                }
            }
            if (!needAddMap.isEmpty()) {
                context.getPartitionAndRefExpressionMap().putAll(needAddMap);
            }
            return super.visit(producerPlan, context);
        }

        @Override
        public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
                PartitionIncrementCheckContext context) {
            // should visit by logical cte consumer
            return null;
        }

        @Override
        public Void visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
                PartitionIncrementCheckContext context) {
            return super.visitLogicalCTEAnchor(cteAnchor, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                PartitionIncrementCheckContext context) {
            if (join.isMarkJoin()) {
                context.addFailReason("partition track doesn't support mark join");
                return null;
            }
            // calculate equal slot set from join condition
            Map<NamedExpression, RelatedTableColumnInfo> toAdd = new HashMap<>();
            for (NamedExpression partitionSlotToCheck : context.getPartitionAndRefExpressionMap().keySet()) {
                if (!(partitionSlotToCheck instanceof SlotReference)) {
                    continue;
                }
                Pair<Set<Slot>, Set<Slot>> partitionEqualSlotPair =
                        calEqualSet((SlotReference) partitionSlotToCheck, join);
                if (!partitionEqualSlotPair.value().isEmpty()) {
                    context.getShuttledEqualSlotSet().add(partitionEqualSlotPair.value());
                }
                for (Slot partitionEqualSlot : partitionEqualSlotPair.key()) {
                    // If equal slot set founded, add the slot and ref expression to checker context
                    Optional<Expression> replacedPartitionExpression = replace(partitionEqualSlot, partitionSlotToCheck,
                            context.getPartitionAndRefExpressionMap().get(partitionSlotToCheck)
                                    .getPartitionExpression());
                    toAdd.put(partitionEqualSlot,
                            RelatedTableColumnInfo.of(partitionEqualSlot, replacedPartitionExpression.orElse(null),
                                    false, false));
                }
            }
            if (!toAdd.isEmpty()) {
                context.getPartitionAndRefExpressionMap().putAll(toAdd);
            }
            // check join type and partition column side
            Set<Slot> leftColumnSet = join.child(0).getOutputSet();
            Set<NamedExpression> namedExpressions = new HashSet<>(context.getPartitionAndRefExpressionMap().keySet());
            for (NamedExpression partitionSlotToCheck : namedExpressions) {
                if (!(partitionSlotToCheck instanceof SlotReference)) {
                    continue;
                }
                boolean useLeft = leftColumnSet.contains(partitionSlotToCheck);
                JoinType joinType = join.getJoinType();
                if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
                    visit(join, context);
                } else if ((joinType.isLeftJoin()
                        || joinType.isLeftSemiJoin()
                        || joinType.isLeftAntiJoin()) && useLeft) {
                    context.collectInvalidTableSet(join.right());
                    visit(join, context);
                } else if ((joinType.isRightJoin()
                        || joinType.isRightAntiJoin()
                        || joinType.isRightSemiJoin()) && !useLeft) {
                    context.collectInvalidTableSet(join.left());
                    visit(join, context);
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
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            if (!(table instanceof MTMVRelatedTableIf)) {
                context.addFailReason(String.format("relation base table is not MTMVRelatedTableIf, the table is %s",
                        table.getName()));
                return null;
            }
            List<RelatedTableColumnInfo> relatedTableColumnInfosByTable = getRelatedTableColumnInfosByTable(context,
                    new BaseTableInfo(table));
            // mark reach relation check
            if (!context.getInvalidCatalogRelationToCheck().contains(relation)
                    && !context.getShouldFailCatalogRelation().contains(relation)) {
                relatedTableColumnInfosByTable.forEach(
                        tableColumnInfo -> tableColumnInfo.setReachRelationCheck(true));
            }
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            PartitionType type = relatedTable.getPartitionType(MvccUtil.getSnapshotFromContext(relatedTable));
            if (PartitionType.UNPARTITIONED.equals(type)) {
                context.addFailReason(String.format("related base table is not partition table, the table is %s",
                        table.getName()));
                return null;
            }
            Set<String> relatedTablePartitionColumnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            relatedTablePartitionColumnSet.addAll(relatedTable.getPartitionColumns(
                    MvccUtil.getSnapshotFromContext(relatedTable)).stream()
                    .map(Column::getName).collect(Collectors.toSet()));
            for (Map.Entry<NamedExpression, RelatedTableColumnInfo> contextPartitionColumnEntry
                    : context.getPartitionAndRefExpressionMap().entrySet()) {
                NamedExpression partitionNamedExpression = contextPartitionColumnEntry.getKey();
                if (!(partitionNamedExpression instanceof SlotReference)) {
                    continue;
                }
                SlotReference contextPartitionColumn = (SlotReference) partitionNamedExpression;
                if (!contextPartitionColumn.getOriginalTable()
                        .map(TableIf::getFullQualifiers).orElse(ImmutableList.of())
                        .equals(((LogicalCatalogRelation) relation).getTable().getFullQualifiers())) {
                    // mv partition column name is not belonged to current table, continue check
                    continue;
                }
                RelatedTableColumnInfo mvReferenceColumnInfo = contextPartitionColumnEntry.getValue();
                Column mvReferenceColumnToCheck = mvReferenceColumnInfo.getColumn();
                if (mvReferenceColumnToCheck == null) {
                    context.addFailReason(String.format("related base table mvReferenceColumnToCheck is null"
                                    + " mvReferenceColumnInfo is %s", mvReferenceColumnInfo));
                    continue;
                }
                if (!relatedTablePartitionColumnSet.contains(mvReferenceColumnToCheck.getName())) {
                    context.addFailReason(String.format("related base table partition column doesn't contain the mv"
                                    + " partition, the mvReferenceColumnToCheck is %s",
                            mvReferenceColumnToCheck));
                    continue;
                }
                if ((mvReferenceColumnToCheck.isAllowNull() && !relatedTable.isPartitionColumnAllowNull())) {
                    context.addFailReason(String.format("related base table partition column"
                                    + " partition nullable check fail, the mvReferenceColumnToCheck is %s",
                            mvReferenceColumnToCheck));
                    continue;
                }
                SlotReference currentPartitionSlot = null;
                for (Slot catalogSlot : logicalCatalogRelation.getOutputSet()) {
                    if (catalogSlot instanceof SlotReference
                            && Objects.equals(((SlotReference) catalogSlot).getOriginalColumn().orElse(null),
                            mvReferenceColumnToCheck)) {
                        currentPartitionSlot = (SlotReference) catalogSlot;
                        break;
                    }
                }
                // If self join such as inner join
                // should also check the partition column is in the shuttled equal set
                boolean tableHasChecked = context.getPartitionAndRefExpressionMap().values().stream()
                        .anyMatch(tableColumnInfo ->
                                tableColumnInfo.isFromTablePartitionColumn()
                                        && new BaseTableInfo(((LogicalCatalogRelation) relation).getTable())
                                        .equals(tableColumnInfo.getTableInfo())
                        );
                if (tableHasChecked || context.getInvalidCatalogRelationToCheck().contains(relation)) {
                    boolean checkSuccess = false;
                    for (Set<Slot> equalSlotSet : context.getShuttledEqualSlotSet()) {
                        checkSuccess = equalSlotSet.contains(contextPartitionColumn)
                                && equalSlotSet.contains(currentPartitionSlot);
                        if (checkSuccess) {
                            break;
                        }
                    }
                    if (!checkSuccess) {
                        context.addFailReason(String.format("partition column is in join invalid side, "
                                        + "but is not in join condition, the mvReferenceColumnToCheck is %s",
                                mvReferenceColumnToCheck));
                        relatedTableColumnInfosByTable.forEach(
                                columnInfo -> columnInfo.setFromTablePartitionColumn(false));
                    } else {
                        mvReferenceColumnInfo.setReachRelationCheck(true);
                        mvReferenceColumnInfo.setFromTablePartitionColumn(true);
                    }
                }
                if (context.getShouldFailCatalogRelation().stream().noneMatch(catalog ->
                        new BaseTableInfo(catalog.getTable()).equals(mvReferenceColumnInfo.getTableInfo()))
                        && context.getInvalidCatalogRelationToCheck().stream().noneMatch(catalog ->
                        new BaseTableInfo(catalog.getTable()).equals(mvReferenceColumnInfo.getTableInfo()))) {
                    mvReferenceColumnInfo.setReachRelationCheck(true);
                    mvReferenceColumnInfo.setFromTablePartitionColumn(true);
                } else {
                    context.addFailReason(String.format("partition column is in invalid catalog relation to check, "
                                    + "InvalidCatalogRelationToCheck is %s, ShouldFailCatalogRelation is %s",
                            context.getInvalidCatalogRelationToCheck().stream()
                                    .map(LogicalCatalogRelation::getTable)
                                    .collect(Collectors.toList()),
                            context.getShouldFailCatalogRelation().stream()
                                    .map(LogicalCatalogRelation::getTable)
                                    .collect(Collectors.toList())));
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
                context.setFailFast(true);
                context.collectFailedTableSet(aggregate);
                return visit(aggregate, context);
            }
            if (!checkPartition(groupByExprSet, aggregate, context)) {
                context.setFailFast(true);
                context.collectFailedTableSet(aggregate);
            }
            return visit(aggregate, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, PartitionIncrementCheckContext context) {
            List<NamedExpression> windowExpressions = window.getWindowExpressions();
            if (windowExpressions.isEmpty()) {
                context.addFailReason("window expression is empty, doesn't contain the target partition");
                context.collectFailedTableSet(window);
                context.setFailFast(true);
                return visit(window, context);
            }
            for (NamedExpression namedExpression : windowExpressions) {
                if (!checkWindowPartition(namedExpression, context)) {
                    context.addFailReason("window partition sets doesn't contain the target partition");
                    context.collectFailedTableSet(window);
                    context.setFailFast(true);
                    break;
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
                    || plan instanceof LogicalWindow
                    || (plan instanceof LogicalUnion
                    && ((LogicalUnion) plan).getQualifier() == SetOperation.Qualifier.ALL)
                    || plan instanceof LogicalCTEAnchor
                    || plan instanceof LogicalCTEConsumer
                    || plan instanceof LogicalCTEProducer
                    || plan instanceof LogicalSort
                    || plan instanceof LogicalTopN
            ) {
                return super.visit(plan, context);
            }
            context.addFailReason(String.format("Unsupported plan operate in track partition, "
                    + "the invalid plan node is %s", plan.getClass().getSimpleName()));
            context.setFailFast(true);
            context.collectFailedTableSet(plan);
            return super.visit(plan, context);
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
            boolean checked = false;
            for (Map.Entry<NamedExpression, RelatedTableColumnInfo> partitionExpressionEntry
                    : partitionAndExprEntrySet) {
                NamedExpression partitionNamedExpression = partitionExpressionEntry.getKey();
                RelatedTableColumnInfo partitionTableColumnInfo = partitionExpressionEntry.getValue();
                Optional<Expression> partitionExpressionOpt = partitionTableColumnInfo.getPartitionExpression();
                Expression partitionExpressionActual = partitionExpressionOpt
                        .map(expr -> ExpressionUtils.shuttleExpressionWithLineage(expr,
                                context.getOriginalPlan()))
                        .orElseGet(() -> ExpressionUtils.shuttleExpressionWithLineage(partitionNamedExpression,
                                context.getOriginalPlan()));
                // merge date_trunc
                partitionExpressionActual = new ExpressionNormalization().rewrite(partitionExpressionActual,
                        new ExpressionRewriteContext(context.getCascadesContext()));
                OUTER_CHECK:
                for (Expression projectSlotToCheck : expressionsToCheck) {
                    Expression expressionShuttledToCheck =
                            ExpressionUtils.shuttleExpressionWithLineage(projectSlotToCheck,
                                    context.getOriginalPlan());
                    // merge date_trunc
                    expressionShuttledToCheck = new ExpressionNormalization().rewrite(expressionShuttledToCheck,
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
                    context.getPartitionAndRefExpressionMap().put(checkedPartitionSlot,
                            RelatedTableColumnInfo.of(checkedPartitionSlot, partitionExpressionActual,
                                    partitionTableColumnInfo.isOriginalPartition(),
                                    partitionTableColumnInfo.isFromTablePartitionColumn()));
                    checked = true;
                }
            }
            return checked;
        }
    }

    /**
     * Get valid related table column info from check context by predicate
     */
    public static List<RelatedTableColumnInfo> getRelatedTableColumnInfosWithCheck(
            PartitionIncrementCheckContext checkContext,
            Predicate<RelatedTableColumnInfo> predicate) {
        Set<DataType> dataTypeSet = new HashSet<>();
        List<RelatedTableColumnInfo> checkedTableColumnInfos = new ArrayList<>();
        boolean anyIsFromTablePartitionColumn = false;
        // if predicate use isReachRelationCheck, this also need to check isFromTablePartitionColumn
        Set<BaseTableInfo> checkedTableSet = new HashSet<>();
        for (Map.Entry<NamedExpression, RelatedTableColumnInfo> entry
                : checkContext.getPartitionAndRefExpressionMap().entrySet()) {
            NamedExpression partitionColumn = entry.getKey();
            RelatedTableColumnInfo tableColumnInfo = entry.getValue();
            if (!partitionColumn.isColumnFromTable() || tableColumnInfo.getColumn() == null) {
                continue;
            }
            dataTypeSet.add(partitionColumn.getDataType());
            if (dataTypeSet.size() > 1) {
                return null;
            }
            if (checkedTableSet.contains(tableColumnInfo.getTableInfo())) {
                // remove duplicate table info
                continue;
            }
            if (predicate.test(tableColumnInfo)) {
                checkedTableColumnInfos.add(tableColumnInfo);
                checkedTableSet.add(tableColumnInfo.getTableInfo());
            }
            anyIsFromTablePartitionColumn
                    = anyIsFromTablePartitionColumn || tableColumnInfo.isFromTablePartitionColumn();
        }
        return anyIsFromTablePartitionColumn ? checkedTableColumnInfos : ImmutableList.of();
    }

    /**
     * Get valid related table column info from check context by predicate
     */
    public static List<RelatedTableColumnInfo> getRelatedTableColumnInfosByTable(
            PartitionIncrementCheckContext checkContext,
            BaseTableInfo tableInfo) {
        List<RelatedTableColumnInfo> checkedTableColumnInfos = new ArrayList<>();
        for (Map.Entry<NamedExpression, RelatedTableColumnInfo> entry
                : checkContext.getPartitionAndRefExpressionMap().entrySet()) {
            RelatedTableColumnInfo tableColumnInfo = entry.getValue();
            if (tableColumnInfo.getTableInfo() != null && tableColumnInfo.getTableInfo().equals(tableInfo)) {
                checkedTableColumnInfos.add(tableColumnInfo);
            }
        }
        return checkedTableColumnInfos;
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
        // This record the invalid relation, such as the right side of left join
        private final Set<LogicalCatalogRelation> invalidCatalogRelationToCheck = new HashSet<>();
        // This record should fail relation, such as invalid child of checked plan node
        private final Set<LogicalCatalogRelation> shouldFailCatalogRelation = new HashSet<>();
        // This is used to record the equal slot set shuttled from children which are equals to partition column
        // to check, this expends the partition slot to check
        private final Set<Set<Slot>> shuttledEqualSlotSet = new HashSet<>();
        private final Map<CTEId, Plan> producerCteIdToPlanMap;
        private final Plan originalPlan;
        private boolean failFast = false;

        public PartitionIncrementCheckContext(NamedExpression mvPartitionColumn,
                Expression mvPartitionExpression, Map<CTEId, Plan> producerCteIdToPlanMap,
                Plan originalPlan,
                CascadesContext cascadesContext) {
            this.partitionAndRefExpressionMap.put(mvPartitionColumn, RelatedTableColumnInfo.of(
                    mvPartitionColumn, mvPartitionExpression, true, false));
            this.cascadesContext = cascadesContext;
            this.producerCteIdToPlanMap = producerCteIdToPlanMap;
            this.originalPlan = originalPlan;
        }

        public Set<String> getFailReasons() {
            return failReasons;
        }

        public void addFailReason(String failReason) {
            if (failFast) {
                return;
            }
            this.failReasons.add(failReason);
        }

        public Set<LogicalCatalogRelation> getInvalidCatalogRelationToCheck() {
            return invalidCatalogRelationToCheck;
        }

        public Set<LogicalCatalogRelation> getShouldFailCatalogRelation() {
            return shouldFailCatalogRelation;
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

        public Map<CTEId, Plan> getProducerCteIdToPlanMap() {
            return producerCteIdToPlanMap;
        }

        public boolean isFailFast() {
            return failFast;
        }

        public void setFailFast(boolean failFast) {
            this.failFast = failFast;
        }

        public Plan getOriginalPlan() {
            return originalPlan;
        }

        /**
         * collect invalid table set to check self join
         */
        public void collectInvalidTableSet(Plan plan) {
            plan.accept(new DefaultPlanVisitor<Void, Set<LogicalCatalogRelation>>() {
                @Override
                public Void visitLogicalCatalogRelation(LogicalCatalogRelation relation,
                        Set<LogicalCatalogRelation> invalidTableSet) {
                    invalidTableSet.add(relation);
                    return null;
                }
            }, this.invalidCatalogRelationToCheck);
        }

        /**
         * collect invalid table set to check self join
         */
        public void collectFailedTableSet(Plan plan) {
            plan.accept(new DefaultPlanVisitor<Void, Set<LogicalCatalogRelation>>() {
                @Override
                public Void visitLogicalCatalogRelation(LogicalCatalogRelation relation,
                        Set<LogicalCatalogRelation> failedTableSet) {
                    failedTableSet.add(relation);
                    return null;
                }
            }, this.shouldFailCatalogRelation);
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
        // construct shuttled partitionEqualSlotSet
        Set<Slot> shuttledPartitionEqualSlotSet = new HashSet<>();
        if (partitionEqualSlotSet.isEmpty()) {
            return Pair.of(partitionEqualSlotSet, shuttledPartitionEqualSlotSet);
        }
        List<Expression> extendedPartitionEqualSlotSet = new ArrayList<>(partitionEqualSlotSet);
        extendedPartitionEqualSlotSet.add(slot);
        List<? extends Expression> shuttledEqualExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                extendedPartitionEqualSlotSet, join);
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
