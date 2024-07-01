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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Update mv by partition
 */
public class UpdateMvByPartitionCommand extends InsertOverwriteTableCommand {
    private UpdateMvByPartitionCommand(LogicalPlan logicalQuery) {
        super(logicalQuery, Optional.empty(), Optional.empty());
    }

    /**
     * Construct command
     *
     * @param mv materialize view
     * @param partitionNames update partitions in mv and tables
     * @param tableWithPartKey the partitions key for different table
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<String> partitionNames,
            Map<TableIf, String> tableWithPartKey) throws UserException {
        NereidsParser parser = new NereidsParser();
        Map<TableIf, Set<Expression>> predicates =
                constructTableWithPredicates(mv, partitionNames, tableWithPartKey);
        List<String> parts = constructPartsForMv(partitionNames);
        Plan plan = parser.parseSingle(mv.getQuerySql());
        plan = plan.accept(new PredicateAdder(), new PredicateAddContext(predicates));
        if (plan instanceof Sink) {
            plan = plan.child(0);
        }
        LogicalSink<? extends Plan> sink = UnboundTableSinkCreator.createUnboundTableSink(mv.getFullQualifiers(),
                ImmutableList.of(), ImmutableList.of(), parts, plan);
        return new UpdateMvByPartitionCommand(sink);
    }

    private static List<String> constructPartsForMv(Set<String> partitionNames) {
        return Lists.newArrayList(partitionNames);
    }

    private static Map<TableIf, Set<Expression>> constructTableWithPredicates(MTMV mv,
            Set<String> partitionNames, Map<TableIf, String> tableWithPartKey) throws AnalysisException {
        Set<PartitionItem> items = Sets.newHashSet();
        for (String partitionName : partitionNames) {
            PartitionItem partitionItem = mv.getPartitionItemOrAnalysisException(partitionName);
            items.add(partitionItem);
        }
        ImmutableMap.Builder<TableIf, Set<Expression>> builder = new ImmutableMap.Builder<>();
        tableWithPartKey.forEach((table, colName) ->
                builder.put(table, constructPredicates(items, colName))
        );
        return builder.build();
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, String colName) {
        UnboundSlot slot = new UnboundSlot(colName);
        return constructPredicates(partitions, slot);
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot) {
        Set<Expression> predicates = new HashSet<>();
        if (partitions.isEmpty()) {
            return Sets.newHashSet(BooleanLiteral.TRUE);
        }
        if (partitions.iterator().next() instanceof ListPartitionItem) {
            for (PartitionItem item : partitions) {
                predicates.add(convertListPartitionToIn(item, colSlot));
            }
        } else {
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, colSlot));
            }
        }
        return predicates;
    }

    private static Expression convertPartitionKeyToLiteral(PartitionKey key) {
        return Literal.fromLegacyLiteral(key.getKeys().get(0),
                Type.fromPrimitiveType(key.getTypes().get(0)));
    }

    private static Expression convertListPartitionToIn(PartitionItem item, Slot col) {
        List<Expression> inValues = ((ListPartitionItem) item).getItems().stream()
                .map(UpdateMvByPartitionCommand::convertPartitionKeyToLiteral)
                .collect(ImmutableList.toImmutableList());
        List<Expression> predicates = new ArrayList<>();
        if (inValues.stream().anyMatch(NullLiteral.class::isInstance)) {
            inValues = inValues.stream()
                    .filter(e -> !(e instanceof NullLiteral))
                    .collect(Collectors.toList());
            Expression isNullPredicate = new IsNull(col);
            predicates.add(isNullPredicate);
        }
        if (!inValues.isEmpty()) {
            predicates.add(new InPredicate(col, inValues));
        }
        if (predicates.isEmpty()) {
            return BooleanLiteral.of(true);
        }
        return ExpressionUtils.or(predicates);
    }

    private static Expression convertRangePartitionToCompare(PartitionItem item, Slot col) {
        Range<PartitionKey> range = item.getItems();
        List<Expression> expressions = new ArrayList<>();
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            PartitionKey key = range.lowerEndpoint();
            expressions.add(new GreaterThanEqual(col, convertPartitionKeyToLiteral(key)));
        }
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            PartitionKey key = range.upperEndpoint();
            expressions.add(new LessThan(col, convertPartitionKeyToLiteral(key)));
        }
        if (expressions.isEmpty()) {
            return BooleanLiteral.of(true);
        }
        Expression predicate = ExpressionUtils.and(expressions);
        // The partition without can be the first partition of LESS THAN PARTITIONS
        // The null value can insert into this partition, so we need to add or is null condition
        if (!range.hasLowerBound() || range.lowerEndpoint().isMinValue()) {
            predicate = ExpressionUtils.or(predicate, new IsNull(col));
        }
        return predicate;
    }

    /**
     * Add predicates on base table when mv can partition update, Also support plan that contain cte and view
     */
    public static class PredicateAdder extends DefaultPlanRewriter<PredicateAddContext> {

        // record view and cte name parts, these should be ignored and visit it's actual plan
        public Set<List<String>> virtualRelationNamePartSet = new HashSet<>();

        @Override
        public Plan visitUnboundRelation(UnboundRelation unboundRelation, PredicateAddContext predicates) {

            if (predicates.getPredicates() == null || predicates.getPredicates().isEmpty()) {
                return unboundRelation;
            }
            if (virtualRelationNamePartSet.contains(unboundRelation.getNameParts())) {
                return unboundRelation;
            }
            List<String> tableQualifier = RelationUtil.getQualifierName(ConnectContext.get(),
                    unboundRelation.getNameParts());
            TableIf table = RelationUtil.getTable(tableQualifier, Env.getCurrentEnv());
            if (predicates.getPredicates().containsKey(table)) {
                return new LogicalFilter<>(ImmutableSet.of(ExpressionUtils.or(predicates.getPredicates().get(table))),
                        unboundRelation);
            }
            return unboundRelation;
        }

        @Override
        public Plan visitLogicalCTE(LogicalCTE<? extends Plan> cte, PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return cte;
            }
            for (LogicalSubQueryAlias<Plan> subQueryAlias : cte.getAliasQueries()) {
                this.virtualRelationNamePartSet.add(subQueryAlias.getQualifier());
                subQueryAlias.children().forEach(subQuery -> subQuery.accept(this, predicates));
            }
            return super.visitLogicalCTE(cte, predicates);
        }

        @Override
        public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> subQueryAlias,
                PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return subQueryAlias;
            }
            this.virtualRelationNamePartSet.add(subQueryAlias.getQualifier());
            return super.visitLogicalSubQueryAlias(subQueryAlias, predicates);
        }

        @Override
        public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation,
                PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return catalogRelation;
            }
            TableIf table = catalogRelation.getTable();
            if (predicates.getPredicates() != null) {
                if (predicates.getPredicates().containsKey(table)) {
                    return new LogicalFilter<>(
                            ImmutableSet.of(ExpressionUtils.or(predicates.getPredicates().get(table))),
                            catalogRelation);
                }
            }
            if (predicates.getPartition() != null && predicates.getPartitionName() != null) {
                if (!(table instanceof MTMVRelatedTableIf)) {
                    return catalogRelation;
                }
                for (Map.Entry<BaseTableInfo, Set<String>> filterTableEntry : predicates.getPartition().entrySet()) {
                    if (!Objects.equals(new BaseTableInfo(table), filterTableEntry.getKey())) {
                        continue;
                    }
                    Slot partitionSlot = null;
                    for (Slot slot : catalogRelation.getOutput()) {
                        if (((SlotReference) slot).getName().equals(predicates.getPartitionName())) {
                            partitionSlot = slot;
                            break;
                        }
                    }
                    if (partitionSlot == null) {
                        predicates.setHandleSuccess(false);
                        return catalogRelation;
                    }
                    // if partition has no data, doesn't add filter
                    Set<PartitionItem> partitionHasDataItems = new HashSet<>();
                    MTMVRelatedTableIf targetTable = (MTMVRelatedTableIf) table;
                    for (String partitionName : filterTableEntry.getValue()) {
                        Partition partition = targetTable.getPartition(partitionName);
                        if (!(targetTable instanceof OlapTable)) {
                            // check partition is have data or not, only support olap table
                            break;
                        }
                        if (!((OlapTable) targetTable).selectNonEmptyPartitionIds(
                                Lists.newArrayList(partition.getId())).isEmpty()) {
                            // Add filter only when partition has data
                            partitionHasDataItems.add(
                                    ((OlapTable) targetTable).getPartitionInfo().getItem(partition.getId()));
                        }
                    }
                    if (partitionHasDataItems.isEmpty()) {
                        predicates.setNeedAddFilter(false);
                    }
                    if (!partitionHasDataItems.isEmpty()) {
                        Set<Expression> partitionExpressions =
                                constructPredicates(partitionHasDataItems, partitionSlot);
                        return new LogicalFilter<>(ImmutableSet.of(ExpressionUtils.or(partitionExpressions)),
                                catalogRelation);
                    }
                }
            }
            return catalogRelation;
        }
    }

    /**
     * Predicate context, which support add predicate by expression or by partition name
     * Add by predicates has high priority
     */
    public static class PredicateAddContext {

        private final Map<TableIf, Set<Expression>> predicates;
        private final Map<BaseTableInfo, Set<String>> partition;
        private final String partitionName;
        private boolean handleSuccess = true;
        // when add filter by partition, if partition has no data, doesn't need to add filter. should be false
        private boolean needAddFilter = true;

        public PredicateAddContext(Map<TableIf, Set<Expression>> predicates) {
            this(predicates, null, null);
        }

        public PredicateAddContext(Map<BaseTableInfo, Set<String>> partition,
                String partitionName) {
            this(null, partition, partitionName);
        }

        public PredicateAddContext(Map<TableIf, Set<Expression>> predicates, Map<BaseTableInfo, Set<String>> partition,
                String partitionName) {
            this.predicates = predicates;
            this.partition = partition;
            this.partitionName = partitionName;
        }

        public Map<TableIf, Set<Expression>> getPredicates() {
            return predicates;
        }

        public Map<BaseTableInfo, Set<String>> getPartition() {
            return partition;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public boolean isEmpty() {
            return predicates == null && partition == null;
        }

        public boolean isHandleSuccess() {
            return handleSuccess;
        }

        public void setHandleSuccess(boolean handleSuccess) {
            this.handleSuccess = handleSuccess;
        }

        public boolean isNeedAddFilter() {
            return needAddFilter;
        }

        public void setNeedAddFilter(boolean needAddFilter) {
            this.needAddFilter = needAddFilter;
        }
    }
}
