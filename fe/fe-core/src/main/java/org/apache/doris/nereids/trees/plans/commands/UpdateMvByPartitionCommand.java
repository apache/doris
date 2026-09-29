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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.StatementContext;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(UpdateMvByPartitionCommand.class);

    private UpdateMvByPartitionCommand(LogicalPlan logicalQuery) {
        super(logicalQuery, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public boolean isForceDropPartition() {
        // After refreshing the data in MTMV, it will be synchronized with the base table
        // and there is no need to put it in the recycle bin
        return true;
    }

    /**
     * Construct command
     *
     * @param mv materialize view
     * @param partitionNames update partitions in mv and tables
     * @param tableWithPartKey the partitions key for different table
     * @param statementContext statementContext
     * @param readableBasePartitions the base partitions each base table may be read from, by base table,
     *                               named with the partition names of the base table. A base table named
     *                               with an empty set is not read at all; a base table absent from the map,
     *                               or a null map, keeps the MV partition's own key range. The tables named
     *                               are olap ones, and the partitions are looked up on them
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<String> partitionNames,
            Map<TableIf, String> tableWithPartKey, StatementContext statementContext,
            Map<TableIf, Set<String>> readableBasePartitions) throws UserException {
        NereidsParser parser = new NereidsParser();
        Map<TableIf, Set<Expression>> predicates =
                constructTableWithPredicates(mv, partitionNames, tableWithPartKey, readableBasePartitions);
        List<String> parts = constructPartsForMv(partitionNames);
        Plan plan = parser.parseSingle(mv.getQuerySql());
        if (plan instanceof Sink) {
            plan = plan.child(0);
        }
        List<String> sinkColumns = mv.isIvm() ? mv.getInsertedColumnNames() : ImmutableList.of();
        LogicalSink<? extends Plan> sink = UnboundTableSinkCreator.createUnboundTableSink(mv.getFullQualifiers(),
                sinkColumns, ImmutableList.of(), parts, plan);
        if (LOG.isDebugEnabled()) {
            LOG.debug("MTMVTask plan for mvName: {}, partitionNames: {}, plan: {}", mv.getName(), partitionNames,
                    sink.treeString());
        }
        statementContext.setMvRefreshPredicates(predicates);
        return new UpdateMvByPartitionCommand(sink);
    }

    private static List<String> constructPartsForMv(Set<String> partitionNames) {
        return Lists.newArrayList(partitionNames);
    }

    /**
     * The predicate every base table of the MV definition is read through.
     *
     * <p>A table the caller scopes is read from exactly the base partitions it named. Those are the ones
     * the refresh is about to record as this MV partition's, and the read is what has to match the record:
     * reading the MV partition's own key range instead also reads base partitions no snapshot describes,
     * and a later silent change to one of them -- dropped, with the base partition set back to what it
     * was -- leaves the rows it put in this MV partition behind while the partition is still judged
     * synchronized, so the transparent rewrite serves them and no refresh plans it again.
     *
     * <p>Every other table keeps the MV partition's own key range, which is what the tables the caller
     * does not scope were always read through. Scoped tables are olap ones; the partition names are
     * looked up on one, see the caller.
     */
    private static Map<TableIf, Set<Expression>> constructTableWithPredicates(MTMV mv,
            Set<String> partitionNames, Map<TableIf, String> tableWithPartKey,
            Map<TableIf, Set<String>> readableBasePartitions) throws AnalysisException {
        Set<PartitionItem> mvItems = Sets.newHashSet();
        for (String partitionName : partitionNames) {
            mvItems.add(mv.getPartitionItemOrAnalysisException(partitionName));
        }
        ImmutableMap.Builder<TableIf, Set<Expression>> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<TableIf, String> entry : tableWithPartKey.entrySet()) {
            TableIf table = entry.getKey();
            String colName = entry.getValue();
            Set<String> readable = readableBasePartitions == null ? null : readableBasePartitions.get(table);
            if (readable == null) {
                builder.put(table, constructPredicates(mvItems, colName));
                continue;
            }
            if (readable.isEmpty()) {
                // No partition of this table feeds the MV partitions being refreshed, which is "no row"
                // rather than "every row": constructPredicates answers the other way for an empty set,
                // and that answer would put every row of the table into each of them.
                builder.put(table, Sets.newHashSet(BooleanLiteral.FALSE));
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            Set<PartitionItem> items = Sets.newHashSet();
            for (String partitionName : readable) {
                items.add(olapTable.getPartitionItemOrAnalysisException(partitionName));
            }
            // Built from the key at the position the MV's partition column has in this table, which is
            // what the mapping is keyed by; a partition of a list partitioned table can hold more than
            // one key, and the MV's column is not necessarily the first of them.
            builder.put(table, constructPredicates(items, new UnboundSlot(colName),
                    mv.getMvPartitionInfo().getPctColPos(olapTable)));
        }
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
        return constructPredicates(partitions, colSlot, 0);
    }

    /**
     * construct predicates for partition items, on the key each of them holds at {@code keyPos}.
     *
     * @param keyPos the position of the column the predicate is about among a partition's keys. A partition
     *               of the MV itself holds exactly the one key its column has, so a caller passing those
     *               passes 0; a caller passing a base partition passes the position the MV's partition
     *               column has in that base table, which a list partitioned table can hold more than one
     *               key of
     */
    @VisibleForTesting
    static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot, int keyPos) {
        Set<Expression> predicates = new HashSet<>();
        if (partitions.isEmpty()) {
            return Sets.newHashSet(BooleanLiteral.TRUE);
        }
        if (partitions.iterator().next() instanceof ListPartitionItem) {
            for (PartitionItem item : partitions) {
                predicates.add(convertListPartitionToIn(item, colSlot, keyPos));
            }
        } else {
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, colSlot, keyPos));
            }
        }
        return predicates;
    }

    private static Expression convertPartitionKeyToLiteral(PartitionKey key, int keyPos) {
        return Literal.fromLegacyLiteral(key.getKeys().get(keyPos),
                Type.fromPrimitiveType(key.getTypes().get(keyPos)));
    }

    private static Expression convertListPartitionToIn(PartitionItem item, Slot col, int keyPos) {
        List<Expression> inValues = ((ListPartitionItem) item).getItems().stream()
                .map(key -> convertPartitionKeyToLiteral(key, keyPos))
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

    private static Expression convertRangePartitionToCompare(PartitionItem item, Slot col, int keyPos) {
        Range<PartitionKey> range = item.getItems();
        List<Expression> expressions = new ArrayList<>();
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            PartitionKey key = range.lowerEndpoint();
            expressions.add(new GreaterThanEqual(col, convertPartitionKeyToLiteral(key, keyPos)));
        }
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            PartitionKey key = range.upperEndpoint();
            expressions.add(new LessThan(col, convertPartitionKeyToLiteral(key, keyPos)));
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
            TableIf table = RelationUtil.getTable(tableQualifier, Env.getCurrentEnv(), Optional.empty());
            if (predicates.getPredicates().containsKey(table)) {
                return new LogicalFilter<>(
                        ExpressionUtils.extractConjunctionToSet(
                                ExpressionUtils.or(predicates.getPredicates().get(table))
                        ),
                        unboundRelation
                );
            }
            return unboundRelation;
        }

        @Override
        public Plan visitLogicalCTE(LogicalCTE<? extends Plan> cte, PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return cte;
            }
            List<LogicalSubQueryAlias<Plan>> rewrittenSubQueryAlias = new ArrayList<>();
            for (LogicalSubQueryAlias<Plan> subQueryAlias : cte.getAliasQueries()) {
                List<Plan> subQueryAliasChildren = new ArrayList<>();
                this.virtualRelationNamePartSet.add(subQueryAlias.getQualifier());
                subQueryAlias.children().forEach(subQuery ->
                        subQueryAliasChildren.add(subQuery.accept(this, predicates))
                );
                rewrittenSubQueryAlias.add(subQueryAlias.withChildren(subQueryAliasChildren));
            }
            return super.visitLogicalCTE(new LogicalCTE<>(cte.isRecursive(),
                    rewrittenSubQueryAlias, cte.child()), predicates);
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
                            ExpressionUtils.extractConjunctionToSet(
                                    ExpressionUtils.or(predicates.getPredicates().get(table))
                            ),
                            catalogRelation);
                }
            }
            if (predicates.getPartitions() != null) {
                if (!(table instanceof MTMVRelatedTableIf)) {
                    return catalogRelation;
                }
                for (Map.Entry<BaseColInfo, Set<String>> filterTableEntry : predicates.getPartitions().entrySet()) {
                    BaseColInfo relatedTableColumnInfo = filterTableEntry.getKey();
                    if (!Objects.equals(new BaseTableInfo(table), relatedTableColumnInfo.getTableInfo())) {
                        continue;
                    }
                    Slot partitionSlot = null;
                    for (Slot slot : catalogRelation.getOutput()) {
                        if (slot.getName().equals(relatedTableColumnInfo.getColName())) {
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
                        if (targetTable instanceof OlapTable) {
                            Partition partition = targetTable.getPartition(partitionName);
                            if (partition == null) {
                                // partition maybe deleted, skip it
                                continue;
                            }
                            if (!((OlapTable) targetTable).selectNonEmptyPartitionIds(
                                    Lists.newArrayList(partition.getId()), Optional.empty()).isEmpty()) {
                                // Add filter only when partition has data when olap table
                                partitionHasDataItems.add(
                                        ((OlapTable) targetTable).getPartitionInfo().getItem(partition.getId()));
                            }
                        }
                        if (targetTable instanceof ExternalTable) {
                            PartitionItem partitionItem = ((ExternalTable) targetTable).getNameToPartitionItems(
                                    MvccUtil.getSnapshotFromContext(targetTable)).get(partitionName);
                            // Add filter only when partition has data when external table
                            if (partitionItem != null) {
                                partitionHasDataItems.add(partitionItem);
                            }
                        }
                    }
                    if (partitionHasDataItems.isEmpty()) {
                        predicates.setNeedAddFilter(false);
                    }
                    if (!partitionHasDataItems.isEmpty()) {
                        return new LogicalFilter<>(
                                ExpressionUtils.extractConjunctionToSet(
                                        ExpressionUtils.or(constructPredicates(partitionHasDataItems, partitionSlot))
                                ),
                                catalogRelation
                        );
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
        private final Map<BaseColInfo, Set<String>> partitions;
        private boolean handleSuccess = true;
        // when add filter by partition, if partition has no data, doesn't need to add filter. should be false
        private boolean needAddFilter = true;

        public PredicateAddContext(Map<TableIf, Set<Expression>> predicates,
                Map<BaseColInfo, Set<String>> partitions) {
            this.predicates = predicates;
            this.partitions = partitions;
        }

        public Map<TableIf, Set<Expression>> getPredicates() {
            return predicates;
        }

        public Map<BaseColInfo, Set<String>> getPartitions() {
            return partitions;
        }

        public boolean isEmpty() {
            return predicates == null && partitions == null;
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
