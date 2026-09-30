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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
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
import org.apache.doris.nereids.trees.expressions.EqualTo;
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
import com.google.common.base.Preconditions;
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
            Map<BaseTableInfo, Set<String>> readableBasePartitions) throws UserException {
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
            Map<BaseTableInfo, Set<String>> readableBasePartitions) throws AnalysisException {
        Set<PartitionItem> mvItems = Sets.newHashSet();
        for (String partitionName : partitionNames) {
            mvItems.add(mv.getPartitionItemOrAnalysisException(partitionName));
        }
        ImmutableMap.Builder<TableIf, Set<Expression>> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<TableIf, String> entry : tableWithPartKey.entrySet()) {
            TableIf table = entry.getKey();
            String colName = entry.getValue();
            Set<String> readable = readableBasePartitions == null ? null
                    : readableBasePartitions.get(new BaseTableInfo(table));
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
            if (hasDefaultListPartition(olapTable)) {
                // A list partitioned table's default partition takes the rows no other partition of it
                // claims, and it is not a partition of that table the MV's own partition is recorded with:
                // a partition of the MV takes the rows whose own key falls in it, wherever the base table
                // put them, so the rows this refresh is about are the ones the MV partition's key range
                // names rather than the ones the base partition it is recorded with holds. A table that has
                // such a partition is therefore read the way an unscoped one is. That read can be seen to be
                // too wide -- it is the one this scope exists to narrow -- rather than one that drops rows
                // which belong to the MV partition being refreshed.
                builder.put(table, constructPredicates(mvItems, colName));
                continue;
            }
            builder.put(table, constructPredicatesOfBasePartitions(items, olapTable, colName));
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
                predicates.add(convertRangePartitionToCompare(item, colSlot, Optional.empty()));
            }
        }
        return predicates;
    }

    /**
     * The predicate a base table is read through when the refresh is to read exactly these partitions of it.
     *
     * <p>A partition of a list partitioned table holds one key per partition column, and the column the MV
     * partition is named by is only one of them. A predicate on that column alone also reaches the
     * partitions whose other keys differ -- a table partitioned by (d, region) has one partition of
     * (d0, 'US') and one of (d0, 'EU'), and `d = d0` reaches both, while only the second is a partition
     * this refresh is to read; a later drop of the first would then leave its rows in the MV partition
     * while the snapshot, which names only the second, still calls it synchronized. So a list partition is
     * pinned to its whole key. A range partition is pinned to its bounds, which is the same thing: a base
     * table partitioned by range has a single partition column, see
     * {@code RangePartitionItem#toPartitionKeyDesc(int)}.
     *
     * <p>The partitions are never empty: a table the caller scopes with no partition is read as nothing
     * before this is reached, see {@code constructTableWithPredicates}.
     */
    private static Set<Expression> constructPredicatesOfBasePartitions(Set<PartitionItem> partitions,
            OlapTable baseTable, String colName) throws AnalysisException {
        List<Column> partitionColumns = baseTable.getPartitionColumns();
        List<Type> partitionColumnTypes = Lists.transform(partitionColumns, Column::getType);
        if (!(partitions.iterator().next() instanceof ListPartitionItem)) {
            Set<Expression> predicates = new HashSet<>();
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, new UnboundSlot(colName),
                        Optional.of(partitionColumnTypes.get(0))));
            }
            return predicates;
        }
        List<Slot> partitionSlots = Lists.newArrayList();
        for (Column partitionColumn : partitionColumns) {
            partitionSlots.add(new UnboundSlot(partitionColumn.getName()));
        }
        Set<Expression> predicates = new HashSet<>();
        for (PartitionItem item : partitions) {
            predicates.add(convertListPartitionToKey(item, partitionSlots, partitionColumnTypes));
        }
        return predicates;
    }

    /**
     * Whether this table has a list partition that takes the rows no other partition of it claims. Such a
     * partition holds rows for every key its table can be read by, so which rows of it belong to a partition
     * of the MV is the MV partition's own question and not the partition's.
     */
    private static boolean hasDefaultListPartition(OlapTable table) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (!(partitionInfo instanceof ListPartitionInfo)) {
            return false;
        }
        return ((ListPartitionInfo) partitionInfo).getIdToItem(false).values().stream()
                .anyMatch(PartitionItem::isDefaultPartition);
    }

    /**
     * One partition of a list partitioned table, pinned to the whole of each key it holds: the keys are
     * what tells it apart from a partition that shares a key with it, and the value of a key a row does not
     * have is asked for as {@code IS NULL}, since no comparison to it is ever true.
     *
     * <p>A list partitioned table's default partition is not one of these: its key is the sentinel the rows
     * no other partition claims are placed by rather than a value, and a partition of the MV takes the rows
     * whose own key falls in it wherever the base table put them. What it holds cannot be said with a
     * predicate on the partition columns, so a table that has one is read the way an unscoped one is, see
     * {@code constructTableWithPredicates}.
     */
    private static Expression convertListPartitionToKey(PartitionItem item, List<Slot> partitionSlots,
            List<Type> partitionColumnTypes) {
        List<Expression> keys = new ArrayList<>();
        for (PartitionKey key : ((ListPartitionItem) item).getItems()) {
            List<Expression> oneKey = new ArrayList<>();
            for (int pos = 0; pos < partitionSlots.size(); pos++) {
                Expression value = convertPartitionKeyToLiteral(key, pos,
                        Optional.of(partitionColumnTypes.get(pos)));
                oneKey.add(value instanceof NullLiteral ? new IsNull(partitionSlots.get(pos))
                        : new EqualTo(partitionSlots.get(pos), value));
            }
            keys.add(ExpressionUtils.and(oneKey));
        }
        Preconditions.checkState(!keys.isEmpty(), "a list partition holds at least one key: %s", item);
        return ExpressionUtils.or(keys);
    }

    /**
     * A partition key is a value of the partition column it is written against, and what tells two of them
     * apart can be a scale the key's primitive type does not carry: a literal rounded to a coarser one is
     * one no row of the partition compares equal to, and the rows of the partition are then read as none.
     * The callers that have the column pass its type; the ones that do not leave the key's own primitive
     * type, which is what a reader of these predicates was given before.
     */
    private static Expression convertPartitionKeyToLiteral(PartitionKey key, int keyPos,
            Optional<Type> columnType) {
        return Literal.fromLegacyLiteral(key.getKeys().get(keyPos),
                columnType.orElseGet(() -> Type.fromPrimitiveType(key.getTypes().get(keyPos))));
    }

    private static Expression convertListPartitionToIn(PartitionItem item, Slot col) {
        List<Expression> inValues = ((ListPartitionItem) item).getItems().stream()
                .map(key -> convertPartitionKeyToLiteral(key, 0, Optional.empty()))
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

    private static Expression convertRangePartitionToCompare(PartitionItem item, Slot col,
            Optional<Type> columnType) {
        Range<PartitionKey> range = item.getItems();
        List<Expression> expressions = new ArrayList<>();
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            PartitionKey key = range.lowerEndpoint();
            expressions.add(new GreaterThanEqual(col, convertPartitionKeyToLiteral(key, 0, columnType)));
        }
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            PartitionKey key = range.upperEndpoint();
            expressions.add(new LessThan(col, convertPartitionKeyToLiteral(key, 0, columnType)));
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
