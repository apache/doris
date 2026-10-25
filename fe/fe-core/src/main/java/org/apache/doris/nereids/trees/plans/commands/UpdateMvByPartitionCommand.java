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
import org.apache.doris.datasource.mvcc.MvccSnapshot;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
     *                               or a null map, keeps the MV partition's own key range. The partitions
     *                               are looked up on the table at the snapshot pinned in the statement
     *                               context, which is the one the mapping was built from
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<String> partitionNames,
            Map<TableIf, String> tableWithPartKey, StatementContext statementContext,
            Map<BaseTableInfo, Set<String>> readableBasePartitions) throws UserException {
        NereidsParser parser = new NereidsParser();
        Map<TableIf, Set<Expression>> predicates = constructTableWithPredicates(mv, partitionNames,
                tableWithPartKey, statementContext, readableBasePartitions);
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
     * does not scope were always read through, and a scoped range table keeps it as well: a partition of
     * a base range table can hold a row past the MV partition it is rolled up into, see below. Scoped
     * tables are the MV's partition tables, olap or external, and their partitions are resolved from the
     * snapshot pinned in the statement context -- the same one the mapping was built from, so the read is
     * pinned to the partitions the batch records even when the connector moves between the two.
     */
    private static Map<TableIf, Set<Expression>> constructTableWithPredicates(MTMV mv,
            Set<String> partitionNames, Map<TableIf, String> tableWithPartKey,
            StatementContext statementContext,
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
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            Optional<MvccSnapshot> snapshot = statementContext.getSnapshot(table);
            List<Column> partitionColumns = relatedTable.getPartitionColumns(snapshot);
            // Each name is resolved where it lives rather than through a copy of the table's whole partition
            // map: commands are built once per refresh batch and the default batch is one partition, so a
            // full copy per batch would visit every partition of the table once per partition refreshed. An
            // olap table answers the name from its own concurrent map; an external table answers it from the
            // map of the snapshot pinned for this refresh, which is read-only and needs no copy either.
            Set<PartitionItem> items = Sets.newHashSet();
            if (relatedTable instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) relatedTable;
                for (String partitionName : readable) {
                    try {
                        items.add(olapTable.getPartitionItemOrAnalysisException(partitionName));
                    } catch (AnalysisException e) {
                        // The batch records this partition, so the rows it holds are the batch's to write; a
                        // read that cannot find it would write less than the snapshot claims. Refused rather
                        // than skipped, like the olap lookup this replaces.
                        throw new AnalysisException("base partition " + partitionName + " of table "
                                + table.getName() + " is not in the snapshot this refresh reads", e);
                    }
                }
            } else {
                Map<String, PartitionItem> itemsByName = ((ExternalTable) relatedTable)
                        .getNameToPartitionItems(snapshot);
                for (String partitionName : readable) {
                    PartitionItem item = itemsByName.get(partitionName);
                    if (item == null) {
                        // Refused for the same reason as above: the snapshot this refresh is recorded with
                        // names a partition it cannot read.
                        throw new AnalysisException("base partition " + partitionName + " of table "
                                + table.getName() + " is not in the snapshot this refresh reads");
                    }
                    items.add(item);
                }
            }
            if (items.stream().anyMatch(PartitionItem::isDefaultPartition)) {
                // One of the partitions this MV partition is recorded with is a list partitioned table's
                // default partition, which takes the rows no other partition of it claims. Those rows are
                // the ones the MV partition's own key range names, wherever the base table put them, and a
                // partition of the MV takes them by that key rather than by the partition they were placed
                // in. So a table whose mapped partitions include one is read the way an unscoped one is:
                // the MV partition's key range, at the partition column's own type. Which of the rows that
                // range reaches sit in an explicit partition and which in the default one is not something a
                // predicate on the partition columns can say -- ADD PARTITION claims a key without moving the
                // rows a default partition already holds under it -- so this table is not scoped at all, and
                // the mapping is kept whole for it for the same reason: see
                // MTMVRelatedPartitionDescSyncLimitGenerator, which leaves such a table out of the window.
                // The mapping names the default partition in every MV partition that reads the table, so this
                // is reached for each of them and not only for the one the sentinel key maps to.
                builder.put(table, constructPredicates(mvItems, colName,
                        Optional.of(partitionColumnType(partitionColumns, colName))));
                continue;
            }
            if (items.isEmpty()) {
                // No partition of this table feeds the MV partitions being refreshed, which is "no row"
                // rather than "every row": constructPredicates answers the other way for an empty set,
                // and that answer would put every row of the table into each of them.
                builder.put(table, Sets.newHashSet(BooleanLiteral.FALSE));
                continue;
            }
            if (items.iterator().next() instanceof ListPartitionItem) {
                // A key of a list partition is a value the MV partition's own key set was written from, so
                // the whole key pinned here is inside the range this batch writes and nothing is added to it.
                builder.put(table, constructPredicatesOfBasePartitions(items, partitionColumns, colName));
                continue;
            }
            // A partition of a base range table can end past the MV partition it is rolled up into, and the
            // roll-up keeps such a partition: MTMVPartitionExprDateTrunc takes the last second off the base's
            // upper bound before truncating it, so a DATETIME(3) partition ending at 2025-01-01 00:00:00.123
            // is rolled up into the year 2024. This batch writes the year 2024 partition alone, and a row of
            // that partition just past its upper bound -- 2025-01-01 00:00:00.100 -- is then read by a batch
            // that has no partition to put it in: the sink counts it as filtered and `enable_insert_strict`,
            // on by default, fails the refresh for it. So the mapped partitions' bounds are read together
            // with the MV partitions' own ranges -- the term every other table is read through -- and what is
            // read is the rows of the recorded partitions that these MV partitions hold.
            // The two sides are one disjunction each rather than one conjunction per base partition, so that
            // the MV side is written once: this predicate is injected before the plan is bound, and an
            // analyzer rebinds each disjunct, so a batch of N partitions would otherwise rewrite the same
            // MV range N times, giving a predicate of size O(N^2) to build for the same read.
            // Both sides are written at the base partition column's type. The MV partition's own key carries
            // only its primitive type, and Type.fromPrimitiveType(DATETIMEV2) is scale 0: an MV partition
            // whose bounds come from the base partitions -- one partitioned by the base's own column -- would
            // have its upper bound written as .000, and the rows of the last fraction of it would be left
            // out of the read. They are rows this batch does write, so they go missing from the rebuilt
            // partition while the snapshot records it, and nothing reports them.
            builder.put(table, Sets.newHashSet(ExpressionUtils.and(
                    ExpressionUtils.or(constructPredicatesOfBasePartitions(items, partitionColumns, colName)),
                    ExpressionUtils.or(constructPredicates(mvItems, colName,
                            Optional.of(partitionColumnType(partitionColumns, colName)))))));
        }
        return builder.build();
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, String colName) {
        return constructPredicates(partitions, colName, Optional.empty());
    }

    /** The same, for the callers that know the partition column the predicate is written against. */
    private static Set<Expression> constructPredicates(Set<PartitionItem> partitions, String colName,
            Optional<Type> columnType) {
        return constructPredicates(partitions, new UnboundSlot(colName), columnType);
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot) {
        return constructPredicates(partitions, colSlot, Optional.empty());
    }

    private static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot,
            Optional<Type> columnType) {
        return constructPredicates(partitions, colSlot, columnType, 0);
    }

    /**
     * The same, for the callers whose partition items carry the partition columns in the base table's order
     * rather than the MV's: the key value of the column the predicate is written against is at
     * {@code keyPos}, not at the first position.
     */
    private static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot,
            Optional<Type> columnType, int keyPos) {
        Set<Expression> predicates = new HashSet<>();
        if (partitions.isEmpty()) {
            return Sets.newHashSet(BooleanLiteral.TRUE);
        }
        if (partitions.iterator().next() instanceof ListPartitionItem) {
            for (PartitionItem item : partitions) {
                predicates.add(convertListPartitionToIn(item, colSlot, columnType, keyPos));
            }
        } else {
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, colSlot, columnType));
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
            List<Column> partitionColumns, String colName) {
        return constructPredicatesOfBasePartitions(partitions, partitionColumns, colName, UnboundSlot::new);
    }

    /**
     * The same, with the partition columns read through the slots the caller names them by. A caller that
     * writes these predicates into a plan that is already bound and is never bound again -- the union
     * compensation -- has to pass that plan's own slots: an unbound slot there fails the rewrite instead of
     * narrowing the read.
     */
    private static Set<Expression> constructPredicatesOfBasePartitions(Set<PartitionItem> partitions,
            List<Column> partitionColumns, String colName, Function<String, Slot> slotOfColumn) {
        List<Type> partitionColumnTypes = Lists.transform(partitionColumns, Column::getType);
        if (!(partitions.iterator().next() instanceof ListPartitionItem)) {
            Set<Expression> predicates = new HashSet<>();
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, slotOfColumn.apply(colName),
                        Optional.of(partitionColumnTypes.get(0))));
            }
            return predicates;
        }
        List<Slot> partitionSlots = Lists.newArrayList();
        for (Column partitionColumn : partitionColumns) {
            partitionSlots.add(slotOfColumn.apply(partitionColumn.getName()));
        }
        Set<Expression> predicates = new HashSet<>();
        for (PartitionItem item : partitions) {
            predicates.add(convertListPartitionToKey(item, partitionSlots, partitionColumnTypes));
        }
        return predicates;
    }

    /** The type of the partition column of this table the MV partition is named by. */
    private static Type partitionColumnType(List<Column> partitionColumns, String colName) {
        return partitionColumn(partitionColumns, colName).getType();
    }

    /** The position of that column among the table's partition columns, which is where its key value is. */
    private static int partitionColumnPosition(List<Column> partitionColumns, String colName) {
        return partitionColumns.indexOf(partitionColumn(partitionColumns, colName));
    }

    private static Column partitionColumn(List<Column> partitionColumns, String colName) {
        for (Column partitionColumn : partitionColumns) {
            if (partitionColumn.getName().equalsIgnoreCase(colName)) {
                return partitionColumn;
            }
        }
        throw new IllegalStateException("no partition column " + colName);
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

    private static Expression convertListPartitionToIn(PartitionItem item, Slot col, Optional<Type> columnType,
            int keyPos) {
        List<Expression> inValues = ((ListPartitionItem) item).getItems().stream()
                .map(key -> convertPartitionKeyToLiteral(key, keyPos, columnType))
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

        /** The slot this relation reads that column by, or null when it does not read it. */
        private static Slot slotByName(LogicalCatalogRelation catalogRelation, String colName) {
            for (Slot slot : catalogRelation.getOutput()) {
                if (slot.getName().equals(colName)) {
                    return slot;
                }
            }
            return null;
        }

        /**
         * The slots this relation reads the base table's partition columns by, by column name, or null when
         * one of them is not read at all. Such a key has no column to be written against, so the read cannot
         * be pinned to it; see the caller.
         */
        private static Map<String, Slot> partitionColumnSlots(List<Column> partitionColumns,
                LogicalCatalogRelation catalogRelation) {
            Map<String, Slot> res = new HashMap<>();
            for (Column partitionColumn : partitionColumns) {
                Slot slot = slotByName(catalogRelation, partitionColumn.getName());
                if (slot == null) {
                    return null;
                }
                res.put(partitionColumn.getName(), slot);
            }
            return res;
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
                    Slot partitionSlot = slotByName(catalogRelation, relatedTableColumnInfo.getColName());
                    if (partitionSlot == null) {
                        predicates.setHandleSuccess(false);
                        return catalogRelation;
                    }
                    MTMVRelatedTableIf targetTable = (MTMVRelatedTableIf) table;
                    // The base table's own partition columns, in its own order: the whole key of a partition
                    // is written from them, and the column the MV partition is named by need not be the
                    // first of them.
                    List<Column> basePartitionColumns = targetTable.getPartitionColumns(
                            targetTable instanceof OlapTable ? Optional.empty()
                                    : MvccUtil.getSnapshotFromContext(targetTable));
                    // One partition map per relation, not one per partition name: an external table builds
                    // the map from its pinned snapshot, and asking again for each name would build it again.
                    Map<String, PartitionItem> externalItemsByName = targetTable instanceof ExternalTable
                            ? ((ExternalTable) targetTable).getNameToPartitionItems(
                                    MvccUtil.getSnapshotFromContext(targetTable))
                            : null;
                    // if partition has no data, doesn't add filter
                    Set<PartitionItem> partitionHasDataItems = new HashSet<>();
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
                            PartitionItem partitionItem = externalItemsByName.get(partitionName);
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
                        // The partitions are pinned the way a refresh pins them: the whole key of each, at
                        // the partition columns' own types. A predicate on the MV's partition column alone
                        // reads the partitions that differ in the other keys too, and those rows are ones
                        // this branch must not add -- the MV branch of the union already supplies them, or
                        // the compensation would count them twice -- while a key written with a scale has to
                        // be compared at that scale or its rows are read as none. An external table's
                        // partition items are read from the snapshot below the same way as an olap table's:
                        // its keys name its own partitions just as fully.
                        // A list partitioned table's default partition is not pinned the way the others
                        // are: its own key is the sentinel the rows no other partition claims were placed
                        // by, so pinning it to that key reads none of them. It is read the way it was
                        // before the whole key was pinned -- on the MV's partition column alone, at that
                        // column's type -- which can be seen to be too wide rather than one that drops its
                        // rows.
                        boolean hasDefaultPartition = partitionHasDataItems.stream()
                                .anyMatch(PartitionItem::isDefaultPartition);
                        Set<Expression> preds;
                        if (!hasDefaultPartition) {
                            // This plan is already bound and nothing binds it again, so the whole-key
                            // predicates are written against the slots this relation reads.
                            Map<String, Slot> keySlots = partitionColumnSlots(basePartitionColumns,
                                    catalogRelation);
                            if (keySlots == null) {
                                // A partition column the relation does not read has no slot to write the key
                                // against, and pinning the read to the MV's partition column alone would
                                // read the partitions differing in the others -- rows the MV branch of the
                                // union already supplies, which would then be counted twice. Refused rather
                                // than guessed: the query is answered from the base table.
                                predicates.setHandleSuccess(false);
                                return catalogRelation;
                            }
                            preds = constructPredicatesOfBasePartitions(partitionHasDataItems,
                                    basePartitionColumns, relatedTableColumnInfo.getColName(), keySlots::get);
                        } else {
                            preds = constructPredicates(partitionHasDataItems, partitionSlot,
                                    Optional.of(partitionColumnType(basePartitionColumns,
                                            relatedTableColumnInfo.getColName())),
                                    partitionColumnPosition(basePartitionColumns,
                                            relatedTableColumnInfo.getColName()));
                        }
                        return new LogicalFilter<>(
                                ExpressionUtils.extractConjunctionToSet(ExpressionUtils.or(preds)),
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
