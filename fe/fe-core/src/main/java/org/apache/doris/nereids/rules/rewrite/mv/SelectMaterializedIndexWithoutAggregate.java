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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Select materialized index, i.e., both for rollup and materialized view when aggregate is not present.
 * <p>
 * Scan OLAP table with aggregate is handled in {@link SelectMaterializedIndexWithAggregate}.
 * <p>
 * Note that we should first apply {@link SelectMaterializedIndexWithAggregate} and then
 * {@link SelectMaterializedIndexWithoutAggregate}.
 * Besides, these two rules should run in isolated batches, thus when enter this rule, it's guaranteed that there is
 * no aggregation on top of the scan.
 */
public class SelectMaterializedIndexWithoutAggregate extends AbstractSelectMaterializedIndexRule
        implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // project with pushdown filter.
                // Project(Filter(Scan))
                logicalProject(logicalFilter(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg)))
                        .thenApplyNoThrow(ctx -> {
                            if (ctx.connectContext.getSessionVariable().isEnableSyncMvCostBasedRewrite()) {
                                return ctx.root;
                            }
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = ctx.root;
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, filter::getConjuncts,
                                    Suppliers.memoize(() ->
                                            Utils.concatToSet(filter.getExpressions(), project.getExpressions())
                                    )
                            );
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject<>(
                                    generateProjectsAlias(project.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                        project.withChildren(filter.withChildren(mvPlan)), mvPlan));
                        }).toRule(RuleType.MATERIALIZED_INDEX_PROJECT_FILTER_SCAN),

                // project with filter that cannot be pushdown.
                // Filter(Project(Scan))
                logicalFilter(logicalProject(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg)))
                        .thenApplyNoThrow(ctx -> {
                            if (ctx.connectContext.getSessionVariable().isEnableSyncMvCostBasedRewrite()) {
                                return ctx.root;
                            }
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = ctx.root;
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, ImmutableSet::of,
                                    () -> Utils.fastToImmutableSet(project.getExpressions()));
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                    generateProjectsAlias(project.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                    filter.withChildren(project.withChildren(mvPlan)), mvPlan));
                        }).toRule(RuleType.MATERIALIZED_INDEX_FILTER_PROJECT_SCAN),

                // scan with filters could be pushdown.
                // Filter(Scan)
                logicalFilter(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg))
                        .thenApplyNoThrow(ctx -> {
                            if (ctx.connectContext.getSessionVariable().isEnableSyncMvCostBasedRewrite()) {
                                return ctx.root;
                            }
                            LogicalFilter<LogicalOlapScan> filter = ctx.root;
                            LogicalOlapScan scan = filter.child();
                            LogicalOlapScan mvPlan = select(
                                    scan, filter::getOutputSet, filter::getConjuncts,
                                    Suppliers.memoize(() ->
                                            Utils.concatToSet(filter.getExpressions(), filter.getOutputSet())
                                    )
                            );
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                generateProjectsAlias(scan.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                        new LogicalProject(mvPlan.getOutput(), filter.withChildren(mvPlan)), mvPlan));
                        })
                        .toRule(RuleType.MATERIALIZED_INDEX_FILTER_SCAN),

                // project and scan.
                // Project(Scan)
                logicalProject(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg))
                        .thenApplyNoThrow(ctx -> {
                            if (ctx.connectContext.getSessionVariable().isEnableSyncMvCostBasedRewrite()) {
                                return ctx.root;
                            }
                            LogicalProject<LogicalOlapScan> project = ctx.root;
                            LogicalOlapScan scan = project.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, ImmutableSet::of,
                                    Suppliers.memoize(() -> Utils.fastToImmutableSet(project.getExpressions()))
                            );
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                    generateProjectsAlias(project.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                    project.withChildren(mvPlan), mvPlan));
                        })
                        .toRule(RuleType.MATERIALIZED_INDEX_PROJECT_SCAN),

                // only scan.
                logicalOlapScan()
                        .when(this::shouldSelectIndexWithoutAgg)
                        .thenApplyNoThrow(ctx -> {
                            if (ctx.connectContext.getSessionVariable().isEnableSyncMvCostBasedRewrite()) {
                                return ctx.root;
                            }
                            LogicalOlapScan scan = ctx.root;

                            LogicalOlapScan mvPlan = select(
                                    scan, scan::getOutputSet, ImmutableSet::of,
                                    () -> (Set) scan.getOutputSet());
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                    generateProjectsAlias(mvPlan.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                        new LogicalProject(mvPlan.getOutput(), mvPlan), mvPlan));
                        })
                        .toRule(RuleType.MATERIALIZED_INDEX_SCAN)
        );
    }

    /**
     * Select materialized index when aggregate node is not present.
     *
     * @param scan Scan node.
     * @param requiredScanOutputSupplier Supplier to get the required scan output.
     * @param predicatesSupplier Supplier to get pushdown predicates.
     * @return Result scan node.
     */
    public static LogicalOlapScan select(
            LogicalOlapScan scan,
            Supplier<Set<Slot>> requiredScanOutputSupplier,
            Supplier<Set<Expression>> predicatesSupplier,
            Supplier<Set<Expression>> requiredExpr) {
        OlapTable table = scan.getTable();
        long baseIndexId = table.getBaseIndexId();
        KeysType keysType = scan.getTable().getKeysType();
        switch (keysType) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                break;
            case DUP_KEYS:
                if (table.getIndexIdToMeta().size() == 1) {
                    return scan.withMaterializedIndexSelected(baseIndexId);
                }
                break;
            default:
                throw new RuntimeException("Not supported keys type: " + keysType);
        }

        Supplier<Set<Slot>> requiredSlots = Suppliers.memoize(() -> {
            Set<Slot> set = new HashSet<>();
            set.addAll(requiredScanOutputSupplier.get());
            set.addAll(ExpressionUtils.getInputSlotSet(requiredExpr.get()));
            set.addAll(ExpressionUtils.getInputSlotSet(predicatesSupplier.get()));
            return set;
        });
        if (scan.getTable().isDupKeysOrMergeOnWrite()) {
            // Set pre-aggregation to `on` to keep consistency with legacy logic.
            List<MaterializedIndex> candidates = scan
                    .getTable().getVisibleIndex().stream().filter(index -> index.getId() != baseIndexId)
                    .filter(index -> !indexHasAggregate(index, scan)).filter(index -> containAllRequiredColumns(index,
                            scan, requiredScanOutputSupplier.get(), requiredExpr.get(), predicatesSupplier.get()))
                    .collect(Collectors.toList());
            long bestIndex = selectBestIndex(candidates, scan, predicatesSupplier.get(), requiredExpr.get());
            // this is fail-safe for select mv
            // select baseIndex if bestIndex's slots' data types are different from baseIndex
            bestIndex = isSameDataType(scan, bestIndex, requiredSlots.get()) ? bestIndex : baseIndexId;
            return scan.withMaterializedIndexSelected(bestIndex);
        } else {
            if (table.getIndexIdToMeta().size() == 1) {
                return scan.withMaterializedIndexSelected(baseIndexId);
            }
            int baseIndexKeySize = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
            // No aggregate on scan.
            // So only base index and indexes that have all the keys could be used.
            List<MaterializedIndex> candidates = table.getVisibleIndex().stream()
                    .filter(index -> table.getKeyColumnsByIndexId(index.getId()).size() == baseIndexKeySize)
                    .filter(index -> containAllRequiredColumns(index, scan, requiredScanOutputSupplier.get(),
                            requiredExpr.get(), predicatesSupplier.get()))
                    .collect(Collectors.toList());

            if (candidates.size() == 1) {
                // `candidates` only have base index.
                return scan.withMaterializedIndexSelected(baseIndexId);
            } else {
                long bestIndex = selectBestIndex(candidates, scan, predicatesSupplier.get(), requiredExpr.get());
                // this is fail-safe for select mv
                // select baseIndex if bestIndex's slots' data types are different from baseIndex
                bestIndex = isSameDataType(scan, bestIndex, requiredSlots.get()) ? bestIndex : baseIndexId;
                return scan.withMaterializedIndexSelected(bestIndex);
            }
        }
    }

    private static boolean isSameDataType(LogicalOlapScan scan, long selectIndex, Set<Slot> slots) {
        if (selectIndex != scan.getTable().getBaseIndexId()) {
            Map<String, PrimitiveType> columnTypes =
                    scan.getTable().getSchemaByIndexId(selectIndex).stream().collect(Collectors
                            .toMap(Column::getNameWithoutMvPrefix, column -> column.getDataType()));
            return slots.stream().allMatch(slot -> {
                PrimitiveType dataType =
                        columnTypes.getOrDefault(slot.getName(), PrimitiveType.NULL_TYPE);
                return dataType == PrimitiveType.NULL_TYPE
                        || dataType == slot.getDataType().toCatalogDataType().getPrimitiveType();
            });
        }
        return true;
    }

    private static boolean indexHasAggregate(MaterializedIndex index, LogicalOlapScan scan) {
        return scan.getTable().getSchemaByIndexId(index.getId())
                .stream()
                .anyMatch(Column::isAggregated);
    }
}
