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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.mv.AbstractSelectMaterializedIndexRule.ReplaceExpressions;
import org.apache.doris.nereids.rules.rewrite.mv.AbstractSelectMaterializedIndexRule.SlotContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                        .thenApply(ctx -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = ctx.root;
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, filter::getConjuncts,
                                    Stream.concat(filter.getExpressions().stream(),
                                        project.getExpressions().stream()).collect(ImmutableSet.toImmutableSet()));
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                    generateProjectsAlias(project.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                        project.withChildren(filter.withChildren(mvPlan)), mvPlan));
                        }).toRule(RuleType.MATERIALIZED_INDEX_PROJECT_FILTER_SCAN),

                // project with filter that cannot be pushdown.
                // Filter(Project(Scan))
                logicalFilter(logicalProject(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg)))
                        .thenApply(ctx -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = ctx.root;
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, ImmutableSet::of,
                                    new HashSet<>(project.getExpressions()));
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            return new LogicalProject(
                                    generateProjectsAlias(project.getOutput(), slotContext),
                                    new ReplaceExpressions(slotContext).replace(
                                    filter.withChildren(project.withChildren(mvPlan)), mvPlan));
                        }).toRule(RuleType.MATERIALIZED_INDEX_FILTER_PROJECT_SCAN),

                // scan with filters could be pushdown.
                // Filter(Scan)
                logicalFilter(logicalOlapScan().when(this::shouldSelectIndexWithoutAgg))
                        .thenApply(ctx -> {
                            LogicalFilter<LogicalOlapScan> filter = ctx.root;
                            LogicalOlapScan scan = filter.child();
                            LogicalOlapScan mvPlan = select(
                                    scan, filter::getOutputSet, filter::getConjuncts,
                                    new HashSet<>(filter.getExpressions()));
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
                        .thenApply(ctx -> {
                            LogicalProject<LogicalOlapScan> project = ctx.root;
                            LogicalOlapScan scan = project.child();

                            LogicalOlapScan mvPlan = select(
                                    scan, project::getInputSlots, ImmutableSet::of,
                                    new HashSet<>(project.getExpressions()));
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
                        .thenApply(ctx -> {
                            LogicalOlapScan scan = ctx.root;

                            LogicalOlapScan mvPlan = select(
                                    scan, scan::getOutputSet, ImmutableSet::of,
                                    scan.getOutputSet());
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
    private LogicalOlapScan select(
            LogicalOlapScan scan,
            Supplier<Set<Slot>> requiredScanOutputSupplier,
            Supplier<Set<Expression>> predicatesSupplier,
            Set<? extends Expression> requiredExpr) {
        OlapTable table = scan.getTable();
        long baseIndexId = table.getBaseIndexId();
        KeysType keysType = scan.getTable().getKeysType();
        switch (keysType) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                break;
            case DUP_KEYS:
                if (table.getIndexIdToMeta().size() == 1) {
                    return scan.withMaterializedIndexSelected(PreAggStatus.on(), baseIndexId);
                }
                break;
            default:
                throw new RuntimeException("Not supported keys type: " + keysType);
        }
        if (scan.getTable().isDupKeysOrMergeOnWrite()) {
            // Set pre-aggregation to `on` to keep consistency with legacy logic.
            List<MaterializedIndex> candidates = scan
                    .getTable().getVisibleIndex().stream().filter(index -> index.getId() != baseIndexId)
                    .filter(index -> !indexHasAggregate(index, scan)).filter(index -> containAllRequiredColumns(index,
                            scan, requiredScanOutputSupplier.get(), requiredExpr, predicatesSupplier.get()))
                    .collect(Collectors.toList());
            long bestIndex = selectBestIndex(candidates, scan, predicatesSupplier.get());
            return scan.withMaterializedIndexSelected(PreAggStatus.on(), bestIndex);
        } else {
            final PreAggStatus preAggStatus;
            if (preAggEnabledByHint(scan)) {
                // PreAggStatus could be enabled by pre-aggregation hint for agg-keys and unique-keys.
                preAggStatus = PreAggStatus.on();
            } else {
                // if PreAggStatus is OFF, we use the message from SelectMaterializedIndexWithAggregate
                preAggStatus = scan.getPreAggStatus().isOff() ? scan.getPreAggStatus()
                        : PreAggStatus.off("No aggregate on scan.");
            }
            if (table.getIndexIdToMeta().size() == 1) {
                return scan.withMaterializedIndexSelected(preAggStatus, baseIndexId);
            }
            int baseIndexKeySize = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
            // No aggregate on scan.
            // So only base index and indexes that have all the keys could be used.
            List<MaterializedIndex> candidates = table.getVisibleIndex().stream()
                    .filter(index -> table.getKeyColumnsByIndexId(index.getId()).size() == baseIndexKeySize)
                    .filter(index -> containAllRequiredColumns(index, scan, requiredScanOutputSupplier.get(),
                            requiredExpr, predicatesSupplier.get()))
                    .collect(Collectors.toList());

            if (candidates.size() == 1) {
                // `candidates` only have base index.
                return scan.withMaterializedIndexSelected(preAggStatus, baseIndexId);
            } else {
                return scan.withMaterializedIndexSelected(preAggStatus,
                        selectBestIndex(candidates, scan, predicatesSupplier.get()));
            }
        }
    }

    private boolean indexHasAggregate(MaterializedIndex index, LogicalOlapScan scan) {
        return scan.getTable().getSchemaByIndexId(index.getId())
                .stream()
                .anyMatch(Column::isAggregated);
    }
}
