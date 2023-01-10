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

package org.apache.doris.nereids.rules.mv;

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Select materialized index, i.e., both for rollup and materialized view when aggregate is present.
 * TODO: optimize queries with aggregate not on top of scan directly, e.g., aggregate -> join -> scan
 *   to use materialized index.
 */
@Developing
public class SelectMaterializedIndexWithAggregate extends AbstractSelectMaterializedIndexRule
        implements RewriteRuleFactory {
    ///////////////////////////////////////////////////////////////////////////
    // All the patterns
    ///////////////////////////////////////////////////////////////////////////
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only agg above scan
                // Aggregate(Scan)
                logicalAggregate(logicalOlapScan().when(this::shouldSelectIndex)).then(agg -> {
                    LogicalOlapScan scan = agg.child();
                    SelectResult result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableSet.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            agg.getGroupByExpressions());
                    if (result.exprRewriteMap.isEmpty()) {
                        return agg.withChildren(
                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                        );
                    } else {
                        return new LogicalAggregate<>(
                                agg.getGroupByExpressions(),
                                replaceAggOutput(agg, Optional.empty(), Optional.empty(), result.exprRewriteMap),
                                agg.isNormalized(),
                                agg.getSourceRepeat(),
                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                        );
                    }
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_SCAN),

                // filter could push down scan.
                // Aggregate(Filter(Scan))
                logicalAggregate(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();

                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    agg.getGroupByExpressions()
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(filter.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                ));
                            } else {
                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.empty(), Optional.empty(),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        // Note that no need to replace slots in the filter, because the slots to
                                        // replace are value columns, which shouldn't appear in filters.
                                        filter.withChildren(
                                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableSet.of(),
                                    extractAggFunctionAndReplaceSlot(agg,
                                            Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(
                                        project.withChildren(
                                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                        )
                                );
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(
                                        newProjectList,
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));
                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        newProject
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(project.withChildren(filter.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                )));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(newProjectList,
                                        filter.withChildren(scan.withMaterializedIndexSelected(result.preAggStatus,
                                                result.indexId)));

                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        newProject
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableSet.of(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(filter.withChildren(project.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                )));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(newProjectList,
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));

                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        filter.withChildren(newProject)
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_PROJECT_SCAN),

                // only agg above scan
                // Aggregate(Repeat(Scan))
                logicalAggregate(logicalRepeat(logicalOlapScan().when(this::shouldSelectIndex))).then(agg -> {
                    LogicalRepeat<LogicalOlapScan> repeat = agg.child();
                    LogicalOlapScan scan = repeat.child();
                    SelectResult result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableSet.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            nonVirtualGroupByExprs(agg));
                    if (result.exprRewriteMap.isEmpty()) {
                        return agg.withChildren(
                                repeat.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                        );
                    } else {
                        return new LogicalAggregate<>(
                                agg.getGroupByExpressions(),
                                replaceAggOutput(agg, Optional.empty(), Optional.empty(), result.exprRewriteMap),
                                agg.isNormalized(),
                                agg.getSourceRepeat(),
                                repeat.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                        );
                    }
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_SCAN),

                // filter could push down scan.
                // Aggregate(Repeat(Filter(Scan)))
                logicalAggregate(logicalRepeat(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex))))
                        .then(agg -> {
                            LogicalRepeat<LogicalFilter<LogicalOlapScan>> repeat = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = repeat.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();

                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    nonVirtualGroupByExprs(agg)
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(
                                        repeat.withChildren(
                                                filter.withChildren(
                                                        scan.withMaterializedIndexSelected(result.preAggStatus,
                                                                result.indexId))
                                        ));
                            } else {
                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.empty(), Optional.empty(),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        // Not that no need to replace slots in the filter, because the slots to replace
                                        // are value columns, which shouldn't appear in filters.
                                        repeat.withChildren(filter.withChildren(
                                                scan.withMaterializedIndexSelected(result.preAggStatus,
                                                        result.indexId)))
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Repeat(Project(Scan)))
                logicalAggregate(logicalRepeat(logicalProject(logicalOlapScan().when(this::shouldSelectIndex))))
                        .then(agg -> {
                            LogicalRepeat<LogicalProject<LogicalOlapScan>> repeat = agg.child();
                            LogicalProject<LogicalOlapScan> project = repeat.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableSet.of(),
                                    extractAggFunctionAndReplaceSlot(agg,
                                            Optional.of(project)),
                                    ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(
                                        repeat.withChildren(
                                                project.withChildren(
                                                        scan.withMaterializedIndexSelected(result.preAggStatus,
                                                                result.indexId)
                                                ))
                                );
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(
                                        newProjectList,
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));
                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        repeat.withChildren(newProject)
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Repeat(Project(Filter(Scan))))
                logicalAggregate(logicalRepeat(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex))))).then(agg -> {
                            LogicalRepeat<LogicalProject<LogicalFilter<LogicalOlapScan>>> repeat = agg.child();
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = repeat.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(repeat.withChildren(project.withChildren(filter.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                                )));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(newProjectList,
                                        filter.withChildren(scan.withMaterializedIndexSelected(result.preAggStatus,
                                                result.indexId)));

                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        repeat.withChildren(newProject)
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Repeat(Filter(Project(Scan))))
                logicalAggregate(logicalRepeat(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex))))).then(agg -> {
                            LogicalRepeat<LogicalFilter<LogicalProject<LogicalOlapScan>>> repeat = agg.child();
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = repeat.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableSet.of(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                            project.getAliasToProducer())
                            );

                            if (result.exprRewriteMap.isEmpty()) {
                                return agg.withChildren(repeat.withChildren(filter.withChildren(project.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                                )));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(newProjectList,
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));

                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        repeat.withChildren(filter.withChildren(newProject))
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_FILTER_PROJECT_SCAN)
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // Main entrance of select materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Select materialized index ids.
     * <p>
     * 1. find candidate indexes by pre-agg status:
     * checking input aggregate functions and group by expressions and pushdown predicates.
     * 2. filter indexes that have all the required columns.
     * 3. select best index from all the candidate indexes that could use.
     */
    private SelectResult select(
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            Set<Expression> predicates,
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        // remove virtual slot for grouping sets.
        Set<Slot> nonVirtualRequiredScanOutput = requiredScanOutput.stream()
                .filter(slot -> !(slot instanceof VirtualSlotReference))
                .collect(ImmutableSet.toImmutableSet());
        Preconditions.checkArgument(scan.getOutputSet().containsAll(nonVirtualRequiredScanOutput),
                String.format("Scan's output (%s) should contains all the input required scan output (%s).",
                        scan.getOutput(), nonVirtualRequiredScanOutput));

        OlapTable table = scan.getTable();

        switch (table.getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS: {
                // Only checking pre-aggregation status by base index is enough for aggregate-keys and
                // unique-keys OLAP table.
                // Because the schemas in non-base materialized index are subsets of the schema of base index.
                PreAggStatus preAggStatus = checkPreAggStatus(scan, table.getBaseIndexId(), predicates,
                        aggregateFunctions, groupingExprs);
                if (preAggStatus.isOff()) {
                    // return early if pre agg status if off.
                    return new SelectResult(preAggStatus, scan.getTable().getBaseIndexId(), new ExprRewriteMap());
                } else {
                    List<MaterializedIndex> rollupsWithAllRequiredCols = table.getVisibleIndex().stream()
                            .filter(index -> containAllRequiredColumns(index, scan, nonVirtualRequiredScanOutput))
                            .collect(Collectors.toList());
                    return new SelectResult(preAggStatus, selectBestIndex(rollupsWithAllRequiredCols, scan, predicates),
                            new ExprRewriteMap());
                }
            }
            case DUP_KEYS: {
                Map<Boolean, List<MaterializedIndex>> indexesGroupByIsBaseOrNot = table.getVisibleIndex()
                        .stream()
                        .collect(Collectors.groupingBy(index -> index.getId() == table.getBaseIndexId()));

                // Duplicate-keys table could use base index and indexes that pre-aggregation status is on.
                Set<MaterializedIndex> candidatesWithoutRewriting = Stream.concat(
                        indexesGroupByIsBaseOrNot.get(true).stream(),
                        indexesGroupByIsBaseOrNot.getOrDefault(false, ImmutableList.of())
                                .stream()
                                .filter(index -> checkPreAggStatus(scan, index.getId(), predicates,
                                        aggregateFunctions, groupingExprs).isOn())
                ).collect(ImmutableSet.toImmutableSet());

                // try to rewrite bitmap, hll by materialized index columns.
                List<AggRewriteResult> candidatesWithRewriting = indexesGroupByIsBaseOrNot.getOrDefault(false,
                                ImmutableList.of())
                        .stream()
                        .filter(index -> !candidatesWithoutRewriting.contains(index))
                        .map(index -> rewriteAgg(index, scan, nonVirtualRequiredScanOutput, predicates,
                                aggregateFunctions,
                                groupingExprs))
                        .filter(aggRewriteResult -> checkPreAggStatus(scan, aggRewriteResult.index.getId(),
                                predicates,
                                // check pre-agg status of aggregate function that couldn't rewrite.
                                aggFuncsDiff(aggregateFunctions, aggRewriteResult),
                                groupingExprs).isOn())
                        .filter(result -> result.success)
                        .collect(Collectors.toList());

                List<MaterializedIndex> haveAllRequiredColumns = Streams.concat(
                        candidatesWithoutRewriting.stream()
                                .filter(index -> containAllRequiredColumns(index, scan, nonVirtualRequiredScanOutput)),
                        candidatesWithRewriting
                                .stream()
                                .filter(aggRewriteResult -> containAllRequiredColumns(aggRewriteResult.index, scan,
                                        aggRewriteResult.requiredScanOutput))
                                .map(aggRewriteResult -> aggRewriteResult.index)
                ).collect(Collectors.toList());

                long selectIndexId = selectBestIndex(haveAllRequiredColumns, scan, predicates);
                Optional<AggRewriteResult> rewriteResultOpt = candidatesWithRewriting.stream()
                        .filter(aggRewriteResult -> aggRewriteResult.index.getId() == selectIndexId)
                        .findAny();
                // Pre-aggregation is set to `on` by default for duplicate-keys table.
                return new SelectResult(PreAggStatus.on(), selectIndexId,
                        rewriteResultOpt.map(r -> r.exprRewriteMap).orElse(new ExprRewriteMap()));
            }
            default:
                throw new RuntimeException("Not supported keys type: " + table.getKeysType());
        }
    }

    private List<AggregateFunction> aggFuncsDiff(List<AggregateFunction> aggregateFunctions,
            AggRewriteResult aggRewriteResult) {
        if (aggRewriteResult.success) {
            return ImmutableList.copyOf(Sets.difference(ImmutableSet.copyOf(aggregateFunctions),
                    aggRewriteResult.exprRewriteMap.aggFuncMap.keySet()));
        } else {
            return aggregateFunctions;
        }
    }

    private static class SelectResult {
        public final PreAggStatus preAggStatus;
        public final long indexId;
        public ExprRewriteMap exprRewriteMap;

        public SelectResult(PreAggStatus preAggStatus, long indexId, ExprRewriteMap exprRewriteMap) {
            this.preAggStatus = preAggStatus;
            this.indexId = indexId;
            this.exprRewriteMap = exprRewriteMap;
        }
    }

    /**
     * Do aggregate function extraction and replace aggregate function's input slots by underlying project.
     * <p>
     * 1. extract aggregate functions in aggregate plan.
     * <p>
     * 2. replace aggregate function's input slot by underlying project expression if project is present.
     * <p>
     * For example:
     * <pre>
     * input arguments:
     * agg: Aggregate(sum(v) as sum_value)
     * underlying project: Project(a + b as v)
     *
     * output:
     * sum(a + b)
     * </pre>
     */
    private List<AggregateFunction> extractAggFunctionAndReplaceSlot(
            LogicalAggregate<?> agg,
            Optional<LogicalProject<?>> project) {
        Optional<Map<Slot, Expression>> slotToProducerOpt = project.map(Project::getAliasToProducer);
        return agg.getOutputExpressions().stream()
                // extract aggregate functions.
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                // replace aggregate function's input slot by its producing expression.
                .map(expr -> slotToProducerOpt.map(slotToExpressions
                                -> (AggregateFunction) ExpressionUtils.replace(expr, slotToExpressions))
                        .orElse(expr)
                )
                .collect(Collectors.toList());
    }

    private static AggregateFunction replaceAggFuncInput(AggregateFunction aggFunc,
            Optional<Map<Slot, Expression>> slotToProducerOpt) {
        return slotToProducerOpt.map(
                        slotToExpressions -> (AggregateFunction) ExpressionUtils.replace(aggFunc, slotToExpressions))
                .orElse(aggFunc);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Set pre-aggregation status.
    ///////////////////////////////////////////////////////////////////////////
    private PreAggStatus checkPreAggStatus(
            LogicalOlapScan olapScan,
            long indexId,
            Set<Expression> predicates,
            List<AggregateFunction> aggregateFuncs,
            List<Expression> groupingExprs) {
        CheckContext checkContext = new CheckContext(olapScan, indexId);
        return checkAggregateFunctions(aggregateFuncs, checkContext)
                .offOrElse(() -> checkGroupingExprs(groupingExprs, checkContext))
                .offOrElse(() -> checkPredicates(ImmutableList.copyOf(predicates), checkContext));
    }

    /**
     * Check pre agg status according to aggregate functions.
     */
    private PreAggStatus checkAggregateFunctions(
            List<AggregateFunction> aggregateFuncs,
            CheckContext checkContext) {
        return aggregateFuncs.stream()
                .map(f -> AggregateFunctionChecker.INSTANCE.check(f, checkContext))
                .filter(PreAggStatus::isOff)
                .findAny()
                .orElse(PreAggStatus.on());
    }

    // TODO: support all the aggregate function types in storage engine.
    private static class AggregateFunctionChecker extends ExpressionVisitor<PreAggStatus, CheckContext> {

        public static final AggregateFunctionChecker INSTANCE = new AggregateFunctionChecker();

        public PreAggStatus check(AggregateFunction aggFun, CheckContext ctx) {
            return aggFun.accept(INSTANCE, ctx);
        }

        @Override
        public PreAggStatus visit(Expression expr, CheckContext context) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction, CheckContext context) {
            return PreAggStatus.off(String.format("Aggregate %s function is not supported in storage engine.",
                    aggregateFunction.getName()));
        }

        @Override
        public PreAggStatus visitMax(Max max, CheckContext context) {
            return checkAggFunc(max, AggregateType.MAX, extractSlotId(max.child()), context, true);
        }

        @Override
        public PreAggStatus visitMin(Min min, CheckContext context) {
            return checkAggFunc(min, AggregateType.MIN, extractSlotId(min.child()), context, true);
        }

        @Override
        public PreAggStatus visitSum(Sum sum, CheckContext context) {
            return checkAggFunc(sum, AggregateType.SUM, extractSlotId(sum.child()), context, false);
        }

        @Override
        public PreAggStatus visitCount(Count count, CheckContext context) {
            if (count.isDistinct() && count.arity() == 1) {
                Optional<ExprId> exprIdOpt = extractSlotId(count.child(0));
                if (exprIdOpt.isPresent() && context.exprIdToKeyColumn.containsKey(exprIdOpt.get())) {
                    return PreAggStatus.on();
                }
            }
            return PreAggStatus.off(String.format(
                    "Count distinct is only valid for key columns, but meet %s.", count.toSql()));
        }

        @Override
        public PreAggStatus visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, CheckContext context) {
            Expression expr = bitmapUnionCount.child();
            if (expr instanceof ToBitmap) {
                expr = expr.child(0);
            }
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(expr);
            if (slotOpt.isPresent() && context.exprIdToValueColumn.containsKey(slotOpt.get().getExprId())) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid bitmap_union_count: " + bitmapUnionCount.toSql());
            }
        }

        @Override
        public PreAggStatus visitHllUnionAgg(HllUnionAgg hllUnionAgg, CheckContext context) {
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllUnionAgg.child());
            if (slotOpt.isPresent() && context.exprIdToValueColumn.containsKey(slotOpt.get().getExprId())) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid hll_union_agg: " + hllUnionAgg.toSql());
            }
        }

        private PreAggStatus checkAggFunc(
                AggregateFunction aggFunc,
                AggregateType matchingAggType,
                Optional<ExprId> exprIdOpt,
                CheckContext ctx,
                boolean canUseKeyColumn) {
            return exprIdOpt.map(exprId -> {
                if (ctx.exprIdToKeyColumn.containsKey(exprId)) {
                    if (canUseKeyColumn) {
                        return PreAggStatus.on();
                    } else {
                        Column column = ctx.exprIdToKeyColumn.get(exprId);
                        return PreAggStatus.off(String.format("Aggregate function %s contains key column %s.",
                                aggFunc.toSql(), column.getName()));
                    }
                } else if (ctx.exprIdToValueColumn.containsKey(exprId)) {
                    AggregateType aggType = ctx.exprIdToValueColumn.get(exprId).getAggregationType();
                    if (aggType == matchingAggType) {
                        return PreAggStatus.on();
                    } else {
                        return PreAggStatus.off(String.format("Aggregate operator don't match, aggregate function: %s"
                                + ", column aggregate type: %s", aggFunc.toSql(), aggType));
                    }
                } else {
                    return PreAggStatus.off(String.format("Slot(%s) in %s is neither key column nor value column.",
                            exprId, aggFunc.toSql()));
                }
            }).orElse(PreAggStatus.off(String.format("Input of aggregate function %s should be slot or cast on slot.",
                    aggFunc.toSql())));
        }

        // TODO: support more type of expressions, such as case when.
        private Optional<ExprId> extractSlotId(Expression expr) {
            return ExpressionUtils.isSlotOrCastOnSlot(expr);
        }
    }

    private static class CheckContext {
        public final Map<ExprId, Column> exprIdToKeyColumn;
        public final Map<ExprId, Column> exprIdToValueColumn;

        public final LogicalOlapScan scan;

        public CheckContext(LogicalOlapScan scan, long indexId) {
            this.scan = scan;
            // map<is_key, map<column_name, column>>
            Map<Boolean, Map<String, Column>> nameToColumnGroupingByIsKey
                    = scan.getTable().getSchemaByIndexId(indexId)
                    .stream()
                    .collect(Collectors.groupingBy(
                            Column::isKey,
                            Collectors.toMap(Column::getNameWithoutMvPrefix, Function.identity())));
            Map<String, Column> keyNameToColumn = nameToColumnGroupingByIsKey.get(true);
            Map<String, Column> valueNameToColumn = nameToColumnGroupingByIsKey.getOrDefault(false, ImmutableMap.of());
            Map<String, ExprId> nameToExprId = Stream.concat(
                            scan.getOutput().stream(), scan.getNonUserVisibleOutput().stream())
                    .collect(Collectors.toMap(
                            NamedExpression::getName,
                            NamedExpression::getExprId)
                    );
            this.exprIdToKeyColumn = keyNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
            this.exprIdToValueColumn = valueNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
        }
    }

    /**
     * Grouping expressions should not have value type columns.
     */
    private PreAggStatus checkGroupingExprs(
            List<Expression> groupingExprs,
            CheckContext checkContext) {
        return disablePreAggIfContainsAnyValueColumn(groupingExprs, checkContext,
                "Grouping expression %s contains value column %s");
    }

    /**
     * Predicates should not have value type columns.
     */
    private PreAggStatus checkPredicates(
            List<Expression> predicates,
            CheckContext checkContext) {
        return disablePreAggIfContainsAnyValueColumn(predicates, checkContext,
                "Predicate %s contains value column %s");
    }

    /**
     * Check the input expressions have no referenced slot to underlying value type column.
     */
    private PreAggStatus disablePreAggIfContainsAnyValueColumn(List<Expression> exprs, CheckContext ctx,
            String errorMsg) {
        Map<ExprId, Column> exprIdToValueColumn = ctx.exprIdToValueColumn;
        return exprs.stream()
                .map(expr -> expr.getInputSlots()
                        .stream()
                        .filter(slot -> exprIdToValueColumn.containsKey(slot.getExprId()))
                        .findAny()
                        .map(slot -> Pair.of(expr, exprIdToValueColumn.get(slot.getExprId())))
                )
                .filter(Optional::isPresent)
                .findAny()
                .orElse(Optional.empty())
                .map(exprToColumn -> PreAggStatus.off(String.format(errorMsg,
                        exprToColumn.key().toSql(), exprToColumn.value().getName())))
                .orElse(PreAggStatus.on());
    }

    /**
     * rewrite for bitmap and hll
     */
    private AggRewriteResult rewriteAgg(MaterializedIndex index,
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            Set<Expression> predicates,
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        ExprRewriteMap exprRewriteMap = new ExprRewriteMap();
        RewriteContext context = new RewriteContext(new CheckContext(scan, index.getId()), exprRewriteMap);
        aggregateFunctions.forEach(aggFun -> AggFuncRewriter.rewrite(aggFun, context));

        // has rewritten agg functions
        Map<Slot, Slot> slotMap = exprRewriteMap.slotMap;
        if (!slotMap.isEmpty()) {
            // Note that the slots in the rewritten agg functions shouldn't appear in filters or grouping expressions.
            // For example: we have a duplicated-type table t(c1, c2) and a materialized index that has
            // a bitmap_union column `mv_bitmap_union_c2` for the column c2.
            // The query `select c1, count(distinct c2) from t where c2 > 0 group by c1` can't use the materialized
            // index because we have a filter `c2 > 0` for the aggregated column c2.
            Set<Slot> slotsToReplace = slotMap.keySet();
            if (!isInputSlotsContainsAny(ImmutableList.copyOf(predicates), slotsToReplace)
                    && !isInputSlotsContainsAny(groupingExprs, slotsToReplace)) {
                ImmutableSet<Slot> newRequiredSlots = requiredScanOutput.stream()
                        .map(slot -> (Slot) ExpressionUtils.replace(slot, slotMap))
                        .collect(ImmutableSet.toImmutableSet());
                return new AggRewriteResult(index, true, newRequiredSlots, exprRewriteMap);
            }
        }

        return new AggRewriteResult(index, false, null, null);
    }

    private static class ExprRewriteMap {

        /**
         * Replace map for scan output slot.
         */
        public final Map<Slot, Slot> slotMap;

        /**
         * Replace map for expressions in project.
         */
        public final Map<Expression, Expression> projectExprMap;
        /**
         * Replace map for aggregate functions.
         */
        public final Map<AggregateFunction, AggregateFunction> aggFuncMap;

        public ExprRewriteMap() {
            this.slotMap = Maps.newHashMap();
            this.projectExprMap = Maps.newHashMap();
            this.aggFuncMap = Maps.newHashMap();
        }

        public boolean isEmpty() {
            return slotMap.isEmpty();
        }
    }

    private static class AggRewriteResult {
        public final MaterializedIndex index;
        public final boolean success;
        public final Set<Slot> requiredScanOutput;
        public ExprRewriteMap exprRewriteMap;

        public AggRewriteResult(MaterializedIndex index,
                boolean success,
                Set<Slot> requiredScanOutput,
                ExprRewriteMap exprRewriteMap) {
            this.index = index;
            this.success = success;
            this.requiredScanOutput = requiredScanOutput;
            this.exprRewriteMap = exprRewriteMap;
        }
    }

    private boolean isInputSlotsContainsAny(List<Expression> expressions, Set<Slot> slotsToCheck) {
        Set<Slot> inputSlotSet = ExpressionUtils.getInputSlotSet(expressions);
        return !Sets.intersection(inputSlotSet, slotsToCheck).isEmpty();
    }

    private static class RewriteContext {
        public final CheckContext checkContext;
        public final ExprRewriteMap exprRewriteMap;

        public RewriteContext(CheckContext context, ExprRewriteMap exprRewriteMap) {
            this.checkContext = context;
            this.exprRewriteMap = exprRewriteMap;
        }
    }

    private static class AggFuncRewriter extends DefaultExpressionRewriter<RewriteContext> {
        public static final AggFuncRewriter INSTANCE = new AggFuncRewriter();

        private static Expression rewrite(Expression expr, RewriteContext context) {
            return expr.accept(INSTANCE, context);
        }

        /**
         * count(distinct col) -> bitmap_union_count(mv_bitmap_union_col)
         * count(col) -> sum(mv_count_col)
         */
        @Override
        public Expression visitCount(Count count, RewriteContext context) {
            if (count.isDistinct() && count.arity() == 1) {
                // count(distinct col) -> bitmap_union_count(mv_bitmap_union_col)
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(count.child(0));

                // count distinct a value column.
                if (slotOpt.isPresent() && !context.checkContext.exprIdToKeyColumn.containsKey(
                        slotOpt.get().getExprId())) {
                    String bitmapUnionColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(bitmapUnionColumn);
                    // has bitmap_union column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                        Slot bitmapUnionSlot = context.checkContext.scan.getNonUserVisibleOutput()
                                .stream()
                                .filter(s -> s.getName().equals(bitmapUnionColumn))
                                .findFirst()
                                .get();

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), bitmapUnionSlot);
                        context.exprRewriteMap.projectExprMap.put(slotOpt.get(), bitmapUnionSlot);
                        BitmapUnionCount bitmapUnionCount = new BitmapUnionCount(bitmapUnionSlot);
                        context.exprRewriteMap.aggFuncMap.put(count, bitmapUnionCount);
                        return bitmapUnionCount;
                    }
                }
            } else if (!count.isDistinct() && count.arity() == 1) {
                // count(col) -> sum(mv_count_col)

                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(count.child(0));
                // count a value column.
                if (slotOpt.isPresent() && !context.checkContext.exprIdToKeyColumn.containsKey(
                        slotOpt.get().getExprId())) {
                    String countColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder("count", slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(countColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                        Slot countSlot = context.checkContext.scan.getNonUserVisibleOutput()
                                .stream()
                                .filter(s -> s.getName().equals(countColumn))
                                .findFirst()
                                .get();

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), countSlot);
                        context.exprRewriteMap.projectExprMap.put(slotOpt.get(), countSlot);
                        Sum sum = new Sum(countSlot);
                        context.exprRewriteMap.aggFuncMap.put(count, sum);
                        return sum;
                    }
                }
            }
            return count;
        }

        /**
         * bitmap_union_count(to_bitmap(col)) -> bitmap_union_count(mv_bitmap_union_col)
         */
        @Override
        public Expression visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, RewriteContext context) {
            if (bitmapUnionCount.child() instanceof ToBitmap) {
                ToBitmap toBitmap = (ToBitmap) bitmapUnionCount.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(toBitmap.child());
                if (slotOpt.isPresent()) {
                    String bitmapUnionCountColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(bitmapUnionCountColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {

                        Slot bitmapUnionCountSlot = context.checkContext.scan.getNonUserVisibleOutput()
                                .stream()
                                .filter(s -> s.getName().equals(bitmapUnionCountColumn))
                                .findFirst()
                                .get();

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), bitmapUnionCountSlot);
                        context.exprRewriteMap.projectExprMap.put(toBitmap, bitmapUnionCountSlot);
                        BitmapUnionCount newBitmapUnionCount = new BitmapUnionCount(bitmapUnionCountSlot);
                        context.exprRewriteMap.aggFuncMap.put(bitmapUnionCount, newBitmapUnionCount);
                        return newBitmapUnionCount;
                    }
                }
            }

            return bitmapUnionCount;
        }

        /**
         * hll_union(hll_hash(col)) to hll_union(mv_hll_union_col)
         */
        @Override
        public Expression visitHllUnion(HllUnion hllUnion, RewriteContext context) {
            if (hllUnion.child() instanceof HllHash) {
                HllHash hllHash = (HllHash) hllUnion.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllHash.child());
                if (slotOpt.isPresent()) {
                    String hllUnionColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.HLL_UNION.name().toLowerCase(), slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(hllUnionColumn);
                    // has hll_union column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                        Slot hllUnionSlot = context.checkContext.scan.getNonUserVisibleOutput()
                                .stream()
                                .filter(s -> s.getName().equals(hllUnionColumn))
                                .findFirst()
                                .get();

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), hllUnionSlot);
                        context.exprRewriteMap.projectExprMap.put(hllHash, hllUnionSlot);
                        HllUnion newHllUnion = new HllUnion(hllUnionSlot);
                        context.exprRewriteMap.aggFuncMap.put(hllUnion, newHllUnion);
                        return newHllUnion;
                    }
                }
            }

            return hllUnion;
        }

        /**
         * hll_union_agg(hll_hash(col)) -> hll_union-agg(mv_hll_union_col)
         */
        @Override
        public Expression visitHllUnionAgg(HllUnionAgg hllUnionAgg, RewriteContext context) {
            if (hllUnionAgg.child() instanceof HllHash) {
                HllHash hllHash = (HllHash) hllUnionAgg.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllHash.child());
                if (slotOpt.isPresent()) {
                    String hllUnionColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.HLL_UNION.name().toLowerCase(), slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(hllUnionColumn);
                    // has hll_union column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                        Slot hllUnionSlot = context.checkContext.scan.getNonUserVisibleOutput()
                                .stream()
                                .filter(s -> s.getName().equals(hllUnionColumn))
                                .findFirst()
                                .get();

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), hllUnionSlot);
                        context.exprRewriteMap.projectExprMap.put(hllHash, hllUnionSlot);
                        HllUnionAgg newHllUnionAgg = new HllUnionAgg(hllUnionSlot);
                        context.exprRewriteMap.aggFuncMap.put(hllUnionAgg, newHllUnionAgg);
                        return newHllUnionAgg;
                    }
                }
            }

            return hllUnionAgg;
        }

        /**
         * ndv(col) -> hll_union_agg(mv_hll_union_col)
         */
        @Override
        public Expression visitNdv(Ndv ndv, RewriteContext context) {
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(ndv.child(0));
            // ndv on a value column.
            if (slotOpt.isPresent() && !context.checkContext.exprIdToKeyColumn.containsKey(
                    slotOpt.get().getExprId())) {
                String hllUnionColumn = CreateMaterializedViewStmt
                        .mvColumnBuilder(AggregateType.HLL_UNION.name().toLowerCase(), slotOpt.get().getName());

                Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(hllUnionColumn);
                // has hll_union column
                if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                    Slot hllUnionSlot = context.checkContext.scan.getNonUserVisibleOutput()
                            .stream()
                            .filter(s -> s.getName().equals(hllUnionColumn))
                            .findFirst()
                            .get();

                    context.exprRewriteMap.slotMap.put(slotOpt.get(), hllUnionSlot);
                    context.exprRewriteMap.projectExprMap.put(slotOpt.get(), hllUnionSlot);
                    HllUnionAgg hllUnionAgg = new HllUnionAgg(hllUnionSlot);
                    context.exprRewriteMap.aggFuncMap.put(ndv, hllUnionAgg);
                    return hllUnionAgg;
                }
            }
            return ndv;
        }
    }

    private List<NamedExpression> replaceAggOutput(
            LogicalAggregate<? extends Plan> agg,
            Optional<Project> oldProjectOpt,
            Optional<Project> newProjectOpt,
            ExprRewriteMap exprRewriteMap) {
        ResultAggFuncRewriteCtx ctx = new ResultAggFuncRewriteCtx(oldProjectOpt, newProjectOpt, exprRewriteMap);
        return agg.getOutputExpressions()
                .stream()
                .map(expr -> (NamedExpression) ResultAggFuncRewriter.rewrite(expr, ctx))
                .collect(ImmutableList.toImmutableList());
    }

    private static class ResultAggFuncRewriteCtx {
        public final Optional<Map<Slot, Expression>> oldProjectSlotToProducerOpt;
        public final Optional<Map<Expression, Slot>> newProjectExprMapOpt;
        public final ExprRewriteMap exprRewriteMap;

        public ResultAggFuncRewriteCtx(
                Optional<Project> oldProject,
                Optional<Project> newProject,
                ExprRewriteMap exprRewriteMap) {
            this.oldProjectSlotToProducerOpt = oldProject.map(Project::getAliasToProducer);
            this.newProjectExprMapOpt = newProject.map(project -> project.getProjects()
                    .stream()
                    .filter(Alias.class::isInstance)
                    .collect(
                            Collectors.toMap(
                                    // Avoid cast to alias, retrieving the first child expression.
                                    alias -> alias.child(0),
                                    NamedExpression::toSlot
                            )
                    ));
            this.exprRewriteMap = exprRewriteMap;
        }
    }

    private static class ResultAggFuncRewriter extends DefaultExpressionRewriter<ResultAggFuncRewriteCtx> {
        public static final ResultAggFuncRewriter INSTANCE = new ResultAggFuncRewriter();

        public static Expression rewrite(Expression expr, ResultAggFuncRewriteCtx ctx) {
            return expr.accept(INSTANCE, ctx);
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction,
                ResultAggFuncRewriteCtx ctx) {
            // normalize aggregate function to match the agg func replace map.
            AggregateFunction aggFunc = replaceAggFuncInput(aggregateFunction, ctx.oldProjectSlotToProducerOpt);
            Map<AggregateFunction, AggregateFunction> aggFuncMap = ctx.exprRewriteMap.aggFuncMap;
            if (aggFuncMap.containsKey(aggFunc)) {
                AggregateFunction replacedAggFunc = aggFuncMap.get(aggFunc);
                // replace the input slot by new project expr mapping.
                return ctx.newProjectExprMapOpt.map(map -> ExpressionUtils.replace(replacedAggFunc, map))
                        .orElse(replacedAggFunc);
            } else {
                return aggregateFunction;
            }
        }
    }

    private List<NamedExpression> replaceProjectList(
            LogicalProject<? extends Plan> project,
            Map<Expression, Expression> projectMap) {
        return project.getProjects().stream()
                .map(expr -> (NamedExpression) ExpressionUtils.replace(expr, projectMap))
                .collect(ImmutableList.toImmutableList());
    }

    private List<Expression> nonVirtualGroupByExprs(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .filter(expr -> !(expr instanceof VirtualSlotReference))
                .collect(ImmutableList.toImmutableList());
    }
}
