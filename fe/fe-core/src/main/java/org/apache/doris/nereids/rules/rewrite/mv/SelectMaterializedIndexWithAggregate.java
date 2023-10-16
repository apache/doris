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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.SlotReference;
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
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmapWithCheck;
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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.PlanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
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
                logicalAggregate(logicalOlapScan().when(this::shouldSelectIndex)).thenApply(ctx -> {
                    LogicalAggregate<LogicalOlapScan> agg = ctx.root;
                    LogicalOlapScan scan = agg.child();
                    SelectResult result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableSet.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            agg.getGroupByExpressions(),
                            new HashSet<>(agg.getExpressions()));

                    LogicalOlapScan mvPlan = scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                    SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                    if (result.exprRewriteMap.isEmpty()) {
                        return new ReplaceExpressions(slotContext)
                            .replace(agg.withChildren(mvPlan), mvPlan);
                    } else {
                        return new LogicalProject<>(
                            generateProjectsAlias(agg.getOutputs(), slotContext),
                                new ReplaceExpressions(slotContext).replace(
                                    new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(
                                            agg, Optional.empty(), Optional.empty(), result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        mvPlan
                                    ), mvPlan));
                    }
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_SCAN),

                // filter could push down scan.
                // Aggregate(Filter(Scan))
                logicalAggregate(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex)))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg = ctx.root;
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(agg.getExpressions())
                                    .addAll(filter.getExpressions())
                                    .build();

                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    agg.getGroupByExpressions(),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(filter.withChildren(mvPlan)), mvPlan));
                            } else {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.empty(), Optional.empty(),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                // Note that no need to replace slots in the filter,
                                                // because the slots to
                                                // replace are value columns, which shouldn't appear in filters.
                                                filter.withChildren(mvPlan)
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(this::shouldSelectIndex)))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalProject<LogicalOlapScan>> agg = ctx.root;
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableSet.of(),
                                    extractAggFunctionAndReplaceSlot(agg,
                                            Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer()),
                                    collectRequireExprWithAggAndProject(agg.getExpressions(), project.getProjects())
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            project.withProjectsAndChild(
                                                generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                mvPlan)), mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList),
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                newProject
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).thenApply(ctx -> {
                            LogicalAggregate<LogicalProject<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(collectRequireExprWithAggAndProject(
                                            agg.getExpressions(), project.getProjects()))
                                    .addAll(filter.getExpressions())
                                    .build();
                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer()),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            project.withProjectsAndChild(
                                                generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                filter.withChildren(mvPlan))), mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList),
                                        filter.withChildren(mvPlan));

                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                newProject
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).thenApply(ctx -> {
                            LogicalAggregate<LogicalFilter<LogicalProject<LogicalOlapScan>>> agg = ctx.root;
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(collectRequireExprWithAggAndProject(
                                            agg.getExpressions(), project.getProjects()))
                                    .addAll(collectRequireExprWithAggAndProject(
                                            filter.getExpressions(), project.getProjects()))
                                    .build();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer()),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            filter.withChildren(
                                                project.withProjectsAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                    mvPlan))), mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList), mvPlan);

                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                filter.withChildren(newProject)
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_PROJECT_SCAN),

                // only agg above scan
                // Aggregate(Repeat(Scan))
                logicalAggregate(logicalRepeat(logicalOlapScan().when(this::shouldSelectIndex))).thenApply(ctx -> {
                    LogicalAggregate<LogicalRepeat<LogicalOlapScan>> agg = ctx.root;
                    LogicalRepeat<LogicalOlapScan> repeat = agg.child();
                    LogicalOlapScan scan = repeat.child();
                    SelectResult result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableSet.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            nonVirtualGroupByExprs(agg),
                            new HashSet<>(agg.getExpressions()));

                    LogicalOlapScan mvPlan = scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                    SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                    if (result.exprRewriteMap.isEmpty()) {
                        return new LogicalProject<>(
                            generateProjectsAlias(agg.getOutputs(), slotContext),
                                new ReplaceExpressions(slotContext).replace(
                                agg.withChildren(
                                    repeat.withAggOutputAndChild(
                                        generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()), mvPlan)
                                ), mvPlan));
                    } else {
                        return new LogicalProject<>(
                            generateProjectsAlias(agg.getOutputs(), slotContext),
                                new ReplaceExpressions(slotContext).replace(
                                    new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(
                                            agg, Optional.empty(), Optional.empty(), result.exprRewriteMap),
                                        agg.isNormalized(),
                                        agg.getSourceRepeat(),
                                        repeat.withAggOutputAndChild(
                                            generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()), mvPlan)
                                    ), mvPlan));
                    }
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_SCAN),

                // filter could push down scan.
                // Aggregate(Repeat(Filter(Scan)))
                logicalAggregate(logicalRepeat(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex))))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalRepeat<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                            LogicalRepeat<LogicalFilter<LogicalOlapScan>> repeat = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = repeat.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(agg.getExpressions())
                                    .addAll(filter.getExpressions())
                                    .build();

                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    nonVirtualGroupByExprs(agg),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                    filter.withChildren(mvPlan)
                                            )), mvPlan));
                            } else {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.empty(), Optional.empty(),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                // Not that no need to replace slots in the filter,
                                                // because the slots to replace
                                                // are value columns, which shouldn't appear in filters.
                                                repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                    filter.withChildren(mvPlan))
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Repeat(Project(Scan)))
                logicalAggregate(logicalRepeat(logicalProject(logicalOlapScan().when(this::shouldSelectIndex))))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalRepeat<LogicalProject<LogicalOlapScan>>> agg = ctx.root;
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
                                            project.getAliasToProducer()),
                                    collectRequireExprWithAggAndProject(agg.getExpressions(), project.getProjects())
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                    project.withProjectsAndChild(
                                                        generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                        mvPlan))
                                        ), mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList),
                                        mvPlan);
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(
                                                        mvPlan, repeat.getOutputs()), newProject)
                                            ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Repeat(Project(Filter(Scan))))
                logicalAggregate(logicalRepeat(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex))))).thenApply(ctx -> {
                            LogicalAggregate<LogicalRepeat<LogicalProject
                                    <LogicalFilter<LogicalOlapScan>>>> agg = ctx.root;
                            LogicalRepeat<LogicalProject<LogicalFilter<LogicalOlapScan>>> repeat = agg.child();
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = repeat.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(collectRequireExprWithAggAndProject(
                                            agg.getExpressions(), project.getProjects()))
                                    .addAll(filter.getExpressions())
                                    .build();
                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                            project.getAliasToProducer()),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            repeat.withAggOutputAndChild(
                                                generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                project.withProjectsAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                    filter.withChildren(
                                                        mvPlan
                                        )))),
                                            mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList),
                                        filter.withChildren(mvPlan));

                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(
                                                        mvPlan, repeat.getOutputs()), newProject)
                                        ), mvPlan));
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_REPEAT_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Repeat(Filter(Project(Scan))))
                logicalAggregate(logicalRepeat(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex))))).thenApply(ctx -> {
                            LogicalAggregate<LogicalRepeat<LogicalFilter
                                    <LogicalProject<LogicalOlapScan>>>> agg = ctx.root;
                            LogicalRepeat<LogicalFilter<LogicalProject<LogicalOlapScan>>> repeat = agg.child();
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = repeat.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            ImmutableSet<Expression> requiredExpr = ImmutableSet.<Expression>builder()
                                    .addAll(collectRequireExprWithAggAndProject(
                                            agg.getExpressions(), project.getProjects()))
                                    .addAll(collectRequireExprWithAggAndProject(
                                            filter.getExpressions(), project.getProjects()))
                                    .build();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                            project.getAliasToProducer()),
                                    requiredExpr
                            );

                            LogicalOlapScan mvPlan =
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId);
                            SlotContext slotContext = generateBaseScanExprToMvExpr(mvPlan);

                            if (result.exprRewriteMap.isEmpty()) {
                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                        agg.withChildren(
                                            repeat.withAggOutputAndChild(
                                                generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                filter.withChildren(
                                                    project.withProjectsAndChild(
                                                        generateNewOutputsWithMvOutputs(mvPlan, project.getProjects()),
                                                        mvPlan
                                        )))),
                                            mvPlan));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project,
                                        result.exprRewriteMap.projectExprMap);
                                LogicalProject<Plan> newProject = new LogicalProject<>(
                                        generateNewOutputsWithMvOutputs(mvPlan, newProjectList),
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));

                                return new LogicalProject<>(
                                    generateProjectsAlias(agg.getOutputs(), slotContext),
                                        new ReplaceExpressions(slotContext).replace(
                                            new LogicalAggregate<>(
                                                agg.getGroupByExpressions(),
                                                replaceAggOutput(agg, Optional.of(project), Optional.of(newProject),
                                                        result.exprRewriteMap),
                                                agg.isNormalized(),
                                                agg.getSourceRepeat(),
                                                repeat.withAggOutputAndChild(
                                                    generateNewOutputsWithMvOutputs(mvPlan, repeat.getOutputs()),
                                                    filter.withChildren(newProject))
                                            ), mvPlan));
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
    private SelectResult select(LogicalOlapScan scan, Set<Slot> requiredScanOutput, Set<Expression> predicates,
            List<AggregateFunction> aggregateFunctions, List<Expression> groupingExprs,
            Set<? extends Expression> requiredExpr) {
        // remove virtual slot for grouping sets.
        Set<Slot> nonVirtualRequiredScanOutput = requiredScanOutput.stream()
                .filter(slot -> !(slot instanceof VirtualSlotReference))
                .collect(ImmutableSet.toImmutableSet());

        // use if condition to skip String.format() and speed up
        if (!scan.getOutputSet().containsAll(nonVirtualRequiredScanOutput)) {
            throw new AnalysisException(
                    String.format("Scan's output (%s) should contains all the input required scan output (%s).",
                            scan.getOutput(), nonVirtualRequiredScanOutput));
        }

        OlapTable table = scan.getTable();

        Map<Boolean, List<MaterializedIndex>> indexesGroupByIsBaseOrNot = table.getVisibleIndex()
                .stream()
                .collect(Collectors.groupingBy(index -> index.getId() == table.getBaseIndexId()));

        Set<MaterializedIndex> candidatesWithoutRewriting = indexesGroupByIsBaseOrNot
                .getOrDefault(false, ImmutableList.of()).stream()
                .filter(index -> preAggEnabledByHint(scan)
                        || checkPreAggStatus(scan, index.getId(), predicates, aggregateFunctions, groupingExprs).isOn())
                .collect(Collectors.toSet());

        // try to rewrite bitmap, hll by materialized index columns.
        List<AggRewriteResult> candidatesWithRewriting = indexesGroupByIsBaseOrNot
                .getOrDefault(false, ImmutableList.of()).stream()
                .filter(index -> !candidatesWithoutRewriting.contains(index))
                .map(index -> rewriteAgg(index, scan, nonVirtualRequiredScanOutput, predicates, aggregateFunctions,
                        groupingExprs))
                .filter(aggRewriteResult -> checkPreAggStatus(scan, aggRewriteResult.index.getId(), predicates,
                        // check pre-agg status of aggregate function that couldn't rewrite.
                        aggFuncsDiff(aggregateFunctions, aggRewriteResult), groupingExprs).isOn())
                .filter(result -> result.success).collect(Collectors.toList());

        List<MaterializedIndex> haveAllRequiredColumns = Streams.concat(
                candidatesWithoutRewriting.stream()
                        .filter(index -> containAllRequiredColumns(index, scan, nonVirtualRequiredScanOutput,
                                requiredExpr, predicates)),
                candidatesWithRewriting.stream()
                        .filter(aggRewriteResult -> containAllRequiredColumns(aggRewriteResult.index, scan,
                                aggRewriteResult.requiredScanOutput,
                                requiredExpr.stream().map(e -> aggRewriteResult.exprRewriteMap.replaceAgg(e))
                                        .collect(Collectors.toSet()),
                                predicates))
                        .map(aggRewriteResult -> aggRewriteResult.index))
                .collect(Collectors.toList());

        long selectIndexId = selectBestIndex(haveAllRequiredColumns, scan, predicates);
        // Pre-aggregation is set to `on` by default for duplicate-keys table.
        // In other cases where mv is not hit, preagg may turn off from on.
        if (!table.isDupKeysOrMergeOnWrite() && (new CheckContext(scan, selectIndexId)).isBaseIndex()) {
            PreAggStatus preagg = scan.getPreAggStatus();
            if (preagg.isOn()) {
                preagg = checkPreAggStatus(scan, scan.getTable().getBaseIndexId(), predicates, aggregateFunctions,
                        groupingExprs);
            }
            return new SelectResult(preagg, selectIndexId, new ExprRewriteMap());
        }

        Optional<AggRewriteResult> rewriteResultOpt = candidatesWithRewriting.stream()
                .filter(aggRewriteResult -> aggRewriteResult.index.getId() == selectIndexId).findAny();
        return new SelectResult(PreAggStatus.on(), selectIndexId,
                rewriteResultOpt.map(r -> r.exprRewriteMap).orElse(new ExprRewriteMap()));
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
            return checkAggFunc(aggregateFunction, AggregateType.NONE, context, false);
        }

        @Override
        public PreAggStatus visitMax(Max max, CheckContext context) {
            return checkAggFunc(max, AggregateType.MAX, context, true);
        }

        @Override
        public PreAggStatus visitMin(Min min, CheckContext context) {
            return checkAggFunc(min, AggregateType.MIN, context, true);
        }

        @Override
        public PreAggStatus visitSum(Sum sum, CheckContext context) {
            return checkAggFunc(sum, AggregateType.SUM, context, false);
        }

        @Override
        public PreAggStatus visitCount(Count count, CheckContext context) {
            if (count.isDistinct() && count.arity() == 1) {
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(count.child(0));
                if (slotOpt.isPresent() && context.keyNameToColumn.containsKey(
                        normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
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
            if (slotOpt.isPresent() && context.valueNameToColumn
                    .containsKey(normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid bitmap_union_count: " + bitmapUnionCount.toSql());
            }
        }

        @Override
        public PreAggStatus visitHllUnionAgg(HllUnionAgg hllUnionAgg, CheckContext context) {
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllUnionAgg.child());
            if (slotOpt.isPresent() && context.valueNameToColumn.containsKey(
                    normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid hll_union_agg: " + hllUnionAgg.toSql());
            }
        }

        private PreAggStatus checkAggFunc(
                AggregateFunction aggFunc,
                AggregateType matchingAggType,
                CheckContext ctx,
                boolean canUseKeyColumn) {
            String childNameWithFuncName = ctx.isBaseIndex()
                    ? normalizeName(aggFunc.child(0).toSqlWithoutQualifier())
                    : normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(
                        matchingAggType, normalizeName(aggFunc.child(0).toSqlWithoutQualifier())));

            boolean contains = containsAllColumn(aggFunc.child(0), ctx.keyNameToColumn.keySet());
            if (contains || ctx.keyNameToColumn.containsKey(childNameWithFuncName)) {
                if (canUseKeyColumn || (!ctx.isBaseIndex() && contains)) {
                    return PreAggStatus.on();
                } else {
                    Column column = ctx.keyNameToColumn.get(childNameWithFuncName);
                    return PreAggStatus.off(String.format("Aggregate function %s contains key column %s.",
                            aggFunc.toSql(), column == null ? "empty column" : column.getName()));
                }
            } else if (ctx.valueNameToColumn.containsKey(childNameWithFuncName)) {
                AggregateType aggType = ctx.valueNameToColumn.get(childNameWithFuncName).getAggregationType();
                if (aggType == matchingAggType) {
                    if (aggFunc.isDistinct()) {
                        return PreAggStatus.off(
                                String.format("Aggregate function %s is distinct aggregation", aggFunc.toSql()));
                    }
                    return PreAggStatus.on();
                } else {
                    return PreAggStatus.off(String.format("Aggregate operator don't match, aggregate function: %s"
                            + ", column aggregate type: %s", aggFunc.toSql(), aggType));
                }
            } else {
                return PreAggStatus.off(String.format("Slot(%s) in %s is neither key column nor value column.",
                        childNameWithFuncName, aggFunc.toSql()));
            }
        }
    }

    private static class CheckContext {

        public final LogicalOlapScan scan;
        public final long index;
        public final Map<String, Column> keyNameToColumn;
        public final Map<String, Column> valueNameToColumn;

        public CheckContext(LogicalOlapScan scan, long indexId) {
            this.scan = scan;

            Supplier<Map<String, Column>> supplier = () -> Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

            // map<is_key, map<column_name, column>>
            Map<Boolean, Map<String, Column>> baseNameToColumnGroupingByIsKey = scan.getTable()
                    .getSchemaByIndexId(indexId).stream()
                    .collect(Collectors.groupingBy(Column::isKey,
                            Collectors.toMap(c -> normalizeName(parseMvColumnToSql(c.getName())), Function.identity(),
                                    (v1, v2) -> v1, supplier)));
            Map<Boolean, Map<String, Column>> mvNameToColumnGroupingByIsKey = scan.getTable()
                    .getSchemaByIndexId(indexId).stream()
                    .collect(Collectors.groupingBy(Column::isKey,
                            Collectors.toMap(
                                    c -> normalizeName(parseMvColumnToMvName(c.getNameWithoutMvPrefix(),
                                            c.isAggregated() ? Optional.of(c.getAggregationType().name())
                                                    : Optional.empty())),
                                    Function.identity(), (v1, v2) -> v1, supplier)));

            this.keyNameToColumn = mvNameToColumnGroupingByIsKey.getOrDefault(true,
                    Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER));
            for (String name : baseNameToColumnGroupingByIsKey
                    .getOrDefault(true, Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER)).keySet()) {
                this.keyNameToColumn.putIfAbsent(name, baseNameToColumnGroupingByIsKey.get(true).get(name));
            }
            this.valueNameToColumn = mvNameToColumnGroupingByIsKey.getOrDefault(false,
                    Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER));
            for (String key : baseNameToColumnGroupingByIsKey.getOrDefault(false, ImmutableMap.of()).keySet()) {
                this.valueNameToColumn.putIfAbsent(key, baseNameToColumnGroupingByIsKey.get(false).get(key));
            }
            this.index = indexId;
        }

        public boolean isBaseIndex() {
            return index == scan.getTable().getBaseIndexId();
        }

        public MaterializedIndexMeta getMeta() {
            return scan.getTable().getIndexMetaByIndexId(index);
        }

        public Column getColumn(String name) {
            return getMeta().getColumnByDefineName(name);
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
    private PreAggStatus checkPredicates(List<Expression> predicates, CheckContext checkContext) {
        Set<String> indexConjuncts = PlanNode
                .splitAndCompoundPredicateToConjuncts(checkContext.getMeta().getWhereClause()).stream()
                .map(e -> new NereidsParser().parseExpression(e.toSql()).toSqlWithoutQualifier())
                .collect(Collectors.toSet());
        return disablePreAggIfContainsAnyValueColumn(
                predicates.stream()
                        .filter(e -> !indexConjuncts.contains(e.toSqlWithoutQualifier()))
                        .collect(Collectors.toList()),
                checkContext, "Predicate %s contains value column %s");
    }

    /**
     * Check the input expressions have no referenced slot to underlying value type column.
     */
    private PreAggStatus disablePreAggIfContainsAnyValueColumn(List<Expression> exprs, CheckContext ctx,
            String errorMsg) {
        return exprs.stream()
                .map(expr -> expr.getInputSlots()
                        .stream()
                        .filter(slot -> ctx.valueNameToColumn.containsKey(normalizeName(slot.toSqlWithoutQualifier())))
                        .findAny()
                        .map(slot -> Pair.of(expr, ctx.valueNameToColumn.get(
                                normalizeName(slot.toSqlWithoutQualifier()))))
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
            Set<String> indexConjuncts = PlanNode
                    .splitAndCompoundPredicateToConjuncts(context.checkContext.getMeta().getWhereClause()).stream()
                    .map(e -> new NereidsParser().parseExpression(e.toSql()).toSqlWithoutQualifier())
                    .collect(Collectors.toSet());
            if (isInputSlotsContainsNone(
                    predicates.stream()
                            .filter(e -> !indexConjuncts.contains(e.toSqlWithoutQualifier()))
                            .collect(Collectors.toList()),
                    slotsToReplace) && isInputSlotsContainsNone(groupingExprs, slotsToReplace)) {
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

        private Map<String, AggregateFunction> aggFuncStrMap;

        public ExprRewriteMap() {
            this.slotMap = Maps.newHashMap();
            this.projectExprMap = Maps.newHashMap();
            this.aggFuncMap = Maps.newHashMap();
        }

        public boolean isEmpty() {
            return slotMap.isEmpty();
        }

        private void buildStrMap() {
            if (aggFuncStrMap != null) {
                return;
            }
            this.aggFuncStrMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (AggregateFunction e : aggFuncMap.keySet()) {
                this.aggFuncStrMap.put(e.toSql(), aggFuncMap.get(e));
            }
        }

        public Expression replaceAgg(Expression e) {
            while (e instanceof Alias) {
                e = e.child(0);
            }
            if (!(e instanceof AggregateFunction)) {
                return e;
            }
            buildStrMap();
            return aggFuncStrMap.getOrDefault(e.toSqlWithoutQualifier(), (AggregateFunction) e);
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

    private boolean isInputSlotsContainsNone(List<Expression> expressions, Set<Slot> slotsToCheck) {
        Set<Slot> inputSlotSet = ExpressionUtils.getInputSlotSet(expressions);
        return Sets.intersection(inputSlotSet, slotsToCheck).isEmpty();
    }

    private static class RewriteContext {
        public final CheckContext checkContext;
        public final ExprRewriteMap exprRewriteMap;

        public RewriteContext(CheckContext context, ExprRewriteMap exprRewriteMap) {
            this.checkContext = context;
            this.exprRewriteMap = exprRewriteMap;
        }
    }

    private static Expression castIfNeed(Expression expr, DataType targetType) {
        if (expr.getDataType().equals(targetType)) {
            return expr;
        }
        return new Cast(expr, targetType);
    }

    private static class AggFuncRewriter extends DefaultExpressionRewriter<RewriteContext> {
        public static final AggFuncRewriter INSTANCE = new AggFuncRewriter();

        private static void rewrite(Expression expr, RewriteContext context) {
            expr.accept(INSTANCE, context);
        }

        /**
         * count(distinct col) -> bitmap_union_count(mva_BITMAP_UNION__to_bitmap__with_check(col))
         * count(col) -> sum(mva_SUM__CASE WHEN col IS NULL THEN 0 ELSE 1 END)
         */
        @Override
        public Expression visitCount(Count count, RewriteContext context) {
            Expression result = visitAggregateFunction(count, context);
            if (result != count) {
                return result;
            }
            if (count.isDistinct() && count.arity() == 1) {
                // count(distinct col) -> bitmap_union_count(mv_bitmap_union_col)
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(count.child(0));

                Expression expr = new ToBitmapWithCheck(castIfNeed(count.child(0), BigIntType.INSTANCE));
                // count distinct a value column.
                if (slotOpt.isPresent() && !context.checkContext.keyNameToColumn.containsKey(
                        normalizeName(expr.toSqlWithoutQualifier()))) {
                    String bitmapUnionColumn = normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(
                            AggregateType.BITMAP_UNION, CreateMaterializedViewStmt
                                    .mvColumnBuilder(expr.toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(bitmapUnionColumn);
                    // has bitmap_union column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                        Slot bitmapUnionSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> bitmapUnionColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException(
                                        "cannot find bitmap union slot when select mv"));

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), bitmapUnionSlot);
                        context.exprRewriteMap.projectExprMap.put(slotOpt.get(), bitmapUnionSlot);
                        BitmapUnionCount bitmapUnionCount = new BitmapUnionCount(bitmapUnionSlot);
                        context.exprRewriteMap.aggFuncMap.put(count, bitmapUnionCount);
                        return bitmapUnionCount;
                    }
                }
            } else if (!count.isDistinct() && count.arity() == 1) {
                // count(col) -> sum(mva_SUM__CASE WHEN col IS NULL THEN 0 ELSE 1 END)

                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(count.child(0));
                // count a value column.
                if (slotOpt.isPresent() && !context.checkContext.keyNameToColumn.containsKey(
                        normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
                    String countColumn = normalizeName(CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.SUM,
                                CreateMaterializedViewStmt.mvColumnBuilder(
                                        slotToCaseWhen(slotOpt.get()).toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(countColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                        Slot countSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> countColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException(
                                        "cannot find count slot when select mv"));

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
         * bitmap_union_count(to_bitmap(col)) -> bitmap_union_count(mva_BITMAP_UNION__to_bitmap_with_check(col))
         */
        @Override
        public Expression visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, RewriteContext context) {
            Expression result = visitAggregateFunction(bitmapUnionCount, context);
            if (result != bitmapUnionCount) {
                return result;
            }
            if (bitmapUnionCount.child() instanceof ToBitmap) {
                ToBitmap toBitmap = (ToBitmap) bitmapUnionCount.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(toBitmap.child());
                if (slotOpt.isPresent()) {
                    String bitmapUnionCountColumn = normalizeName(CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.BITMAP_UNION, CreateMaterializedViewStmt
                                    .mvColumnBuilder(new ToBitmapWithCheck(toBitmap.child()).toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(bitmapUnionCountColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {

                        Slot bitmapUnionCountSlot = context.checkContext.scan
                                .getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> bitmapUnionCountColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException(
                                        "cannot find bitmap union count slot when select mv"));

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), bitmapUnionCountSlot);
                        context.exprRewriteMap.projectExprMap.put(toBitmap, bitmapUnionCountSlot);
                        BitmapUnionCount newBitmapUnionCount = new BitmapUnionCount(bitmapUnionCountSlot);
                        context.exprRewriteMap.aggFuncMap.put(bitmapUnionCount, newBitmapUnionCount);
                        return newBitmapUnionCount;
                    }
                }
            } else if (bitmapUnionCount.child() instanceof BitmapHash) {
                BitmapHash bitmapHash = (BitmapHash) bitmapUnionCount.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(bitmapHash.child());
                if (slotOpt.isPresent()) {
                    String bitmapUnionCountColumn = normalizeName(
                            CreateMaterializedViewStmt.mvColumnBuilder(AggregateType.BITMAP_UNION,
                                    CreateMaterializedViewStmt.mvColumnBuilder(bitmapHash.toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(bitmapUnionCountColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {

                        Slot bitmapUnionCountSlot = context.checkContext.scan
                                .getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> bitmapUnionCountColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException(
                                        "cannot find bitmap union count slot when select mv"));

                        context.exprRewriteMap.slotMap.put(slotOpt.get(), bitmapUnionCountSlot);
                        context.exprRewriteMap.projectExprMap.put(bitmapHash, bitmapUnionCountSlot);
                        BitmapUnionCount newBitmapUnionCount = new BitmapUnionCount(bitmapUnionCountSlot);
                        context.exprRewriteMap.aggFuncMap.put(bitmapUnionCount, newBitmapUnionCount);
                        return newBitmapUnionCount;
                    }
                }
            }

            return bitmapUnionCount;
        }

        /**
         * hll_union(hll_hash(col)) to hll_union(mva_HLL_UNION__hll_hash_(col))
         */
        @Override
        public Expression visitHllUnion(HllUnion hllUnion, RewriteContext context) {
            Expression result = visitAggregateFunction(hllUnion, context);
            if (result != hllUnion) {
                return result;
            }
            if (hllUnion.child() instanceof HllHash) {
                HllHash hllHash = (HllHash) hllUnion.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllHash.child());
                if (slotOpt.isPresent()) {
                    String hllUnionColumn = normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(
                            AggregateType.HLL_UNION,
                            CreateMaterializedViewStmt.mvColumnBuilder(hllHash.toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(hllUnionColumn);
                    // has hll_union column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                        Slot hllUnionSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> hllUnionColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException("cannot find hll union slot when select mv"));

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
         * hll_union_agg(hll_hash(col)) -> hll_union_agg(mva_HLL_UNION__hll_hash_(col))
         */
        @Override
        public Expression visitHllUnionAgg(HllUnionAgg hllUnionAgg, RewriteContext context) {
            Expression result = visitAggregateFunction(hllUnionAgg, context);
            if (result != hllUnionAgg) {
                return result;
            }
            if (hllUnionAgg.child() instanceof HllHash) {
                HllHash hllHash = (HllHash) hllUnionAgg.child();
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(hllHash.child());
                if (slotOpt.isPresent()) {
                    String hllUnionColumn = normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(
                            AggregateType.HLL_UNION,
                            CreateMaterializedViewStmt.mvColumnBuilder(hllHash.toSqlWithoutQualifier())));

                    Column mvColumn = context.checkContext.getColumn(hllUnionColumn);
                    // has hll_union column
                    if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                        Slot hllUnionSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index)
                                .stream()
                                .filter(s -> hllUnionColumn.equalsIgnoreCase(normalizeName(s.getName())))
                                .findFirst()
                                .orElseThrow(() -> new AnalysisException(
                                        "cannot find hll union slot when select mv"));

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
         * ndv(col) -> hll_union_agg(mva_HLL_UNION__hll_hash_(col))
         */
        @Override
        public Expression visitNdv(Ndv ndv, RewriteContext context) {
            Expression result = visitAggregateFunction(ndv, context);
            if (result != ndv) {
                return result;
            }
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(ndv.child(0));
            // ndv on a value column.
            if (slotOpt.isPresent() && !context.checkContext.keyNameToColumn.containsKey(
                    normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
                Expression expr = castIfNeed(ndv.child(), VarcharType.SYSTEM_DEFAULT);
                String hllUnionColumn = normalizeName(
                        CreateMaterializedViewStmt.mvColumnBuilder(AggregateType.HLL_UNION,
                                CreateMaterializedViewStmt.mvColumnBuilder(new HllHash(expr).toSqlWithoutQualifier())));

                Column mvColumn = context.checkContext.getColumn(hllUnionColumn);
                // has hll_union column
                if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                    Slot hllUnionSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index)
                            .stream()
                            .filter(s -> hllUnionColumn.equalsIgnoreCase(normalizeName(s.getName())))
                            .findFirst()
                            .orElseThrow(() -> new AnalysisException("cannot find hll union slot when select mv"));

                    context.exprRewriteMap.slotMap.put(slotOpt.get(), hllUnionSlot);
                    context.exprRewriteMap.projectExprMap.put(slotOpt.get(), hllUnionSlot);
                    HllUnionAgg hllUnionAgg = new HllUnionAgg(hllUnionSlot);
                    context.exprRewriteMap.aggFuncMap.put(ndv, hllUnionAgg);
                    return hllUnionAgg;
                }
            }
            return ndv;
        }

        @Override
        public Expression visitSum(Sum sum, RewriteContext context) {
            Expression result = visitAggregateFunction(sum, context);
            if (result != sum) {
                return result;
            }
            Optional<Slot> slotOpt = ExpressionUtils.extractSlotOrCastOnSlot(sum.child(0));
            if (!sum.isDistinct() && slotOpt.isPresent()
                    && !context.checkContext.keyNameToColumn.containsKey(
                            normalizeName(slotOpt.get().toSqlWithoutQualifier()))) {
                Expression expr = castIfNeed(sum.child(), BigIntType.INSTANCE);
                String sumColumn = normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(AggregateType.SUM,
                        CreateMaterializedViewStmt.mvColumnBuilder(expr.toSqlWithoutQualifier())));
                Column mvColumn = context.checkContext.getColumn(sumColumn);
                if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                    Slot sumSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index).stream()
                            .filter(s -> sumColumn.equalsIgnoreCase(normalizeName(s.getName()))).findFirst()
                            .orElseThrow(() -> new AnalysisException("cannot find sum slot when select mv"));
                    context.exprRewriteMap.slotMap.put(slotOpt.get(), sumSlot);
                    context.exprRewriteMap.projectExprMap.put(sum.child(), sumSlot);
                    Sum newSum = new Sum(sumSlot);
                    context.exprRewriteMap.aggFuncMap.put(sum, newSum);
                    return newSum;
                }
            }
            return sum;
        }

        /**
         * agg(col) -> agg_merge(mva_generic_aggregation__agg_state(col)) eg: max_by(k2,
         * k3) -> max_by_merge(mva_generic_aggregation__max_by_state(k2, k3))
         */
        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction, RewriteContext context) {
            String aggStateName = normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(
                    AggregateType.GENERIC_AGGREGATION,
                    StateCombinator.create(aggregateFunction).toSqlWithoutQualifier()));

            Column mvColumn = context.checkContext.getColumn(aggStateName);
            if (mvColumn != null && context.checkContext.valueNameToColumn.containsValue(mvColumn)) {
                Slot aggStateSlot = context.checkContext.scan.getOutputByIndex(context.checkContext.index).stream()
                        .filter(s -> aggStateName.equalsIgnoreCase(normalizeName(s.getName()))).findFirst()
                        .orElseThrow(() -> new AnalysisException("cannot find agg state slot when select mv"));

                Set<Slot> slots = aggregateFunction.collect(SlotReference.class::isInstance);
                for (Slot slot : slots) {
                    if (!context.checkContext.keyNameToColumn
                            .containsKey(normalizeName(slot.toSqlWithoutQualifier()))) {
                        context.exprRewriteMap.slotMap.put(slot, aggStateSlot);
                        context.exprRewriteMap.projectExprMap.put(slot, aggStateSlot);
                    }
                }

                MergeCombinator mergeCombinator = new MergeCombinator(Arrays.asList(aggStateSlot), aggregateFunction);
                context.exprRewriteMap.aggFuncMap.put(aggregateFunction, mergeCombinator);
                return mergeCombinator;
            }
            return aggregateFunction;
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
                .collect(Collectors.toList());
    }

    private List<Expression> nonVirtualGroupByExprs(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .filter(expr -> !(expr instanceof VirtualSlotReference))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     *  Put all the slots provided by mv into the project,
     *  and cannot simply replace them in the subsequent ReplaceExpressionWithMvColumn rule,
     *  because one base column in mv may correspond to multiple mv columns: eg. k2 -> max_k2/min_k2/sum_k2
     *  TODO: Do not add redundant columns
     */
    private List<NamedExpression> generateNewOutputsWithMvOutputs(
            LogicalOlapScan mvPlan, List<NamedExpression> outputs) {
        if (mvPlan.getSelectedIndexId() == mvPlan.getTable().getBaseIndexId()) {
            return outputs;
        }
        return ImmutableList.<NamedExpression>builder()
                .addAll(mvPlan.getOutputByIndex(mvPlan.getSelectedIndexId()))
                .addAll(outputs.stream()
                        .filter(s -> !(s instanceof Slot))
                        .collect(ImmutableList.toImmutableList()))
                .addAll(outputs.stream()
                        .filter(SlotNotFromChildren.class::isInstance)
                        .collect(ImmutableList.toImmutableList()))
                .build();
    }

    /**
     * eg: select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;
     *  +--LogicalAggregate[88] ( groupByExpr=[t#4], outputExpr=[t#4, sum(abs((k2#1 + 1))) AS `sum(abs(k2 + 1))`#5])
     *      +--LogicalProject[87] ( distinct=false, projects=[(abs(k1#0) + 1) AS `t`#4, k2#1])
     *          +--LogicalOlapScan()
     * t -> abs(k1#0) + 1
     */
    private Set<Expression> collectRequireExprWithAggAndProject(
            List<? extends Expression> aggExpressions, List<NamedExpression> projectExpressions) {
        Map<ExprId, Expression> exprIdToExpression = projectExpressions.stream()
                .collect(Collectors.toMap(NamedExpression::getExprId, e -> {
                    if (e instanceof Alias) {
                        return ((Alias) e).child();
                    }
                    return e;
                }));
        return aggExpressions.stream()
                .map(e -> {
                    if ((e instanceof NamedExpression)
                            && exprIdToExpression.containsKey(((NamedExpression) e).getExprId())) {
                        return exprIdToExpression.get(((NamedExpression) e).getExprId());
                    }
                    return e;
                }).collect(ImmutableSet.toImmutableSet());
    }
}
