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

package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewrite;
import org.apache.doris.nereids.rules.mv.SelectMaterializedIndexWithAggregate;
import org.apache.doris.nereids.rules.mv.SelectMaterializedIndexWithoutAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.logical.BuildAggForUnion;
import org.apache.doris.nereids.rules.rewrite.logical.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.logical.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.logical.CountDistinctRewrite;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateLimit;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateOrderByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractAndNormalizeWindowExpression;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractFilterFromCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractSingleTableExpressionFromDisjunction;
import org.apache.doris.nereids.rules.rewrite.logical.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.logical.InferPredicates;
import org.apache.doris.nereids.rules.rewrite.logical.InnerToCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.LimitPushDown;
import org.apache.doris.nereids.rules.rewrite.logical.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.logical.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.logical.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanTablet;
import org.apache.doris.nereids.rules.rewrite.logical.PushFilterInsideJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;

import com.google.common.collect.ImmutableList;

/**
 * Apply rules to optimize logical plan.
 */
public class NereidsRewriteJobExecutor extends BatchRulesJob {

    /**
     * Constructor.
     *
     * @param cascadesContext context for applying rules.
     */
    public NereidsRewriteJobExecutor(CascadesContext cascadesContext) {
        super(cascadesContext);
        ImmutableList<Job> jobs = new ImmutableList.Builder<Job>()
                .addAll(new EliminateSpecificPlanUnderApplyJob(cascadesContext).rulesJob)
                // MergeProjects depends on this rule
                .add(bottomUpBatch(ImmutableList.of(new LogicalSubQueryAliasToLogicalProject())))
                // AdjustApplyFromCorrelateToUnCorrelateJob and ConvertApplyToJoinJob
                // and SelectMaterializedIndexWithAggregate depends on this rule
                .add(topDownBatch(ImmutableList.of(new MergeProjects())))
                .add(topDownBatch(ImmutableList.of(new ExpressionNormalization(cascadesContext.getConnectContext()))))
                .add(topDownBatch(ImmutableList.of(new ExpressionOptimization())))
                .add(topDownBatch(ImmutableList.of(new ExtractSingleTableExpressionFromDisjunction())))
                /*
                 * Subquery unnesting.
                 * 1. Adjust the plan in correlated logicalApply
                 *    so that there are no correlated columns in the subquery.
                 * 2. Convert logicalApply to a logicalJoin.
                 *  TODO: group these rules to make sure the result plan is what we expected.
                 */
                .addAll(new AdjustApplyFromCorrelateToUnCorrelateJob(cascadesContext).rulesJob)
                .addAll(new ConvertApplyToJoinJob(cascadesContext).rulesJob)
                .add(bottomUpBatch(ImmutableList.of(new AdjustAggregateNullableForEmptySet())))
                .add(topDownBatch(ImmutableList.of(new EliminateGroupByConstant())))
                .add(topDownBatch(ImmutableList.of(new NormalizeAggregate())))
                .add(topDownBatch(ImmutableList.of(new ExtractAndNormalizeWindowExpression())))
                //execute NormalizeAggregate() again to resolve nested AggregateFunctions in WindowExpression,
                //e.g. sum(sum(c1)) over(partition by avg(c1))
                .add(topDownBatch(ImmutableList.of(new NormalizeAggregate())))
                .add(topDownBatch(ImmutableList.of(new CheckAndStandardizeWindowFunctionAndFrame())))
                .add(topDownBatch(RuleSet.PUSH_DOWN_FILTERS, false))
                .add(visitorJob(RuleType.INFER_PREDICATES, new InferPredicates()))
                .add(topDownBatch(ImmutableList.of(new ExtractFilterFromCrossJoin())))
                .add(topDownBatch(ImmutableList.of(new MergeFilters())))
                .add(topDownBatch(ImmutableList.of(new ReorderJoin())))
                .add(topDownBatch(ImmutableList.of(new ColumnPruning())))
                .add(topDownBatch(RuleSet.PUSH_DOWN_FILTERS, false))
                .add(visitorJob(RuleType.INFER_PREDICATES, new InferPredicates()))
                .add(topDownBatch(RuleSet.PUSH_DOWN_FILTERS, false))
                .add(visitorJob(RuleType.INFER_PREDICATES, new InferPredicates()))
                .add(topDownBatch(RuleSet.PUSH_DOWN_FILTERS, false))
                .add(topDownBatch(ImmutableList.of(new PushFilterInsideJoin())))
                .add(topDownBatch(ImmutableList.of(new FindHashConditionForJoin())))
                .add(topDownBatch(RuleSet.PUSH_DOWN_FILTERS, false))
                .add(topDownBatch(ImmutableList.of(new InnerToCrossJoin())))
                .add(topDownBatch(ImmutableList.of(new EliminateLimit())))
                .add(topDownBatch(ImmutableList.of(new EliminateFilter())))
                .add(topDownBatch(ImmutableList.of(new PruneOlapScanPartition())))
                .add(topDownBatch(ImmutableList.of(new CountDistinctRewrite())))
                // we need to execute this rule at the end of rewrite
                // to avoid two consecutive same project appear when we do optimization.
                .add(topDownBatch(ImmutableList.of(new EliminateOrderByConstant())))
                .add(topDownBatch(ImmutableList.of(new EliminateUnnecessaryProject())))
                .add(topDownBatch(ImmutableList.of(new SelectMaterializedIndexWithAggregate())))
                .add(topDownBatch(ImmutableList.of(new SelectMaterializedIndexWithoutAggregate())))
                .add(topDownBatch(ImmutableList.of(new PruneOlapScanTablet())))
                .add(topDownBatch(ImmutableList.of(new EliminateAggregate())))
                .add(bottomUpBatch(ImmutableList.of(new MergeSetOperations())))
                .add(topDownBatch(ImmutableList.of(new LimitPushDown())))
                .add(topDownBatch(ImmutableList.of(new BuildAggForUnion())))
                // this rule batch must keep at the end of rewrite to do some plan check
                .add(bottomUpBatch(ImmutableList.of(
                        new AdjustNullable(),
                        new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE),
                        new CheckAfterRewrite()))
                )
                .build();

        rulesJob.addAll(jobs);
    }
}
