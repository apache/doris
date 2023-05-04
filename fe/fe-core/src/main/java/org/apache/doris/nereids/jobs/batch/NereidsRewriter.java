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
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.processor.pre.EliminateLogicalSelectHint;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AvgDistinctToSumDivCount;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.mv.SelectMaterializedIndexWithoutAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.logical.AggScalarSubQueryToWindowFunction;
import org.apache.doris.nereids.rules.rewrite.logical.BuildAggForUnion;
import org.apache.doris.nereids.rules.rewrite.logical.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.logical.CheckDataTypes;
import org.apache.doris.nereids.rules.rewrite.logical.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.logical.ConvertInnerOrCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.CountDistinctRewrite;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateDedupJoinCondition;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateLimit;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateNotNull;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateNullAwareLeftAntiJoin;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateOrderByConstant;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractAndNormalizeWindowExpression;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractFilterFromCrossJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ExtractSingleTableExpressionFromDisjunction;
import org.apache.doris.nereids.rules.rewrite.logical.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.logical.InferAggNotNull;
import org.apache.doris.nereids.rules.rewrite.logical.InferFilterNotNull;
import org.apache.doris.nereids.rules.rewrite.logical.InferJoinNotNull;
import org.apache.doris.nereids.rules.rewrite.logical.InferPredicates;
import org.apache.doris.nereids.rules.rewrite.logical.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.logical.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.logical.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeSort;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanTablet;
import org.apache.doris.nereids.rules.rewrite.logical.PushFilterInsideJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownLimit;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.rules.rewrite.logical.SemiJoinAggTranspose;
import org.apache.doris.nereids.rules.rewrite.logical.SemiJoinAggTransposeProject;
import org.apache.doris.nereids.rules.rewrite.logical.SemiJoinCommute;
import org.apache.doris.nereids.rules.rewrite.logical.SemiJoinLogicalJoinTranspose;
import org.apache.doris.nereids.rules.rewrite.logical.SemiJoinLogicalJoinTransposeProject;
import org.apache.doris.nereids.rules.rewrite.logical.SimplifyAggGroupBy;
import org.apache.doris.nereids.rules.rewrite.logical.SplitLimit;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Apply rules to optimize logical plan.
 */
public class NereidsRewriter extends BatchRewriteJob {
    private static final List<RewriteJob> REWRITE_JOBS = jobs(
            topic("Normalization",
                topDown(
                    new EliminateOrderByConstant(),
                    new EliminateGroupByConstant(),

                    // MergeProjects depends on this rule
                    new LogicalSubQueryAliasToLogicalProject(),

                    // rewrite expressions, no depends
                    new ExpressionNormalization(),
                    new ExpressionOptimization(),
                    new AvgDistinctToSumDivCount(),
                    new CountDistinctRewrite(),

                    new ExtractFilterFromCrossJoin()
                ),

                // ExtractSingleTableExpressionFromDisjunction conflict to InPredicateToEqualToRule
                // in the ExpressionNormalization, so must invoke in another job, or else run into
                // dead loop
                topDown(
                    new ExtractSingleTableExpressionFromDisjunction()
                )
            ),

            topic("Subquery unnesting",
                custom(RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION, AggScalarSubQueryToWindowFunction::new),

                bottomUp(
                    new EliminateUselessPlanUnderApply(),

                    // CorrelateApplyToUnCorrelateApply and ApplyToJoin
                    // and SelectMaterializedIndexWithAggregate depends on this rule
                    new MergeProjects(),

                    /*
                     * Subquery unnesting.
                     * 1. Adjust the plan in correlated logicalApply
                     *    so that there are no correlated columns in the subquery.
                     * 2. Convert logicalApply to a logicalJoin.
                     *  TODO: group these rules to make sure the result plan is what we expected.
                     */
                    new CorrelateApplyToUnCorrelateApply(),
                    new ApplyToJoin()
                )
            ),

            // please note: this rule must run before NormalizeAggregate
            topDown(
                new AdjustAggregateNullableForEmptySet()
            ),

            // we should eliminate hint again because some hint maybe exist in the CTE or subquery.
            // so this rule should invoke after "Subquery unnesting"
            custom(RuleType.ELIMINATE_HINT, EliminateLogicalSelectHint::new),

            // The rule modification needs to be done after the subquery is unnested,
            // because for scalarSubQuery, the connection condition is stored in apply in the analyzer phase,
            // but when normalizeAggregate/normalizeSort is performed, the members in apply cannot be obtained,
            // resulting in inconsistent output results and results in apply
            topDown(
                new SimplifyAggGroupBy(),
                new NormalizeAggregate(),
                new NormalizeSort()
            ),

            topic("Window analysis",
                topDown(
                    new ExtractAndNormalizeWindowExpression(),
                    new CheckAndStandardizeWindowFunctionAndFrame()
                )
            ),

            topic("Rewrite join",
                // infer not null filter, then push down filter, and then reorder join(cross join to inner join)
                topDown(
                    new InferAggNotNull(),
                    new InferFilterNotNull(),
                    new InferJoinNotNull()
                ),
                // ReorderJoin depends PUSH_DOWN_FILTERS
                // the PUSH_DOWN_FILTERS depends on lots of rules, e.g. merge project, eliminate outer,
                // sometimes transform the bottom plan make some rules usable which can apply to the top plan,
                // but top-down traverse can not cover this case in one iteration, so bottom-up is more
                // efficient because it can find the new plans and apply transform wherever it is
                bottomUp(RuleSet.PUSH_DOWN_FILTERS),

                topDown(
                    new MergeFilters(),
                    new ReorderJoin(),
                    new PushFilterInsideJoin(),
                    new FindHashConditionForJoin(),
                    new ConvertInnerOrCrossJoin(),
                    new EliminateNullAwareLeftAntiJoin()
                ),

                // pushdown SEMI Join
                bottomUp(
                    new SemiJoinCommute(),
                    new SemiJoinLogicalJoinTranspose(),
                    new SemiJoinLogicalJoinTransposeProject(),
                    new SemiJoinAggTranspose(),
                    new SemiJoinAggTransposeProject()
                ),

                topDown(
                    new EliminateDedupJoinCondition()
                )
            ),

            topic("Column pruning and infer predicate",
                custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),

                custom(RuleType.INFER_PREDICATES, InferPredicates::new),

                // column pruning create new project, so we should use PUSH_DOWN_FILTERS
                // to change filter-project to project-filter
                bottomUp(RuleSet.PUSH_DOWN_FILTERS),

                // after eliminate outer join in the PUSH_DOWN_FILTERS, we can infer more predicate and push down
                custom(RuleType.INFER_PREDICATES, InferPredicates::new),

                bottomUp(RuleSet.PUSH_DOWN_FILTERS),

                // after eliminate outer join, we can move some filters to join.otherJoinConjuncts,
                // this can help to translate plan to backend
                topDown(
                    new PushFilterInsideJoin()
                )
            ),

            custom(RuleType.CHECK_DATATYPES, CheckDataTypes::new),

            // this rule should invoke after ColumnPruning
            custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),

            // we need to execute this rule at the end of rewrite
            // to avoid two consecutive same project appear when we do optimization.
            topic("Others optimization",
                bottomUp(ImmutableList.<RuleFactory>builder().addAll(ImmutableList.of(
                    new EliminateNotNull(),
                    new EliminateLimit(),
                    new EliminateFilter(),
                    new EliminateAggregate(),
                    new MergeSetOperations(),
                    new PushdownLimit(),
                    new BuildAggForUnion()
                    // after eliminate filter, the project maybe can push down again,
                    // so we add push down rules
                )).addAll(RuleSet.PUSH_DOWN_FILTERS).build())
            ),

            // TODO: I think these rules should be implementation rules, and generate alternative physical plans.
            topic("Table/Physical optimization",
                topDown(
                    // TODO: the logical plan should not contains any phase information,
                    //       we should refactor like AggregateStrategies, e.g. LimitStrategies,
                    //       generate one PhysicalLimit if current distribution is gather or two
                    //       PhysicalLimits with gather exchange
                    new SplitLimit(),
                    new PruneOlapScanPartition()
                )
            ),

            topic("MV optimization",
                topDown(
                    // TODO: enable this rule after https://github.com/apache/doris/issues/18263 is fixed
                    // new SelectMaterializedIndexWithAggregate(),
                    new SelectMaterializedIndexWithoutAggregate(),
                    new PushdownFilterThroughProject(),
                    new MergeProjects(),
                    new PruneOlapScanTablet()
                )
            ),

            // this rule batch must keep at the end of rewrite to do some plan check
            topic("Final rewrite and check", bottomUp(
                new AdjustNullable(),
                new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE),
                new CheckAfterRewrite()
            ))
    );

    public NereidsRewriter(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    @Override
    public List<RewriteJob> getJobs() {
        return REWRITE_JOBS;
    }
}
