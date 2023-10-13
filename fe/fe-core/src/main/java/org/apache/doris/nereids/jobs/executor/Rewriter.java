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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.rewrite.CostBasedRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AvgDistinctToSumDivCount;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.expression.CheckLegalityAfterRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.rewrite.AddDefaultLimit;
import org.apache.doris.nereids.rules.rewrite.AdjustConjunctsReturnType;
import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.AggScalarSubQueryToWindowFunction;
import org.apache.doris.nereids.rules.rewrite.BuildAggForUnion;
import org.apache.doris.nereids.rules.rewrite.CTEInline;
import org.apache.doris.nereids.rules.rewrite.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.CheckDataTypes;
import org.apache.doris.nereids.rules.rewrite.CheckMatchExpression;
import org.apache.doris.nereids.rules.rewrite.CheckMultiDistinct;
import org.apache.doris.nereids.rules.rewrite.CollectFilterAboveConsumer;
import org.apache.doris.nereids.rules.rewrite.CollectJoinConstraint;
import org.apache.doris.nereids.rules.rewrite.CollectProjectAboveConsumer;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.ConvertInnerOrCrossJoin;
import org.apache.doris.nereids.rules.rewrite.CountDistinctRewrite;
import org.apache.doris.nereids.rules.rewrite.CountLiteralToCountStar;
import org.apache.doris.nereids.rules.rewrite.CreatePartitionTopNFromWindow;
import org.apache.doris.nereids.rules.rewrite.DeferMaterializeTopNResult;
import org.apache.doris.nereids.rules.rewrite.EliminateAggregate;
import org.apache.doris.nereids.rules.rewrite.EliminateAssertNumRows;
import org.apache.doris.nereids.rules.rewrite.EliminateDedupJoinCondition;
import org.apache.doris.nereids.rules.rewrite.EliminateEmptyRelation;
import org.apache.doris.nereids.rules.rewrite.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.EliminateLimit;
import org.apache.doris.nereids.rules.rewrite.EliminateNotNull;
import org.apache.doris.nereids.rules.rewrite.EliminateNullAwareLeftAntiJoin;
import org.apache.doris.nereids.rules.rewrite.EliminateOrderByConstant;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.rules.rewrite.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.EnsureProjectOnTopJoin;
import org.apache.doris.nereids.rules.rewrite.ExtractAndNormalizeWindowExpression;
import org.apache.doris.nereids.rules.rewrite.ExtractFilterFromCrossJoin;
import org.apache.doris.nereids.rules.rewrite.ExtractSingleTableExpressionFromDisjunction;
import org.apache.doris.nereids.rules.rewrite.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.InferAggNotNull;
import org.apache.doris.nereids.rules.rewrite.InferFilterNotNull;
import org.apache.doris.nereids.rules.rewrite.InferJoinNotNull;
import org.apache.doris.nereids.rules.rewrite.InferPredicates;
import org.apache.doris.nereids.rules.rewrite.InferSetOperatorDistinct;
import org.apache.doris.nereids.rules.rewrite.LeadingJoin;
import org.apache.doris.nereids.rules.rewrite.LimitSortToTopN;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeOneRowRelationIntoUnion;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.NormalizeSort;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanTablet;
import org.apache.doris.nereids.rules.rewrite.PullUpCteAnchor;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.PushConjunctsIntoEsScan;
import org.apache.doris.nereids.rules.rewrite.PushConjunctsIntoJdbcScan;
import org.apache.doris.nereids.rules.rewrite.PushFilterInsideJoin;
import org.apache.doris.nereids.rules.rewrite.PushProjectIntoOneRowRelation;
import org.apache.doris.nereids.rules.rewrite.PushProjectThroughUnion;
import org.apache.doris.nereids.rules.rewrite.PushdownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.PushdownLimit;
import org.apache.doris.nereids.rules.rewrite.PushdownLimitDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushdownTopNThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushdownTopNThroughWindow;
import org.apache.doris.nereids.rules.rewrite.ReorderJoin;
import org.apache.doris.nereids.rules.rewrite.RewriteCteChildren;
import org.apache.doris.nereids.rules.rewrite.SemiJoinCommute;
import org.apache.doris.nereids.rules.rewrite.SimplifyAggGroupBy;
import org.apache.doris.nereids.rules.rewrite.SplitLimit;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinAgg;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinAggProject;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinLogicalJoin;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinLogicalJoinProject;
import org.apache.doris.nereids.rules.rewrite.batch.ApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.batch.CorrelateApplyToUnCorrelateApply;
import org.apache.doris.nereids.rules.rewrite.batch.EliminateUselessPlanUnderApply;
import org.apache.doris.nereids.rules.rewrite.mv.SelectMaterializedIndexWithAggregate;
import org.apache.doris.nereids.rules.rewrite.mv.SelectMaterializedIndexWithoutAggregate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Apply rules to rewrite logical plan.
 */
public class Rewriter extends AbstractBatchJobExecutor {

    private static final List<RewriteJob> CTE_CHILDREN_REWRITE_JOBS = jobs(
            topic("Plan Normalization",
                    topDown(
                            new EliminateOrderByConstant(),
                            new EliminateGroupByConstant(),
                            // MergeProjects depends on this rule
                            new LogicalSubQueryAliasToLogicalProject(),
                            // TODO: we should do expression normalization after plan normalization
                            //   because some rewritten depends on sub expression tree matching
                            //   such as group by key matching and replaced
                            //   but we need to do some normalization before subquery unnesting,
                            //   such as extract common expression.
                            new ExpressionNormalization(),
                            new ExpressionOptimization(),
                            new AvgDistinctToSumDivCount(),
                            new CountDistinctRewrite(),
                            new ExtractFilterFromCrossJoin()
                    ),
                    topDown(
                            // ExtractSingleTableExpressionFromDisjunction conflict to InPredicateToEqualToRule
                            // in the ExpressionNormalization, so must invoke in another job, otherwise dead loop.
                            new ExtractSingleTableExpressionFromDisjunction()
                    )
            ),
            // subquery unnesting relay on ExpressionNormalization to extract common factor expression
            topic("Subquery unnesting",
                    // after doing NormalizeAggregate in analysis job
                    // we need run the following 2 rules to make AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION work
                    bottomUp(new PullUpProjectUnderApply()),
                    topDown(new PushdownFilterThroughProject()),
                    costBased(
                            custom(RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION,
                                    AggScalarSubQueryToWindowFunction::new)
                    ),
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
            topic("Eliminate optimization",
                    bottomUp(
                            new EliminateLimit(),
                            new EliminateFilter(),
                            new EliminateAggregate(),
                            new EliminateAssertNumRows()
                    )
            ),
            // please note: this rule must run before NormalizeAggregate
            topDown(new AdjustAggregateNullableForEmptySet()),
            // The rule modification needs to be done after the subquery is unnested,
            // because for scalarSubQuery, the connection condition is stored in apply in the analyzer phase,
            // but when normalizeAggregate/normalizeSort is performed, the members in apply cannot be obtained,
            // resulting in inconsistent output results and results in apply
            topDown(
                    new SimplifyAggGroupBy(),
                    new NormalizeAggregate(),
                    new CountLiteralToCountStar(),
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
                    // push down SEMI Join
                    bottomUp(
                            new SemiJoinCommute(),
                            new TransposeSemiJoinLogicalJoin(),
                            new TransposeSemiJoinLogicalJoinProject(),
                            new TransposeSemiJoinAgg(),
                            new TransposeSemiJoinAggProject()
                    ),
                    topDown(
                            new EliminateDedupJoinCondition()
                    ),
                    // eliminate useless not null or inferred not null
                    // TODO: wait InferPredicates to infer more not null.
                    bottomUp(new EliminateNotNull()),
                    topDown(new ConvertInnerOrCrossJoin())
            ),
            topic("LEADING JOIN",
                bottomUp(
                    new CollectJoinConstraint()
                ),
                custom(RuleType.LEADING_JOIN, LeadingJoin::new),
                bottomUp(
                    new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE)
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
                    topDown(new PushFilterInsideJoin()),
                    topDown(new ExpressionNormalization())
            ),

            // this rule should invoke after ColumnPruning
            custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),

            topic("Set operation optimization",
                    // Do MergeSetOperation first because we hope to match pattern of Distinct SetOperator.
                    topDown(new PushProjectThroughUnion(), new MergeProjects()),
                    bottomUp(new MergeSetOperations()),
                    bottomUp(new PushProjectIntoOneRowRelation()),
                    topDown(new MergeOneRowRelationIntoUnion()),
                    costBased(topDown(new InferSetOperatorDistinct())),
                    topDown(new BuildAggForUnion())
            ),

            // topic("Distinct",
            //         costBased(custom(RuleType.PUSH_DOWN_DISTINCT_THROUGH_JOIN, PushdownDistinctThroughJoin::new))
            // ),

            topic("Limit optimization",
                    topDown(
                            // TODO: the logical plan should not contains any phase information,
                            //       we should refactor like AggregateStrategies, e.g. LimitStrategies,
                            //       generate one PhysicalLimit if current distribution is gather or two
                            //       PhysicalLimits with gather exchange
                            new LimitSortToTopN(),
                            new SplitLimit(),
                            new PushdownLimit(),
                            new PushdownTopNThroughJoin(),
                            new PushdownLimitDistinctThroughJoin(),
                            new PushdownTopNThroughWindow(),
                            new CreatePartitionTopNFromWindow()
                    )
            ),
            // TODO: these rules should be implementation rules, and generate alternative physical plans.
            topic("Table/Physical optimization",
                    topDown(
                            new PruneOlapScanPartition(),
                            new PruneFileScanPartition(),
                            new PushConjunctsIntoJdbcScan(),
                            new PushConjunctsIntoEsScan()
                    )
            ),
            topic("MV optimization",
                    topDown(
                            new SelectMaterializedIndexWithAggregate(),
                            new SelectMaterializedIndexWithoutAggregate(),
                            new EliminateFilter(),
                            new PushdownFilterThroughProject(),
                            new MergeProjects(),
                            new PruneOlapScanTablet()
                    ),
                    custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                    bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                    custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new)
            ),
            topic("topn optimize",
                    topDown(new DeferMaterializeTopNResult())
            ),
            // this rule batch must keep at the end of rewrite to do some plan check
            topic("Final rewrite and check",
                    custom(RuleType.CHECK_DATA_TYPES, CheckDataTypes::new),
                    custom(RuleType.ENSURE_PROJECT_ON_TOP_JOIN, EnsureProjectOnTopJoin::new),
                    topDown(
                            new PushdownFilterThroughProject(),
                            new MergeProjects()
                    ),
                    // SORT_PRUNING should be applied after mergeLimit
                    custom(RuleType.ELIMINATE_SORT, EliminateSort::new),
                    custom(RuleType.ADJUST_CONJUNCTS_RETURN_TYPE, AdjustConjunctsReturnType::new),
                    bottomUp(
                            new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE),
                            new CheckMatchExpression(),
                            new CheckMultiDistinct(),
                            new CheckAfterRewrite()
                    )
            ),
            topic("Push project and filter on cte consumer to cte producer",
                    topDown(
                            new CollectFilterAboveConsumer(),
                            new CollectProjectAboveConsumer()
                    )
            ),

            topic("eliminate empty relation",
                bottomUp(new EliminateEmptyRelation())
            )
    );

    private static final List<RewriteJob> WHOLE_TREE_REWRITE_JOBS
            = getWholeTreeRewriteJobs(true);

    private static final List<RewriteJob> WHOLE_TREE_REWRITE_JOBS_WITHOUT_COST_BASED
            = getWholeTreeRewriteJobs(false);

    private final List<RewriteJob> rewriteJobs;

    private Rewriter(CascadesContext cascadesContext, List<RewriteJob> rewriteJobs) {
        super(cascadesContext);
        this.rewriteJobs = rewriteJobs;
    }

    public static Rewriter getWholeTreeRewriterWithoutCostBasedJobs(CascadesContext cascadesContext) {
        return new Rewriter(cascadesContext, WHOLE_TREE_REWRITE_JOBS_WITHOUT_COST_BASED);
    }

    public static Rewriter getWholeTreeRewriter(CascadesContext cascadesContext) {
        return new Rewriter(cascadesContext, WHOLE_TREE_REWRITE_JOBS);
    }

    public static Rewriter getCteChildrenRewriter(CascadesContext cascadesContext, List<RewriteJob> jobs) {
        return new Rewriter(cascadesContext, jobs);
    }

    public static Rewriter getWholeTreeRewriterWithCustomJobs(CascadesContext cascadesContext, List<RewriteJob> jobs) {
        return new Rewriter(cascadesContext, getWholeTreeRewriteJobs(jobs));
    }

    private static List<RewriteJob> getWholeTreeRewriteJobs(boolean withCostBased) {
        List<RewriteJob> withoutCostBased = Rewriter.CTE_CHILDREN_REWRITE_JOBS.stream()
                    .filter(j -> !(j instanceof CostBasedRewriteJob))
                    .collect(Collectors.toList());
        return getWholeTreeRewriteJobs(withCostBased ? CTE_CHILDREN_REWRITE_JOBS : withoutCostBased);
    }

    private static List<RewriteJob> getWholeTreeRewriteJobs(List<RewriteJob> jobs) {
        return jobs(
                topic("cte inline and pull up all cte anchor",
                        custom(RuleType.PULL_UP_CTE_ANCHOR, PullUpCteAnchor::new),
                        custom(RuleType.CTE_INLINE, CTEInline::new)
                ),
                topic("process limit session variables",
                        custom(RuleType.ADD_DEFAULT_LIMIT, AddDefaultLimit::new)
                ),
                topic("rewrite cte sub-tree",
                        custom(RuleType.REWRITE_CTE_CHILDREN, () -> new RewriteCteChildren(jobs))
                ),
                topic("whole plan check",
                        custom(RuleType.ADJUST_NULLABLE, AdjustNullable::new)
                )
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return rewriteJobs;
    }
}
