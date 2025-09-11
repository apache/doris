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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.CostBasedRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AvgDistinctToSumDivCount;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.expression.CheckLegalityAfterRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionNormalizationAndOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.NullableDependentExpressionRewrite;
import org.apache.doris.nereids.rules.expression.QueryColumnCollector;
import org.apache.doris.nereids.rules.rewrite.AddDefaultLimit;
import org.apache.doris.nereids.rules.rewrite.AddProjectForJoin;
import org.apache.doris.nereids.rules.rewrite.AddProjectForUniqueFunction;
import org.apache.doris.nereids.rules.rewrite.AdjustConjunctsReturnType;
import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.AggScalarSubQueryToWindowFunction;
import org.apache.doris.nereids.rules.rewrite.BuildAggForUnion;
import org.apache.doris.nereids.rules.rewrite.CTEInline;
import org.apache.doris.nereids.rules.rewrite.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.CheckDataTypes;
import org.apache.doris.nereids.rules.rewrite.CheckMatchExpression;
import org.apache.doris.nereids.rules.rewrite.CheckMultiDistinct;
import org.apache.doris.nereids.rules.rewrite.CheckPrivileges;
import org.apache.doris.nereids.rules.rewrite.CheckRestorePartition;
import org.apache.doris.nereids.rules.rewrite.CheckScoreUsage;
import org.apache.doris.nereids.rules.rewrite.ClearContextStatus;
import org.apache.doris.nereids.rules.rewrite.CollectCteConsumerOutput;
import org.apache.doris.nereids.rules.rewrite.CollectFilterAboveConsumer;
import org.apache.doris.nereids.rules.rewrite.CollectPredicateOnScan;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.ConstantPropagation;
import org.apache.doris.nereids.rules.rewrite.ConvertInnerOrCrossJoin;
import org.apache.doris.nereids.rules.rewrite.ConvertOuterJoinToAntiJoin;
import org.apache.doris.nereids.rules.rewrite.CountDistinctRewrite;
import org.apache.doris.nereids.rules.rewrite.CountLiteralRewrite;
import org.apache.doris.nereids.rules.rewrite.CreatePartitionTopNFromWindow;
import org.apache.doris.nereids.rules.rewrite.DecoupleEncodeDecode;
import org.apache.doris.nereids.rules.rewrite.DeferMaterializeTopNResult;
import org.apache.doris.nereids.rules.rewrite.DistinctAggStrategySelector;
import org.apache.doris.nereids.rules.rewrite.DistinctAggregateRewriter;
import org.apache.doris.nereids.rules.rewrite.DistinctWindowExpression;
import org.apache.doris.nereids.rules.rewrite.EliminateAggCaseWhen;
import org.apache.doris.nereids.rules.rewrite.EliminateAggregate;
import org.apache.doris.nereids.rules.rewrite.EliminateAssertNumRows;
import org.apache.doris.nereids.rules.rewrite.EliminateConstHashJoinCondition;
import org.apache.doris.nereids.rules.rewrite.EliminateDedupJoinCondition;
import org.apache.doris.nereids.rules.rewrite.EliminateEmptyRelation;
import org.apache.doris.nereids.rules.rewrite.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.EliminateGroupBy;
import org.apache.doris.nereids.rules.rewrite.EliminateGroupByKey;
import org.apache.doris.nereids.rules.rewrite.EliminateGroupByKeyByUniform;
import org.apache.doris.nereids.rules.rewrite.EliminateJoinByFK;
import org.apache.doris.nereids.rules.rewrite.EliminateJoinByUnique;
import org.apache.doris.nereids.rules.rewrite.EliminateJoinCondition;
import org.apache.doris.nereids.rules.rewrite.EliminateLimit;
import org.apache.doris.nereids.rules.rewrite.EliminateNotNull;
import org.apache.doris.nereids.rules.rewrite.EliminateNullAwareLeftAntiJoin;
import org.apache.doris.nereids.rules.rewrite.EliminateOrderByConstant;
import org.apache.doris.nereids.rules.rewrite.EliminateOrderByKey;
import org.apache.doris.nereids.rules.rewrite.EliminateSemiJoin;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.rules.rewrite.EliminateSortUnderSubqueryOrView;
import org.apache.doris.nereids.rules.rewrite.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.ExtractAndNormalizeWindowExpression;
import org.apache.doris.nereids.rules.rewrite.ExtractFilterFromCrossJoin;
import org.apache.doris.nereids.rules.rewrite.ExtractSingleTableExpressionFromDisjunction;
import org.apache.doris.nereids.rules.rewrite.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.InferAggNotNull;
import org.apache.doris.nereids.rules.rewrite.InferFilterNotNull;
import org.apache.doris.nereids.rules.rewrite.InferInPredicateFromOr;
import org.apache.doris.nereids.rules.rewrite.InferJoinNotNull;
import org.apache.doris.nereids.rules.rewrite.InferPredicates;
import org.apache.doris.nereids.rules.rewrite.InferSetOperatorDistinct;
import org.apache.doris.nereids.rules.rewrite.InitJoinOrder;
import org.apache.doris.nereids.rules.rewrite.InlineLogicalView;
import org.apache.doris.nereids.rules.rewrite.LimitAggToTopNAgg;
import org.apache.doris.nereids.rules.rewrite.LimitSortToTopN;
import org.apache.doris.nereids.rules.rewrite.LogicalResultSinkToShortCircuitPointQuery;
import org.apache.doris.nereids.rules.rewrite.MergeAggregate;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeOneRowRelationIntoUnion;
import org.apache.doris.nereids.rules.rewrite.MergePercentileToArray;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.rules.rewrite.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.MergeSetOperationsExcept;
import org.apache.doris.nereids.rules.rewrite.MergeTopNs;
import org.apache.doris.nereids.rules.rewrite.NormalizeSort;
import org.apache.doris.nereids.rules.rewrite.OperativeColumnDerive;
import org.apache.doris.nereids.rules.rewrite.OrExpansion;
import org.apache.doris.nereids.rules.rewrite.ProjectOtherJoinConditionForNestedLoopJoin;
import org.apache.doris.nereids.rules.rewrite.PruneEmptyPartition;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanTablet;
import org.apache.doris.nereids.rules.rewrite.PullUpCteAnchor;
import org.apache.doris.nereids.rules.rewrite.PullUpJoinFromUnionAll;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectBetweenTopNAndAgg;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderLimit;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderTopN;
import org.apache.doris.nereids.rules.rewrite.PushCountIntoUnionAll;
import org.apache.doris.nereids.rules.rewrite.PushDownAggThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownAggThroughJoinOnPkFk;
import org.apache.doris.nereids.rules.rewrite.PushDownAggThroughJoinOneSide;
import org.apache.doris.nereids.rules.rewrite.PushDownAggWithDistinctThroughJoinOneSide;
import org.apache.doris.nereids.rules.rewrite.PushDownDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownEncodeSlot;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterIntoSchemaScan;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.PushDownLimit;
import org.apache.doris.nereids.rules.rewrite.PushDownLimitDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownLimitDistinctThroughUnion;
import org.apache.doris.nereids.rules.rewrite.PushDownProjectThroughLimit;
import org.apache.doris.nereids.rules.rewrite.PushDownScoreTopNIntoOlapScan;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNDistinctThroughUnion;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNThroughUnion;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNThroughWindow;
import org.apache.doris.nereids.rules.rewrite.PushDownVectorTopNIntoOlapScan;
import org.apache.doris.nereids.rules.rewrite.PushDownVirtualColumnsIntoOlapScan;
import org.apache.doris.nereids.rules.rewrite.PushFilterInsideJoin;
import org.apache.doris.nereids.rules.rewrite.PushProjectIntoUnion;
import org.apache.doris.nereids.rules.rewrite.PushProjectThroughUnion;
import org.apache.doris.nereids.rules.rewrite.RecordPlanForMvPreRewrite;
import org.apache.doris.nereids.rules.rewrite.ReduceAggregateChildOutputRows;
import org.apache.doris.nereids.rules.rewrite.ReorderJoin;
import org.apache.doris.nereids.rules.rewrite.RewriteCteChildren;
import org.apache.doris.nereids.rules.rewrite.SaltJoin;
import org.apache.doris.nereids.rules.rewrite.SetPreAggStatus;
import org.apache.doris.nereids.rules.rewrite.SimplifyEncodeDecode;
import org.apache.doris.nereids.rules.rewrite.SimplifyWindowExpression;
import org.apache.doris.nereids.rules.rewrite.SkewJoin;
import org.apache.doris.nereids.rules.rewrite.SplitLimit;
import org.apache.doris.nereids.rules.rewrite.SumLiteralRewrite;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinAgg;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinAggProject;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinLogicalJoin;
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinLogicalJoinProject;
import org.apache.doris.nereids.rules.rewrite.VariantSubPathPruning;
import org.apache.doris.nereids.rules.rewrite.batch.ApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.batch.CorrelateApplyToUnCorrelateApply;
import org.apache.doris.nereids.rules.rewrite.batch.EliminateUselessPlanUnderApply;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.MoreFieldsThread;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Apply rules to rewrite logical plan.
 */
public class Rewriter extends AbstractBatchJobExecutor {

    public static final List<RewriteJob> CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED =
            notTraverseChildrenOf(
                    ImmutableSet.of(LogicalCTEAnchor.class),
                    () -> jobs(
                            topic("Plan Normalization",
                                    topDown(
                                            new EliminateOrderByConstant(),
                                            new EliminateSortUnderSubqueryOrView(),
                                            // MergeProjects depends on this rule
                                            new LogicalSubQueryAliasToLogicalProject(),
                                            ExpressionNormalizationAndOptimization.FULL_RULE_INSTANCE,
                                            new AvgDistinctToSumDivCount(),
                                            new CountDistinctRewrite(),
                                            new ExtractFilterFromCrossJoin()
                                    ),
                                    topDown(
                                            // ExtractSingleTableExpressionFromDisjunction conflict to
                                            // InPredicateToEqualToRule
                                            // in the ExpressionNormalization, so must invoke in another job,
                                            // otherwise dead loop.
                                            new ExtractSingleTableExpressionFromDisjunction()
                                    )
                            ),
                            // subquery unnesting relay on ExpressionNormalization to extract common factor expression
                            topic("Subquery unnesting",
                                    // after doing NormalizeAggregate in analysis job
                                    // we need run the following 2 rules to make
                                    // AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION work
                                    bottomUp(new PullUpProjectUnderApply()),
                                    topDown(
                                            new PushDownFilterThroughProject(),
                                            // the subquery may have where and having clause
                                            // so there may be two filters we need to merge them
                                            new MergeFilters()
                                    ),
                                    custom(RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION,
                                            AggScalarSubQueryToWindowFunction::new),
                                    bottomUp(
                                            new EliminateUselessPlanUnderApply(),
                                            // CorrelateApplyToUnCorrelateApply and ApplyToJoin
                                            // and SelectMaterializedIndexWithAggregate depends on this rule
                                            new MergeProjectable(),
                                            /*
                                             * Subquery unnesting.
                                             * 1. Adjust the plan in correlated logicalApply
                                             *    so that there are no correlated columns in the subquery.
                                             * 2. Convert logicalApply to a logicalJoin.
                                             *  TODO: group these rules to make sure the result plan is what
                                             *   we expected.
                                             */
                                            new CorrelateApplyToUnCorrelateApply(),
                                            new ApplyToJoin(),
                                            // UnCorrelatedApplyAggregateFilter rule will create new aggregate outputs,
                                            // The later rule CheckPrivileges which inherent from ColumnPruning
                                            // only works
                                            // if the aggregation node is normalized, so we need call
                                            // NormalizeAggregate here
                                            new NormalizeAggregate()
                                    )
                            ),
                            // before `Subquery unnesting` topic, some correlate slots should have appeared at
                            // LogicalApply.left,
                            // but it appeared at LogicalApply.right. After the `Subquery unnesting` topic, all
                            // slots is placed in a
                            // normal position, then we can check column privileges by these steps
                            //
                            // 1. use ColumnPruning rule to derive the used slots in LogicalView
                            // 2. and then check the column privileges
                            // 3. finally, we can eliminate the LogicalView
                            topic("Inline view and check column privileges",
                                    bottomUp(new InlineLogicalView())
                            ),
                            topic("Eliminate optimization",
                                    bottomUp(
                                            new EliminateLimit(),
                                            new EliminateFilter(),
                                            new EliminateAggregate(),
                                            new EliminateAggCaseWhen(),
                                            new ReduceAggregateChildOutputRows(),
                                            new EliminateJoinCondition(),
                                            new EliminateAssertNumRows(),
                                            new EliminateSemiJoin()
                                    )
                            ),
                            // The rule modification needs to be done after the subquery is unnested,
                            // because for scalarSubQuery, the connection condition is stored in apply in
                            // the analyzer phase,
                            // but when normalizeAggregate/normalizeSort is performed, the members in apply
                            // cannot be obtained,
                            // resulting in inconsistent output results and results in apply
                            topDown(
                                    new NormalizeAggregate(),
                                    new CountLiteralRewrite(),
                                    new NormalizeSort()
                            ),

                            topDown(// must behind NormalizeAggregate/NormalizeSort
                                    new MergeProjectable(),
                                    new PushDownEncodeSlot(),
                                    new DecoupleEncodeDecode()
                            ),

                            topic("Window analysis",
                                    topDown(
                                            new ExtractAndNormalizeWindowExpression(),
                                            new CheckAndStandardizeWindowFunctionAndFrame(),
                                            new SimplifyWindowExpression()
                                    )
                            ),
                            topic("Rewrite join",
                                    // infer not null filter, then push down filter, and then reorder join
                                    // (cross join to inner join)
                                    topDown(
                                            new InferAggNotNull(),
                                            new InferFilterNotNull(),
                                            new InferJoinNotNull()
                                    ),
                                    // ReorderJoin depends PUSH_DOWN_FILTERS
                                    // the PUSH_DOWN_FILTERS depends on lots of rules, e.g. merge project,
                                    // eliminate outer,
                                    // sometimes transform the bottom plan make some rules usable which can apply to
                                    // the top plan,
                                    // but top-down traverse can not cover this case in one iteration,
                                    // so bottom-up is more
                                    // efficient because it can find the new plans and apply transform wherever it is
                                    bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                                    // after push down, some new filters are generated, which needs to be optimized.
                                    // (example: tpch q19)
                                    // topDown(new ExpressionOptimization()),
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
                            topic("Set operation optimization",
                                    // Do MergeSetOperation first because we hope to match pattern of
                                    // Distinct SetOperator.
                                    topDown(new PushProjectThroughUnion(), new MergeProjectable()),
                                    bottomUp(new MergeSetOperations(), new MergeSetOperationsExcept()),
                                    topDown(new MergeOneRowRelationIntoUnion()),
                                    topDown(new BuildAggForUnion()),
                                    bottomUp(new EliminateEmptyRelation()),
                                    // when union has empty relation child and constantExprsList is not empty,
                                    // after EliminateEmptyRelation, project can be pushed into union
                                    topDown(new PushProjectIntoUnion())
                            ),
                            topic("infer In-predicate from Or-predicate",
                                    topDown(new InferInPredicateFromOr())
                            ),
                            // putting the "Column pruning and infer predicate" topic behind the "Set operation
                            // optimization"
                            // is because that pulling up predicates from union needs EliminateEmptyRelation in
                            // union child
                            topic("Column pruning and infer predicate",
                                    custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                                    custom(RuleType.INFER_PREDICATES, InferPredicates::new),
                                    // column pruning create new project, so we should use PUSH_DOWN_FILTERS
                                    // to change filter-project to project-filter
                                    bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                                    // after eliminate outer join in the PUSH_DOWN_FILTERS,
                                    // we can infer more predicate and push down
                                    custom(RuleType.INFER_PREDICATES, InferPredicates::new),
                                    bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                                    // after eliminate outer join, we can move some filters to join.otherJoinConjuncts,
                                    // this can help to translate plan to backend
                                    topDown(new PushFilterInsideJoin()),
                                    topDown(new FindHashConditionForJoin()),
                                    // ProjectOtherJoinConditionForNestedLoopJoin will push down the expression
                                    // in the non-equivalent join condition and turn it into slotReference,
                                    // This results in the inability to obtain Cast child information in
                                    // INFER_PREDICATES,
                                    // which will affect predicate inference with cast. So put this rule
                                    // behind the INFER_PREDICATES
                                    topDown(new ProjectOtherJoinConditionForNestedLoopJoin())
                            ),
                            // this rule should invoke after ColumnPruning
                            custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),
                            // Add necessary rules from mv pre rewrite
                            topic("Push project and filter on cte consumer to cte producer",
                                    topDown(
                                            new CollectFilterAboveConsumer(),
                                            new CollectCteConsumerOutput())
                            ),
                            topic("Table/Physical optimization",
                                    topDown(
                                            new PruneOlapScanPartition(),
                                            new PruneEmptyPartition(),
                                            new PruneFileScanPartition(),
                                            new PushDownFilterIntoSchemaScan()
                                    )
                            ),
                            topic("necessary rules before record mv",
                                    topDown(new SplitLimit()),
                                    custom(RuleType.SET_PREAGG_STATUS, SetPreAggStatus::new),
                                    custom(RuleType.OPERATIVE_COLUMN_DERIVE, OperativeColumnDerive::new),
                                    custom(RuleType.ADJUST_NULLABLE, () -> new AdjustNullable(false))
                            ),
                            topic("add projection for join",
                                    // this is for hint project join rewrite rule
                                    custom(RuleType.ADD_PROJECT_FOR_JOIN, AddProjectForJoin::new),
                                    topDown(new MergeProjectable())
                            )
                    )
            );

    private static final List<RewriteJob> CTE_CHILDREN_REWRITE_JOBS_BEFORE_SUB_PATH_PUSH_DOWN = notTraverseChildrenOf(
            ImmutableSet.of(LogicalCTEAnchor.class),
            () -> jobs(
                topic("Plan Normalization",
                        // move MergeProjects rule from analyze phase
                        // because SubqueryToApply and BindSink rule may create extra project node
                        // we need merge them at the beginning of rewrite phase to let later rules happy
                        topDown(new MergeProjectable()),
                        topDown(
                                new EliminateOrderByConstant(),
                                new EliminateSortUnderSubqueryOrView(),
                                // MergeProjects depends on this rule
                                new LogicalSubQueryAliasToLogicalProject(),
                                // TODO: we should do expression normalization after plan normalization
                                //   because some rewritten depends on sub expression tree matching
                                //   such as group by key matching and replaced
                                //   but we need to do some normalization before subquery unnesting,
                                //   such as extract common expression.
                                ExpressionNormalizationAndOptimization.FULL_RULE_INSTANCE,
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
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalApply.class),
                        // after doing NormalizeAggregate in analysis job
                        // we need run the following 2 rules to make AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION work
                        bottomUp(new PullUpProjectUnderApply()),
                        topDown(
                                new PushDownFilterThroughProject(),
                                // the subquery may have where and having clause
                                // so there may be two filters we need to merge them
                                new MergeFilters()
                        ),
                        custom(RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION, AggScalarSubQueryToWindowFunction::new),
                        bottomUp(
                                new EliminateUselessPlanUnderApply(),
                                // CorrelateApplyToUnCorrelateApply and ApplyToJoin
                                // and SelectMaterializedIndexWithAggregate depends on this rule
                                new MergeProjectable(),
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

                // before `Subquery unnesting` topic, some correlate slots should have appeared at LogicalApply.left,
                // but it appeared at LogicalApply.right. After the `Subquery unnesting` topic, all slots is placed in a
                // normal position, then we can check column privileges by these steps
                //
                // 1. use ColumnPruning rule to derive the used slots in LogicalView
                // 2. and then check the column privileges
                // 3. finally, we can eliminate the LogicalView
                topic("Inline view and check column privileges",
                    bottomUp(
                        // The later rule CHECK_PRIVILEGES which inherent from ColumnPruning only works
                        // if the aggregation node is normalized, so we need call NormalizeAggregate here
                        new NormalizeAggregate()
                    ),
                    // ReorderJoin expect no LogicalProject on the LogicalJoin,
                    // so the CheckPrivileges should not change the plan
                    custom(RuleType.CHECK_PRIVILEGES, CheckPrivileges::new),
                    bottomUp(new InlineLogicalView())
                ),
                topic("Eliminate optimization",
                        bottomUp(
                                new EliminateLimit(),
                                new EliminateFilter(),
                                new EliminateAggregate(),
                                new EliminateAggCaseWhen(),
                                new ReduceAggregateChildOutputRows(),
                                new EliminateJoinCondition(),
                                new EliminateAssertNumRows(),
                                new EliminateSemiJoin(),
                                new SimplifyEncodeDecode()
                        )
                ),
                // The rule modification needs to be done after the subquery is unnested,
                // because for scalarSubQuery, the connection condition is stored in apply in the analyzer phase,
                // but when normalizeAggregate/normalizeSort is performed, the members in apply cannot be obtained,
                // resulting in inconsistent output results and results in apply
                topDown(
                        new NormalizeAggregate(),
                        new CountLiteralRewrite(),
                        new NormalizeSort()
                ),

                topDown(// must behind NormalizeAggregate/NormalizeSort
                        new MergeProjectable(),
                        new PushDownEncodeSlot(),
                        new DecoupleEncodeDecode()
                ),

                topic("Window analysis",
                        topDown(
                                new ExtractAndNormalizeWindowExpression(),
                                new DistinctWindowExpression(),
                                new CheckAndStandardizeWindowFunctionAndFrame(),
                                new SimplifyWindowExpression()
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

                        topic("",
                                cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalJoin.class),
                                topDown(
                                        new ReorderJoin(),
                                        new PushFilterInsideJoin(),
                                        new FindHashConditionForJoin(),
                                        new ConvertInnerOrCrossJoin(),
                                        new EliminateNullAwareLeftAntiJoin()
                                ),
                                // push down SEMI Join
                                bottomUp(
                                        new TransposeSemiJoinLogicalJoin(),
                                        new TransposeSemiJoinLogicalJoinProject(),
                                        new TransposeSemiJoinAgg(),
                                        new TransposeSemiJoinAggProject()
                                ),
                                topDown(
                                        new EliminateDedupJoinCondition()
                                )
                        ),

                        // eliminate useless not null or inferred not null
                        // TODO: wait InferPredicates to infer more not null.
                        bottomUp(new EliminateNotNull()),
                        topDown(new ConvertInnerOrCrossJoin())
                ),
                topic("Set operation optimization",
                        topic("",
                                cascadesContext -> cascadesContext.rewritePlanContainsTypes(SetOperation.class),
                                // Do MergeSetOperation first because we hope to match pattern of Distinct SetOperator.
                                bottomUp(
                                        new MergeProjectable(),
                                        new PushProjectThroughUnion(),
                                        new MergeSetOperations(),
                                        new MergeSetOperationsExcept()
                                )
                        ),
                        topic("",
                                cascadesContext -> cascadesContext.rewritePlanContainsTypes(SetOperation.class),
                                topDown(new MergeOneRowRelationIntoUnion()),
                                costBased(topDown(new InferSetOperatorDistinct())),
                                topDown(new BuildAggForUnion()),
                                bottomUp(new EliminateEmptyRelation()),
                                // when union has empty relation child and constantExprsList is not empty,
                                // after EliminateEmptyRelation, project can be pushed into union
                                topDown(new PushProjectIntoUnion())
                        )
                ),
                topic("infer In-predicate from Or-predicate",
                        topDown(new InferInPredicateFromOr())
                ),
                // putting the "Column pruning and infer predicate" topic behind the "Set operation optimization"
                // is because that pulling up predicates from union needs EliminateEmptyRelation in union child
                topic("Column pruning and infer predicate",
                    custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                    topic("",
                            cascadesContext -> !cascadesContext.rewritePlanContainsTypes(
                                    LogicalJoin.class, LogicalSetOperation.class
                            ),
                            bottomUp(RuleSet.PUSH_DOWN_FILTERS)
                    ),
                    topic("infer predicate",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(
                                LogicalFilter.class, LogicalJoin.class, LogicalSetOperation.class
                        ),
                        custom(RuleType.CONSTANT_PROPAGATION, ConstantPropagation::new),
                        custom(RuleType.INFER_PREDICATES, InferPredicates::new),
                        // column pruning create new project, so we should use PUSH_DOWN_FILTERS
                        // to change filter-project to project-filter
                        bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                        // after eliminate outer join in the PUSH_DOWN_FILTERS,
                        // we can infer more predicate and push down
                        custom(RuleType.INFER_PREDICATES, InferPredicates::new),
                        bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                        // after eliminate outer join, we can move some filters to join.otherJoinConjuncts,
                        // this can help to translate plan to backend
                        topDown(new PushFilterInsideJoin()),
                        topDown(new FindHashConditionForJoin()),
                        // ProjectOtherJoinConditionForNestedLoopJoin will push down the expression
                        // in the non-equivalent join condition and turn it into slotReference,
                        // This results in the inability to obtain Cast child information in INFER_PREDICATES,
                        // which will affect predicate inference with cast. So put this rule behind the INFER_PREDICATES
                        topDown(
                                new ProjectOtherJoinConditionForNestedLoopJoin(),
                                // constant propagation and push down condition may change join conditions empty or not
                                new ConvertInnerOrCrossJoin())
                    )
                ),
                // this rule should invoke after ColumnPruning
                custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),
                topic("Eliminate Order By Key",
                        topDown(new EliminateOrderByKey())),
                topic("Eliminate GroupBy",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalAggregate.class),
                        topDown(
                                new EliminateGroupBy(),
                                new MergeAggregate()
                        )
                ),
                topic("Eager aggregation",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(
                                LogicalAggregate.class, LogicalJoin.class
                        ),
                        costBased(topDown(
                                new PushDownAggWithDistinctThroughJoinOneSide(),
                                new PushDownAggThroughJoinOneSide(),
                                new PushDownAggThroughJoin()
                        )),
                        costBased(custom(RuleType.PUSH_DOWN_DISTINCT_THROUGH_JOIN, PushDownDistinctThroughJoin::new)),
                        topDown(new PushCountIntoUnionAll())
                ),

                // this rule should invoke after infer predicate and push down distinct, and before push down limit
                topic("eliminate join according unique or foreign key",
                    cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalJoin.class),
                    bottomUp(new EliminateJoinByFK()),
                    topDown(new EliminateJoinByUnique())
                ),
                topic("join skew salting rewrite",
                        topDown(new SaltJoin())),
                topic("eliminate Aggregate according to fd items",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalAggregate.class)
                                || cascadesContext.rewritePlanContainsTypes(LogicalJoin.class)
                                || cascadesContext.rewritePlanContainsTypes(LogicalUnion.class),
                        topDown(new EliminateGroupByKey()),
                        topDown(new PushDownAggThroughJoinOnPkFk()),
                        topDown(new PullUpJoinFromUnionAll())
                ),

                topic("Limit optimization",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalLimit.class)
                                || cascadesContext.rewritePlanContainsTypes(LogicalTopN.class)
                                || cascadesContext.rewritePlanContainsTypes(LogicalWindow.class),
                        // TODO: the logical plan should not contains any phase information,
                        //       we should refactor like AggregateStrategies, e.g. LimitStrategies,
                        //       generate one PhysicalLimit if current distribution is gather or two
                        //       PhysicalLimits with gather exchange
                        topDown(new LimitSortToTopN()),
                        topDown(new MergeTopNs()),
                        topDown(new LimitAggToTopNAgg()),
                        topDown(new SplitLimit()),
                        topDown(
                                new PushDownLimit(),
                                new PushDownLimitDistinctThroughJoin(),
                                new PushDownLimitDistinctThroughUnion(),
                                new PushDownTopNDistinctThroughJoin(),
                                new PushDownTopNDistinctThroughUnion(),
                                new PushDownTopNThroughJoin(),
                                new PushDownTopNThroughWindow(),
                                new PushDownTopNThroughUnion()
                        ),
                        topDown(new CreatePartitionTopNFromWindow()),
                        topDown(
                                new PullUpProjectUnderTopN(),
                                new PullUpProjectUnderLimit()
                        )
                ),
                // TODO: these rules should be implementation rules, and generate alternative physical plans.
                topic("Table/Physical optimization",
                        cascadesContext -> cascadesContext.rewritePlanContainsTypes(LogicalCatalogRelation.class),
                        topDown(
                                new PruneOlapScanPartition(),
                                new PruneEmptyPartition(),
                                new PruneFileScanPartition(),
                                new PushDownFilterIntoSchemaScan(),
                                new PruneOlapScanTablet()
                        ),
                        bottomUp(RuleSet.PUSH_DOWN_FILTERS)
                ),
                custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),
                topic("adjust preagg status",
                        custom(RuleType.SET_PREAGG_STATUS, SetPreAggStatus::new)
                ),
                topic("Point query short circuit",
                        topDown(new LogicalResultSinkToShortCircuitPointQuery())),
                topic("eliminate",
                        // SORT_PRUNING should be applied after mergeLimit
                        custom(RuleType.ELIMINATE_SORT, EliminateSort::new),
                        bottomUp(
                                new EliminateEmptyRelation(),
                                // after eliminate empty relation under union, we could get
                                // limit
                                // +-- project
                                //     +-- limit
                                //         + project
                                // so, we need push project through limit to satisfy translator's assumptions
                                new PushDownFilterThroughProject(),
                                new PushDownProjectThroughLimit(),
                                new MergeProjectable())
                ),
                topic("set initial join order",
                        bottomUp(ImmutableList.of(new InitJoinOrder())),
                        topDown(new SkewJoin())),
                topic("agg rewrite",
                    // these rules should be put after mv optimization to avoid mv matching fail
                    topDown(new SumLiteralRewrite(),
                            new MergePercentileToArray())
                ),
                topic("add projection for unique function",
                        // separate AddProjectForUniqueFunction and MergeProjectable
                        // to avoid dead loop if code has bug
                        topDown(new AddProjectForUniqueFunction()),
                        topDown(new MergeProjectable())
                ),
                topic("collect scan filter for hbo",
                    // this rule is to collect filter on basic table for hbo usage
                    topDown(new CollectPredicateOnScan())
                ),
                topic("Push project and filter on cte consumer to cte producer",
                        topDown(
                                new CollectFilterAboveConsumer(),
                                new CollectCteConsumerOutput()
                        )
                ),
                topic("Collect used column", custom(RuleType.COLLECT_COLUMNS, QueryColumnCollector::new))
        )
    );

    private static final List<RewriteJob> CTE_CHILDREN_REWRITE_JOBS_AFTER_SUB_PATH_PUSH_DOWN = notTraverseChildrenOf(
            ImmutableSet.of(LogicalCTEAnchor.class),
            () -> jobs(
                // after variant sub path pruning, we need do column pruning again
                bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                bottomUp(ImmutableList.of(
                        new PushDownFilterThroughProject(),
                        new MergeProjectable()
                )),
                topDown(DistinctAggregateRewriter.INSTANCE),
                custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),
                topDown(new PushDownVectorTopNIntoOlapScan()),
                topDown(new PushDownVirtualColumnsIntoOlapScan()),
                topic("score optimize",
                        topDown(new PushDownScoreTopNIntoOlapScan(),
                                new CheckScoreUsage())
                ),
                topic("topn optimize",
                        topDown(new DeferMaterializeTopNResult())
                ),
                topic("add projection for join",
                        custom(RuleType.ADD_PROJECT_FOR_JOIN, AddProjectForJoin::new),
                        topDown(new MergeProjectable())
                ),
                topic("adjust topN project",
                        topDown(new PullUpProjectBetweenTopNAndAgg())),
                topic("remove const hash join condition",
                        topDown(new EliminateConstHashJoinCondition())),

                // this rule batch must keep at the end of rewrite to do some plan check
                topic("final rewrite and check",
                        custom(RuleType.CHECK_DATA_TYPES, CheckDataTypes::new),
                        topDown(new PushDownFilterThroughProject(), new MergeProjectable()),
                        custom(RuleType.ADJUST_CONJUNCTS_RETURN_TYPE, AdjustConjunctsReturnType::new),
                        bottomUp(
                                new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE),
                                new CheckMatchExpression(),
                                new CheckMultiDistinct(),
                                new CheckRestorePartition(),
                                new CheckAfterRewrite()
                        )
                ),
                topDown(new CollectCteConsumerOutput()),
                custom(RuleType.OPERATIVE_COLUMN_DERIVE, OperativeColumnDerive::new)
            )
    );

    private static final List<RewriteJob> WHOLE_TREE_REWRITE_JOBS
            = getWholeTreeRewriteJobs(true);

    private static final List<RewriteJob> WHOLE_TREE_REWRITE_JOBS_WITHOUT_COST_BASED
            = getWholeTreeRewriteJobs(false);

    private final List<RewriteJob> rewriteJobs;
    private final boolean runCboRules;

    private Rewriter(CascadesContext cascadesContext, List<RewriteJob> rewriteJobs, boolean runCboRules) {
        super(cascadesContext);
        this.rewriteJobs = rewriteJobs;
        this.runCboRules = runCboRules;
    }

    public static Rewriter getWholeTreeRewriterWithoutCostBasedJobs(CascadesContext cascadesContext) {
        return new Rewriter(cascadesContext, WHOLE_TREE_REWRITE_JOBS_WITHOUT_COST_BASED, false);
    }

    public static Rewriter getWholeTreeRewriter(CascadesContext cascadesContext) {
        return new Rewriter(cascadesContext, WHOLE_TREE_REWRITE_JOBS, true);
    }

    public static Rewriter getCteChildrenRewriter(CascadesContext cascadesContext, List<RewriteJob> jobs) {
        return new Rewriter(cascadesContext, jobs, true);
    }

    public static Rewriter getCteChildrenRewriter(
            CascadesContext cascadesContext, List<RewriteJob> jobs, boolean runCboRules) {
        return new Rewriter(cascadesContext, jobs, runCboRules);
    }

    /**
     * only
     */
    public static Rewriter getWholeTreeRewriterWithCustomJobs(
            CascadesContext cascadesContext, List<RewriteJob> jobs) {
        List<RewriteJob> wholeTreeRewriteJobs = getWholeTreeRewriteJobs(
                false, false, jobs, ImmutableList.of(), true);
        return new Rewriter(cascadesContext, wholeTreeRewriteJobs, true);
    }

    private static List<RewriteJob> getWholeTreeRewriteJobs(boolean runCboRules) {
        return getWholeTreeRewriteJobs(true, true,
                CTE_CHILDREN_REWRITE_JOBS_BEFORE_SUB_PATH_PUSH_DOWN,
                CTE_CHILDREN_REWRITE_JOBS_AFTER_SUB_PATH_PUSH_DOWN, runCboRules);
    }

    private static List<RewriteJob> getWholeTreeRewriteJobs(
            boolean needSubPathPushDown,
            boolean needOrExpansion,
            List<RewriteJob> beforePushDownJobs,
            List<RewriteJob> afterPushDownJobs,
            boolean runCboRules) {

        return notTraverseChildrenOf(
            ImmutableSet.of(LogicalCTEAnchor.class),
            () -> {
                List<RewriteJob> rewriteJobs = Lists.newArrayListWithExpectedSize(300);

                rewriteJobs.addAll(jobs(
                        topic("cte inline and pull up all cte anchor",
                                custom(RuleType.PULL_UP_CTE_ANCHOR, PullUpCteAnchor::new),
                                custom(RuleType.CTE_INLINE, CTEInline::new)
                        ),
                        topic("process limit session variables",
                                custom(RuleType.ADD_DEFAULT_LIMIT, AddDefaultLimit::new)
                        ),
                        topic("record query tmp plan for mv pre rewrite",
                                custom(RuleType.RECORD_PLAN_FOR_MV_PRE_REWRITE, RecordPlanForMvPreRewrite::new)
                        ),
                        topic("rewrite cte sub-tree before sub path push down",
                                custom(RuleType.REWRITE_CTE_CHILDREN,
                                        () -> new RewriteCteChildren(beforePushDownJobs, runCboRules)
                                )
                        )));
                rewriteJobs.addAll(jobs(topic("convert outer join to anti",
                        custom(RuleType.CONVERT_OUTER_JOIN_TO_ANTI, ConvertOuterJoinToAntiJoin::new))));
                rewriteJobs.addAll(jobs(topic("eliminate group by key by uniform",
                        custom(RuleType.ELIMINATE_GROUP_BY_KEY_BY_UNIFORM, EliminateGroupByKeyByUniform::new))));
                if (needOrExpansion) {
                    rewriteJobs.addAll(jobs(topic("or expansion",
                            custom(RuleType.OR_EXPANSION, () -> OrExpansion.INSTANCE))));
                }

                rewriteJobs.addAll(jobs(topic("split multi distinct",
                        custom(RuleType.DISTINCT_AGG_STRATEGY_SELECTOR, () -> DistinctAggStrategySelector.INSTANCE))));

                if (needSubPathPushDown) {
                    rewriteJobs.addAll(jobs(
                            topic("variant element_at push down",
                                    custom(RuleType.VARIANT_SUB_PATH_PRUNING, VariantSubPathPruning::new)
                            )
                    ));
                }
                rewriteJobs.addAll(jobs(
                        topic("rewrite cte sub-tree after sub path push down",
                                custom(RuleType.CLEAR_CONTEXT_STATUS, ClearContextStatus::new),
                                custom(RuleType.REWRITE_CTE_CHILDREN,
                                        () -> new RewriteCteChildren(afterPushDownJobs, runCboRules)
                                )
                        ),
                        topic("whole plan check",
                                custom(RuleType.ADJUST_NULLABLE, () -> new AdjustNullable(false))
                        ),
                        // NullableDependentExpressionRewrite need to be done after nullable fixed
                        topic("condition function", bottomUp(ImmutableList.of(
                                new NullableDependentExpressionRewrite())))
                ));
                return rewriteJobs;
            }
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return rewriteJobs;
    }

    @Override
    protected boolean shouldRun(RewriteJob rewriteJob, JobContext jobContext, List<RewriteJob> jobs, int jobIndex) {
        if (rewriteJob instanceof CostBasedRewriteJob) {
            if (runCboRules) {
                jobContext.setRemainJobs(jobs.subList(jobIndex + 1, jobs.size()));
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public void execute() {
        MoreFieldsThread.keepFunctionSignature(() -> {
            super.execute();
            return null;
        });
    }
}
