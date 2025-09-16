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
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.analysis.BindSkewExpr;
import org.apache.doris.nereids.rules.analysis.CheckAfterBind;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.CollectJoinConstraint;
import org.apache.doris.nereids.rules.analysis.CollectSubQueryAlias;
import org.apache.doris.nereids.rules.analysis.CompressedMaterialize;
import org.apache.doris.nereids.rules.analysis.EliminateDistinctConstant;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalPreAggOnHint;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalSelectHint;
import org.apache.doris.nereids.rules.analysis.FillUpMissingSlots;
import org.apache.doris.nereids.rules.analysis.FillUpQualifyMissingSlot;
import org.apache.doris.nereids.rules.analysis.HavingToFilter;
import org.apache.doris.nereids.rules.analysis.LeadingJoin;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.analysis.NormalizeGenerate;
import org.apache.doris.nereids.rules.analysis.NormalizeRepeat;
import org.apache.doris.nereids.rules.analysis.OneRowRelationExtractAggregate;
import org.apache.doris.nereids.rules.analysis.OneRowRelationToProject;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectWithDistinctToAggregate;
import org.apache.doris.nereids.rules.analysis.QualifyToFilter;
import org.apache.doris.nereids.rules.analysis.ReplaceExpressionByChildOutput;
import org.apache.doris.nereids.rules.analysis.SubqueryToApply;
import org.apache.doris.nereids.rules.analysis.VariableToLiteral;
import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.SemiJoinCommute;
import org.apache.doris.nereids.rules.rewrite.SimplifyAggGroupBy;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.util.MoreFieldsThread;

import com.google.common.collect.ImmutableSet;

import java.util.List;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class Analyzer extends AbstractBatchJobExecutor {

    public static final List<RewriteJob> ANALYZE_JOBS = buildAnalyzeJobs();

    /**
     * constructor of Analyzer. For view, we only do bind relation since other analyze step will do by outer Analyzer.
     *
     * @param cascadesContext current context for analyzer
     */
    public Analyzer(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    @Override
    public List<RewriteJob> getJobs() {
        return ANALYZE_JOBS;
    }

    /**
     * nereids analyze sql.
     */
    public void analyze() {
        execute();
    }

    @Override
    public void execute() {
        MoreFieldsThread.keepFunctionSignature(false, () -> {
            super.execute();
            return null;
        });
    }

    /** buildCustomAnalyzer */
    public static Analyzer buildCustomAnalyzer(CascadesContext cascadesContext, List<RewriteJob> customJobs) {
        List<RewriteJob> wrappedJobs = notTraverseChildrenOf(
                ImmutableSet.of(LogicalView.class, LogicalCTEAnchor.class),
                () -> customJobs
        );
        return new Analyzer(cascadesContext) {
            @Override
            public List<RewriteJob> getJobs() {
                return wrappedJobs;
            }
        };
    }

    private static List<RewriteJob> buildAnalyzeJobs() {
        return notTraverseChildrenOf(
                ImmutableSet.of(LogicalView.class, LogicalCTEAnchor.class),
                Analyzer::buildAnalyzerJobs
        );
    }

    private static List<RewriteJob> buildAnalyzerJobs() {
        return jobs(
            // we should eliminate hint before "Subquery unnesting".
            topDown(new AnalyzeCTE()),
            topDown(new EliminateLogicalSelectHint(),
                    new EliminateLogicalPreAggOnHint()),
            bottomUp(
                    new BindRelation(),
                    new CheckPolicy(),
                    new BindExpression()
            ),
            topDown(new BindSink()),
            bottomUp(new CheckAfterBind()),
            topDown(new FillUpQualifyMissingSlot()),
            bottomUp(
                    new OneRowRelationToProject(),
                    new ProjectToGlobalAggregate(),
                    // this rule check's the logicalProject node's isDistinct property
                    // and replace the logicalProject node with a LogicalAggregate node
                    // so any rule before this, if create a new logicalProject node
                    // should make sure isDistinct property is correctly passed around.
                    // please see rule BindSlotReference or BindFunction for example
                    new EliminateDistinctConstant(),
                    new ProjectWithDistinctToAggregate(),
                    new ReplaceExpressionByChildOutput(),
                    new OneRowRelationExtractAggregate(),

                    // ProjectToGlobalAggregate may generate an aggregate with empty group by expressions.
                    // for sort / having, need to adjust their agg functions' nullable.
                    // for example: select sum(a) from t having sum(b) > 10 order by sum(c),
                    // then will have:
                    // sort(sum(c))                    sort(sum(c))
                    //     |                                |
                    // having(sum(b) > 10)       ==>   having(sum(b) > 10)
                    //     |                                |
                    // project(sum(a))                 agg(sum(a))
                    // then need to adjust SORT and HAVING's sum to nullable.
                    new AdjustAggregateNullableForEmptySet()
            ),
            topDown(
                    new FillUpMissingSlots(),
                    // We should use NormalizeRepeat to compute nullable properties for LogicalRepeat in the analysis
                    // stage. NormalizeRepeat will compute nullable property, add virtual slot, LogicalAggregate and
                    // LogicalProject for normalize. This rule depends on FillUpMissingSlots to fill up slots.
                    new NormalizeRepeat()
            ),
            // consider sql with user defined var @t_zone
            // set @t_zone='GMT';
            // SELECT
            //     DATE_FORMAT(convert_tz(dt, time_zone, @t_zone),'%Y-%m-%d') day
            // FROM
            //     t
            // GROUP BY
            //     1;
            // @t_zone must be replaced as 'GMT' before EliminateGroupByConstant and NormalizeAggregate rule.
            // So need run VariableToLiteral rule before the two rules.
            topDown(new VariableToLiteral()),
            // run CheckAnalysis before EliminateGroupByConstant in order to report error message correctly like bellow
            // select SUM(lo_tax) FROM lineorder group by 1;
            // errCode = 2, detailMessage = GROUP BY expression must not contain aggregate functions: sum(lo_tax)
            bottomUp(new CheckAnalysis()),
            topDown(new SimplifyAggGroupBy()),
            // compressedMaterialize should be applied before NormalizeAggregate, because the plan generated by
            // compressedMaterialize also needs normalize. for example:
            //    Agg(groupby=[A]
            //        -->scan（T)
            // after applying CompressedMaterialize plan transformed to
            //   Agg(groupby=[encode(A)]
            //       -->scan（T)
            // encode(A) has to be pushed down
            //    Agg(groupby=[x]
            //       -->project(encode A as x)
            //            -->scan（T)
            bottomUp(new CompressedMaterialize()),
            topDown(new NormalizeAggregate()),
            topDown(new HavingToFilter()),
            topDown(new QualifyToFilter()),
            bottomUp(new SemiJoinCommute()),
            bottomUp(
                    new CollectSubQueryAlias(),
                    new CollectJoinConstraint()
            ),
            topDown(new LeadingJoin()),
            topDown(new BindSkewExpr()),
            bottomUp(new NormalizeGenerate()),
            /*
             * Notice, MergeProjects rule should NOT be placed after SubqueryToApply in analyze phase.
             * because in SubqueryToApply, we may add assert_true function with subquery output slot in projects list.
             * on the other hand, the assert_true function should be not be in final output.
             * in order to keep the plan unchanged, we add a new project node to prune the extra assert_true slot.
             * but MergeProjects rule will merge the two projects and keep assert_true anyway.
             * so we move MergeProjects from analyze to rewrite phase.
             */
            bottomUp(new SubqueryToApply()),
            topDown(
                    // merge normal filter and hidden column filter
                    new MergeFilters()
            ),
            // for cte: analyze producer -> analyze consumer -> rewrite consumer -> rewrite producer,
            // in order to ensure cte consumer had right nullable attribute, need adjust nullable at analyze phase.
            custom(RuleType.ADJUST_NULLABLE, () -> new AdjustNullable(true))
        );
    }
}
