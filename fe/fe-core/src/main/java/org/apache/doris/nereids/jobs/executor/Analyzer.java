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
import org.apache.doris.nereids.rules.analysis.AddInitMaterializationHook;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindRelation.CustomTableResolver;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.analysis.BuildAggForRandomDistributedTable;
import org.apache.doris.nereids.rules.analysis.CheckAfterBind;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.CollectJoinConstraint;
import org.apache.doris.nereids.rules.analysis.CollectSubQueryAlias;
import org.apache.doris.nereids.rules.analysis.EliminateDistinctConstant;
import org.apache.doris.nereids.rules.analysis.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalSelectHint;
import org.apache.doris.nereids.rules.analysis.FillUpMissingSlots;
import org.apache.doris.nereids.rules.analysis.HavingToFilter;
import org.apache.doris.nereids.rules.analysis.LeadingJoin;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.analysis.NormalizeRepeat;
import org.apache.doris.nereids.rules.analysis.OneRowRelationExtractAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectWithDistinctToAggregate;
import org.apache.doris.nereids.rules.analysis.ReplaceExpressionByChildOutput;
import org.apache.doris.nereids.rules.analysis.SubqueryToApply;
import org.apache.doris.nereids.rules.analysis.VariableToLiteral;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.SemiJoinCommute;
import org.apache.doris.nereids.rules.rewrite.SimplifyAggGroupBy;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class Analyzer extends AbstractBatchJobExecutor {

    public static final List<RewriteJob> ANALYZE_JOBS = buildAnalyzeJobs(Optional.empty());

    private final List<RewriteJob> jobs;

    /**
     * Execute the analysis job with scope.
     * @param cascadesContext planner context for execute job
     */
    public Analyzer(CascadesContext cascadesContext) {
        this(cascadesContext, Optional.empty());
    }

    /**
     * constructor of Analyzer. For view, we only do bind relation since other analyze step will do by outer Analyzer.
     *
     * @param cascadesContext current context for analyzer
     * @param customTableResolver custom resolver for outer catalog.
     */
    public Analyzer(CascadesContext cascadesContext, Optional<CustomTableResolver> customTableResolver) {
        super(cascadesContext);
        Objects.requireNonNull(customTableResolver, "customTableResolver cannot be null");

        if (customTableResolver.isPresent()) {
            this.jobs = buildAnalyzeJobs(customTableResolver);
        } else {
            this.jobs = ANALYZE_JOBS;
        }
    }

    @Override
    public List<RewriteJob> getJobs() {
        return jobs;
    }

    /**
     * nereids analyze sql.
     */
    public void analyze() {
        execute();
    }

    private static List<RewriteJob> buildAnalyzeJobs(Optional<CustomTableResolver> customTableResolver) {
        return notTraverseChildrenOf(
                ImmutableSet.of(LogicalView.class, LogicalCTEAnchor.class),
                () -> buildAnalyzerJobs(customTableResolver)
        );
    }

    private static List<RewriteJob> buildAnalyzerJobs(Optional<CustomTableResolver> customTableResolver) {
        return jobs(
            // we should eliminate hint before "Subquery unnesting".
            topDown(new AnalyzeCTE()),
            topDown(new EliminateLogicalSelectHint()),
            bottomUp(
                    new BindRelation(customTableResolver),
                    new CheckPolicy()
            ),
            bottomUp(new BindExpression()),
            topDown(new BindSink()),
            bottomUp(new CheckAfterBind()),
            bottomUp(new AddInitMaterializationHook()),
            bottomUp(
                    new ProjectToGlobalAggregate(),
                    // this rule check's the logicalProject node's isDistinct property
                    // and replace the logicalProject node with a LogicalAggregate node
                    // so any rule before this, if create a new logicalProject node
                    // should make sure isDistinct property is correctly passed around.
                    // please see rule BindSlotReference or BindFunction for example
                    new EliminateDistinctConstant(),
                    new ProjectWithDistinctToAggregate(),
                    new ReplaceExpressionByChildOutput(),
                    new OneRowRelationExtractAggregate()
            ),
            topDown(
                    new FillUpMissingSlots(),
                    // We should use NormalizeRepeat to compute nullable properties for LogicalRepeat in the analysis
                    // stage. NormalizeRepeat will compute nullable property, add virtual slot, LogicalAggregate and
                    // LogicalProject for normalize. This rule depends on FillUpMissingSlots to fill up slots.
                    new NormalizeRepeat()
            ),
            bottomUp(new AdjustAggregateNullableForEmptySet()),
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
            topDown(new EliminateGroupByConstant()),

            topDown(new SimplifyAggGroupBy()),
            // run BuildAggForRandomDistributedTable before NormalizeAggregate in order to optimize the agg plan
            topDown(new BuildAggForRandomDistributedTable()),
            topDown(new NormalizeAggregate()),
            topDown(new HavingToFilter()),
            bottomUp(new SemiJoinCommute()),
            bottomUp(
                    new CollectSubQueryAlias(),
                    new CollectJoinConstraint()
            ),
            topDown(new LeadingJoin()),
            bottomUp(new SubqueryToApply()),
            topDown(new MergeProjects())
        );
    }
}
