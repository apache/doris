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
import org.apache.doris.nereids.processor.pre.EliminateLogicalSelectHint;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindRelation.CustomTableResolver;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.analysis.CheckAfterBind;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.EliminateGroupByConstant;
import org.apache.doris.nereids.rules.analysis.FillUpMissingSlots;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.analysis.NormalizeRepeat;
import org.apache.doris.nereids.rules.analysis.OneRowRelationExtractAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectWithDistinctToAggregate;
import org.apache.doris.nereids.rules.analysis.ReplaceExpressionByChildOutput;
import org.apache.doris.nereids.rules.analysis.ResolveOrdinalInOrderByAndGroupBy;
import org.apache.doris.nereids.rules.analysis.SubqueryToApply;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class Analyzer extends AbstractBatchJobExecutor {

    public static final List<RewriteJob> DEFAULT_ANALYZE_JOBS = buildAnalyzeJobs(Optional.empty());

    private final List<RewriteJob> jobs;

    /**
     * Execute the analysis job with scope.
     * @param cascadesContext planner context for execute job
     */
    public Analyzer(CascadesContext cascadesContext) {
        this(cascadesContext, Optional.empty());
    }

    public Analyzer(CascadesContext cascadesContext, Optional<CustomTableResolver> customTableResolver) {
        super(cascadesContext);
        Objects.requireNonNull(customTableResolver, "customTableResolver cannot be null");
        this.jobs = !customTableResolver.isPresent() ? DEFAULT_ANALYZE_JOBS : buildAnalyzeJobs(customTableResolver);
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
        return jobs(
            // we should eliminate hint after "Subquery unnesting" because some hint maybe exist in the CTE or subquery.
            custom(RuleType.ELIMINATE_HINT, EliminateLogicalSelectHint::new),
            topDown(new AnalyzeCTE()),
            bottomUp(
                new BindRelation(customTableResolver),
                new CheckPolicy(),
                new UserAuthentication(),
                new BindExpression()
            ),
            topDown(new BindSink()),
            bottomUp(new CheckAfterBind()),
            bottomUp(
                new ProjectToGlobalAggregate(),
                // this rule check's the logicalProject node's isDistinct property
                // and replace the logicalProject node with a LogicalAggregate node
                // so any rule before this, if create a new logicalProject node
                // should make sure isDistinct property is correctly passed around.
                // please see rule BindSlotReference or BindFunction for example
                new ProjectWithDistinctToAggregate(),
                new ResolveOrdinalInOrderByAndGroupBy(),
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
            // run CheckAnalysis before EliminateGroupByConstant in order to report error message correctly like bellow
            // select SUM(lo_tax) FROM lineorder group by 1;
            // errCode = 2, detailMessage = GROUP BY expression must not contain aggregate functions: sum(lo_tax)
            bottomUp(new CheckAnalysis()),
            topDown(new EliminateGroupByConstant()),
            topDown(new NormalizeAggregate()),
            bottomUp(new SubqueryToApply())
        );
    }
}
