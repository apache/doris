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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.jobs.batch.BatchRewriteJob;
import org.apache.doris.nereids.rules.analysis.AdjustAggregateNullableForEmptySet;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckAnalysis;
import org.apache.doris.nereids.rules.analysis.CheckBound;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.FillUpMissingSlots;
import org.apache.doris.nereids.rules.analysis.NormalizeRepeat;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectWithDistinctToAggregate;
import org.apache.doris.nereids.rules.analysis.RegisterCTE;
import org.apache.doris.nereids.rules.analysis.ReplaceExpressionByChildOutput;
import org.apache.doris.nereids.rules.analysis.ResolveOrdinalInOrderByAndGroupBy;
import org.apache.doris.nereids.rules.analysis.SubqueryToApply;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.rules.rewrite.logical.HideOneRowRelationUnderUnion;

import java.util.List;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class NereidsAnalyzer extends BatchRewriteJob {
    public static final List<RewriteJob> ANALYZE_JOBS = jobs(
            topDown(
                new RegisterCTE()
            ),
            bottomUp(
                new BindRelation(),
                new CheckPolicy(),
                new UserAuthentication(),
                new BindExpression()
            ),
            bottomUp(
                new CheckBound()
            ),
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
                new HideOneRowRelationUnderUnion()
            ),
            topDown(
                new FillUpMissingSlots(),
                // We should use NormalizeRepeat to compute nullable properties for LogicalRepeat in the analysis
                // stage. NormalizeRepeat will compute nullable property, add virtual slot, LogicalAggregate and
                // LogicalProject for normalize. This rule depends on FillUpMissingSlots to fill up slots.
                new NormalizeRepeat()
            ),
            bottomUp(new SubqueryToApply()),
            bottomUp(new AdjustAggregateNullableForEmptySet()),
            bottomUp(new CheckAnalysis())
    );

    /**
     * Execute the analysis job with scope.
     * @param cascadesContext planner context for execute job
     */
    public NereidsAnalyzer(CascadesContext cascadesContext) {
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
}
