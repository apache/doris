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
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionNormalization;
import org.apache.doris.nereids.rules.mv.SelectRollup;
import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.rules.rewrite.logical.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.logical.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.logical.LogicalLimitZeroToLogicalEmptyRelation;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveFilters;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveLimits;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveProjects;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.logical.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.logical.PushPredicateThroughJoin;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.rules.rewrite.logical.SwapFilterAndProject;

import com.google.common.collect.ImmutableList;

/**
 * Apply rules to normalize expressions.
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
                /*
                 * Subquery unnesting.
                 * 1. Adjust the plan in correlated logicalApply
                 *    so that there are no correlated columns in the subquery.
                 * 2. Convert logicalApply to a logicalJoin.
                 */
                .addAll(new AdjustApplyFromCorrelatToUnCorrelatJob(cascadesContext).rulesJob)
                .addAll(new ConvertApplyToJoinJob(cascadesContext).rulesJob)
                .add(topDownBatch(ImmutableList.of(new ExpressionNormalization())))
                .add(topDownBatch(ImmutableList.of(new NormalizeAggregate())))
                .add(topDownBatch(ImmutableList.of(new ReorderJoin())))
                .add(topDownBatch(ImmutableList.of(new FindHashConditionForJoin())))
                .add(topDownBatch(ImmutableList.of(new PushPredicateThroughJoin())))
                .add(topDownBatch(ImmutableList.of(new NormalizeAggregate())))
                .add(topDownBatch(ImmutableList.of(new ColumnPruning())))
                .add(topDownBatch(ImmutableList.of(new AggregateDisassemble())))
                .add(topDownBatch(ImmutableList.of(new SwapFilterAndProject())))
                .add(bottomUpBatch(ImmutableList.of(new MergeConsecutiveProjects())))
                .add(topDownBatch(ImmutableList.of(new MergeConsecutiveFilters())))
                .add(bottomUpBatch(ImmutableList.of(new MergeConsecutiveLimits())))
                .add(bottomUpBatch(ImmutableList.of(new LogicalLimitZeroToLogicalEmptyRelation())))
                .add(topDownBatch(ImmutableList.of(new PruneOlapScanPartition())))
                .add(topDownBatch(ImmutableList.of(new SelectRollup())))
                .build();

        rulesJob.addAll(jobs);
    }
}
