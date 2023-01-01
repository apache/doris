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
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.rewrite.logical.MergeProjects;

import com.google.common.collect.ImmutableList;

/**
 * Analyze subquery.
 */
public class UnnestSubqueryJob extends BatchRulesJob {
    /**
     * Unnest subquery.
     */
    public UnnestSubqueryJob(CascadesContext cascadesContext) {
        super(cascadesContext);
        ImmutableList<Job> jobs = new ImmutableList.Builder<Job>()
                // MergeProjects depends on this rule
                .add(bottomUpBatch(ImmutableList.of(new LogicalSubQueryAliasToLogicalProject())))
                // AdjustApplyFromCorrelateToUnCorrelateJob and ConvertApplyToJoinJob
                // and SelectMaterializedIndexWithAggregate depends on this rule
                .add(topDownBatch(ImmutableList.of(new MergeProjects())))
                /*
                 * Subquery unnesting.
                 * 1. Adjust the plan in correlated logicalApply
                 *    so that there are no correlated columns in the subquery.
                 * 2. Convert logicalApply to a logicalJoin.
                 *  TODO: group these rules to make sure the result plan is what we expected.
                 */
                .addAll(new AdjustApplyFromCorrelateToUnCorrelateJob(cascadesContext).rulesJob)
                .addAll(new ConvertApplyToJoinJob(cascadesContext).rulesJob)
                .build();
        rulesJob.addAll(jobs);
    }
}
