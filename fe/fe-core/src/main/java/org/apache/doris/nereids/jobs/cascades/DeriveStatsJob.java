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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.stats.StatsCalculator;

/**
 * Job to derive stats for {@link GroupExpression} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class DeriveStatsJob extends Job {
    private final GroupExpression groupExpression;
    private boolean deriveChildren;

    /**
     * Constructor for DeriveStatsJob.
     *
     * @param groupExpression Derive stats on this {@link GroupExpression}
     * @param context context of current job
     */
    public DeriveStatsJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.DERIVE_STATS, context);
        this.groupExpression = groupExpression;
        this.deriveChildren = false;
    }

    /**
     * Copy constructor for DeriveStatsJob.
     *
     * @param other DeriveStatsJob copied from
     */
    public DeriveStatsJob(DeriveStatsJob other) {
        super(JobType.DERIVE_STATS, other.context);
        this.groupExpression = other.groupExpression;
        this.deriveChildren = other.deriveChildren;
    }

    @Override
    public void execute() {
        if (!deriveChildren) {
            deriveChildren = true;
            pushJob(new DeriveStatsJob(this));
            for (Group child : groupExpression.children()) {
                GroupExpression childGroupExpr = child.getLogicalExpressions().get(0);
                if (!child.getLogicalExpressions().isEmpty() && !childGroupExpr.isStatDerived()) {
                    pushJob(new DeriveStatsJob(childGroupExpr, context));
                }
            }
        } else {
            StatsCalculator.estimate(groupExpression);
        }
    }
}
