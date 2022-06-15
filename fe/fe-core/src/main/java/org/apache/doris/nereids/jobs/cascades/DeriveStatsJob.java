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

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Job to derive stats for {@link GroupExpression} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class DeriveStatsJob extends Job<Plan> {
    private final GroupExpression groupExpression;
    private boolean deriveChildren;

    /**
     * Constructor for DeriveStatsJob.
     *
     * @param groupExpression Derive stats on this {@link GroupExpression}
     * @param context context of optimization
     */
    public DeriveStatsJob(GroupExpression groupExpression, PlannerContext context) {
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
            pushTask(new DeriveStatsJob(this));
            for (Group child : groupExpression.children()) {
                if (!child.getLogicalExpressions().isEmpty()) {
                    pushTask(new DeriveStatsJob(child.getLogicalExpressions().get(0), context));
                }
            }
        } else {
            // TODO: derive stat here
            groupExpression.setStatDerived(true);
        }
    }
}
