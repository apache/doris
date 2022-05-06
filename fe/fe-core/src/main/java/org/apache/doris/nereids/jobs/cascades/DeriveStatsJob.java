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
import org.apache.doris.nereids.memo.PlanReference;

/**
 * Job to derive stats for {@link PlanReference} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class DeriveStatsJob extends Job {
    private final PlanReference planReference;
    private boolean deriveChildren;

    /**
     * Constructor for DeriveStatsJob.
     *
     * @param planReference Derive stats on this {@link PlanReference}
     * @param context context of optimization
     */
    public DeriveStatsJob(PlanReference planReference, PlannerContext context) {
        super(JobType.DERIVE_STATS, context);
        this.planReference = planReference;
        this.deriveChildren = false;
    }

    /**
     * Copy constructor for DeriveStatsJob.
     *
     * @param other DeriveStatsJob copied from
     */
    public DeriveStatsJob(DeriveStatsJob other) {
        super(JobType.DERIVE_STATS, other.context);
        this.planReference = other.planReference;
        this.deriveChildren = other.deriveChildren;
    }

    @Override
    public void execute() {
        if (!deriveChildren) {
            deriveChildren = true;
            pushTask(new DeriveStatsJob(this));
            for (Group childSet : planReference.getChildren()) {
                if (!childSet.getLogicalPlanList().isEmpty()) {
                    pushTask(new DeriveStatsJob(childSet.getLogicalPlanList().get(0), context));
                }
            }
        } else {
            // TODO: derive stat here
            planReference.setStatDerived(true);
        }

    }
}
