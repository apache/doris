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
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.StatsStateEvent;
import org.apache.doris.nereids.stats.StatsCalculator;

import java.util.List;

/**
 * Job to derive stats for {@link GroupExpression} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class DeriveStatsJob extends Job {
    private static final EventProducer STATS_STATE_TRACER = new EventProducer(
            StatsStateEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(StatsStateEvent.class, EventChannel.LOG)));
    private final GroupExpression groupExpression;
    private boolean deriveChildren;

    /**
     * Constructor for DeriveStatsJob.
     *
     * @param groupExpression Derive stats on this {@link GroupExpression}
     * @param context context of current job
     */
    public DeriveStatsJob(GroupExpression groupExpression, JobContext context) {
        this(groupExpression, false, context);
    }

    private DeriveStatsJob(GroupExpression groupExpression, boolean deriveChildren, JobContext context) {
        super(JobType.DERIVE_STATS, context);
        this.groupExpression = groupExpression;
        this.deriveChildren = deriveChildren;
    }

    @Override
    public void execute() {
        countJobExecutionTimesOfGroupExpressions(groupExpression);
        if (groupExpression.isStatDerived()) {
            return;
        }
        if (!deriveChildren && groupExpression.arity() > 0) {
            pushJob(new DeriveStatsJob(groupExpression, true, context));

            List<Group> children = groupExpression.children();
            // rule maybe return new logical plans to wrap some new physical plans,
            // so we should check derive stats for it if no stats
            for (int i = children.size() - 1; i >= 0; i--) {
                Group childGroup = children.get(i);

                List<GroupExpression> logicalExpressions = childGroup.getLogicalExpressions();
                for (int j = logicalExpressions.size() - 1; j >= 0; j--) {
                    GroupExpression logicalChild = logicalExpressions.get(j);
                    if (!logicalChild.isStatDerived()) {
                        pushJob(new DeriveStatsJob(logicalChild, context));
                    }
                }

                List<GroupExpression> physicalExpressions = childGroup.getPhysicalExpressions();
                for (int j = physicalExpressions.size() - 1; j >= 0; j--) {
                    GroupExpression physicalChild = physicalExpressions.get(j);
                    if (!physicalChild.isStatDerived()) {
                        pushJob(new DeriveStatsJob(physicalChild, context));
                    }
                }
            }
        } else {
            StatsCalculator.estimate(groupExpression);
            STATS_STATE_TRACER.log(StatsStateEvent.of(groupExpression,
                    groupExpression.getOwnerGroup().getStatistics()));
        }
    }
}
