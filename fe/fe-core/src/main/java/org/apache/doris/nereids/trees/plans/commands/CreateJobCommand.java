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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateJobInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * syntax:
 * CREATE
 * [DEFINER = user]
 * JOB
 * event_name
 * ON SCHEDULE schedule
 * [COMMENT 'string']
 * DO event_body;
 * schedule: {
 * [STREAMING] AT timestamp
 * | EVERY interval
 * [STARTS timestamp ]
 * [ENDS timestamp ]
 * }
 * interval:
 * quantity { DAY | HOUR | MINUTE |
 * WEEK | SECOND }
 */
public class CreateJobCommand extends Command implements ForwardWithSync {

    private CreateJobInfo createJobInfo;

    public CreateJobCommand(CreateJobInfo jobInfo) {
        super(PlanType.CREATE_JOB_COMMAND);
        this.createJobInfo = jobInfo;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        AbstractJob job = createJobInfo.analyzeAndBuildJobInfo(ctx);
        Env.getCurrentEnv().getJobManager().registerJob(job);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateJobCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

}
