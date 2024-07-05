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

import org.apache.doris.catalog.Env;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.BatchInsertJob;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.BatchInsertJobInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Command to create a batch insert job
 */
public class CreateBatchInsertJobCommand extends Command implements ForwardWithSync, NotAllowFallback {

    private BatchInsertJobInfo batchInsertJobInfo;

    public CreateBatchInsertJobCommand(BatchInsertJobInfo batchInsertJobInfo) {
        super(PlanType.CREATE_BATCH_INSERT_JOB_COMMAND);
        this.batchInsertJobInfo = batchInsertJobInfo;

    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateBatchInsertJobCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        //analyze
        batchInsertJobInfo.analyze(ctx, executor);
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        jobExecutionConfiguration.setExecuteType(JobExecuteType.INSTANT);
        jobExecutionConfiguration.setMaxConcurrentTaskNum(1);
        long jobId = Env.getCurrentEnv().getNextId();
        long currentDbId = ctx.getCurrentDbId();
        String currentDbName = ctx.getCurrentCatalog().getDbOrAnalysisException(currentDbId).getFullName();
        BatchInsertJob batchInsertJob = new BatchInsertJob(jobId, "batch_job" + jobId,
                JobStatus.RUNNING,
                currentDbName, "", ctx.getCurrentUserIdentity(), jobExecutionConfiguration,
                batchInsertJobInfo.getInsertSql(), batchInsertJobInfo.getSplitColumnInfo(),
                batchInsertJobInfo.getBatchSize(),
                Long.toString(batchInsertJobInfo.getLowerBound()), Long.toString(batchInsertJobInfo.getUpperBound())
        );
        Env.getCurrentEnv().getJobManager().registerJob(batchInsertJob);
        ctx.getState().setOk(0, 0, "create batch insert job success, job id: " + jobId);
    }
}
