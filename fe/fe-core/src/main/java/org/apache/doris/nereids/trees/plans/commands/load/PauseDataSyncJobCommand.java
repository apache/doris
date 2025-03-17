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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * PAUSE SYNC statement used to cancel sync job.
 * syntax:
 *      PAUSE SYNC JOB [db.]jobName
 */
public class PauseDataSyncJobCommand extends Command {
    private SyncJobName syncJobName;

    public PauseDataSyncJobCommand(SyncJobName syncJobName) {
        super(PlanType.PAUSE_DATA_SYNC_JOB_COMMAND);
        this.syncJobName = syncJobName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getSyncJobManager().pauseSyncJob(this);
    }

    public String getJobName() {
        return syncJobName.getName();
    }

    public String getDbFullName() {
        return syncJobName.getDbName();
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        syncJobName.analyze(ctx);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPauseDataSyncJobCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.PAUSE;
    }
}
