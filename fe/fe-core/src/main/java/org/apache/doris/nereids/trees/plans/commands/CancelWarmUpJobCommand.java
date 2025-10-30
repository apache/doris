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

import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * cancel warm up job command
 */
public class CancelWarmUpJobCommand extends Command implements ForwardWithSync {
    private Expression whereClause;
    private long jobId;

    public CancelWarmUpJobCommand(Expression whereClause) {
        super(PlanType.CANCEL_WARM_UP_JOB_COMMAND);
        this.whereClause = whereClause;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (Config.isCloudMode()) {
            ((CloudEnv) ctx.getEnv()).cancelCloudWarmUp(this);
        }
    }

    public long getJobId() {
        return jobId;
    }

    /**
     * validate cloud warm up job
     * @param ctx connect context
     * @throws AnalysisException check whether this sql is legal
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode ");
        }

        if (whereClause == null) {
            throw new AnalysisException("Missing job id");
        }

        if (!analyzeWhereClause()) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123");
        }
    }

    private boolean analyzeWhereClause() {
        if (!(whereClause instanceof EqualTo)) {
            return false;
        }

        // left child
        if (!(whereClause.child(0) instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) whereClause.child(0)).getName();
        if (leftKey.equalsIgnoreCase("id") && (whereClause.child(1) instanceof IntegerLikeLiteral)) {
            jobId = ((IntegerLikeLiteral) whereClause.child(1)).getLongValue();
        } else {
            return false;
        }
        return true;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelWarmUpJobCommand(this, context);
    }
}
