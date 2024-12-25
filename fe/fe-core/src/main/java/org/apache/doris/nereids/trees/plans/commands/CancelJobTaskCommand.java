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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * base class for all drop commands
 */
public class CancelJobTaskCommand extends CancelCommand implements ForwardWithSync {
    private static final String jobNameKey = "jobName";

    private static final String taskIdKey = "taskId";

    private String jobName;

    private Long taskId;

    private Expression expr;

    public CancelJobTaskCommand(Expression expr) {
        super(PlanType.CANCEL_JOB_COMMAND);
        this.expr = expr;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelTaskCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        doRun(ctx);
    }

    private void validate() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        if (!(expr instanceof And)) {
            throw new AnalysisException("Only allow compound predicate with operator AND");
        }
        if (!(expr.child(0).child(0) instanceof UnboundSlot)
                && jobNameKey.equals(((UnboundSlot) expr.child(0).child(0)).getName())) {
            throw new AnalysisException("Current not support " + ((UnboundSlot) expr.child(0).child(0)).getName());
        }

        if (!(expr.child(0).child(1) instanceof StringLikeLiteral)) {
            throw new AnalysisException("JobName value must is string");
        }
        this.jobName = ((StringLikeLiteral) expr.child(0).child(1)).getStringValue();
        String taskIdInput = ((StringLikeLiteral) expr.child(1).child(0)).getStringValue();
        if (!taskIdKey.equalsIgnoreCase(taskIdInput)) {
            throw new AnalysisException("Current not support " + taskIdInput);
        }
        if (!(expr.child(1).child(1) instanceof LargeIntLiteral)) {
            throw new AnalysisException("task id  value must is large int");
        }
        this.taskId = ((LargeIntLiteral) expr.child(1).child(1)).getLongValue();
    }

    public void doRun(ConnectContext ctx) throws Exception {
        try {
            ctx.getEnv().getJobManager().cancelTaskById(jobName, taskId);
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }
    }
}
