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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
 * base class for all drop commands
 */
public abstract class AlterJobStatusCommand extends Command implements ForwardWithSync {
    // exclude job name prefix, which is used by inner job
    private static final String excludeJobNamePrefix = "inner_";
    private final Expression wildWhere;
    private String jobName;

    public AlterJobStatusCommand(PlanType type, Expression wildWhere) {
        super(type);
        this.wildWhere = wildWhere;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        doRun(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterJobStatusCommand(this, context);
    }

    private void validate() throws Exception {
        if (!(wildWhere instanceof EqualTo)) {
            throw new AnalysisException("Alter job status only support equal condition, but not: " + wildWhere.toSql());
        }
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        Expression left = ((EqualTo) wildWhere).left();
        Expression right = ((EqualTo) wildWhere).right();
        if (!(left instanceof UnboundSlot && ((UnboundSlot) left).getName().equalsIgnoreCase("jobName"))) {
            throw new AnalysisException("Current not support left child of where: " + left);
        }
        if (!(right instanceof StringLikeLiteral)) {
            throw new AnalysisException("Value must is string");
        }

        if (Strings.isNullOrEmpty(((StringLikeLiteral) right).getStringValue())) {
            throw new AnalysisException("Value can't is null");
        }
        this.jobName = ((StringLikeLiteral) right).getStringValue();
        if (jobName.startsWith(excludeJobNamePrefix)) {
            throw new AnalysisException("Can't alter inner job status");
        }
    }

    public abstract void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception;

}
