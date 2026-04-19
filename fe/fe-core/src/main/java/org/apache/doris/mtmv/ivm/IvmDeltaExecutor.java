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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.Command;

import java.util.List;

/**
 * Executes IVM delta commands against the MV target table.
 */
public class IvmDeltaExecutor {

    /**
     * Executes all delta commands.
     *
     * @param exprIdStart the next ExprId value produced by the plan analysis StatementContext.
     *                    Each command execution creates a fresh StatementContext initialised from
     *                    this value so that ExprIds assigned during execution never collide with
     *                    ExprIds already embedded in the pre-built plan trees.
     */
    public void execute(IvmRefreshContext context, List<Command> commands, int exprIdStart)
            throws AnalysisException {
        for (Command command : commands) {
            executeCommand(context, command, exprIdStart);
        }
    }

    private void executeCommand(IvmRefreshContext context, Command command, int exprIdStart)
            throws AnalysisException {
        StatementContext stmtCtx = new StatementContext(exprIdStart);
        String auditStmt = String.format("IVM delta refresh, mvName: %s, command: %s",
                context.getMtmv().getName(), command.getClass().getSimpleName());
        try {
            // normalPlan had applied ivm normal mtmv plan rule, so no need enable this rule then.
            MTMVPlanUtil.executeCommand(context.getMtmv(), command,
                    stmtCtx, auditStmt, false);
        } catch (Exception e) {
            throw new AnalysisException("IVM delta execution failed for "
                    + command.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }
}
