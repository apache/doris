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
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * Executes IVM delta command bundles against the MV target table.
 */
public class IVMDeltaExecutor {

    public void execute(IVMRefreshContext context, List<DeltaCommandBundle> bundles)
            throws AnalysisException {
        for (DeltaCommandBundle bundle : bundles) {
            executeBundle(context, bundle);
        }
    }

    private void executeBundle(IVMRefreshContext context, DeltaCommandBundle bundle)
            throws AnalysisException {
        Command command = bundle.getCommand();
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(
                context.getMtmv(), MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        StatementContext stmtCtx = new StatementContext();
        ctx.setStatementContext(stmtCtx);
        ctx.getState().setNereids(true);
        StmtExecutor executor = new StmtExecutor(ctx,
                new LogicalPlanAdapter(command, stmtCtx));
        ctx.setExecutor(executor);
        try {
            command.run(ctx, executor);
        } catch (Exception e) {
            throw new AnalysisException("IVM delta execution failed for "
                    + bundle.getBaseTableInfo() + ": " + e.getMessage(), e);
        }
        if (ctx.getState().getStateType() != MysqlStateType.OK) {
            throw new AnalysisException("IVM delta execution failed for "
                    + bundle.getBaseTableInfo() + ": " + ctx.getState().getErrorMessage());
        }
    }
}
