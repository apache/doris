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
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.hint.OutlineInfo;
import org.apache.doris.nereids.hint.OutlineMgr;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * create outline command
 */
public class CreateOutlineCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(CreateOutlineCommand.class);

    private final String outlineName;

    private final Boolean ignoreExist;

    private final LogicalPlan query;

    private final String originalQuery;

    private final int startIndex;

    /**
     * create outline command
     * @param outlineName outline name which is unique
     * @param ignoreExist if true create anyway
     * @param query query behind on expression parsed into logical plan
     * @param originalQuery original query string
     * @param startIndex start index of original query
     */
    public CreateOutlineCommand(String outlineName, boolean ignoreExist,
                                LogicalPlan query, String originalQuery, int startIndex) {
        super(PlanType.CREATE_OUTLINE_COMMAND);
        this.outlineName = outlineName;
        this.ignoreExist = ignoreExist;
        this.query = query;
        this.originalQuery = originalQuery;
        this.startIndex = startIndex;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        String visibleSignature = OutlineMgr.replaceConstant(originalQuery,
                executor.getContext().getStatementContext().getConstantExpressionMap(), startIndex);
        //        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(query, ctx.getStatementContext());
        executor.setParsedStmt(logicalPlanAdapter);
        if (ctx.getSessionVariable().isEnableMaterializedViewRewrite()) {
            ctx.getStatementContext().addPlannerHook(InitMaterializationContextHook.INSTANCE);
        }
        logicalPlanAdapter.setOrigStmt(new OriginStatement(originalQuery, 0));
        //        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        //        executor.setPlanner(planner);
        executor.checkBlockRules();
        String sqlId = DigestUtils.md5Hex(visibleSignature);
        String outlineData = OutlineMgr.createOutlineData(ctx.getSessionVariable());
        OutlineInfo outlineInfo = new OutlineInfo(outlineName, visibleSignature, sqlId,
                originalQuery, "outlineTarget", outlineData);
        OutlineMgr.createOutlineInternal(outlineInfo, ignoreExist, false);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateOutlineCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}

