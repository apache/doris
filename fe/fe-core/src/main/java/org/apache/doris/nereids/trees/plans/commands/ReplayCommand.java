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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.minidump.Minidump;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.json.JSONObject;

import java.util.List;

/**
 * replay command.
 */
public class ReplayCommand extends Command implements NoForward {

    private final String dumpFileFullPath;

    public ReplayCommand(PlanType type, String dumpFileFullPath) {
        super(type);
        this.dumpFileFullPath = dumpFileFullPath;
    }

    public String getDumpFileFullPath() {
        return dumpFileFullPath;
    }

    /**
     * explain level.
     */
    public enum ExplainLevel {
        NONE(false),
        NORMAL(false),
        VERBOSE(false),
        TREE(false),
        GRAPH(false),
        DUMP(false),
        PARSED_PLAN(true),
        ANALYZED_PLAN(true),
        REWRITTEN_PLAN(true),
        OPTIMIZED_PLAN(true),
        SHAPE_PLAN(true),
        MEMO_PLAN(true),
        DISTRIBUTED_PLAN(true),
        ALL_PLAN(true)
        ;

        public final boolean isPlanLevel;

        ExplainLevel(boolean isPlanLevel) {
            this.isPlanLevel = isPlanLevel;
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 1. check fe version, if not matched throw exception
        // 2. load every thing from minidump file and replace original ones
        Minidump minidump = MinidumpUtils.loadMinidumpInputs(dumpFileFullPath);
        // 3. run nereids planner with sql in minidump file
        StatementContext statementContext = new StatementContext(ConnectContext.get(),
                new OriginStatement(minidump.getSql(), 0));
        ConnectContext.get().setStatementContext(statementContext);
        JSONObject resultPlan = MinidumpUtils.executeSql(minidump.getSql());
        JSONObject minidumpResult = new JSONObject(minidump.getResultPlanJson());

        List<String> differences = MinidumpUtils.compareJsonObjects(minidumpResult, resultPlan, "");
        assert (differences.isEmpty());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitReplayCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REPLAY;
    }
}
