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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * alter multi table materialized view
 */
public class AlterMTMVCommand extends Command implements ForwardWithSync, NotAllowFallback {

    public static final Logger LOG = LogManager.getLogger(AlterMTMVCommand.class);
    private final AlterMTMVInfo alterMTMVInfo;

    /**
     * constructor
     */
    public AlterMTMVCommand(AlterMTMVInfo alterMTMVInfo) {
        super(PlanType.ALTER_MTMV_COMMAND);
        this.alterMTMVInfo = Objects.requireNonNull(alterMTMVInfo, "require AlterMTMVInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        alterMTMVInfo.analyze(ctx);
        alterMTMVInfo.run();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMTMVCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
