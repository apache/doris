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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 * resume mtmv
 */
public class ResumeMTMVCommand extends Command implements ForwardWithSync, NotAllowFallback {
    private final ResumeMTMVInfo resumeMTMVInfo;

    public ResumeMTMVCommand(ResumeMTMVInfo resumeMTMVInfo) {
        super(PlanType.RESUME_MTMV_COMMAND);
        this.resumeMTMVInfo = Objects.requireNonNull(resumeMTMVInfo, "require resumeMTMVInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        resumeMTMVInfo.analyze(ctx);
        Env.getCurrentEnv().getMtmvService().resumeMTMV(resumeMTMVInfo);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitResumeMTMVCommand(this, context);
    }
}
