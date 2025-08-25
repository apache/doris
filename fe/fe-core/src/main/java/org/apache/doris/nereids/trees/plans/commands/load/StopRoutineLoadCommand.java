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
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
  Stop routine load job by name

  syntax:
      STOP ROUTINE LOAD [database.]name
 */
public class StopRoutineLoadCommand extends Command implements ForwardWithSync {

    private final LabelNameInfo labelNameInfo;

    public StopRoutineLoadCommand(LabelNameInfo labelNameInfo) {
        super(PlanType.STOP_ROUTINE_LOAD_COMMAND);
        this.labelNameInfo = labelNameInfo;
    }

    public String getLabel() {
        return labelNameInfo.getLabel();
    }

    public String getDbFullName() {
        return labelNameInfo.getDb();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getRoutineLoadManager().stopRoutineLoadJob(this);
    }

    public void validate(ConnectContext ctx) throws AnalysisException {
        labelNameInfo.validate(ctx);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitStopRoutineLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.STOP;
    }
}
