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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
  Resume routine load job by name

  syntax:
      RESUME ROUTINE LOAD [database.]name
 */
public class ResumeRoutineLoadCommand extends Command implements ForwardWithSync {
    private final LabelNameInfo labelNameInfo;
    private final boolean isAll;
    private String db;

    public ResumeRoutineLoadCommand(LabelNameInfo labelNameInfo) {
        super(PlanType.RESUME_ROUTINE_LOAD_COMMAND);
        this.labelNameInfo = labelNameInfo;
        this.isAll = false;
    }

    public ResumeRoutineLoadCommand() {
        super(PlanType.RESUME_ALL_ROUTINE_COMMAND);
        this.labelNameInfo = null;
        this.isAll = true;
    }

    public String getLabel() {
        return labelNameInfo.getLabel();
    }

    public boolean isAll() {
        return isAll;
    }

    public String getDbFullName() {
        return db;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getRoutineLoadManager().resumeRoutineLoadJob(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (labelNameInfo != null) {
            labelNameInfo.validate(ctx);
            db = labelNameInfo.getDb();
        } else {
            if (Strings.isNullOrEmpty(ctx.getDatabase())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            db = ctx.getDatabase();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitResumeRoutineLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.RESUME;
    }
}
