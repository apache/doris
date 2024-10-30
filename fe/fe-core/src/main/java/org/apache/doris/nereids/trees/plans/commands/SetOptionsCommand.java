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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.SetLdapPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetVarOp;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * SetOptionsCommand
 */
public class SetOptionsCommand extends Command implements Forward, NeedAuditEncryption {
    private final List<SetVarOp> setVarOpList;

    public SetOptionsCommand(List<SetVarOp> setVarOpList) {
        super(PlanType.SET_OPTIONS_COMMAND);
        this.setVarOpList = setVarOpList;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        for (SetVarOp varOp : setVarOpList) {
            if (varOp.getType() == SetType.GLOBAL || varOp instanceof SetPassVarOp
                    || varOp instanceof SetLdapPassVarOp) {
                return RedirectStatus.FORWARD_WITH_SYNC;
            }
        }
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        for (SetVarOp varOp : setVarOpList) {
            varOp.validate(ctx);
            varOp.run(ctx);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSetOptionsCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SET;
    }

    @Override
    public boolean needAuditEncryption() {
        for (SetVarOp setVarOp : setVarOpList) {
            if (setVarOp.needAuditEncryption()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SET ");
        int idx = 0;
        for (SetVarOp variableInfo : setVarOpList) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(variableInfo.toSql());
            idx++;
        }
        return sb.toString();
    }

    @Override
    public void afterForwardToMaster(ConnectContext ctx) throws Exception {
        for (SetVarOp varOp : setVarOpList) {
            varOp.validate(ctx);
            varOp.afterForwardToMaster(ctx);
        }
    }
}
