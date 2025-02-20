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

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AddBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterSystemOp;
import org.apache.doris.nereids.trees.plans.commands.info.DecommissionBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropObserverOp;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

/**
 * Alter System
 */
public class AlterSystemCommand extends Command implements ForwardWithSync {
    private AlterSystemOp alterSystemOp;

    public AlterSystemCommand(AlterSystemOp alterSystemOp) {
        super(PlanType.ALTER_SYSTEM);
        this.alterSystemOp = alterSystemOp;
    }

    /**
     * getOps
     */
    public AlterClause getAlterClause() {
        return alterSystemOp.translateToLegacyAlterClause();
    }

    /**
     * validate
     */
    private void validate(ConnectContext ctx) throws UserException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "NODE");
        }

        Preconditions.checkState((alterSystemOp instanceof AddBackendOp
                || alterSystemOp instanceof DropBackendOp
                || alterSystemOp instanceof DecommissionBackendOp
                || alterSystemOp instanceof AddObserverOp
                || alterSystemOp instanceof DropObserverOp
                || alterSystemOp instanceof AddFollowerOp
                || alterSystemOp instanceof DropFollowerOp)
        );

        alterSystemOp.validate(ctx);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().alterSystem(this);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
