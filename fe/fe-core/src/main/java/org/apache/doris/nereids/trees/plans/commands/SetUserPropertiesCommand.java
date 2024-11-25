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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.SetUserPropertyVarOp;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * SetUserPropertiesCommand
 */
public class SetUserPropertiesCommand extends Command implements ForwardWithSync {
    private final String user;
    private final List<SetUserPropertyVarOp> setUserPropertyVarOpList;

    public SetUserPropertiesCommand(String user, List<SetUserPropertyVarOp> setUserPropertyVarOpList) {
        super(PlanType.SET_OPTIONS_COMMAND);
        this.user = user != null ? user : ConnectContext.get().getQualifiedUser();
        this.setUserPropertyVarOpList = setUserPropertyVarOpList;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<Pair<String, String>> properties = new ArrayList<>(setUserPropertyVarOpList.size());
        for (SetUserPropertyVarOp op : setUserPropertyVarOpList) {
            op.validate(ctx);
            properties.add(Pair.of(op.getPropertyKey(), op.getPropertyValue()));
        }
        ctx.getEnv().getAuth().updateUserPropertyInternal(user, properties, false);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSetUserPropertiesCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SET;
    }
}
