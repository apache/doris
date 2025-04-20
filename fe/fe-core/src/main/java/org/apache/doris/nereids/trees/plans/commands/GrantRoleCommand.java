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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * GrantRoleCommand
 */
public class GrantRoleCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private final UserIdentity userIdentity;
    private final List<String> roles;

    public GrantRoleCommand(UserIdentity userIdentity, List<String> roles) {
        super(PlanType.GRANT_ROLE_COMMAND);
        this.userIdentity = userIdentity;
        this.roles = roles;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getAuth().grantRoleCommand(this);
    }

    public void validate() throws AnalysisException {
        userIdentity.analyze();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRoleCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.GRANT;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public List<String> getRoles() {
        return roles;
    }
}
