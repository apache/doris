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

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * switch current connection to a temporary user with specified roles.
 */
public class SuCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(SuCommand.class);

    private final UserIdentity user;
    private final List<String> roles;

    public SuCommand(UserIdentity user, List<String> roles) {
        super(PlanType.SU_COMMAND);
        this.user = user;
        this.roles = roles == null ? Collections.emptyList() : roles;
    }

    public UserIdentity getUser() {
        return user;
    }

    public List<String> getRoles() {
        return roles;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        UserIdentity currentUser = ctx.getCurrentUserIdentity();
        if (currentUser == null || !currentUser.isRootUser()) {
            throw new AnalysisException("Only root can execute su");
        }
        if (ctx.isSuUser()) {
            throw new AnalysisException("Do not support nested su");
        }
        ctx.setCurrentUserIdentity(user);

        Set<String> roleOverride = Sets.newHashSet();
        for (String roleName : roles) {
            if (!Env.getCurrentEnv().getAuth().doesRoleExist(roleName)) {
                // compatible behavior: ignore non-existing roles, but warn.
                LOG.warn("su to user {} with non-existing role {}", user, roleName);
                continue;
            }
            roleOverride.add(roleName);
        }
        ctx.setCurrentRoles(roleOverride);
        ctx.getState().setOk();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.OTHER;
    }
}
