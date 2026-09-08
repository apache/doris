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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SU 'user'[@'host'] WITH ROLES ('role1'[, ...]) [WORKLOAD GROUP 'wg']
 *
 * <p>Session-narrowed identity switch, MySQL's proxy-user model with a mandatory role list: the
 * session becomes the TARGET identity (audit, current_user(), user-bound row policies, user
 * properties) with a role set that REPLACES every role source, enforced at the single choke point
 * {@code Auth.getRolesByUserWithLdap}. Session-only state; nothing is persisted.
 *
 * <p>Contract:
 * <ul>
 * <li>Allowed only for accounts holding {@code PROXY_PRIV} (or {@code ADMIN_PRIV}, which implies
 *     it); a session that does not switch keeps the switching account's own grants.</li>
 * <li>Narrowing only: every requested role must already be GRANTED to the target user, so the
 *     session can never exceed the person's real authority.</li>
 * <li>One-shot: a switched session cannot SU again, and nothing widens it. A connection reset
 *     reverts to the authenticated identity ({@link ConnectContext#revertSessionNarrowing}).</li>
 * </ul>
 */
public class SuUserCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(SuUserCommand.class);

    private final UserIdentity userIdentity;
    private final List<String> roles;
    private final String workloadGroup; // nullable

    public SuUserCommand(UserIdentity userIdentity, List<String> roles, String workloadGroup) {
        super(PlanType.SU_USER_COMMAND);
        Preconditions.checkArgument(!roles.isEmpty(), "SU requires at least one role");
        this.userIdentity = Objects.requireNonNull(userIdentity, "userIdentity is null");
        this.roles = Objects.requireNonNull(roles, "roles is null");
        this.workloadGroup = workloadGroup;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // a switched session must never widen or re-target itself
        if (ctx.getAuthenticatedIdentity() != null) {
            throw new AnalysisException("SU is not allowed in an already-switched session");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.PROXY)) {
            throw new AnalysisException("Access denied: SU requires PROXY_PRIV (or ADMIN_PRIV)");
        }

        userIdentity.analyze();
        for (String role : roles) {
            FeNameFormat.checkRoleName(role, true /* can be admin */, "Invalid role in SU");
        }

        Auth auth = Env.getCurrentEnv().getAuth();
        Set<String> targetRoles = auth.getGrantedRoleNames(userIdentity);
        if (targetRoles.isEmpty()) {
            // every existing user holds at least its default role
            throw new AnalysisException("SU target user does not exist: " + userIdentity);
        }
        // narrowing only: the requested roles must be a subset of the target's granted roles
        for (String role : roles) {
            if (!targetRoles.contains(role)) {
                throw new AnalysisException("SU role '" + role + "' is not granted to the target user"
                        + " -- SU can only narrow, never mint authority");
            }
        }

        UserIdentity authenticated = ctx.getCurrentUserIdentity();
        ctx.setAuthenticatedIdentity(authenticated);
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setSessionRoleOverride(ImmutableSet.copyOf(roles));
        if (!Strings.isNullOrEmpty(workloadGroup)) {
            // Placement only. USAGE on the session's workload group (this clause, or the
            // target's default_workload_group when omitted) is checked per query against the
            // narrowed set widened with the target's own granted roles
            // (Auth.getRolesForWorkloadGroupCheck): the person's lane follows the person.
            ctx.getSessionVariable().setWorkloadGroup(workloadGroup);
        }
        // the switch itself: authenticated + effective identity, roles, connection -- the
        // statement also lands in the audit log under the effective user
        LOG.info("SU: connection {} narrowed. authenticated={} effective={} roles={} workloadGroup={}",
                ctx.getConnectionId(), authenticated, userIdentity, roles,
                workloadGroup == null ? "<unchanged>" : workloadGroup);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.OTHER;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public List<String> getRoles() {
        return roles;
    }

    public String getWorkloadGroup() {
        return workloadGroup;
    }
}
