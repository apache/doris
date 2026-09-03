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
import org.apache.doris.common.Config;
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
 * <p>Session-narrowed identity switch: the session becomes the TARGET identity (audit,
 * current_user(), user-bound row policies, user properties) with a role set that REPLACES
 * every role source — enforced at the single choke point {@code Auth.getRolesByUserWithLdap}.
 * Session-only state; nothing is persisted.
 *
 * <p>Fail-closed contract:
 * <ul>
 * <li>Allowed only for ADMIN or accounts listed in {@code Config.switch_user_users}; a session
 *     that skips SU keeps the switching account's own (minimal) grants.</li>
 * <li>NARROWING-ONLY LAW: every requested role must already be GRANTED to the ceiling identity —
 *     the target user ({@code switch_user_role_ceiling=target}, default; the session can never
 *     exceed the person's real authority) or the switching account ({@code =switcher}).</li>
 * <li>An SU'd session can never SU again, and no SET ROLE exists to widen it. Connection reset
 *     reverts to the authenticated identity ({@link ConnectContext#revertSessionNarrowing}).</li>
 * <li>Roles matching {@code Config.su_only_roles_pattern} are dormant in normal sessions and
 *     activate only through this command (see {@code Auth.isSuOnlyRole}).</li>
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
        // Re-SU refusal: a narrowed session must never widen or re-target itself.
        if (ctx.getAuthenticatedIdentity() != null) {
            throw new AnalysisException("SU is not allowed in an already-switched session");
        }
        // Gate: ADMIN, or explicitly listed switcher accounts (per-FE config).
        boolean isAdmin = Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        if (!isAdmin && !isListedSwitcher(ctx.getQualifiedUser())) {
            throw new AnalysisException("Access denied: SU requires ADMIN or membership in "
                    + "the switch_user_users FE config");
        }

        userIdentity.analyze();
        for (String role : roles) {
            FeNameFormat.checkRoleName(role, true /* can be admin */, "Invalid role in SU");
        }

        Auth auth = Env.getCurrentEnv().getAuth();
        Set<String> targetRoles = auth.getGrantedRoleNamesRaw(userIdentity);
        if (targetRoles.isEmpty()) {
            // every existing user holds at least its default role
            throw new AnalysisException("SU target user does not exist: " + userIdentity);
        }
        // NARROWING-ONLY LAW: requested roles must be a subset of the ceiling identity's
        // RAW granted roles (dormant roles count as granted — SU is their only activation).
        boolean switcherCeiling = "switcher".equalsIgnoreCase(Config.switch_user_role_ceiling);
        Set<String> ceiling = switcherCeiling
                ? auth.getGrantedRoleNamesRaw(ctx.getCurrentUserIdentity())
                : targetRoles;
        for (String role : roles) {
            if (!ceiling.contains(role)) {
                throw new AnalysisException("SU role '" + role + "' is not granted to the "
                        + (switcherCeiling ? "switching account" : "target user")
                        + " — SU can only narrow, never mint authority");
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
        // The SU linkage event: authenticated + effective identity, roles, connection — the
        // statement itself also lands in the audit log under the effective user.
        LOG.info("SU: connection {} narrowed. authenticated={} effective={} roles={} workloadGroup={}",
                ctx.getConnectionId(), authenticated, userIdentity, roles,
                workloadGroup == null ? "<unchanged>" : workloadGroup);
    }

    private static boolean isListedSwitcher(String qualifiedUser) {
        if (Strings.isNullOrEmpty(Config.switch_user_users) || Strings.isNullOrEmpty(qualifiedUser)) {
            return false;
        }
        for (String allowed : Config.switch_user_users.split(",")) {
            if (qualifiedUser.equals(allowed.trim())) {
                return true;
            }
        }
        return false;
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
