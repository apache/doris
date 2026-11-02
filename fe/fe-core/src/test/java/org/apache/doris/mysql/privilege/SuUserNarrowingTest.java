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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.AuthProcDir;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.SuUserCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetUserPropertyVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

/**
 * SU (session-narrowed identity switch) acceptance tests:
 * (a) a narrowed session loses the target's personal grants (override REPLACES the role union);
 * (c) re-SU is refused; (d) a session that skips SU keeps only the switcher's own grants;
 * (f) narrowing never leaks into checks of OTHER identities; (g) the narrowed session keeps the
 * person's own WORKLOAD GROUP usage (placement follows the person; never wider than their own
 * session); plus the PROXY_PRIV gate, the target-grants ceiling, the parser round trip and the
 * no-ConnectContext metadata-RPC path.
 * End-to-end row-policy + audit coverage belongs to the regression suite, not here.
 */
public class SuUserNarrowingTest extends TestWithFeService {

    private static final String CTL = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Override
    protected void runBeforeAll() throws Exception {
        // GRANT validates database existence on the internal catalog
        createDatabase("test");
        createDatabase("perso");
        createDatabase("perso2");
    }

    private UserIdentity ident(String name) {
        return UserIdentity.createAnalyzedUserIdentWithIp(name, "%");
    }

    private boolean canSelectDb(UserIdentity user, String db) {
        return Env.getCurrentEnv().getAuth().checkDbPriv(user, CTL, db, PrivPredicate.SELECT);
    }

    private boolean canSelectInfoSchema(UserIdentity user) {
        return Env.getCurrentEnv().getAuth().checkDbPriv(user, CTL,
                org.apache.doris.catalog.InfoSchemaDb.DATABASE_NAME, PrivPredicate.SELECT);
    }

    private boolean canUseWorkloadGroup(UserIdentity user, String wg) {
        return Env.getCurrentEnv().getAuth().checkWorkloadGroupPriv(user, wg, PrivPredicate.USAGE);
    }

    private void createWorkloadGroup(String name) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle("CREATE WORKLOAD GROUP IF NOT EXISTS " + name
                + " PROPERTIES ('min_memory_percent'='10', 'max_memory_percent'='30%')");
        Assertions.assertTrue(plan instanceof CreateWorkloadGroupCommand);
        ((CreateWorkloadGroupCommand) plan).run(connectContext, null);
    }

    private void grantWorkloadGroupUsage(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        ((GrantResourcePrivilegeCommand) plan).run(connectContext, null);
    }

    /** A session of a service account that holds nothing but the right to switch. */
    private ConnectContext proxySession(String switcher) throws Exception {
        addUser(switcher, true);
        grantPriv("GRANT PROXY_PRIV ON *.*.* TO '" + switcher + "'@'%';");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident(switcher));
        ctx.setRemoteIP("127.0.0.1");
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** Runs an administrative statement on the test's admin session, then restores the thread's context. */
    private void runAsAdmin(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof Command, sql);
        ConnectContext current = ConnectContext.get();
        connectContext.setThreadLocalInfo();
        try {
            ((Command) plan).run(connectContext, new StmtExecutor(connectContext, sql));
        } finally {
            current.setThreadLocalInfo();
        }
    }

    private static String shownRoles(UserIdentity user) {
        return Env.getCurrentEnv().getAuth().getAuthInfo(user).get(0).get(AuthProcDir.TITLE_NAMES.indexOf("Roles"));
    }

    private static void assertSwitchRefused(ConnectContext ctx, UserIdentity target, String role) {
        Assertions.assertThrows(AnalysisException.class, () ->
                new SuUserCommand(target, Collections.singletonList(role), null).run(ctx, null));
        // a refused switch leaves the session exactly as it was
        Assertions.assertNull(ctx.getAuthenticatedIdentity());
        Assertions.assertFalse(ctx.isSessionRoleNarrowed());
    }

    @Test
    public void testOverrideReplacesRoleUnionAndDoesNotLeak() throws Exception {
        addUser("alice", true);
        addUser("bystander", true);
        createRole("space_a");
        createRole("bystander_r");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'space_a';");
        grantRole("GRANT 'space_a' TO 'alice'@'%'");
        grantRole("GRANT 'bystander_r' TO 'bystander'@'%'");
        // personal (default-role) grant on another db
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'alice'@'%';");

        UserIdentity alice = ident("alice");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(alice);
        ctx.setThreadLocalInfo();
        try {
            // un-narrowed: personal + role grants both live
            Assertions.assertTrue(canSelectDb(alice, "perso"));
            Assertions.assertTrue(canSelectDb(alice, "test"));

            ctx.setSessionRoleOverride(Collections.singleton("space_a"));
            // (a) the override REPLACES everything: the personal grant is gone...
            Assertions.assertFalse(canSelectDb(alice, "perso"));
            // ...while the requested role's grants remain
            Assertions.assertTrue(canSelectDb(alice, "test"));

            // (f) checks against OTHER identities are untouched by this session's narrowing
            Set<String> bystanderRoles = Env.getCurrentEnv().getAuth()
                    .getRoleNamesByUserWithLdap(ident("bystander"), false);
            Assertions.assertTrue(bystanderRoles.contains("bystander_r"));

            // revert restores the full union
            ctx.setSessionRoleOverride(null);
            Assertions.assertTrue(canSelectDb(alice, "perso"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSuCommandContract() throws Exception {
        addUser("svc", true);
        addUser("bobby", true);
        createRole("r_bobby");
        createRole("r_ungranted");
        createRole("r_svc_only");
        grantRole("GRANT 'r_bobby' TO 'bobby'@'%'");
        grantRole("GRANT 'r_svc_only' TO 'svc'@'%'");

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident("svc"));
        ctx.setThreadLocalInfo();
        try {
            // gate: neither ADMIN_PRIV nor PROXY_PRIV -> refused; (d) the session stays the service account
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));
            Assertions.assertNull(ctx.getAuthenticatedIdentity());
            Assertions.assertFalse(canSelectDb(ident("svc"), "test"));

            // PROXY_PRIV is the native gate (ADMIN_PRIV implies it), and it is global-only; the grants
            // themselves run as the test's admin session, not as the service account
            connectContext.setThreadLocalInfo();
            try {
                Assertions.assertThrows(Exception.class,
                        () -> grantPriv("GRANT PROXY_PRIV ON internal.test.* TO 'svc'@'%';"));
                grantPriv("GRANT PROXY_PRIV ON *.*.* TO 'svc'@'%';");
            } finally {
                ctx.setThreadLocalInfo();
            }
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.PROXY));

            // nonexistent target refused
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("ghost", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));

            // NARROWING-ONLY LAW (default ceiling = target): role exists but is not bobby's
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_ungranted"), null).run(ctx, null));

            // legal switch
            new SuUserCommand(new UserIdentity("bobby", "%"),
                    Collections.singletonList("r_bobby"), "wg_probe").run(ctx, null);
            Assertions.assertEquals("bobby", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assertions.assertEquals(Collections.singleton("r_bobby"), ctx.getSessionRoleOverride());
            Assertions.assertEquals("svc", ctx.getAuthenticatedIdentity().getQualifiedUser());
            Assertions.assertEquals("wg_probe", ctx.getSessionVariable().getWorkloadGroup());

            // (c) re-SU refused in a switched session
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("svc", "%"),
                            Collections.singletonList("r_svc_only"), null).run(ctx, null));

            // reset reverts to the AUTHENTICATED identity, never widens the target
            ctx.revertSessionNarrowing();
            Assertions.assertEquals("svc", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assertions.assertNull(ctx.getSessionRoleOverride());

            // the ceiling is the TARGET's grants: a role the service holds but the target does not is refused
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_svc_only"), null).run(ctx, null));
            Assertions.assertNull(ctx.getSessionRoleOverride());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testNarrowedSessionKeepsPersonsWorkloadGroupUsage() throws Exception {
        addUser("wg_person", true);
        createRole("space_wg");
        createRole("space_wg_lane");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'space_wg';");
        grantRole("GRANT 'space_wg','space_wg_lane' TO 'wg_person'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'wg_person'@'%';");
        createWorkloadGroup("wg_person_lane");
        createWorkloadGroup("wg_via_role");
        createWorkloadGroup("wg_nobody");
        // the login shape: USAGE granted DIRECTLY to the account (its default role), not via a role
        grantWorkloadGroupUsage("GRANT USAGE_PRIV ON WORKLOAD GROUP 'wg_person_lane' TO 'wg_person'@'%';");
        // a group reachable only through a role the person holds
        grantWorkloadGroupUsage("GRANT USAGE_PRIV ON WORKLOAD GROUP 'wg_via_role' TO ROLE 'space_wg_lane';");

        UserIdentity person = ident("wg_person");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(person);
        ctx.setThreadLocalInfo();
        try {
            // the person's own session
            Assertions.assertTrue(canUseWorkloadGroup(person, "wg_person_lane"));
            Assertions.assertTrue(canUseWorkloadGroup(person, "wg_via_role"));
            Assertions.assertFalse(canUseWorkloadGroup(person, "wg_nobody"));

            ctx.setSessionRoleOverride(Collections.singleton("space_wg"));
            // data narrowing is intact: personal grant gone, requested role live
            Assertions.assertFalse(canSelectDb(person, "perso"));
            Assertions.assertTrue(canSelectDb(person, "test"));
            // (g) the direct USAGE survives narrowing — the person's lane follows the person
            Assertions.assertTrue(canUseWorkloadGroup(person, "wg_person_lane"));
            // ...and so does role-carried USAGE, even though that role was not requested
            Assertions.assertTrue(canUseWorkloadGroup(person, "wg_via_role"));
            // never wider than the person's own session
            Assertions.assertFalse(canUseWorkloadGroup(person, "wg_nobody"));

            // the widening is scoped to the session's OWN identity: another identity's check is untouched
            Assertions.assertFalse(canUseWorkloadGroup(ident("bystander_wg"), "wg_person_lane"));
        } finally {
            ctx.setSessionRoleOverride(null);
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testNarrowedSessionPreservesInformationSchemaButDropsDirectGrants() throws Exception {
        addUser("iuser", true);
        createRole("ispace");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'ispace';");
        grantRole("GRANT 'ispace' TO 'iuser'@'%'");
        // a personal (default-role) grant on another db + the implicit information_schema read
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'iuser'@'%';");

        UserIdentity iuser = ident("iuser");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(iuser);
        ctx.setThreadLocalInfo();
        try {
            // un-narrowed: information_schema readable (default role), personal + role grants live
            Assertions.assertTrue(canSelectInfoSchema(iuser));
            Assertions.assertTrue(canSelectDb(iuser, "perso"));

            ctx.setSessionRoleOverride(Collections.singleton("ispace"));
            // narrowed: the requested role's grant stays, the DIRECT/default-role grant is dropped...
            Assertions.assertTrue(canSelectDb(iuser, "test"));
            Assertions.assertFalse(canSelectDb(iuser, "perso"));
            // ...but the implicit information_schema read IS PRESERVED (the narrowing baseline role),
            // so a narrowed session can still do metadata/client operations.
            Assertions.assertTrue(canSelectInfoSchema(iuser));

            ctx.setSessionRoleOverride(null);
            Assertions.assertTrue(canSelectDb(iuser, "perso"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testParserRoundTrip() {
        Object plan = new NereidsParser().parseSingle(
                "SU 'alice'@'%' WITH ROLES ('space_a', 'r2') WORKLOAD GROUP 'wg_space_5'");
        Assertions.assertTrue(plan instanceof SuUserCommand);
        SuUserCommand cmd = (SuUserCommand) plan;
        Assertions.assertEquals(2, cmd.getRoles().size());
        Assertions.assertEquals("wg_space_5", cmd.getWorkloadGroup());

        Object plain = new NereidsParser().parseSingle("SU 'alice' WITH ROLES ('space_a')");
        Assertions.assertTrue(plain instanceof SuUserCommand);
        Assertions.assertNull(((SuUserCommand) plain).getWorkloadGroup());

        // `su` stays usable as an identifier (nonReserved)
        Object select = new NereidsParser().parseSingle("SELECT su FROM t1");
        Assertions.assertNotNull(select);
    }

    // SU narrowing: a BE->FE metadata RPC (getDbNames/getTableNames/listTableStatus/describeTables)
    // runs on a handler thread with NO ConnectContext. Auth.setRpcSessionNarrowing lets the handler
    // apply the calling session's narrowed role subset there, so information_schema name visibility
    // matches the session instead of resolving the target's FULL roles (the pre-fix leak).
    @Test
    public void testOmittedWorkloadGroupClauseClearsSwitchersLane() throws Exception {
        addUser("lane_svc", true);
        addUser("lane_person", true);
        createRole("lane_role");
        grantRole("GRANT 'lane_role' TO 'lane_person'@'%'");
        grantPriv("GRANT PROXY_PRIV ON *.*.* TO 'lane_svc'@'%';");

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident("lane_svc"));
        // the switcher's own session runs in a lane of its own
        ctx.getSessionVariable().setWorkloadGroup("wg_switcher_lane");
        ctx.setThreadLocalInfo();
        try {
            new SuUserCommand(new UserIdentity("lane_person", "%"),
                    Collections.singletonList("lane_role"), null).run(ctx, null);
            // no WORKLOAD GROUP clause: the switcher's lane is dropped, so the per-query
            // resolution falls through to the target's default_workload_group
            Assertions.assertEquals("", ctx.getSessionVariable().getWorkloadGroup());

            ctx.revertSessionNarrowing();
            ctx.getSessionVariable().setWorkloadGroup("wg_switcher_lane");
            new SuUserCommand(new UserIdentity("lane_person", "%"),
                    Collections.singletonList("lane_role"), "wg_named").run(ctx, null);
            // an explicit clause places the session exactly there
            Assertions.assertEquals("wg_named", ctx.getSessionVariable().getWorkloadGroup());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testRpcSessionNarrowingAppliesOnMetadataRpcThread() throws Exception {
        addUser("rpcuser", true);
        createRole("rpc_space");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'rpc_space';");
        grantRole("GRANT 'rpc_space' TO 'rpcuser'@'%'");
        // a personal (default-role) grant on another db, which the narrowing must DROP
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'rpcuser'@'%';");
        UserIdentity rpcuser = ident("rpcuser");
        try {
            // no narrowing installed -> full roles resolved (the pre-fix behavior/leak):
            // both the personal grant and the role grant are visible.
            Assertions.assertTrue(canSelectDb(rpcuser, "perso"));
            Assertions.assertTrue(canSelectDb(rpcuser, "test"));

            // install exactly what the metadata RPC carries in current_roles
            Auth.setRpcSessionNarrowing(rpcuser, Collections.singleton("rpc_space"));
            Assertions.assertFalse(canSelectDb(rpcuser, "perso")); // personal grant DROPPED
            Assertions.assertTrue(canSelectDb(rpcuser, "test"));   // requested role KEPT
            Assertions.assertTrue(canSelectInfoSchema(rpcuser));    // info_schema baseline preserved
        } finally {
            Auth.clearRpcSessionNarrowing();
        }
        // cleared -> full roles restored (no leak of the narrowing to later requests on this thread)
        Assertions.assertTrue(canSelectDb(rpcuser, "perso"));
    }

    /**
     * PROXY_PRIV is a lesser privilege than ADMIN_PRIV: the built-in accounts are never a target, the
     * built-in administrative roles and the per-user default role are never requested, and a role that
     * confers a global ADMIN_PRIV / NODE_PRIV / GRANT_PRIV is refused unless the switching account
     * already holds that bit itself.
     */
    @Test
    public void testSwitchNeverClimbsTheAdministrativeLadder() throws Exception {
        addUser("ladder_person", true);
        addUser("ladder_admin_holder", true);
        createRole("ladder_plain");
        createRole("ladder_ops_admin");
        createRole("ladder_granter");
        grantRole("GRANT 'ladder_plain','ladder_ops_admin','ladder_granter' TO 'ladder_person'@'%'");
        grantRole("GRANT 'admin' TO 'ladder_admin_holder'@'%'");
        grantPriv("GRANT ADMIN_PRIV ON *.*.* TO ROLE 'ladder_ops_admin';");
        grantPriv("GRANT GRANT_PRIV ON *.*.* TO ROLE 'ladder_granter';");
        UserIdentity person = new UserIdentity("ladder_person", "%");

        ConnectContext ctx = proxySession("ladder_svc");
        try {
            // the built-in accounts are never a target, whatever role is named
            assertSwitchRefused(ctx, new UserIdentity("admin", "%"), "admin");
            assertSwitchRefused(ctx, new UserIdentity("root", "%"), "operator");
            // the built-in administrative roles and the default role are never requested, even when
            // the target holds them
            assertSwitchRefused(ctx, new UserIdentity("ladder_admin_holder", "%"), "admin");
            assertSwitchRefused(ctx, person, "operator");
            assertSwitchRefused(ctx, person, "default_role_rbac_ladder_person@%");
            // a granted role that confers a global administrative bit the switcher does not hold
            assertSwitchRefused(ctx, person, "ladder_ops_admin");
            assertSwitchRefused(ctx, person, "ladder_granter");
            // an identity that does not analyze never reaches the switch
            assertSwitchRefused(ctx, new UserIdentity("bad user", "%"), "ladder_plain");
            // the plain role is what PROXY_PRIV buys
            new SuUserCommand(person, Collections.singletonList("ladder_plain"), null).run(ctx, null);
            Assertions.assertEquals("ladder_person", ctx.getQualifiedUser());
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN));
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.GRANT));
        } finally {
            connectContext.setThreadLocalInfo();
        }

        // an administrator already holds every bit: narrowing a person to an administrative role is
        // no escalation for it
        ConnectContext admin = new ConnectContext();
        admin.setCurrentUserIdentity(ident("root"));
        admin.setThreadLocalInfo();
        try {
            new SuUserCommand(person, Collections.singletonList("ladder_ops_admin"), null).run(admin, null);
            Assertions.assertEquals("ladder_person", admin.getQualifiedUser());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    /**
     * After the switch the current identity IS the target, but the switcher is not the person: the
     * "self" exemptions of SET PASSWORD and SET PROPERTY are closed under the switch, so a credential
     * or account-default change needs the GRANT / ADMIN privilege of the narrowed set.
     */
    @Test
    public void testSelfBranchesAreClosedUnderTheSwitch() throws Exception {
        addUser("self_person", true);
        createRole("self_role");
        grantRole("GRANT 'self_role' TO 'self_person'@'%'");
        UserIdentity person = new UserIdentity("self_person", "%");

        // the person's own session keeps both self branches
        ConnectContext own = new ConnectContext();
        own.setCurrentUserIdentity(ident("self_person"));
        own.setThreadLocalInfo();
        try {
            new SetPassVarOp(null, new PassVar("", true)).validate(own);
            new SetPassVarOp(new UserIdentity("self_person", "%"), new PassVar("", true)).validate(own);
            new SetUserPropertyVarOp("self_person", "default_init_catalog", "internal").validate(own);
        } finally {
            connectContext.setThreadLocalInfo();
        }

        ConnectContext ctx = proxySession("self_svc");
        try {
            new SuUserCommand(person, Collections.singletonList("self_role"), null).run(ctx, null);
            Assertions.assertEquals("self_person", ctx.getQualifiedUser());
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SetPassVarOp(null, new PassVar("", true)).validate(ctx));
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SetPassVarOp(new UserIdentity("self_person", "%"), new PassVar("", true)).validate(ctx));
            Assertions.assertThrows(AnalysisException.class, () ->
                    new SetUserPropertyVarOp("self_person", "default_init_catalog", "internal").validate(ctx));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    /**
     * The narrowed set is a live intersection with the target's grants, not a snapshot taken at the
     * switch: a role revoked from the target afterwards (or dropped and re-created under its name,
     * or every role after DROP USER) drops out of the open narrowed session on the next check,
     * exactly as it would for the person's own session. SHOW GRANTS lists that same set, and never
     * the synthetic baseline role.
     */
    @Test
    public void testCeilingIsReEvaluatedOnEveryCheck() throws Exception {
        addUser("ceil_person", true);
        addUser("ceil_gone", true);
        createRole("ceil_role");
        createRole("ceil_gone_role");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'ceil_role';");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'ceil_gone_role';");
        grantRole("GRANT 'ceil_role' TO 'ceil_person'@'%'");
        grantRole("GRANT 'ceil_gone_role' TO 'ceil_gone'@'%'");
        UserIdentity person = ident("ceil_person");
        UserIdentity gone = ident("ceil_gone");

        ConnectContext ctx = proxySession("ceil_svc");
        try {
            new SuUserCommand(new UserIdentity("ceil_person", "%"),
                    Collections.singletonList("ceil_role"), null).run(ctx, null);
            Assertions.assertTrue(canSelectDb(person, "test"));
            Assertions.assertEquals("ceil_role", shownRoles(person));

            // a REVOKE from the target is honoured by the open narrowed session at once
            runAsAdmin("REVOKE 'ceil_role' FROM 'ceil_person'@'%'");
            Assertions.assertFalse(canSelectDb(person, "test"));
            // the information_schema/mysql baseline stays, and is not listed as a role
            Assertions.assertTrue(canSelectInfoSchema(person));
            Assertions.assertEquals("", shownRoles(person));

            // DROP ROLE + CREATE ROLE under the same name is not a grant to the target
            runAsAdmin("DROP ROLE ceil_role");
            runAsAdmin("CREATE ROLE ceil_role");
            runAsAdmin("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'ceil_role'");
            Assertions.assertFalse(canSelectDb(person, "test"));
            // granting it to the target again is
            runAsAdmin("GRANT 'ceil_role' TO 'ceil_person'@'%'");
            Assertions.assertTrue(canSelectDb(person, "test"));
            Assertions.assertEquals("ceil_role", shownRoles(person));
        } finally {
            connectContext.setThreadLocalInfo();
        }

        ConnectContext ctx2 = proxySession("ceil_svc2");
        try {
            new SuUserCommand(new UserIdentity("ceil_gone", "%"),
                    Collections.singletonList("ceil_gone_role"), null).run(ctx2, null);
            Assertions.assertTrue(canSelectDb(gone, "test"));
            // a dropped target holds nothing: the session keeps only the metadata baseline
            runAsAdmin("DROP USER 'ceil_gone'@'%'");
            Assertions.assertFalse(canSelectDb(gone, "test"));
            Assertions.assertTrue(canSelectInfoSchema(gone));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSyntheticBaselineRoleCannotBeNamed() {
        // the default-role prefix keeps it out of CREATE ROLE, out of row-policy binding (which
        // validates the role) and out of every role listing
        Assertions.assertTrue(Auth.SU_NARROWING_BASELINE_ROLE.startsWith(RoleManager.DEFAULT_ROLE_PREFIX));
        Assertions.assertThrows(Exception.class, () -> runAsAdmin("CREATE ROLE " + Auth.SU_NARROWING_BASELINE_ROLE));
        Assertions.assertFalse(Env.getCurrentEnv().getAuth().doesRoleExist(Auth.SU_NARROWING_BASELINE_ROLE));
    }

    /**
     * The authorization-SPI role bridge (what a ranger-hive or third-party source is told when it asks
     * who holds which roles) resolves through the same choke point as the built-in checks: a narrowed
     * session's override, and the narrowing a BE->FE RPC carries, reach role-bound policies of every
     * source; the per-user default role and the synthetic baseline never do.
     */
    @Test
    public void testAuthorizationSpiRoleBridgeIsNarrowed() throws Exception {
        addUser("spi_person", true);
        createRole("spi_tenant_a");
        createRole("spi_tenant_b");
        grantRole("GRANT 'spi_tenant_a','spi_tenant_b' TO 'spi_person'@'%'");
        UserIdentity person = ident("spi_person");
        EngineAuthorizationContext bridge = new EngineAuthorizationContext(
                Env.getCurrentEnv().getAccessManager(), Env.getCurrentEnv().getAuth());

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(person);
        ctx.setThreadLocalInfo();
        try {
            Assertions.assertEquals(Sets.newHashSet("spi_tenant_a", "spi_tenant_b"),
                    bridge.rolesOf(AccessTranslation.subjectOf(person)));
            ctx.setSessionRoleOverride(Collections.singleton("spi_tenant_a"));
            Assertions.assertEquals(Collections.singleton("spi_tenant_a"),
                    bridge.rolesOf(AccessTranslation.subjectOf(person)));
            // another identity's roles are untouched by this session's narrowing
            Assertions.assertTrue(bridge.rolesOf(AccessTranslation.subjectOf(ident("bystander"))).isEmpty());
        } finally {
            connectContext.setThreadLocalInfo();
        }

        // the narrowing a metadata RPC carries (no ConnectContext on the handler thread)
        Auth.setRpcSessionNarrowing(person, Collections.singleton("spi_tenant_b"));
        try {
            Assertions.assertEquals(Collections.singleton("spi_tenant_b"),
                    bridge.rolesOf(AccessTranslation.subjectOf(person)));
        } finally {
            Auth.clearRpcSessionNarrowing();
        }
        Assertions.assertEquals(Sets.newHashSet("spi_tenant_a", "spi_tenant_b"),
                bridge.rolesOf(AccessTranslation.subjectOf(person)));
    }

    /**
     * The placement widening answers "may this session run here?" and nothing else: a direct
     * GRANT_PRIV on a workload group (which lives in the person's default role, outside the
     * narrowed set) lets the person's own session grant the group to others, but never the narrowed
     * session -- GRANT/REVOKE ... ON WORKLOAD GROUP stays on the narrowed set. Both legs of the
     * answer, the live session and the narrowing a BE->FE RPC carries, agree.
     */
    @Test
    public void testPlacementWideningNeverReachesTheGrantGate() throws Exception {
        addUser("wg_granter", true);
        addUser("wg_mallory", true);
        createRole("wg_tenant");
        grantRole("GRANT 'wg_tenant' TO 'wg_granter'@'%'");
        createWorkloadGroup("wg_x");
        grantWorkloadGroupUsage("GRANT USAGE_PRIV, GRANT_PRIV ON WORKLOAD GROUP 'wg_x' TO 'wg_granter'@'%';");
        UserIdentity granter = ident("wg_granter");
        PrivPredicate grantGate = PrivPredicate.of(PrivBitSet.of(Privilege.USAGE_PRIV, Privilege.GRANT_PRIV),
                Operator.AND);
        Auth auth = Env.getCurrentEnv().getAuth();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(granter);
        ctx.setThreadLocalInfo();
        try {
            // the person's own session may use the group and grant it
            Assertions.assertTrue(canUseWorkloadGroup(granter, "wg_x"));
            Assertions.assertTrue(auth.checkWorkloadGroupPriv(granter, "wg_x", grantGate));

            ctx.setSessionRoleOverride(Collections.singleton("wg_tenant"));
            // placement follows the person ...
            Assertions.assertTrue(canUseWorkloadGroup(granter, "wg_x"));
            // ... but the GRANT gate stays on the narrowed set: the direct GRANT_PRIV is inert
            Assertions.assertFalse(auth.checkWorkloadGroupPriv(granter, "wg_x", grantGate));
            Assertions.assertThrows(AnalysisException.class, () -> grantWorkloadGroupUsage(
                    "GRANT USAGE_PRIV ON WORKLOAD GROUP 'wg_x' TO 'wg_mallory'@'%';"));
            Assertions.assertFalse(canUseWorkloadGroup(ident("wg_mallory"), "wg_x"));
        } finally {
            connectContext.setThreadLocalInfo();
        }

        // the RPC leg (no ConnectContext on the handler thread) answers the same
        Auth.setRpcSessionNarrowing(granter, Collections.singleton("wg_tenant"));
        try {
            Assertions.assertTrue(canUseWorkloadGroup(granter, "wg_x"));
            Assertions.assertFalse(auth.checkWorkloadGroupPriv(granter, "wg_x", grantGate));
        } finally {
            Auth.clearRpcSessionNarrowing();
        }
    }

    /**
     * A persisted job re-plans its read later, on a job thread, under the stored identity alone,
     * where the session's narrowing cannot follow: a narrowed session mints none (EXPORT, CREATE JOB
     * in both forms), and a routine load's KEY references are authorized at creation under the live
     * session. A child context built for a task thread inherits the narrowing with the identity.
     */
    @Test
    public void testNarrowedSessionMintsNoPersistedExecutor() throws Exception {
        createTable("create table test.su_job_t (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num' = '1')");
        addUser("job_person", true);
        createRole("job_role");
        grantPriv("GRANT SELECT_PRIV, LOAD_PRIV ON internal.test.* TO ROLE 'job_role';");
        grantRole("GRANT 'job_role' TO 'job_person'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'job_person'@'%';");
        UserIdentity person = ident("job_person");

        ConnectContext ctx = proxySession("job_svc");
        try {
            new SuUserCommand(new UserIdentity("job_person", "%"),
                    Collections.singletonList("job_role"), null).run(ctx, null);
            ctx.setDatabase("test");

            LogicalPlan export = new NereidsParser().parseSingle(
                    "EXPORT TABLE test.su_job_t TO \"file:///tmp/su_job/\"");
            Assertions.assertTrue(export instanceof ExportCommand);
            AnalysisException refused = Assertions.assertThrows(AnalysisException.class,
                    () -> ((ExportCommand) export).run(ctx, null));
            Assertions.assertTrue(refused.getMessage().contains("session-narrowed"), refused.getMessage());

            for (String sql : new String[] {
                    "CREATE JOB su_job_s ON SCHEDULE EVERY 1 DAY DO INSERT INTO test.su_job_t"
                            + " SELECT * FROM test.su_job_t",
                    "CREATE JOB su_job_f ON STREAMING DO INSERT INTO test.su_job_t"
                            + " SELECT * FROM S3(\"uri\" = \"s3://bucket/x.csv\", \"format\" = \"csv\")"}) {
                LogicalPlan job = new NereidsParser().parseSingle(sql);
                Assertions.assertTrue(job instanceof CreateJobCommand, sql);
                AnalysisException jobRefused = Assertions.assertThrows(AnalysisException.class,
                        () -> ((CreateJobCommand) job).run(ctx, null));
                Assertions.assertTrue(jobRefused.getMessage().contains("session-narrowed"), jobRefused.getMessage());
            }

            // the KEY reference of a routine-load column mapping is authorized at creation, as the
            // narrowed session: SHOW on perso lives only in the excluded direct grant
            Expression mapping = new NereidsParser().parseExpression("aes_decrypt(c1, KEY perso.k)");
            Assertions.assertThrows(AnalysisException.class,
                    () -> CreateRoutineLoadInfo.checkEncryptKeyRefs(mapping, ctx));
            CreateRoutineLoadInfo.checkEncryptKeyRefs(
                    new NereidsParser().parseExpression("aes_decrypt(c1, KEY test.k)"), ctx);

            // a child context of the narrowed session (a task thread) denies what the session denies
            ConnectContext child = new ConnectContext();
            child.inheritIdentityFrom(ctx);
            child.setThreadLocalInfo();
            try {
                Assertions.assertEquals("job_svc", child.getAuthenticatedIdentity().getQualifiedUser());
                Assertions.assertTrue(canSelectDb(person, "test"));
                Assertions.assertFalse(canSelectDb(person, "perso"));
            } finally {
                ctx.setThreadLocalInfo();
            }
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }
}
