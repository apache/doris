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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.SuUserCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
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
        Assert.assertTrue(plan instanceof CreateWorkloadGroupCommand);
        ((CreateWorkloadGroupCommand) plan).run(connectContext, null);
    }

    private void grantWorkloadGroupUsage(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assert.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        ((GrantResourcePrivilegeCommand) plan).run(connectContext, null);
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
            Assert.assertTrue(canSelectDb(alice, "perso"));
            Assert.assertTrue(canSelectDb(alice, "test"));

            ctx.setSessionRoleOverride(Collections.singleton("space_a"));
            // (a) the override REPLACES everything: the personal grant is gone...
            Assert.assertFalse(canSelectDb(alice, "perso"));
            // ...while the requested role's grants remain
            Assert.assertTrue(canSelectDb(alice, "test"));

            // (f) checks against OTHER identities are untouched by this session's narrowing
            Set<String> bystanderRoles = Env.getCurrentEnv().getAuth()
                    .getRoleNamesByUserWithLdap(ident("bystander"), false);
            Assert.assertTrue(bystanderRoles.contains("bystander_r"));

            // revert restores the full union
            ctx.setSessionRoleOverride(null);
            Assert.assertTrue(canSelectDb(alice, "perso"));
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
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));
            Assert.assertNull(ctx.getAuthenticatedIdentity());
            Assert.assertFalse(canSelectDb(ident("svc"), "test"));

            // PROXY_PRIV is the native gate (ADMIN_PRIV implies it), and it is global-only
            Assert.assertThrows(Exception.class,
                    () -> grantPriv("GRANT PROXY_PRIV ON internal.test.* TO 'svc'@'%';"));
            grantPriv("GRANT PROXY_PRIV ON *.*.* TO 'svc'@'%';");
            Assert.assertTrue(Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.PROXY));

            // nonexistent target refused
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("ghost", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));

            // NARROWING-ONLY LAW (default ceiling = target): role exists but is not bobby's
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_ungranted"), null).run(ctx, null));

            // legal switch
            new SuUserCommand(new UserIdentity("bobby", "%"),
                    Collections.singletonList("r_bobby"), "wg_probe").run(ctx, null);
            Assert.assertEquals("bobby", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assert.assertEquals(Collections.singleton("r_bobby"), ctx.getSessionRoleOverride());
            Assert.assertEquals("svc", ctx.getAuthenticatedIdentity().getQualifiedUser());
            Assert.assertEquals("wg_probe", ctx.getSessionVariable().getWorkloadGroup());

            // (c) re-SU refused in a switched session
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("svc", "%"),
                            Collections.singletonList("r_svc_only"), null).run(ctx, null));

            // reset reverts to the AUTHENTICATED identity, never widens the target
            ctx.revertSessionNarrowing();
            Assert.assertEquals("svc", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assert.assertNull(ctx.getSessionRoleOverride());

            // the ceiling is the TARGET's grants: a role the service holds but the target does not is refused
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_svc_only"), null).run(ctx, null));
            Assert.assertNull(ctx.getSessionRoleOverride());
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
            Assert.assertTrue(canUseWorkloadGroup(person, "wg_person_lane"));
            Assert.assertTrue(canUseWorkloadGroup(person, "wg_via_role"));
            Assert.assertFalse(canUseWorkloadGroup(person, "wg_nobody"));

            ctx.setSessionRoleOverride(Collections.singleton("space_wg"));
            // data narrowing is intact: personal grant gone, requested role live
            Assert.assertFalse(canSelectDb(person, "perso"));
            Assert.assertTrue(canSelectDb(person, "test"));
            // (g) the direct USAGE survives narrowing — the person's lane follows the person
            Assert.assertTrue(canUseWorkloadGroup(person, "wg_person_lane"));
            // ...and so does role-carried USAGE, even though that role was not requested
            Assert.assertTrue(canUseWorkloadGroup(person, "wg_via_role"));
            // never wider than the person's own session
            Assert.assertFalse(canUseWorkloadGroup(person, "wg_nobody"));

            // the widening is scoped to the session's OWN identity: another identity's check is untouched
            Assert.assertFalse(canUseWorkloadGroup(ident("bystander_wg"), "wg_person_lane"));
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
            Assert.assertTrue(canSelectInfoSchema(iuser));
            Assert.assertTrue(canSelectDb(iuser, "perso"));

            ctx.setSessionRoleOverride(Collections.singleton("ispace"));
            // narrowed: the requested role's grant stays, the DIRECT/default-role grant is dropped...
            Assert.assertTrue(canSelectDb(iuser, "test"));
            Assert.assertFalse(canSelectDb(iuser, "perso"));
            // ...but the implicit information_schema read IS PRESERVED (the narrowing baseline role),
            // so a narrowed session can still do metadata/client operations.
            Assert.assertTrue(canSelectInfoSchema(iuser));

            ctx.setSessionRoleOverride(null);
            Assert.assertTrue(canSelectDb(iuser, "perso"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testParserRoundTrip() {
        Object plan = new NereidsParser().parseSingle(
                "SU 'alice'@'%' WITH ROLES ('space_a', 'r2') WORKLOAD GROUP 'wg_space_5'");
        Assert.assertTrue(plan instanceof SuUserCommand);
        SuUserCommand cmd = (SuUserCommand) plan;
        Assert.assertEquals(2, cmd.getRoles().size());
        Assert.assertEquals("wg_space_5", cmd.getWorkloadGroup());

        Object plain = new NereidsParser().parseSingle("SU 'alice' WITH ROLES ('space_a')");
        Assert.assertTrue(plain instanceof SuUserCommand);
        Assert.assertNull(((SuUserCommand) plain).getWorkloadGroup());

        // `su` stays usable as an identifier (nonReserved)
        Object select = new NereidsParser().parseSingle("SELECT su FROM t1");
        Assert.assertNotNull(select);
    }

    // SU narrowing: a BE->FE metadata RPC (getDbNames/getTableNames/listTableStatus/describeTables)
    // runs on a handler thread with NO ConnectContext. Auth.setRpcSessionNarrowing lets the handler
    // apply the calling session's narrowed role subset there, so information_schema name visibility
    // matches the session instead of resolving the target's FULL roles (the pre-fix leak).
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
            Assert.assertTrue(canSelectDb(rpcuser, "perso"));
            Assert.assertTrue(canSelectDb(rpcuser, "test"));

            // install exactly what the metadata RPC carries in current_roles
            Auth.setRpcSessionNarrowing(rpcuser, Collections.singleton("rpc_space"));
            Assert.assertFalse(canSelectDb(rpcuser, "perso")); // personal grant DROPPED
            Assert.assertTrue(canSelectDb(rpcuser, "test"));   // requested role KEPT
            Assert.assertTrue(canSelectInfoSchema(rpcuser));    // info_schema baseline preserved
        } finally {
            Auth.clearRpcSessionNarrowing();
        }
        // cleared -> full roles restored (no leak of the narrowing to later requests on this thread)
        Assert.assertTrue(canSelectDb(rpcuser, "perso"));
    }
}
