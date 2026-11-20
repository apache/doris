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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * What an account holding grant_priv alone may hand out: a role only when it holds that role itself
 * (a membership, not the privileges the role carries), and never another account's credentials
 * (admin_priv).
 */
public class GrantRoleScopeTest extends TestWithFeService {

    private static final String ROLE = "grant_scope_role";
    private static final String EMPTY_ROLE = "grant_scope_empty_role";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("grant_scope_db");
        run("CREATE ROLE " + ROLE);
        run("GRANT SELECT_PRIV ON internal.grant_scope_db.* TO ROLE '" + ROLE + "'");
        // a role that carries nothing
        run("CREATE ROLE " + EMPTY_ROLE);
        run("CREATE USER 'gs_grantor'@'%' IDENTIFIED BY 'p1'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'gs_grantor'@'%'");
        run("CREATE USER 'gs_target'@'%' IDENTIFIED BY 'p2'");
        run("CREATE USER 'gs_admin'@'%' IDENTIFIED BY 'p3'");
        run("GRANT ADMIN_PRIV ON *.*.* TO 'gs_admin'@'%'");
        // an account that holds the role without grant_priv
        run("CREATE USER 'gs_member'@'%' IDENTIFIED BY 'p4'");
        run("GRANT '" + ROLE + "' TO 'gs_member'@'%'");
        // the accounts of the revoke and admin cases
        run("CREATE USER 'gs_revoker'@'%' IDENTIFIED BY 'p5'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'gs_revoker'@'%'");
        run("CREATE USER 'gs_revokee'@'%' IDENTIFIED BY 'p6'");
        run("GRANT '" + ROLE + "' TO 'gs_revokee'@'%'");
        run("CREATE USER 'gs_admin_target'@'%' IDENTIFIED BY 'p7'");
        // and of the case where the role comes from the session's authentication, not from a grant
        run("CREATE USER 'gs_session_grantor'@'%' IDENTIFIED BY 'p8'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'gs_session_grantor'@'%'");
        run("CREATE USER 'gs_session_target'@'%' IDENTIFIED BY 'p9'");
    }

    private void run(String sql) throws Exception {
        runAs(connectContext, sql);
    }

    private void runAs(ConnectContext ctx, String sql) throws Exception {
        ctx.setThreadLocalInfo();
        try {
            ((Command) new NereidsParser().parseSingle(sql)).run(ctx, null);
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    private ConnectContext as(String user) throws Exception {
        return createCtx(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"), "127.0.0.1");
    }

    private static UserIdentity ident(String user) {
        return UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
    }

    private static boolean holds(String user, String role) {
        return Env.getCurrentEnv().getAuth().getRolesByUser(ident(user), false).contains(role);
    }

    private String refusal(ConnectContext ctx, String sql) {
        Exception e = Assertions.assertThrows(Exception.class, () -> runAs(ctx, sql), sql);
        return e.getMessage();
    }

    private void assertNotHeld(ConnectContext ctx, String sql, String role) {
        String message = refusal(ctx, sql);
        Assertions.assertTrue(message.contains("membership of role '" + role + "'"), message);
    }

    @Test
    public void testARoleIsGrantedOnlyByAnAccountThatHoldsIt() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        // grant_priv alone: not to itself, not to another account, not as a new account's default role
        assertNotHeld(grantor, "GRANT '" + ROLE + "' TO 'gs_grantor'@'%'", ROLE);
        assertNotHeld(grantor, "GRANT '" + ROLE + "' TO 'gs_target'@'%'", ROLE);
        assertNotHeld(grantor, "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'", ROLE);
        // nor the built-in admin role (SUPERUSER is its old name), nor a role that carries nothing
        assertNotHeld(grantor, "GRANT 'admin' TO 'gs_target'@'%'", "admin");
        assertNotHeld(grantor, "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE 'SUPERUSER'", "admin");
        assertNotHeld(grantor, "GRANT '" + EMPTY_ROLE + "' TO 'gs_target'@'%'", EMPTY_ROLE);

        // holding every privilege the role carries is not holding the role
        run("GRANT SELECT_PRIV ON internal.grant_scope_db.* TO 'gs_grantor'@'%'");
        assertNotHeld(grantor, "GRANT '" + ROLE + "' TO 'gs_target'@'%'", ROLE);
        assertNotHeld(grantor, "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'", ROLE);
        Assertions.assertFalse(holds("gs_grantor", ROLE));
        Assertions.assertFalse(holds("gs_target", ROLE));
        Assertions.assertFalse(holds("gs_target", EMPTY_ROLE));
        Assertions.assertFalse(holds("gs_target", "admin"));

        // once the grantor holds the role, the grant is its to make
        run("GRANT '" + ROLE + "' TO 'gs_grantor'@'%'");
        runAs(grantor, "GRANT '" + ROLE + "' TO 'gs_target'@'%'");
        Assertions.assertTrue(holds("gs_target", ROLE));
        runAs(grantor, "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'");
        Assertions.assertTrue(holds("gs_new", ROLE));
        // every role of one statement is checked: one it does not hold refuses the whole grant
        run("REVOKE '" + ROLE + "' FROM 'gs_target'@'%'");
        assertNotHeld(grantor, "GRANT '" + ROLE + "', '" + EMPTY_ROLE + "' TO 'gs_target'@'%'", EMPTY_ROLE);
        Assertions.assertFalse(holds("gs_target", ROLE));
        Assertions.assertFalse(holds("gs_target", EMPTY_ROLE));
    }

    @Test
    public void testHoldingARoleWithoutGrantPrivGrantsNothing() throws Exception {
        String message = refusal(as("gs_member"), "GRANT '" + ROLE + "' TO 'gs_admin_target'@'%'");
        Assertions.assertTrue(message.contains("GRANT/REVOKE"), message);
        Assertions.assertFalse(holds("gs_admin_target", ROLE));
    }

    @Test
    public void testARoleTheSessionCarriesFromOutsideIsNotHeld() throws Exception {
        // to hold a role is to have it granted in Doris: a role the session's authentication mapped in
        // is governed outside, and a grant made on its strength would outlive it as a stored membership
        ConnectContext session = as("gs_session_grantor");
        session.setAuthenticatedRoles(Collections.singleton(ROLE));
        assertNotHeld(session, "GRANT '" + ROLE + "' TO 'gs_session_target'@'%'", ROLE);
        assertNotHeld(session, "GRANT '" + ROLE + "' TO 'gs_session_grantor'@'%'", ROLE);
        Assertions.assertFalse(holds("gs_session_target", ROLE));
        Assertions.assertFalse(holds("gs_session_grantor", ROLE));
    }

    @Test
    public void testAdminPrivGrantsAnyRole() throws Exception {
        ConnectContext admin = as("gs_admin");
        Assertions.assertFalse(holds("gs_admin", EMPTY_ROLE));
        runAs(admin, "GRANT '" + EMPTY_ROLE + "' TO 'gs_admin_target'@'%'");
        Assertions.assertTrue(holds("gs_admin_target", EMPTY_ROLE));
        runAs(admin, "GRANT '" + EMPTY_ROLE + "' TO 'gs_admin'@'%'");
        Assertions.assertTrue(holds("gs_admin", EMPTY_ROLE));
        runAs(admin, "CREATE USER 'gs_admin_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'");
        Assertions.assertTrue(holds("gs_admin_new", ROLE));
    }

    @Test
    public void testARevokeNeedsOnlyGrantPriv() throws Exception {
        // a revoke takes nothing from the one who issues it: grant_priv is enough, the role held or not
        Assertions.assertFalse(holds("gs_revoker", ROLE));
        Assertions.assertTrue(holds("gs_revokee", ROLE));
        runAs(as("gs_revoker"), "REVOKE '" + ROLE + "' FROM 'gs_revokee'@'%'");
        Assertions.assertFalse(holds("gs_revokee", ROLE));
    }

    @Test
    public void testAnotherAccountsCredentialsNeedAdminPriv() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        Assertions.assertTrue(refusal(grantor, "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x'").contains("ADMIN"));
        Assertions.assertTrue(refusal(grantor, "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x')").contains("ADMIN"));
        // a password change that keeps the current password as the secondary one is a password change
        Assertions.assertTrue(refusal(grantor,
                "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD").contains("ADMIN"));
        Assertions.assertTrue(refusal(grantor,
                "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x') RETAIN CURRENT PASSWORD").contains("ADMIN"));
        // an authentication binding and a certificate requirement are credentials too
        Assertions.assertTrue(refusal(grantor,
                "ALTER USER 'gs_target'@'%' IDENTIFIED WITH AUTHENTICATION INTEGRATION gs_integration")
                .contains("ADMIN"));
        Assertions.assertTrue(refusal(grantor, "ALTER USER 'gs_target'@'%' REQUIRE SAN 'DNS:gs-san'")
                .contains("ADMIN"));
        // one's own credentials, and the other administrative operations, stay under grant_priv;
        // DISCARD OLD PASSWORD only takes a credential away, as ACCOUNT_LOCK does
        runAs(grantor, "SET PASSWORD = PASSWORD('q1')");
        runAs(grantor, "ALTER USER 'gs_grantor'@'%' IDENTIFIED BY 'q2'");
        runAs(grantor, "SET PASSWORD = PASSWORD('q3') RETAIN CURRENT PASSWORD");
        runAs(grantor, "ALTER USER 'gs_grantor'@'%' IDENTIFIED BY 'q4' RETAIN CURRENT PASSWORD");
        runAs(grantor, "ALTER USER 'gs_grantor'@'%' REQUIRE NONE");
        runAs(grantor, "ALTER USER 'gs_target'@'%' ACCOUNT_LOCK");
        runAs(grantor, "ALTER USER 'gs_target'@'%' ACCOUNT_UNLOCK");
        runAs(grantor, "ALTER USER 'gs_target'@'%' DISCARD OLD PASSWORD");
        // admin_priv may change another account's credentials
        ConnectContext admin = as("gs_admin");
        runAs(admin, "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x1'");
        runAs(admin, "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x2')");
        runAs(admin, "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x3' RETAIN CURRENT PASSWORD");
        runAs(admin, "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x4') RETAIN CURRENT PASSWORD");
        runAs(admin, "ALTER USER 'gs_target'@'%' REQUIRE SAN 'DNS:gs-san'");
        runAs(admin, "ALTER USER 'gs_target'@'%' REQUIRE NONE");
    }
}
