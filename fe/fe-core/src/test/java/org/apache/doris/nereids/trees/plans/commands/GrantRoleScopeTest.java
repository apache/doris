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

/**
 * What an account holding grant_priv alone may hand out: a role only when it holds every privilege the
 * role carries (as for a direct GRANT), and never another account's credentials (admin_priv).
 */
public class GrantRoleScopeTest extends TestWithFeService {

    private static final String ROLE = "grant_scope_role";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("grant_scope_db");
        run("CREATE ROLE " + ROLE);
        run("GRANT SELECT_PRIV ON internal.grant_scope_db.* TO ROLE '" + ROLE + "'");
        run("CREATE USER 'gs_grantor'@'%' IDENTIFIED BY 'p1'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'gs_grantor'@'%'");
        run("CREATE USER 'gs_target'@'%' IDENTIFIED BY 'p2'");
        run("CREATE USER 'gs_admin'@'%' IDENTIFIED BY 'p3'");
        run("GRANT ADMIN_PRIV ON *.*.* TO 'gs_admin'@'%'");
        // a role carrying node_priv, and one carrying a workload-group privilege
        run("CREATE ROLE gs_node_role");
        run("GRANT NODE_PRIV ON *.*.* TO ROLE 'gs_node_role'");
        run("CREATE WORKLOAD GROUP IF NOT EXISTS gs_wg"
                + " PROPERTIES (\"min_memory_percent\"=\"10\", \"max_memory_percent\"=\"30%\")");
        run("CREATE ROLE gs_wg_role");
        run("GRANT USAGE_PRIV ON WORKLOAD GROUP 'gs_wg' TO ROLE 'gs_wg_role'");
        // and one carrying a storage-vault privilege (a grant on a vault needs no vault to exist)
        run("CREATE ROLE gs_vault_role");
        run("GRANT USAGE_PRIV ON STORAGE VAULT 'gs_vault' TO ROLE 'gs_vault_role'");
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

    private String refusal(ConnectContext ctx, String sql) {
        Exception e = Assertions.assertThrows(Exception.class, () -> runAs(ctx, sql), sql);
        return e.getMessage();
    }

    @Test
    public void testARoleGrantNeedsThePrivilegesTheRoleCarries() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        // grant_priv alone: not to itself, not to another account, not as a new account's default role
        Assertions.assertTrue(refusal(grantor, "GRANT '" + ROLE + "' TO 'gs_grantor'@'%'").contains("Select_priv"));
        Assertions.assertTrue(refusal(grantor, "GRANT '" + ROLE + "' TO 'gs_target'@'%'").contains("Select_priv"));
        Assertions.assertTrue(refusal(grantor,
                "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'").contains("Select_priv"));
        // nor the built-in admin role, which carries admin_priv
        Assertions.assertTrue(refusal(grantor, "GRANT 'admin' TO 'gs_target'@'%'").contains("Admin_priv"));
        Assertions.assertFalse(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_target"), false).contains(ROLE));

        // once the grantor holds what the role carries, the grant is its to make
        run("GRANT SELECT_PRIV ON internal.grant_scope_db.* TO 'gs_grantor'@'%'");
        runAs(grantor, "GRANT '" + ROLE + "' TO 'gs_target'@'%'");
        Assertions.assertTrue(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_target"), false).contains(ROLE));
        runAs(grantor, "CREATE USER 'gs_new'@'%' IDENTIFIED BY 'p' DEFAULT ROLE '" + ROLE + "'");
        Assertions.assertTrue(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_new"), false).contains(ROLE));

        // admin_priv grants any role, as it grants any privilege ...
        runAs(as("gs_admin"), "GRANT '" + ROLE + "' TO 'gs_admin'@'%'");
        Assertions.assertTrue(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_admin"), false).contains(ROLE));
        // ... except node_priv, which a direct grant refuses to an account without it, admin or not
        Assertions.assertTrue(refusal(as("gs_admin"), "GRANT 'gs_node_role' TO 'gs_target'@'%'").contains("NODE_PRIV"));
        Assertions.assertFalse(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_target"), false)
                .contains("gs_node_role"));
    }

    @Test
    public void testARoleCarryingAWorkloadGroupPrivilegeIsCheckedOnThatGroup() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        Assertions.assertTrue(refusal(grantor, "GRANT 'gs_wg_role' TO 'gs_target'@'%'").contains("Usage_priv"));
        run("GRANT USAGE_PRIV ON WORKLOAD GROUP 'gs_wg' TO 'gs_grantor'@'%'");
        runAs(grantor, "GRANT 'gs_wg_role' TO 'gs_target'@'%'");
        Assertions.assertTrue(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_target"), false)
                .contains("gs_wg_role"));
    }

    @Test
    public void testARoleCarryingAStorageVaultPrivilegeIsCheckedOnThatVault() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        // the role path and the direct path both ask the storage-vault table, not the general resource table
        Assertions.assertTrue(refusal(grantor, "GRANT 'gs_vault_role' TO 'gs_target'@'%'").contains("Usage_priv"));
        Assertions.assertTrue(refusal(grantor, "GRANT USAGE_PRIV ON STORAGE VAULT 'gs_vault' TO 'gs_target'@'%'")
                .contains("Usage_priv"));
        run("GRANT USAGE_PRIV ON STORAGE VAULT 'gs_vault' TO 'gs_grantor'@'%'");
        runAs(grantor, "GRANT 'gs_vault_role' TO 'gs_target'@'%'");
        runAs(grantor, "GRANT USAGE_PRIV ON STORAGE VAULT 'gs_vault' TO 'gs_target'@'%'");
        Assertions.assertTrue(Env.getCurrentEnv().getAuth().getRolesByUser(ident("gs_target"), false)
                .contains("gs_vault_role"));
    }

    @Test
    public void testAnotherAccountsCredentialsNeedAdminPriv() throws Exception {
        ConnectContext grantor = as("gs_grantor");
        Assertions.assertTrue(refusal(grantor, "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x'").contains("ADMIN"));
        Assertions.assertTrue(refusal(grantor, "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x')").contains("ADMIN"));
        // an authentication binding and a certificate requirement are credentials too
        Assertions.assertTrue(refusal(grantor,
                "ALTER USER 'gs_target'@'%' IDENTIFIED WITH AUTHENTICATION INTEGRATION gs_integration")
                .contains("ADMIN"));
        Assertions.assertTrue(refusal(grantor, "ALTER USER 'gs_target'@'%' REQUIRE SAN 'DNS:gs-san'")
                .contains("ADMIN"));
        // one's own credentials, and the other administrative operations, stay under grant_priv
        runAs(grantor, "SET PASSWORD = PASSWORD('q1')");
        runAs(grantor, "ALTER USER 'gs_grantor'@'%' IDENTIFIED BY 'q2'");
        runAs(grantor, "ALTER USER 'gs_grantor'@'%' REQUIRE NONE");
        runAs(grantor, "ALTER USER 'gs_target'@'%' ACCOUNT_LOCK");
        runAs(grantor, "ALTER USER 'gs_target'@'%' ACCOUNT_UNLOCK");
        // admin_priv may change another account's credentials
        ConnectContext admin = as("gs_admin");
        runAs(admin, "ALTER USER 'gs_target'@'%' IDENTIFIED BY 'x1'");
        runAs(admin, "SET PASSWORD FOR 'gs_target'@'%' = PASSWORD('x2')");
        runAs(admin, "ALTER USER 'gs_target'@'%' REQUIRE SAN 'DNS:gs-san'");
        runAs(admin, "ALTER USER 'gs_target'@'%' REQUIRE NONE");
    }
}
