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
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.RevokeTablePrivilegeCommand;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

/**
 * {@link AccessControllerManager#findColumnWithPriv}: which of the columns a table has now does the user hold
 * a privilege on, by a grant on the column itself. It is what lets a statement that reads no column of the
 * table run once the planner's own check has refused it.
 */
public class AnyColumnPrivilegeTest extends TestWithFeService {
    private static final String CTL = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String DB = "any_col_priv_db";
    private static final Set<String> COLUMNS = ImmutableSet.of("id", "name", "secret");

    private static final String COLUMN_USER = "any_col_priv_column_user";
    private static final String ROLE_USER = "any_col_priv_role_user";
    private static final String ROLE = "any_col_priv_role";
    private static final String TABLE_USER = "any_col_priv_table_user";
    private static final String REVOKED_USER = "any_col_priv_revoked_user";
    private static final String NO_PRIV_USER = "any_col_priv_no_priv_user";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        String columns = " (id int, name varchar(32), secret bigint) ";
        String tail = "duplicate key(id) distributed by hash(id) buckets 1 properties('replication_num' = '1')";
        createTable("create table " + DB + ".t" + columns + tail);
        createTable("create table " + DB + ".other_t" + columns + tail);

        addUser(COLUMN_USER, true);
        addUser(ROLE_USER, true);
        addUser(TABLE_USER, true);
        addUser(REVOKED_USER, true);
        addUser(NO_PRIV_USER, true);
        createRole(ROLE);
        grantPriv("grant select_priv(name) on " + DB + ".t to " + COLUMN_USER);
        grantPriv("grant select_priv(name) on " + DB + ".t to role '" + ROLE + "'");
        grantRole("grant '" + ROLE + "' to " + ROLE_USER);
        grantPriv("grant select_priv on " + DB + ".t to " + TABLE_USER);
        grantPriv("grant select_priv(name) on " + DB + ".t to " + REVOKED_USER);
        String revoke = "revoke select_priv(name) on " + DB + ".t from " + REVOKED_USER;
        ((RevokeTablePrivilegeCommand) new NereidsParser().parseSingle(revoke))
                .run(connectContext, new StmtExecutor(connectContext, revoke));
    }

    @Test
    public void columnGrantOnTheTableAnswers() throws Exception {
        Assertions.assertEquals(Optional.of("name"), heldColumn(COLUMN_USER, "t", COLUMNS, PrivPredicate.SELECT));
        Assertions.assertEquals(Optional.of("name"), heldColumn(ROLE_USER, "t", COLUMNS, PrivPredicate.SELECT));
    }

    @Test
    public void onlyAColumnTheTableHasNowCounts() throws Exception {
        // a grant left behind on a column that was dropped or renamed is on a name the table no longer has
        Assertions.assertEquals(Optional.empty(),
                heldColumn(COLUMN_USER, "t", ImmutableSet.of("id", "secret"), PrivPredicate.SELECT));
        Assertions.assertEquals(Optional.empty(),
                heldColumn(COLUMN_USER, "t", ImmutableSet.of(), PrivPredicate.SELECT));
    }

    @Test
    public void onlyTheAskedPrivilegeOnTheAskedTableCounts() throws Exception {
        Assertions.assertEquals(Optional.empty(), heldColumn(COLUMN_USER, "other_t", COLUMNS, PrivPredicate.SELECT));
        Assertions.assertEquals(Optional.empty(), heldColumn(COLUMN_USER, "t", COLUMNS, PrivPredicate.LOAD));
        // a privilege that has no column level form
        Assertions.assertEquals(Optional.empty(), heldColumn(COLUMN_USER, "t", COLUMNS, PrivPredicate.ALTER));
        Assertions.assertEquals(Optional.empty(), heldColumn(REVOKED_USER, "t", COLUMNS, PrivPredicate.SELECT));
        Assertions.assertEquals(Optional.empty(), heldColumn(NO_PRIV_USER, "t", COLUMNS, PrivPredicate.SELECT));
    }

    @Test
    public void tablePrivilegeIsTheOtherQuestion() throws Exception {
        // SELECT on the table is what the table level check answers; this one is only ever asked after a
        // refusal.
        useUser(TABLE_USER);
        try {
            AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
            Assertions.assertTrue(accessManager.checkTblPriv(connectContext, CTL, DB, "t", PrivPredicate.SELECT));
            Assertions.assertEquals(Optional.empty(),
                    accessManager.findColumnWithPriv(connectContext, CTL, DB, "t", COLUMNS, PrivPredicate.SELECT));
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }

    @Test
    public void onlyTheBuiltInModelIsAsked() throws Exception {
        // The same account and the same grant, on a catalog governed by another authorization source: the
        // built-in grants are not that catalog's, and the source is not asked a question it has no form for.
        AuthorizationPlugin otherSource = Mockito.mock(AuthorizationPlugin.class);
        AccessControllerManager accessManager = Mockito.spy(Env.getCurrentEnv().getAccessManager());
        Mockito.doReturn(otherSource).when(accessManager).getAccessControllerOrDefault(CTL);
        useUser(COLUMN_USER);
        try {
            Assertions.assertEquals(Optional.empty(),
                    accessManager.findColumnWithPriv(connectContext, CTL, DB, "t", COLUMNS, PrivPredicate.SELECT));
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
        Mockito.verifyNoInteractions(otherSource);
    }

    private Optional<String> heldColumn(String user, String table, Set<String> columns, PrivPredicate wanted)
            throws Exception {
        useUser(user);
        try {
            return Env.getCurrentEnv().getAccessManager()
                    .findColumnWithPriv(connectContext, CTL, DB, table, columns, wanted);
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }
}
