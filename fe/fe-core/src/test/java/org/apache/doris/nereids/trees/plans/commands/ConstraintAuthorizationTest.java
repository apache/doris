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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

class ConstraintAuthorizationTest extends TestWithFeService {
    private static final String DB = "constraint_auth_test";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        connectContext.setDatabase(DB);
        for (String table : new String[] {"unique_table", "fk_parent", "fk_child", "cascade_parent", "cascade_child",
                "race_parent", "race_child", "race_new_child"}) {
            createTable("CREATE TABLE " + table + " (id INT, parent_id INT) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        }
    }

    @Test
    void testSelectCannotAddOrDropConstraint() throws Exception {
        ConnectContext user = createLimitedUser("constraint_select_user");
        String addUnique = "ALTER TABLE unique_table ADD CONSTRAINT uk UNIQUE (id)";
        assertDenied(user, addUnique);
        assertDenied(user, "ALTER TABLE unique_table ADD CONSTRAINT pk PRIMARY KEY (id)");
        Assertions.assertTrue(table("unique_table").getConstraintsMap().isEmpty());

        runAs(connectContext, addUnique);
        assertDenied(user, "ALTER TABLE unique_table DROP CONSTRAINT uk");
        assertDenied(user, "ALTER TABLE unique_table DROP CONSTRAINT missing_constraint");
        Assertions.assertTrue(table("unique_table").getConstraintsMap().containsKey("uk"));

        grantPriv("GRANT ALTER_PRIV ON " + DB + ".unique_table TO 'constraint_select_user'@'%'");
        runAs(user, "ALTER TABLE unique_table DROP CONSTRAINT uk");
        runAs(user, addUnique);
        Assertions.assertTrue(table("unique_table").getConstraintsMap().containsKey("uk"));
    }

    @Test
    void testForeignKeyRequiresAlterOnReferencedTable() throws Exception {
        runAs(connectContext, "ALTER TABLE fk_parent ADD CONSTRAINT pk PRIMARY KEY (id)");
        ConnectContext user = createLimitedUser("constraint_fk_user");
        grantPriv("GRANT ALTER_PRIV ON " + DB + ".fk_child TO 'constraint_fk_user'@'%'");
        String addForeignKey = "ALTER TABLE fk_child ADD CONSTRAINT fk FOREIGN KEY (parent_id) "
                + "REFERENCES fk_parent(id)";
        assertDenied(user, addForeignKey);
        Assertions.assertTrue(table("fk_child").getConstraintsMap().isEmpty());

        grantPriv("GRANT ALTER_PRIV ON " + DB + ".fk_parent TO 'constraint_fk_user'@'%'");
        runAs(user, addForeignKey);
        Assertions.assertTrue(table("fk_child").getConstraintsMap().containsKey("fk"));
    }

    @Test
    void testPrimaryKeyCascadeRequiresAlterOnReferencingTable() throws Exception {
        runAs(connectContext, "ALTER TABLE cascade_parent ADD CONSTRAINT pk PRIMARY KEY (id)");
        runAs(connectContext, "ALTER TABLE cascade_child ADD CONSTRAINT fk FOREIGN KEY (parent_id) "
                + "REFERENCES cascade_parent(id)");
        ConnectContext user = createLimitedUser("constraint_cascade_user");
        grantPriv("GRANT ALTER_PRIV ON " + DB + ".cascade_parent TO 'constraint_cascade_user'@'%'");

        String dropPrimaryKey = "ALTER TABLE cascade_parent DROP CONSTRAINT pk";
        assertDenied(user, dropPrimaryKey);
        Assertions.assertTrue(table("cascade_parent").getConstraintsMap().containsKey("pk"));
        Assertions.assertTrue(table("cascade_child").getConstraintsMap().containsKey("fk"));

        grantPriv("GRANT ALTER_PRIV ON " + DB + ".cascade_child TO 'constraint_cascade_user'@'%'");
        runAs(user, dropPrimaryKey);
        Assertions.assertTrue(table("cascade_parent").getConstraintsMap().isEmpty());
        Assertions.assertTrue(table("cascade_child").getConstraintsMap().isEmpty());
    }

    @Test
    void testConcurrentForeignKeyCannotBypassCascadeAuthorization() throws Exception {
        runAs(connectContext, "ALTER TABLE race_parent ADD CONSTRAINT pk PRIMARY KEY (id)");
        runAs(connectContext, "ALTER TABLE race_child ADD CONSTRAINT fk FOREIGN KEY (parent_id) "
                + "REFERENCES race_parent(id)");
        ConnectContext user = createLimitedUser("constraint_race_user");
        grantPriv("GRANT ALTER_PRIV ON " + DB + ".race_parent TO 'constraint_race_user'@'%'");
        grantPriv("GRANT ALTER_PRIV ON " + DB + ".race_child TO 'constraint_race_user'@'%'");

        AtomicBoolean added = new AtomicBoolean();
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkTblPriv(Invocation invocation, ConnectContext ctx, String ctl,
                    String db, String tableName, PrivPredicate wanted) throws Exception {
                if (ctx == user && tableName.equals("race_child") && wanted == PrivPredicate.ALTER
                        && added.compareAndSet(false, true)) {
                    runAs(connectContext, "ALTER TABLE race_new_child ADD CONSTRAINT fk FOREIGN KEY (parent_id) "
                            + "REFERENCES race_parent(id)");
                }
                return invocation.proceed();
            }
        };

        org.apache.doris.nereids.exceptions.AnalysisException error = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> runAs(user, "ALTER TABLE race_parent DROP CONSTRAINT pk"));
        Assertions.assertTrue(added.get());
        Assertions.assertTrue(error.getMessage().contains("changed while checking privileges"));
        Assertions.assertTrue(table("race_parent").getConstraintsMap().containsKey("pk"));
        Assertions.assertTrue(table("race_child").getConstraintsMap().containsKey("fk"));
        Assertions.assertTrue(table("race_new_child").getConstraintsMap().containsKey("fk"));
    }

    private ConnectContext createLimitedUser(String name) throws Exception {
        addUser(name, false);
        grantPriv("GRANT SELECT_PRIV ON " + DB + ".* TO '" + name + "'@'%'");
        ConnectContext user = createDefaultCtx();
        user.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(name, "%"));
        user.setDatabase(DB);
        user.setNoAuth(false);
        user.setSkipAuth(false);
        connectContext.setThreadLocalInfo();
        return user;
    }

    private TableIf table(String name) throws Exception {
        return Env.getCurrentInternalCatalog().getDbOrMetaException(DB).getTableOrMetaException(name);
    }

    private void assertDenied(ConnectContext user, String sql) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () -> runAs(user, sql));
        Assertions.assertTrue(error.getMessage().contains("denied"), error.getMessage());
    }

    private void runAs(ConnectContext user, String sql) throws Exception {
        user.setThreadLocalInfo();
        try {
            Command command = (Command) new NereidsParser().parseSingle(sql);
            command.run(user, new StmtExecutor(user, sql));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }
}
