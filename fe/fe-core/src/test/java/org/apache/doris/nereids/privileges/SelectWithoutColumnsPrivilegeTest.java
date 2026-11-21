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

package org.apache.doris.nereids.privileges;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A statement that reads rows of a table and none of its columns - COUNT(*), COUNT(1), SELECT 1 FROM t - under
 * the built-in privilege model, for a user who was granted one column of the table and not the table.
 *
 * <p>The column granted here is {@code name}, never the narrowest one. The planner keeps the narrowest column
 * of a table written with an alias, of a view and of a subquery when nothing is read from them, so with this
 * grant the kept column is one the user does not hold.
 */
public class SelectWithoutColumnsPrivilegeTest extends TestWithFeService {
    private static final String DB = "no_col_read_db";
    private static final String T = DB + ".t";
    private static final String UNIQ_T = DB + ".uniq_t";
    private static final String OTHER_T = DB + ".other_t";
    private static final String V = DB + ".v";

    // holds SELECT on the column `name` of t, uniq_t and v, and nothing else
    private static final String COLUMN_USER = "no_col_read_column_user";
    // holds the same column through a role
    private static final String ROLE_USER = "no_col_read_role_user";
    private static final String ROLE = "no_col_read_role";
    // holds LOAD on t and no SELECT of any kind
    private static final String LOAD_USER = "no_col_read_load_user";
    // holds nothing
    private static final String NO_PRIV_USER = "no_col_read_no_priv_user";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        String columns = " (id int, name varchar(32), secret bigint) ";
        String tail = " distributed by hash(id) buckets 1 properties('replication_num' = '1')";
        createTable("create table " + T + columns + "duplicate key(id)" + tail);
        createTable("create table " + UNIQ_T + columns + "unique key(id)" + tail);
        createTable("create table " + OTHER_T + columns + "duplicate key(id)" + tail);
        createView("create view " + V + " as select id, name, secret from " + T);

        addUser(COLUMN_USER, true);
        addUser(ROLE_USER, true);
        addUser(LOAD_USER, true);
        addUser(NO_PRIV_USER, true);
        createRole(ROLE);
        for (String object : ImmutableList.of(T, UNIQ_T, V)) {
            grantPriv("grant select_priv(name) on " + object + " to " + COLUMN_USER);
            grantPriv("grant select_priv(name) on " + object + " to role '" + ROLE + "'");
        }
        grantRole("grant '" + ROLE + "' to " + ROLE_USER);
        grantPriv("grant load_priv on " + T + " to " + LOAD_USER);
    }

    @Test
    public void readingNoColumnTakesOneGrantedColumn() throws Exception {
        List<String> statements = ImmutableList.of(
                // a bare table
                "select count(*) from " + T,
                "select count(1) from " + T,
                "select 1 from " + T,
                // a table written with an alias, a view, a subquery: the planner keeps `id` under them
                "select count(*) from " + T + " a",
                "select count(1) from " + T + " a",
                "select 1 from " + T + " a limit 1",
                "select count(*) from " + V,
                "select count(*) from " + V + " a",
                // columns named in a subquery that nothing reads are not read
                "select count(*) from (select id, secret from " + T + ") q",
                "select count(*) from (select secret as s from " + T + ") q",
                "select count(*) from (select 1 from " + T + ") q",
                "with c as (select id, secret from " + T + ") select count(*) from c",
                // the hidden delete sign of a unique key table is not a column the user is asked about
                "select count(*) from " + UNIQ_T,
                "select count(*) from " + UNIQ_T + " a",
                // two tables, no column of either
                "select count(*) from " + T + " a, " + UNIQ_T + " b"
        );
        for (String user : ImmutableList.of(COLUMN_USER, ROLE_USER)) {
            as(user, () -> assertAllAllowed(statements));
        }
    }

    @Test
    public void grantedColumnIsStillReadAsBefore() throws Exception {
        as(COLUMN_USER, () -> assertAllAllowed(ImmutableList.of(
                "select name from " + T,
                "select count(name) from " + T + " a",
                "select count(*) from " + T + " a where a.name = 'x'",
                "select count(*) from (select name from " + T + " where name > 'a') q"
        )));
    }

    @Test
    public void columnThatIsReadStillTakesItsOwnPrivilege() throws Exception {
        as(COLUMN_USER, () -> assertAllDenied(ImmutableList.of(
                "select * from " + T,
                "select id from " + T + " a",
                "select count(id) from " + T,
                "select count(secret) from " + T + " a",
                // read by a predicate, a join condition, a grouping, an ordering: none of them is in the result
                "select count(*) from " + T + " where id = 1",
                "select count(*) from " + T + " a where a.secret > 0",
                "select count(*) from (select id from " + T + " where id > 5) q",
                "select count(*) from (select 1 from " + T + " where secret > 5) q",
                // computed in a subquery that nothing reads: the expression may still be evaluated
                "select count(*) from (select secret + 1 from " + T + ") q",
                "select count(*) from (select cast(secret as int) from " + T + ") q",
                "select count(*) from (select id, secret + 1 as x from " + T + ") q",
                "select count(*) from (select 1, secret + 1 from " + T + ") q",
                "select count(*) from " + T + " a join " + UNIQ_T + " b on a.id = b.id",
                "select count(*) from (select distinct id from " + T + ") q",
                "select count(*) from (select id from " + T + " group by id) q",
                "select count(*) from (select id from " + T + " order by secret limit 3) q",
                "select count(*) from " + V + " a where a.id > 0"
        )));
    }

    @Test
    public void whatIsNotAPlainPlanKeepsItsAnswer() throws Exception {
        // Under a set operation or a CTE that is not inlined the check is the one made before, and its
        // refusal is final.
        as(COLUMN_USER, () -> assertAllDenied(ImmutableList.of(
                "select count(*) from (select id from " + T + " union all select id from " + T + ") q",
                "select count(*) from (select id from " + T + " union select id from " + T + ") q",
                "select count(*) from (select id from " + T + " intersect select id from " + T + ") q",
                "with c as (select id from " + T + ") select count(*) from c x, c y"
        )));
    }

    @Test
    public void noColumnHeldIsStillRefused() throws Exception {
        List<String> statements = ImmutableList.of(
                "select count(*) from " + T,
                "select count(1) from " + T,
                "select 1 from " + T,
                "select count(*) from " + T + " a",
                "select count(*) from " + V,
                "select count(*) from (select id, secret from " + T + ") q"
        );
        as(NO_PRIV_USER, () -> assertAllDenied(statements));
        // a privilege on the table that is not SELECT, and a column of another table, do not count
        as(LOAD_USER, () -> assertAllDenied(statements));
        as(COLUMN_USER, () -> assertAllDenied(ImmutableList.of(
                "select count(*) from " + OTHER_T,
                "select count(*) from " + OTHER_T + " a"
        )));
    }

    @Test
    public void theSqlCacheReplaysTheCheckThatPassed() throws Exception {
        // NereidsSqlCacheManager#privilegeChanged replays, for every table of a cached statement, the check
        // recorded while it was planned. For a statement let through on a held column that is the column:
        // the replay passes while the grant is there, and the statement can be served from the cache.
        connectContext.getSessionVariable().setEnableSqlCache(true);
        try {
            as(COLUMN_USER, () -> {
                for (String sql : ImmutableList.of("select count(*) from " + T,
                        "select count(*) from " + T + " a", "select count(*) from " + V)) {
                    PlanChecker checker = PlanChecker.from(connectContext).parse(sql).analyze().rewrite();
                    SqlCacheContext sqlCacheContext = checker.getCascadesContext().getStatementContext()
                            .getSqlCacheContext().orElseThrow(() -> new AssertionError("no sql cache context"));
                    Map<FullTableName, Set<String>> recorded = sqlCacheContext.getCheckPrivilegeTablesOrViews();
                    Assertions.assertFalse(recorded.isEmpty(), sql);
                    for (Map.Entry<FullTableName, Set<String>> check : recorded.entrySet()) {
                        TableIf table = Env.getCurrentInternalCatalog().getDbOrAnalysisException(check.getKey().db)
                                .getTableOrAnalysisException(check.getKey().table);
                        Assertions.assertEquals(ImmutableSet.of("name"), check.getValue(), sql);
                        Assertions.assertDoesNotThrow(
                                () -> UserAuthentication.checkPermission(table, connectContext, check.getValue()),
                                sql);
                    }
                }
            });
        } finally {
            connectContext.getSessionVariable().setEnableSqlCache(false);
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    private void as(String user, Body body) throws Exception {
        useUser(user);
        try {
            body.run();
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }

    private void plan(String sql) {
        PlanChecker.from(connectContext).parse(sql).analyze().rewrite();
    }

    private void assertAllAllowed(List<String> statements) {
        List<String> refused = new ArrayList<>();
        for (String sql : statements) {
            try {
                plan(sql);
            } catch (Exception e) {
                refused.add(sql + "  ->  " + e.getMessage());
            }
        }
        Assertions.assertTrue(refused.isEmpty(), "refused for " + connectContext.getCurrentUserIdentity()
                + ", expected allowed:\n" + String.join("\n", refused));
    }

    private void assertAllDenied(List<String> statements) {
        List<String> wrong = new ArrayList<>();
        for (String sql : statements) {
            try {
                plan(sql);
                wrong.add(sql + "  ->  allowed");
            } catch (Exception e) {
                // a refusal for any other reason would prove nothing about privileges
                if (e.getMessage() == null || !e.getMessage().contains("denied")) {
                    wrong.add(sql + "  ->  failed, but not on a privilege: " + e.getMessage());
                }
            }
        }
        Assertions.assertTrue(wrong.isEmpty(), "for " + connectContext.getCurrentUserIdentity()
                + ", expected a privilege refusal:\n" + String.join("\n", wrong));
    }
}
