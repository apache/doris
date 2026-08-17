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

package org.apache.doris.policy;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * What a row policy means must not depend on the {@code sql_mode} of whoever created it.
 *
 * <p>Two bits of {@code sql_mode} change what SQL text means - {@code PIPES_AS_CONCAT} turns {@code ||} from
 * OR into string concatenation, {@code NO_BACKSLASH_ESCAPES} changes how a string literal decodes - and
 * {@code sql_mode} is a session variable any account may set with no privilege at all. The predicate of a row
 * policy is therefore read under one fixed mode ({@link SqlModeHelper#MODE_FOR_POLICY_TEXT}), and it is read
 * under it at every stage: when the statement is accepted, when {@code SHOW ROW POLICY} renders it, and when a
 * query the policy restricts is planned. Any stage disagreeing with the others is a policy that does not do
 * what it says - and the restricted user is the one who would choose which.
 */
public class RowPolicySqlModeTest extends TestWithFeService {

    private static final String DB = "row_policy_sql_mode";
    private static final String TBL = "policy_tbl";
    private static final String USER = "policy_sql_mode_user";

    private static OlapTable table;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB);
        useDatabase(DB);
        createTable("create table " + TBL + " (region varchar(8), path varchar(64))"
                + " distributed by hash(region) buckets 1 properties(\"replication_num\" = \"1\");");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB);
        table = (OlapTable) db.getTableOrAnalysisException(TBL);

        addUser(USER, true);
        grantPriv("GRANT SELECT_PRIV ON internal." + DB + "." + TBL + " TO '" + USER + "'@'%'");
    }

    /**
     * {@code ||} in a policy means OR, whatever the creator's session made it mean.
     *
     * <p>Under {@code PIPES_AS_CONCAT} the same text reads as {@code region = ('cn' || region) = 'us'} - a
     * comparison against a concatenation, which filters something else entirely. What the query is planned
     * with is the OR, so that is what the statement has to be accepted as and what {@code SHOW ROW POLICY} has
     * to render; anything else is a policy showing one predicate and enforcing another.
     */
    @Test
    public void testPipesInAPolicyMeanOrWhateverTheCreatorsSessionMeant() throws Exception {
        long callerMode = connectContext.getSessionVariable().getSqlMode();
        connectContext.getSessionVariable().setSqlMode(callerMode | SqlModeHelper.MODE_PIPES_AS_CONCAT);
        try {
            createPolicy("CREATE ROW POLICY p_pipes ON " + TBL + " AS PERMISSIVE TO " + USER
                    + " USING (region = 'cn' || region = 'us')");

            RowPolicy stored = onlyPolicyOfTheUser();
            Assertions.assertTrue(stored.getWherePredicate() instanceof Or,
                    "the stored predicate is not a disjunction, so SHOW ROW POLICY renders a concatenation"
                            + " while the query is filtered by an OR: " + stored.getWherePredicate().toSql());

            Expression planned = onlyFilterConjunctPlannedFor(USER);
            Assertions.assertTrue(planned instanceof Or,
                    "the query was planned with something other than the disjunction: " + planned.toSql());
        } finally {
            connectContext.getSessionVariable().setSqlMode(callerMode);
            dropPolicy("DROP ROW POLICY p_pipes ON " + TBL);
        }
    }

    /**
     * A predicate only the creator's {@code sql_mode} can read is refused where it is written.
     *
     * <p>{@code 'C:\'} is a complete string literal under {@code NO_BACKSLASH_ESCAPES} and an unterminated one
     * without it. Accepting it would store a policy that parses on no query at all: every statement touching
     * the table would fail, for every user the policy applies to, with an error about a string literal nobody
     * wrote. Refusing it here says so once, to the account that can fix it.
     */
    @Test
    public void testAPredicateOnlyTheCreatorsModeCanReadIsRefusedAtCreation() throws Exception {
        long callerMode = connectContext.getSessionVariable().getSqlMode();
        connectContext.getSessionVariable().setSqlMode(callerMode | SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        try {
            Exception refused = Assertions.assertThrows(Exception.class,
                    () -> createPolicy("CREATE ROW POLICY p_escapes ON " + TBL + " AS PERMISSIVE TO " + USER
                            + " USING (path <> 'C:\\')"));
            Assertions.assertTrue(refused.getMessage().contains("sql_mode"),
                    "the refusal does not say that a policy's text is read under a fixed sql_mode, which is"
                            + " the one thing the creator needs to know here: " + refused.getMessage());
            Assertions.assertTrue(policiesOfTheUser().isEmpty(),
                    "a policy that cannot be read on any query was stored anyway");
        } finally {
            connectContext.getSessionVariable().setSqlMode(callerMode);
        }
    }

    /** An ordinary policy is unaffected: the fixed mode is the one a fresh session already has. */
    @Test
    public void testAnOrdinaryPolicyIsUnaffected() throws Exception {
        createPolicy("CREATE ROW POLICY p_plain ON " + TBL + " AS PERMISSIVE TO " + USER
                + " USING (region = 'cn' or region = 'us')");
        try {
            Assertions.assertTrue(onlyFilterConjunctPlannedFor(USER) instanceof Or);
        } finally {
            dropPolicy("DROP ROW POLICY p_plain ON " + TBL);
        }
    }

    private List<RowPolicy> policiesOfTheUser() throws Exception {
        UserIdentity user = new UserIdentity(USER, "%");
        user.analyze();
        return Env.getCurrentEnv().getPolicyMgr().getUserPolicies("internal", DB, TBL, user);
    }

    private RowPolicy onlyPolicyOfTheUser() throws Exception {
        List<RowPolicy> policies = policiesOfTheUser();
        Assertions.assertEquals(1, policies.size(), "expected exactly one policy, got " + policies);
        return policies.get(0);
    }

    /** The predicate a query by {@code user} is actually planned with. */
    private Expression onlyFilterConjunctPlannedFor(String user) throws Exception {
        useUser(user);
        try {
            LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table,
                    Arrays.asList(DB));
            Plan plan = PlanRewriter.bottomUpRewrite(new LogicalCheckPolicy<>(relation), connectContext,
                    new CheckPolicy());
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            Assertions.assertEquals(1, filter.getConjuncts().size(),
                    "expected one conjunct, got " + filter.getConjuncts());
            return ImmutableList.copyOf(filter.getConjuncts()).get(0);
        } finally {
            useUser("root");
        }
    }
}
