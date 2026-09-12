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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CheckRowPolicyTest extends TestWithFeService {

    private static String dbName = "check_row_policy";
    private static String fullDbName = "" + dbName;
    private static String tableName = "table1";

    private static String tableNameRanddomDist = "tableRandomDist";
    private static String userName = "user1";
    private static String policyName = "policy1";

    private static OlapTable olapTable;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table "
                + tableName
                + " (k1 int, k2 int) distributed by hash(k1) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        createTable("create table "
                + tableNameRanddomDist
                + " (k1 int, k2 int) AGGREGATE KEY(k1, k2) distributed by random buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        olapTable = (OlapTable) db.getTableOrAnalysisException(tableName);

        // create user
        UserIdentity user = new UserIdentity(userName, "%");
        user.analyze();

        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(user)));
        createUserCommand.getInfo().validate();
        Env.getCurrentEnv().getAuth().createUser(createUserCommand.getInfo());

        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV));
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze();
        GrantTablePrivilegeCommand grantTablePrivilegeCommand = new GrantTablePrivilegeCommand(privileges, tablePattern, Optional.of(user), Optional.empty());
        grantTablePrivilegeCommand.validate();
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand(grantTablePrivilegeCommand);

        AccessControllerManager spyAcm = Mockito.spy(Env.getCurrentEnv().getAccessManager());
        // Masks are asked for one table at a time, keyed by the lower-cased column name - that is the shape
        // the planner asks in and reads back, so a stub on the per-column method would never be reached.
        Mockito.doAnswer(invocation -> {
            String tbl = invocation.getArgument(3);
            Set<String> cols = invocation.getArgument(4);
            if (!tbl.equalsIgnoreCase(tableNameRanddomDist)) {
                return Collections.<String, DataMaskSpec>emptyMap();
            }
            Map<String, DataMaskSpec> masks = new LinkedHashMap<>();
            for (String col : cols) {
                String column = col.toLowerCase(Locale.ROOT);
                masks.put(column, new DataMaskSpec(
                        String.format("custom policy: concat(%s, '_****_', %s)", column, column),
                        String.format("concat(%s, '_****_', %s)", column, column)));
            }
            return masks;
        }).when(spyAcm).evalDataMaskPolicies(
                Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anySet());
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", spyAcm);
    }

    @Test
    public void checkUser() throws AnalysisException, org.apache.doris.common.AnalysisException {
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);

        useUser("root");
        Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);

        useUser("notFound");
        plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);
    }

    @Test
    public void checkUserRandomDist() throws AnalysisException, org.apache.doris.common.AnalysisException {
        connectContext.getState().setIsQuery(true);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                        ImmutableList.of(tableNameRanddomDist)), connectContext, new BindRelation());
        LogicalCheckPolicy checkPolicy = new LogicalCheckPolicy(plan);

        useUser("root");
        Plan rewrittenPlan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy(),
                new BindExpression());
        Assertions.assertEquals(plan, rewrittenPlan);

        useUser("notFound");
        rewrittenPlan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy(),
                new BindExpression());
        Assertions.assertEquals(plan, rewrittenPlan.child(0));
    }

    @Test
    public void checkNoPolicy() throws org.apache.doris.common.AnalysisException {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);
    }

    @Test
    public void checkNoPolicyRandomDist() throws org.apache.doris.common.AnalysisException {
        useUser(userName);
        connectContext.getState().setIsQuery(true);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(tableNameRanddomDist)), connectContext, new BindRelation());
        LogicalCheckPolicy checkPolicy = new LogicalCheckPolicy(plan);
        Plan rewrittenPlan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy(),
                new BindExpression());
        Assertions.assertEquals(plan, rewrittenPlan.child(0));
    }

    @Test
    public void checkOnePolicy() throws Exception {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        createPolicy("CREATE ROW POLICY "
                + policyName
                + " ON "
                + tableName
                + " AS PERMISSIVE TO "
                + userName
                + " USING (k1 = 1)");
        Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());

        Assertions.assertTrue(plan instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) plan;
        Assertions.assertEquals(filter.child(), relation);
        Assertions.assertTrue(ImmutableList.copyOf(filter.getConjuncts()).get(0) instanceof EqualTo);
        Assertions.assertTrue(filter.getConjuncts().toString().contains("'k1 = 1"));

        dropPolicy("DROP ROW POLICY "
                + policyName
                + " ON "
                + tableName);
    }

    /**
     * Two permissive policies widen each other: the user sees rows matching either one, so they must be
     * ORed into a single conjunct. A single policy cannot tell OR from AND, so this is what actually pins
     * the merge type the authorization layer carries alongside each filter - drop it and the two predicates
     * become an AND, which lets the user see nothing at all.
     */
    @Test
    public void checkTwoPermissivePoliciesAreOred() throws Exception {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        createPolicy("CREATE ROW POLICY " + policyName + " ON " + tableName
                + " AS PERMISSIVE TO " + userName + " USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY " + policyName + "_second ON " + tableName
                + " AS PERMISSIVE TO " + userName + " USING (k1 = 2)");
        try {
            Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());

            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            Assertions.assertEquals(1, filter.getConjuncts().size(),
                    "permissive policies must merge into one disjunction, not into separate conjuncts");
            Assertions.assertTrue(ImmutableList.copyOf(filter.getConjuncts()).get(0) instanceof Or,
                    "the user must see rows matching either permissive policy");
        } finally {
            // A leaked policy would change what every later test in this class plans.
            dropPolicy("DROP ROW POLICY " + policyName + " ON " + tableName);
            dropPolicy("DROP ROW POLICY " + policyName + "_second ON " + tableName);
        }
    }

    /** Restrictive policies narrow each other, so they stay separate conjuncts (ANDed). */
    @Test
    public void checkTwoRestrictivePoliciesAreAnded() throws Exception {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        createPolicy("CREATE ROW POLICY " + policyName + " ON " + tableName
                + " AS RESTRICTIVE TO " + userName + " USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY " + policyName + "_second ON " + tableName
                + " AS RESTRICTIVE TO " + userName + " USING (k2 = 2)");
        try {
            Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());

            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            Assertions.assertEquals(2, filter.getConjuncts().size(),
                    "restrictive policies must all hold, so each stays its own conjunct");
            filter.getConjuncts().forEach(conjunct -> Assertions.assertTrue(conjunct instanceof EqualTo));
        } finally {
            dropPolicy("DROP ROW POLICY " + policyName + " ON " + tableName);
            dropPolicy("DROP ROW POLICY " + policyName + "_second ON " + tableName);
        }
    }

    @Test
    public void checkOnePolicyRandomDist() throws Exception {
        useUser(userName);
        connectContext.getState().setIsQuery(true);
        connectContext.setStatementContext(new StatementContext());
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(tableNameRanddomDist)), connectContext, new BindRelation());

        LogicalCheckPolicy checkPolicy = new LogicalCheckPolicy(plan);
        createPolicy("CREATE ROW POLICY "
                + policyName
                + " ON "
                + tableNameRanddomDist
                + " AS PERMISSIVE TO "
                + userName
                + " USING (k1 = 1)");
        Plan rewrittenPlan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy(),
                new BindExpression());

        Assertions.assertTrue(rewrittenPlan instanceof LogicalProject
                && rewrittenPlan.child(0) instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) rewrittenPlan.child(0);
        Assertions.assertEquals(filter.child(), plan);
        Assertions.assertTrue(ImmutableList.copyOf(filter.getConjuncts()).get(0) instanceof EqualTo);
        Assertions.assertTrue(filter.getConjuncts().toString().contains("k1#0 = 1"));

        dropPolicy("DROP ROW POLICY "
                + policyName
                + " ON "
                + tableNameRanddomDist);
    }
}
