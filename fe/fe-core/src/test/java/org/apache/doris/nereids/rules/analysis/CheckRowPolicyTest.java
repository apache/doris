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
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy.RelatedPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CheckRowPolicyTest extends TestWithFeService {

    private static String dbName = "check_row_policy";
    private static String fullDbName = "" + dbName;
    private static String tableName = "table1";

    private static String tableNameRanddomDist = "tableRandomDist";
    private static String tableNameMow = "tableMow";
    private static String userName = "user1";
    private static String policyName = "policy1";

    private static OlapTable olapTable;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_feature_binlog = true;
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
        createTable("create table "
                + tableNameMow
                + " (k1 int, k2 int) UNIQUE KEY(k1) distributed by hash(k1) buckets 1"
                + " properties(\"replication_num\" = \"1\","
                + " \"enable_unique_key_merge_on_write\" = \"true\","
                + " \"binlog.enable\" = \"true\", \"binlog.format\" = \"ROW\","
                + " \"binlog.need_historical_value\" = \"true\");");
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
        Mockito.doAnswer(invocation -> {
            String tbl = invocation.getArgument(3);
            String col = invocation.getArgument(4);
            if (tbl.equalsIgnoreCase(tableNameRanddomDist)) {
                return Optional.of(new DataMaskPolicy() {
                    @Override
                    public String getMaskTypeDef() {
                        return String.format("concat(%s, '_****_', %s)", col, col);
                    }

                    @Override
                    public String getPolicyIdent() {
                        return String.format("custom policy: concat(%s, '_****_', %s)", col, col);
                    }
                });
            }
            if (!tbl.equalsIgnoreCase(tableNameMow)) {
                return Optional.empty();
            }
            String column = col.toLowerCase(Locale.ROOT);
            if (column.equalsIgnoreCase("k2")) {
                String mask = "if(assert_true(k2 >= 0, 'post-snapshot row reached mask'), k2, NULL)";
                return Optional.of(new DataMaskPolicy() {
                    @Override
                    public String getMaskTypeDef() {
                        return mask;
                    }

                    @Override
                    public String getPolicyIdent() {
                        return "custom non-movable policy: " + mask;
                    }
                });
            }
            // Mask hidden reconstruction columns too. Their aliases deliberately get new ExprIds,
            // so a reconstruction filter left above the mask would fail CheckAfterRewrite.
            return Optional.of(new DataMaskPolicy() {
                @Override
                public String getMaskTypeDef() {
                    return column;
                }

                @Override
                public String getPolicyIdent() {
                    return "custom identity policy: " + column;
                }
            });
        }).when(spyAcm).evalDataMaskPolicy(
                Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
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

    @Test
    public void checkPolicyOnOlapTableWrapperUsesOriginTable() throws Exception {
        useUser(userName);
        connectContext.setStatementContext(new StatementContext());
        LogicalOlapScan relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                new RenamedOlapTableWrapper(olapTable), Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalOlapScan> checkPolicy = new LogicalCheckPolicy<>(relation);
        createPolicy("CREATE ROW POLICY " + policyName + " ON " + tableName
                + " AS PERMISSIVE TO " + userName + " USING (k1 = 1)");
        try {
            Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext,
                    new CheckPolicy(), new BindExpression());

            Assertions.assertTrue(plan instanceof LogicalFilter);
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            Assertions.assertEquals(relation, filter.child());
            Assertions.assertTrue(filter.getConjuncts().toString().contains("k1"));
        } finally {
            dropPolicy("DROP ROW POLICY " + policyName + " ON " + tableName);
        }
    }

    @Test
    public void checkMvRefreshPolicyOnNestedOlapTableWrapperUsesOriginTable() throws Exception {
        useUser(userName);
        StatementContext statementContext = new StatementContext();
        connectContext.setStatementContext(statementContext);
        OlapTableWrapper wrapper = new OlapTableWrapper(olapTable, Collections.emptyMap());
        OlapTableWrapper nestedWrapper = new OlapTableWrapper(wrapper, Collections.emptyMap());
        LogicalOlapScan relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                nestedWrapper, Arrays.asList(fullDbName));
        Expression predicate = new EqualTo(relation.getOutput().get(0), new IntegerLiteral(1));
        statementContext.setMvRefreshPredicates(ImmutableMap.of(olapTable, ImmutableSet.of(predicate)));

        LogicalCheckPolicy<LogicalOlapScan> checkPolicy = new LogicalCheckPolicy<>(relation);
        RelatedPolicy policy = checkPolicy.findPolicy(relation,
                CascadesContext.initContext(statementContext, relation, PhysicalProperties.GATHER));

        Assertions.assertEquals(Optional.of(predicate), policy.rowPolicyFilter);
    }

    @Test
    public void mowTimeTravelReconstructionFiltersRunBeforeNonMovableMask() throws Exception {
        useUser(userName);
        connectContext.getState().setIsQuery(true);

        Plan rewrittenPlan = PlanChecker.from(connectContext)
                .analyze("select k1, k2 from " + tableNameMow + " for version as of 1001")
                .rewrite()
                .getPlan();
        Set<LogicalUnion> unions = rewrittenPlan.collect(node -> node instanceof LogicalUnion);
        Assertions.assertEquals(1, unions.size());

        LogicalUnion union = unions.iterator().next();
        Assertions.assertEquals(2, union.children().size());
        Set<String> reconstructionFilterSlots = new java.util.HashSet<>();
        for (Plan branch : union.children()) {
            Set<LogicalProject<?>> branchProjects = branch.collect(node -> node instanceof LogicalProject);
            int maskedK2ProjectCount = 0;
            for (LogicalProject<?> project : branchProjects) {
                for (NamedExpression namedExpression : project.getProjects()) {
                    if (namedExpression.getName().equalsIgnoreCase("k2")
                            && namedExpression instanceof Alias
                            && !(((Alias) namedExpression).child() instanceof Slot)) {
                        maskedK2ProjectCount++;
                    }
                }
            }
            Assertions.assertTrue(maskedK2ProjectCount >= 1,
                    "each MOW time-travel branch must retain its k2 data mask");

            Set<LogicalProject<?>> maskProjects = branch.collect(node -> node instanceof LogicalProject
                    && ((LogicalProject<?>) node).containsNoneMovableFunction());
            Assertions.assertFalse(maskProjects.isEmpty(),
                    "each branch must contain the non-movable data mask");
            for (LogicalProject<?> maskProject : maskProjects) {
                Set<LogicalFilter<?>> filtersBelowMask = maskProject.collect(node -> node instanceof LogicalFilter);
                Assertions.assertFalse(filtersBelowMask.isEmpty(),
                        "reconstruction filters must run before every non-movable data mask");
                reconstructionFilterSlots.addAll(filtersBelowMask.stream()
                        .flatMap(filter -> filter.getConjuncts().stream())
                        .flatMap(conjunct -> conjunct.getInputSlots().stream())
                        .map(Slot::getName)
                        .collect(Collectors.toSet()));
            }
        }
        Assertions.assertTrue(reconstructionFilterSlots.contains(Column.DELETE_SIGN));
        Assertions.assertTrue(reconstructionFilterSlots.contains(Column.COMMIT_TSO_COL));
        Assertions.assertTrue(reconstructionFilterSlots.contains(Column.BINLOG_OPERATION_COL));
    }

    private static class RenamedOlapTableWrapper extends OlapTableWrapper {
        private RenamedOlapTableWrapper(OlapTable originTable) {
            super(originTable, "renamed_policy_wrapper", originTable.getBaseSchema(),
                    originTable.getKeysType(), Collections.emptyMap());
        }
    }
}
