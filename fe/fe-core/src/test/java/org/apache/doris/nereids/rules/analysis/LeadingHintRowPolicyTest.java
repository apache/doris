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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The leading hint rebuilds the join from the plans which are remembered in the analysis phase, so the plans
 * which are built on the table relation, e.g. the row policy filter and the data mask project, must be
 * remembered together with the relation. Otherwise they are dropped silently and the user can read the rows
 * which are protected by the row policy.
 */
public class LeadingHintRowPolicyTest extends TestWithFeService {

    private static final String DB_NAME = "leading_hint_row_policy";
    private static final String TABLE_1 = "leading_hint_t1";
    private static final String TABLE_2 = "leading_hint_t2";
    private static final String MASKED_TABLE = "leading_hint_masked";
    private static final String USER_NAME = "leading_hint_user";
    private static final String POLICY_NAME = "leading_hint_policy";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB_NAME);
        useDatabase(DB_NAME);
        createTable("create table " + TABLE_1 + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        createTable("create table " + TABLE_2 + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        createTable("create table " + MASKED_TABLE + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");

        // create user and grant privilege, so that the row policy and the data mask policy can be evaluated
        UserIdentity user = new UserIdentity(USER_NAME, "%");
        user.analyze();
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(user)));
        createUserCommand.getInfo().validate();
        Env.getCurrentEnv().getAuth().createUser(createUserCommand.getInfo());
        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV));
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze();
        GrantTablePrivilegeCommand grantTablePrivilegeCommand = new GrantTablePrivilegeCommand(
                privileges, tablePattern, Optional.of(user), Optional.empty());
        grantTablePrivilegeCommand.validate();
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand(grantTablePrivilegeCommand);

        // the data mask policy is provided by the external auth plugin, mock it for the masked table
        AccessControllerManager spyAcm = Mockito.spy(Env.getCurrentEnv().getAccessManager());
        // Masks are asked for one table at a time, keyed by the lower-cased column name - that is the shape
        // the planner asks in and reads back, so a stub on the per-column method would never be reached.
        Mockito.doAnswer(invocation -> {
            String tbl = invocation.getArgument(3);
            Set<String> cols = invocation.getArgument(4);
            if (!tbl.equalsIgnoreCase(MASKED_TABLE)) {
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
    public void testRowPolicyIsKeptByLeadingHint() throws Exception {
        useUser(USER_NAME);
        createPolicy("CREATE ROW POLICY " + POLICY_NAME + " ON " + TABLE_1
                + " AS RESTRICTIVE TO " + USER_NAME + " USING (k = 1)");

        // the hint reverses the join order, so the rebuilt join proves that the hint is really applied
        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze("SELECT /*+ leading(" + TABLE_2 + " " + TABLE_1 + ") */ "
                        + TABLE_1 + ".k, " + TABLE_1 + ".v, " + TABLE_2 + ".v FROM "
                        + TABLE_1 + " JOIN " + TABLE_2 + " ON " + TABLE_1 + ".k = " + TABLE_2 + ".k");
        Assertions.assertTrue(planChecker.getCascadesContext().getHintMap().get("Leading").isSuccess());
        Plan plan = planChecker.getPlan();

        LogicalJoin<?, ?> join = findJoin(plan);
        Assertions.assertNotNull(join, () -> "join is missing in plan:\n" + plan.treeString());
        Assertions.assertInstanceOf(LogicalOlapScan.class, join.left(),
                () -> "unexpected join order of leading hint:\n" + plan.treeString());
        Assertions.assertEquals(TABLE_2, ((LogicalOlapScan) join.left()).getTable().getName());
        Assertions.assertInstanceOf(LogicalFilter.class, join.right(),
                () -> "row policy filter is dropped by leading hint:\n" + plan.treeString());
        LogicalFilter<?> policyFilter = (LogicalFilter<?>) join.right();
        Assertions.assertEquals(1, policyFilter.getConjuncts().size());
        Assertions.assertTrue(policyFilter.getConjuncts().toString().contains("= 1"),
                () -> "unexpected row policy filter: " + policyFilter.getConjuncts());
        Assertions.assertInstanceOf(LogicalOlapScan.class, policyFilter.child());
        Assertions.assertEquals(TABLE_1, ((LogicalOlapScan) policyFilter.child()).getTable().getName());

        dropPolicy("DROP ROW POLICY " + POLICY_NAME + " ON " + TABLE_1);
    }

    @Test
    public void testRowPolicyAndDataMaskAreKeptByLeadingHint() throws Exception {
        useUser(USER_NAME);
        createPolicy("CREATE ROW POLICY " + POLICY_NAME + " ON " + MASKED_TABLE
                + " AS RESTRICTIVE TO " + USER_NAME + " USING (k = 1)");

        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze("SELECT /*+ leading(" + TABLE_2 + " " + MASKED_TABLE + ") */ "
                        + MASKED_TABLE + ".k, " + MASKED_TABLE + ".v, " + TABLE_2 + ".v FROM "
                        + MASKED_TABLE + " JOIN " + TABLE_2 + " ON " + MASKED_TABLE + ".k = " + TABLE_2 + ".k");
        Assertions.assertTrue(planChecker.getCascadesContext().getHintMap().get("Leading").isSuccess());
        Plan plan = planChecker.getPlan();

        // both the data mask project and the row policy filter are kept on the leaf of the leading hint
        LogicalJoin<?, ?> join = findJoin(plan);
        Assertions.assertNotNull(join, () -> "join is missing in plan:\n" + plan.treeString());
        Assertions.assertInstanceOf(LogicalProject.class, join.right(),
                () -> "data mask project is dropped by leading hint:\n" + plan.treeString());
        Plan policyLeaf = join.right().child(0);
        Assertions.assertInstanceOf(LogicalFilter.class, policyLeaf,
                () -> "row policy filter is dropped by leading hint:\n" + plan.treeString());
        LogicalFilter<?> policyFilter = (LogicalFilter<?>) policyLeaf;
        Assertions.assertEquals(1, policyFilter.getConjuncts().size());
        Assertions.assertTrue(policyFilter.getConjuncts().toString().contains("= 1"),
                () -> "unexpected row policy filter: " + policyFilter.getConjuncts());
        Assertions.assertInstanceOf(LogicalOlapScan.class, policyFilter.child());
        Assertions.assertEquals(MASKED_TABLE,
                ((LogicalOlapScan) policyFilter.child()).getTable().getName());

        dropPolicy("DROP ROW POLICY " + POLICY_NAME + " ON " + MASKED_TABLE);
    }

    private LogicalJoin<?, ?> findJoin(Plan plan) {
        if (plan instanceof LogicalJoin) {
            return (LogicalJoin<?, ?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalJoin<?, ?> join = findJoin(child);
            if (join != null) {
                return join;
            }
        }
        return null;
    }
}
