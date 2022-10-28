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

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Test for Policy.
 **/
public class PolicyTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table table1\n"
                + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        createTable("create table table2\n"
                + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        // create user
        UserIdentity user = new UserIdentity("test_policy", "%");
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(user));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ADMIN_PRIV);
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze(SystemInfoService.DEFAULT_CLUSTER);
        GrantStmt grantStmt = new GrantStmt(user, null, tablePattern, privileges);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
        useUser("test_policy");
    }

    @Test
    public void testNoPolicy() throws Exception {
        useUser("root");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        useUser("test_policy");
        Assertions.assertFalse(explainString.contains("`k1` = 1"));
    }

    @Test
    public void testExistPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        Assertions.assertTrue(Env.getCurrentEnv().getPolicyMgr().existPolicy("default_cluster:test_policy"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
        Assertions.assertFalse(Env.getCurrentEnv().getPolicyMgr().existPolicy("default_cluster:test_policy"));
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1");
        Assertions.assertFalse(Env.getCurrentEnv().getPolicyMgr().existPolicy("default_cluster:test_policy"));
    }

    @Test
    public void testNormalSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }

    @Test
    public void testUnionSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN select * from test.table1 union all select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }

    @Test
    public void testInsertSelectSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        String queryStr = "EXPLAIN insert into test.table1 select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("`k1` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy ON test.table1 FOR test_policy");
    }

    @Test
    public void testDuplicateAddPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY IF NOT EXISTS test_row_policy1 ON test.table1 AS PERMISSIVE"
                + " TO test_policy USING (k1 = 1)");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the policy test_row_policy1 already create",
                () -> createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE"
                        + " TO test_policy USING (k1 = 1)"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
    }

    @Test
    public void testNoAuth() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "CreatePolicyStmt command denied to user 'root'@'%' for table 'table1'",
                () -> createPolicy(
                        "CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)"));
    }

    @Test
    public void testShowPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        ShowPolicyStmt showPolicyStmt =
                (ShowPolicyStmt) parseAndAnalyzeStmt("SHOW ROW POLICY");
        int firstSize = Env.getCurrentEnv().getPolicyMgr().showPolicy(showPolicyStmt).getResultRows().size();
        Assertions.assertTrue(firstSize > 0);
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
        int secondSize = Env.getCurrentEnv().getPolicyMgr().showPolicy(showPolicyStmt).getResultRows().size();
        Assertions.assertEquals(2, firstSize - secondSize);
    }

    @Test
    public void testDropPolicy() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY IF EXISTS test_row_policy5 ON test.table1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "the policy test_row_policy1 not exist",
                () -> dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1"));
    }

    @Test
    public void testMergeFilter() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS RESTRICTIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS RESTRICTIVE TO test_policy USING (k2 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy3 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 2)");
        createPolicy("CREATE ROW POLICY test_row_policy4 ON test.table1 AS PERMISSIVE TO test_policy USING (k2 = 1)");
        String queryStr = "EXPLAIN select * from test.table1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assertions.assertTrue(explainString.contains("`k1` = 1, `k2` = 1, `k2` = 2 OR `k2` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy3 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy4 ON test.table1");
    }

    @Test
    public void testComplexSql() throws Exception {
        createPolicy("CREATE ROW POLICY test_row_policy1 ON test.table1 AS RESTRICTIVE TO test_policy USING (k1 = 1)");
        createPolicy("CREATE ROW POLICY test_row_policy2 ON test.table1 AS RESTRICTIVE TO test_policy USING (k2 = 1)");
        String joinSql = "select * from table1 join table2 on table1.k1=table2.k1";
        System.out.println(getSQLPlanOrErrorMsg(joinSql));
        Assertions.assertTrue(getSQLPlanOrErrorMsg(joinSql).contains("PREDICATES: `k1` = 1, `k2` = 1"));
        String unionSql = "select * from table1 union select * from table2";
        Assertions.assertTrue(getSQLPlanOrErrorMsg(unionSql).contains("PREDICATES: `k1` = 1, `k2` = 1"));
        String subQuerySql = "select * from table2 where k1 in (select k1 from table1)";
        Assertions.assertTrue(getSQLPlanOrErrorMsg(subQuerySql).contains("PREDICATES: `k1` = 1, `k2` = 1"));
        String aliasSql = "select * from table1 t1 join table2 t2 on t1.k1=t2.k1";
        Assertions.assertTrue(getSQLPlanOrErrorMsg(aliasSql).contains("PREDICATES: `t1`.`k1` = 1, `t1`.`k2` = 1"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON test.table1");
        dropPolicy("DROP ROW POLICY test_row_policy2 ON test.table1");
    }

    @Test
    public void testReadWrite() throws IOException, AnalysisException {
        PolicyTypeEnum type = PolicyTypeEnum.ROW;
        String policyName = "policy_name";
        long dbId = 10;
        UserIdentity user = new UserIdentity("test_policy", "%");
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        String originStmt = "CREATE ROW POLICY test_row_policy ON test.table1"
                + " AS PERMISSIVE TO test_policy USING (k1 = 1)";
        long tableId = 100;
        FilterType filterType = FilterType.PERMISSIVE;
        Expr wherePredicate = null;

        Policy rowPolicy = new RowPolicy(10000, policyName, dbId, user, originStmt, tableId, filterType,
                wherePredicate);

        ByteArrayOutputStream emptyOutputStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(emptyOutputStream);
        rowPolicy.write(output);
        byte[] bytes = emptyOutputStream.toByteArray();
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));

        Policy newPolicy = Policy.read(input);
        Assertions.assertTrue(newPolicy instanceof RowPolicy);
        RowPolicy newRowPolicy = (RowPolicy) newPolicy;
        Assertions.assertEquals(rowPolicy.getPolicyId(), newRowPolicy.getPolicyId());
        Assertions.assertEquals(type, newRowPolicy.getType());
        Assertions.assertEquals(policyName, newRowPolicy.getPolicyName());
        Assertions.assertEquals(dbId, newRowPolicy.getDbId());
        user.analyze(SystemInfoService.DEFAULT_CLUSTER);
        newRowPolicy.getUser().analyze(SystemInfoService.DEFAULT_CLUSTER);
        Assertions.assertEquals(user.getQualifiedUser(), newRowPolicy.getUser().getQualifiedUser());
        Assertions.assertEquals(originStmt, newRowPolicy.getOriginStmt());
        Assertions.assertEquals(tableId, newRowPolicy.getTableId());
        Assertions.assertEquals(filterType, newRowPolicy.getFilterType());
    }
}
