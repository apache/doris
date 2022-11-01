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

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CheckRowPolicyTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("CheckRowPolicyTest");
        useDatabase("CheckRowPolicyTest");
        createTable("create table table1\n"
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
    public void checkUser() throws AnalysisException, org.apache.doris.common.AnalysisException {
        UnboundRelation unboundRelation = new UnboundRelation(ImmutableList.of("table1"));

        useUser("root");
        Plan plan = PlanRewriter.topDownRewrite(unboundRelation, connectContext, new CheckRowPolicy());
        Assertions.assertTrue(plan instanceof UnboundRelation);
        Assertions.assertTrue(((UnboundRelation) plan).isPolicyChecked());

        useUser("notFound");
        plan = PlanRewriter.topDownRewrite(unboundRelation, connectContext, new CheckRowPolicy());
        Assertions.assertTrue(plan instanceof UnboundRelation);
        Assertions.assertTrue(((UnboundRelation) plan).isPolicyChecked());

        useUser("test_policy");
    }

    @Test
    public void checkNoPolicy() {
        UnboundRelation unboundRelation = new UnboundRelation(ImmutableList.of("table1"));
        Plan plan = PlanRewriter.topDownRewrite(unboundRelation, connectContext, new CheckRowPolicy());

        Assertions.assertTrue(plan instanceof UnboundRelation);
        Assertions.assertTrue(((UnboundRelation) plan).isPolicyChecked());
    }

    @Test
    public void checkOriginalPolicy() throws Exception {
        connectContext.getSessionVariable().setEnableNereidsPlanner(false);
        UnboundRelation unboundRelation = new UnboundRelation(ImmutableList.of("table1"));
        createPolicy("CREATE ROW POLICY test_row_policy1 ON table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanRewriter.topDownRewrite(unboundRelation, connectContext, new CheckRowPolicy()));
        Assertions.assertTrue(exception.getMessage().contains("Invaild row policy"));
        dropPolicy("DROP ROW POLICY test_row_policy1 ON table1");
    }

    @Test
    public void checkNereidsPolicy() throws Exception {
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        UnboundRelation unboundRelation = new UnboundRelation(ImmutableList.of("table1"));
        createPolicy("CREATE ROW POLICY test_row_policy1 ON table1 AS PERMISSIVE TO test_policy USING (k1 = 1)");
        Plan plan = PlanRewriter.topDownRewrite(unboundRelation, connectContext, new CheckRowPolicy());
        dropPolicy("DROP ROW POLICY test_row_policy1 ON table1");

        Assertions.assertTrue(plan instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) plan;

        Assertions.assertTrue(filter.child() instanceof UnboundRelation);
        UnboundRelation relation = (UnboundRelation) filter.child();
        Assertions.assertTrue(relation.isPolicyChecked());

        Assertions.assertTrue(filter.getPredicates() instanceof EqualTo);
        EqualTo equalTo = (EqualTo) filter.getPredicates();
        Assertions.assertTrue(equalTo.toString().contains("k1 = 1"));
    }
}
