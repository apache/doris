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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class CheckRowPolicyTest extends TestWithFeService {

    private static String dbName = "check_row_policy";
    private static String fullDbName = "" + dbName;
    private static String tableName = "table1";
    private static String userName = "user1";
    private static String policyName = "policy1";

    private static OlapTable olapTable = new OlapTable(0L, tableName,
            ImmutableList.<Column>of(new Column("k1", Type.INT, false, AggregateType.NONE, "0", ""),
                    new Column("k2", Type.INT, false, AggregateType.NONE, "0", "")),
            KeysType.PRIMARY_KEYS, new PartitionInfo(), null);

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table "
                + tableName
                + " (k1 int, k2 int) distributed by hash(k1) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException("table1").getId();
        olapTable.setId(tableId);
        olapTable.setIndexMeta(-1,
                olapTable.getName(),
                olapTable.getFullSchema(),
                0, 0, (short) 0,
                TStorageType.COLUMN,
                KeysType.PRIMARY_KEYS);
        // create user
        UserIdentity user = new UserIdentity(userName, "%");
        user.analyze();
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(user));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV));
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze();
        GrantStmt grantStmt = new GrantStmt(user, null, tablePattern, privileges);
        Analyzer analyzer = new Analyzer(connectContext.getEnv(), connectContext);
        grantStmt.analyze(analyzer);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
    }

    @Test
    public void checkUser() throws AnalysisException, org.apache.doris.common.AnalysisException {
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable, Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);

        useUser("root");
        Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);

        useUser("notFound");
        plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);
    }

    @Test
    public void checkNoPolicy() throws org.apache.doris.common.AnalysisException {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable, Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        Plan plan = PlanRewriter.bottomUpRewrite(checkPolicy, connectContext, new CheckPolicy());
        Assertions.assertEquals(plan, relation);
    }

    @Test
    public void checkOnePolicy() throws Exception {
        useUser(userName);
        LogicalRelation relation = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable, Arrays.asList(fullDbName));
        LogicalCheckPolicy<LogicalRelation> checkPolicy = new LogicalCheckPolicy<>(relation);
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
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

        dropPolicy("DROP ROW POLICY " + policyName);
    }
}
