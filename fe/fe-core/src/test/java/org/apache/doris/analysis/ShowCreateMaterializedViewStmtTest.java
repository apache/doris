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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * test for ShowCreateMaterializedViewStmt
 **/
public class ShowCreateMaterializedViewStmtTest {

    private static String runningDir = "fe/mocked/ShowCreateMaterializedViewStmtTest/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    private static DorisAssert dorisAssert;

    /**
     * init.
     **/
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        dorisAssert = new DorisAssert(connectContext);
        dorisAssert.withDatabase("test")
                .withTable("create table test.table1 (k1 int, k2 int) distributed by hash(k1) buckets 1 properties(\"replication_num\" = \"1\");")
                .withMaterializedView("CREATE MATERIALIZED VIEW test_mv as select k1 from test.table1;");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testNormal() throws Exception {
        String showMvSql = "SHOW CREATE MATERIALIZED VIEW test_mv on test.table1;";
        ShowCreateMaterializedViewStmt showStmt =
                (ShowCreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(showMvSql, connectContext);
        ShowExecutor executor = new ShowExecutor(connectContext, showStmt);
        Assert.assertEquals(executor.execute().getResultRows().get(0).get(2),
                "CREATE MATERIALIZED VIEW test_mv as select k1 from test.table1;");
    }

    @Test
    public void testNoAuth() throws Exception {
        StmtExecutor createUser = new StmtExecutor(connectContext, "create user 'test'");
        createUser.execute();
        UserIdentity userIdentity = new UserIdentity("test", "192.168.1.1");
        userIdentity.setIsAnalyzed();
        ConnectContext.get().setCurrentUserIdentity(userIdentity);
        ConnectContext.get().setQualifiedUser("test");
        String showMvSql = "SHOW CREATE MATERIALIZED VIEW test_mv on test.table1;";
        ExceptionChecker.expectThrows(
                AnalysisException.class, () -> UtFrameUtils.parseAndAnalyzeStmt(showMvSql, connectContext));
    }

}
