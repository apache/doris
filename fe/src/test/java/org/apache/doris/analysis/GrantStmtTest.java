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

import mockit.Expectations;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Mocked;

public class GrantStmtTest {
    private Analyzer analyzer;

    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Mocked
    private Catalog catalog;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        auth = new PaloAuth();

        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.0.1";

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testUser", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("testCluster:testDb", stmt.getTblPattern().getQuolifiedDb());

        privileges = Lists.newArrayList(AccessPrivilege.READ_ONLY, AccessPrivilege.ALL);
        stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testUserFail() throws AnalysisException, UserException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt(new UserIdentity("", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
        Assert.fail("No exeception throws.");
    }
}