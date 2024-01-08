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

import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class GrantStmtTest {
    private Analyzer analyzer;

    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Mocked
    private Env env;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        Auth auth = new Auth();
        AccessControllerManager accessManager = new AccessControllerManager(auth);

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

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        GrantStmt stmt;

        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals("testUser", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("testDb", stmt.getTblPattern().getQualifiedDb());

        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.READ_ONLY), new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
    }

    @Test
    public void testResourceNormal() throws UserException {
        String resourceName = "spark0";
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));
        GrantStmt stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new ResourcePattern(resourceName), privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals(resourceName, stmt.getResourcePattern().getResourceName());
        Assert.assertEquals(Auth.PrivLevel.RESOURCE, stmt.getResourcePattern().getPrivLevel());

        stmt = new GrantStmt(new UserIdentity("testUser", "%"), null, new ResourcePattern("*"), privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals(Auth.PrivLevel.GLOBAL, stmt.getResourcePattern().getPrivLevel());
        Assert.assertEquals("GRANT Usage_priv ON RESOURCE '*' TO 'testUser'@'%'", stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testUserFail() throws AnalysisException, UserException {
        GrantStmt stmt;

        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        stmt = new GrantStmt(new UserIdentity("", "%"), null, new TablePattern("testDb", "*"), privileges);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
