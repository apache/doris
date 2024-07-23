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
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SetPassVarTest {
    private Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        UserIdentity currentUser = new UserIdentity("root", "192.168.1.1");
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        SetPassVar stmt;

        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"),
                new PassVar("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", false));
        stmt.analyze(analyzer);
        Assert.assertEquals("testUser", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", new String(stmt.getPassword()));
        Assert.assertEquals("SET PASSWORD FOR 'testUser'@'%' = '*XXX'",
                stmt.toString());

        // empty password
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), new PassVar("", true));
        stmt.analyze(analyzer);
        Assert.assertEquals("SET PASSWORD FOR 'testUser'@'%' = '*XXX'", stmt.toString());

        // empty user
        // empty password
        stmt = new SetPassVar(null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SET PASSWORD FOR 'root'@'192.168.1.1' = '*XXX'", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testBadPassword() throws UserException, AnalysisException {
        SetPassVar stmt;
        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"),
                new PassVar("*88EEBAHD913688E7278E2AD071FDB5E76D76D34B", false));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

}
