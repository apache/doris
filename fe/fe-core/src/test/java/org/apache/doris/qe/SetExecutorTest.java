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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.SetNamesVar;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SetExecutorTest {
    private Analyzer analyzer;
    private ConnectContext ctx;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private Auth auth;

    @Before
    public void setUp() throws DdlException {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        ctx = new ConnectContext();
        ctx.setEnv(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUser = new UserIdentity("root", "192.168.1.1");
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setThreadLocalInfo();

        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.setPassword((SetPassVar) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar(new UserIdentity("testUser", "%"),
                new PassVar("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", false)));
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SetVar("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        stmt.analyze(analyzer);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
    }
}
