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

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SetStmtTest {
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
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        List<SetVar> vars = Lists.newArrayList(new SetVar("times", new IntLiteral(100L)),
                new SetVar(SetType.GLOBAL, "names", new StringLiteral("utf-8")));
        SetStmt stmt = new SetStmt(vars);

        stmt.analyze(analyzer);

        Assert.assertEquals("SET DEFAULT times = 100, GLOBAL names = 'utf-8'", stmt.toString());
        Assert.assertEquals(vars, stmt.getSetVars());
    }

    @Test(expected = AnalysisException.class)
    public void testNoVar() throws UserException, AnalysisException {
        SetStmt stmt = new SetStmt(Lists.<SetVar>newArrayList());

        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNullVar() throws UserException, AnalysisException {
        SetStmt stmt = new SetStmt(null);

        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
