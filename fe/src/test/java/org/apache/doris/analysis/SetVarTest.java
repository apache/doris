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
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import org.apache.doris.qe.SqlModeHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;
import mockit.internal.startup.Startup;

public class SetVarTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        SetVar var = new SetVar(SetType.DEFAULT, "names", new StringLiteral("utf-8"));
        var.analyze(analyzer);

        Assert.assertEquals(SetType.DEFAULT, var.getType());
        var.setType(SetType.GLOBAL);
        Assert.assertEquals(SetType.GLOBAL, var.getType());
        Assert.assertEquals("names", var.getVariable());
        Assert.assertEquals("utf-8", var.getValue().getStringValue());

        Assert.assertEquals("GLOBAL names = 'utf-8'", var.toString());

        var = new SetVar("times", new IntLiteral(100L));
        var.analyze(analyzer);
        Assert.assertEquals("DEFAULT times = 100", var.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoVariable() throws UserException, AnalysisException {
        SetVar var = new SetVar(SetType.DEFAULT, "", new StringLiteral("utf-8"));
        var.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidSqlModeValue() throws UserException, AnalysisException {
        SetVar var = new SetVar(SetType.SESSION, "sql_mode", new IntLiteral(SqlModeHelper.MODE_LAST));
        var.analyze(analyzer);
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidSqlMode() throws UserException, AnalysisException {
        SetVar var = new SetVar(SetType.SESSION, "sql_mode", new StringLiteral("WRONG_MODE"));
        var.analyze(analyzer);
        Assert.fail("No exception throws");
    }
}