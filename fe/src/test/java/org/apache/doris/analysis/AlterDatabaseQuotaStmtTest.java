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
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.internal.startup.Startup;

public class AlterDatabaseQuotaStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

        new NonStrictExpectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "100mb");
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER DATABASE testCluster:testDb SET DATA QUOTA 100mb",
                stmt.toSql());
        Assert.assertEquals(100 * 1024 * 1024, stmt.getQuota());

        AlterDatabaseQuotaStmt stmt_2 = new AlterDatabaseQuotaStmt("testDb", "102400");
        stmt_2.analyze(analyzer);
        Assert.assertEquals("ALTER DATABASE testCluster:testDb SET DATA QUOTA 102400",
                stmt_2.toSql());
        Assert.assertEquals(102400, stmt_2.getQuota());
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidQuota() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "-100mb");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidUnit() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "100s");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
