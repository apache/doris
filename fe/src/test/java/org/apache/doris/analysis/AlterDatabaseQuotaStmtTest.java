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

public class AlterDatabaseQuotaStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
    }
    
    private void testAlterDatabaseQuotaStmt(String dbName, String quotaQuantity, long quotaSize)
            throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt(dbName, quotaQuantity);
        stmt.analyze(analyzer);
        String expectedSql = "ALTER DATABASE testCluster:testDb SET DATA QUOTA " + quotaQuantity;
        Assert.assertEquals(expectedSql, stmt.toSql());
        Assert.assertEquals(quotaSize, stmt.getQuota());
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        // byte
        testAlterDatabaseQuotaStmt("testDb", "102400", 102400L);
        testAlterDatabaseQuotaStmt("testDb", "102400b", 102400L);

        // kb
        testAlterDatabaseQuotaStmt("testDb", "100kb", 100L * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100Kb", 100L * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100KB", 100L * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100K", 100L * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100k", 100L * 1024);

        // mb
        testAlterDatabaseQuotaStmt("testDb", "100mb", 100L * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100Mb", 100L * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100MB", 100L * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100M", 100L * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100m", 100L * 1024 * 1024);

        // gb
        testAlterDatabaseQuotaStmt("testDb", "100gb", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100Gb", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100GB", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100G", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100g", 100L * 1024 * 1024 * 1024);

        // tb
        testAlterDatabaseQuotaStmt("testDb", "100tb", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100Tb", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100TB", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100T", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100t", 100L * 1024 * 1024 * 1024 * 1024);

        // tb
        testAlterDatabaseQuotaStmt("testDb", "100pb", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100Pb", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100PB", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100P", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseQuotaStmt("testDb", "100p", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
    }

    @Test(expected = AnalysisException.class)
    public void testMinusQuota() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "-100mb");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidUnit() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "100invalid_unit");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidQuantity() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", "invalid_100mb_quota");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
