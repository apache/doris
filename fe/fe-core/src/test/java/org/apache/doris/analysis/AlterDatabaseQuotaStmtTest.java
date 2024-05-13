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

import org.apache.doris.analysis.AlterDatabaseQuotaStmt.QuotaType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AlterDatabaseQuotaStmtTest {
    private Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

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
            }
        };
    }

    private void testAlterDatabaseDataQuotaStmt(String dbName, String quotaQuantity, long quotaSize)
            throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt(dbName, QuotaType.DATA, quotaQuantity);
        stmt.analyze(analyzer);
        String expectedSql = "ALTER DATABASE testDb SET DATA QUOTA " + quotaQuantity;
        Assert.assertEquals(expectedSql, stmt.toSql());
        Assert.assertEquals(quotaSize, stmt.getQuota());
    }

    @Test
    public void testNormalAlterDatabaseDataQuotaStmt() throws AnalysisException, UserException {
        // byte
        testAlterDatabaseDataQuotaStmt("testDb", "102400", 102400L);
        testAlterDatabaseDataQuotaStmt("testDb", "102400b", 102400L);

        // kb
        testAlterDatabaseDataQuotaStmt("testDb", "100kb", 100L * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100Kb", 100L * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100KB", 100L * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100K", 100L * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100k", 100L * 1024);

        // mb
        testAlterDatabaseDataQuotaStmt("testDb", "100mb", 100L * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100Mb", 100L * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100MB", 100L * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100M", 100L * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100m", 100L * 1024 * 1024);

        // gb
        testAlterDatabaseDataQuotaStmt("testDb", "100gb", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100Gb", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100GB", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100G", 100L * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100g", 100L * 1024 * 1024 * 1024);

        // tb
        testAlterDatabaseDataQuotaStmt("testDb", "100tb", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100Tb", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100TB", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100T", 100L * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100t", 100L * 1024 * 1024 * 1024 * 1024);

        // tb
        testAlterDatabaseDataQuotaStmt("testDb", "100pb", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100Pb", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100PB", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100P", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
        testAlterDatabaseDataQuotaStmt("testDb", "100p", 100L * 1024 * 1024 * 1024 * 1024 * 1024);
    }

    @Test(expected = AnalysisException.class)
    public void testDataMinusQuota() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.DATA, "-100mb");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testDataInvalidUnit() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.DATA, "100invalid_unit");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testDataInvalidQuantity() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.DATA, "invalid_100mb_quota");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }


    @Test
    public void testNormalAlterDatabaseReplicaQuotaStmt() throws AnalysisException, UserException {
        long quotaSize = 1000;
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.REPLICA, String.valueOf(quotaSize));
        stmt.analyze(analyzer);
        String expectedSql = "ALTER DATABASE testDb SET REPLICA QUOTA 1000";
        Assert.assertEquals(expectedSql, stmt.toSql());
        Assert.assertEquals(quotaSize, stmt.getQuota());
    }

    @Test(expected = AnalysisException.class)
    public void testReplicaMinusQuota() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.REPLICA, "-100");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testReplicaInvalidQuantity() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.REPLICA, "invalid_100_quota");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testNormalAlterDatabaseTransactionQuotaStmt() throws AnalysisException, UserException {
        long quotaSize = 10;
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.TRANSACTION, String.valueOf(quotaSize));
        stmt.analyze(analyzer);
        String expectedSql = "ALTER DATABASE testDb SET TRANSACTION QUOTA 10";
        Assert.assertEquals(expectedSql, stmt.toSql());
        Assert.assertEquals(quotaSize, stmt.getQuota());
    }

    @Test(expected = AnalysisException.class)
    public void testTransactionMinusQuota() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.TRANSACTION, "-100");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testtransactionInvalidQuantity() throws AnalysisException, UserException {
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt("testDb", QuotaType.TRANSACTION, "invalid_100_quota");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
