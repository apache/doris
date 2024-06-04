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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropTableStmtTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private TableName tbl;
    private TableName noDbTbl;
    private Analyzer analyzer;
    @Mocked
    private Analyzer noDbAnalyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        tbl = new TableName(internalCtl, "db1", "table1");
        noDbTbl = new TableName(internalCtl, "", "table1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);

        new Expectations() {
            {
                noDbAnalyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                noDbAnalyzer.getDefaultDb();
                minTimes = 0;
                result = "";
            }
        };

        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, tbl, true);
        stmt.analyze(analyzer);
        Assert.assertEquals("db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `db1`.`table1`", stmt.toString());
    }

    @Test
    public void testDefaultNormal() throws UserException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl, true);
        stmt.analyze(analyzer);
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `testDb`.`table1`", stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDbFail() throws UserException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl, true);
        stmt.analyze(noDbAnalyzer);
        Assert.fail("No Exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoTableFail() throws UserException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, new TableName(internalCtl, "db1", ""), true);
        stmt.analyze(noDbAnalyzer);
        Assert.fail("No Exception throws.");
    }

}
