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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowTableStmtTest {
    private Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("", null, false, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW TABLES FROM internal.testDb", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertFalse(stmt.isVerbose());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_testDb", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowTableStmt("abc", null, true, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW FULL TABLES FROM internal.abc", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_abc", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Table_type", stmt.getMetaData().getColumn(1).getName());

        stmt = new ShowTableStmt("abc", null, true, "bcd");
        stmt.analyze(analyzer);
        Assert.assertEquals("bcd", stmt.getPattern());
        Assert.assertEquals("SHOW FULL TABLES FROM internal.abc LIKE 'bcd'", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_abc", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Table_type", stmt.getMetaData().getColumn(1).getName());
    }

    @Test
    public void testNoDb() {
        ShowTableStmt stmt = new ShowTableStmt("", null, false, null);
        try {
            stmt.analyze(AccessTestUtil.fetchEmptyDbAnalyzer());
        } catch (AnalysisException e) {
            return;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        Assert.fail("No exception throws");
    }
}
