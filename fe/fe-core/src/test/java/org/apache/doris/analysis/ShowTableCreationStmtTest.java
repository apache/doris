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

import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowTableCreationStmtTest {
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
    public void testNormal() throws UserException {
        ShowTableCreationStmt stmt = new ShowTableCreationStmt("doris", "log");
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW TABLE CREATION FROM `doris` LIKE `log`", stmt.toString());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Table", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("Status", stmt.getMetaData().getColumn(2).getName());
    }

    @Test
    public void testNoDb() throws UserException {
        ShowTableCreationStmt stmt = new ShowTableCreationStmt(null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testDb", stmt.getDbName());
    }
}
