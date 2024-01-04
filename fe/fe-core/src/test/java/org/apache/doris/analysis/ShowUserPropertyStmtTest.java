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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.UserPropertyProcNode;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ShowUserPropertyStmtTest {
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
    public void testNormal() throws UserException {
        ShowUserPropertyStmt stmt = new ShowUserPropertyStmt("testUser", "%load_cluster%", false);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW PROPERTY FOR 'testCluster:testUser' LIKE '%load_cluster%'", stmt.toString());
        List<Column> columns = stmt.getMetaData().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Assert.assertEquals(columns.get(i).getName(), UserPropertyProcNode.TITLE_NAMES.get(i));
        }
    }

    @Test
    public void testAll() throws UserException {
        ShowUserPropertyStmt stmt = new ShowUserPropertyStmt(null, "%load_cluster%", true);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW ALL PROPERTIES LIKE '%load_cluster%'", stmt.toString());
        List<Column> columns = stmt.getMetaData().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Assert.assertEquals(columns.get(i).getName(), UserPropertyProcNode.ALL_USER_TITLE_NAMES.get(i));
        }
    }

    @Test
    public void testError() {
        ShowUserPropertyStmt stmt = new ShowUserPropertyStmt("testUser", "%load_cluster%", true);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("ALL"));
        }

    }
}
