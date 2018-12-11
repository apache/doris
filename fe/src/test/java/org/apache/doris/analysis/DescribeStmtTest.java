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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class, ConnectContext.class })
public class DescribeStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");

        PowerMock.mockStatic(ConnectContext.class);
        EasyMock.expect(ConnectContext.get()).andReturn(ctx).anyTimes();
        PowerMock.replay(ConnectContext.class);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        catalog = AccessTestUtil.fetchAdminCatalog();
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(AccessTestUtil.fetchSystemInfoService()).anyTimes();
        PowerMock.replay(Catalog.class);

    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        DescribeStmt stmt = new DescribeStmt(new TableName("", "testTbl"), false);
        stmt.analyze(analyzer);
        Assert.assertEquals("DESCRIBE `testCluster:testDb.testTbl`", stmt.toString());
        Assert.assertEquals(6, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());
    }

    @Test
    public void testAllNormal() throws AnalysisException, UserException {
        DescribeStmt stmt = new DescribeStmt(new TableName("", "testTbl"), true);
        stmt.analyze(analyzer);
        Assert.assertEquals("DESCRIBE `testCluster:testDb.testTbl` ALL", stmt.toString());
        Assert.assertEquals(7, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());
    }
}
