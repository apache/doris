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


import org.apache.doris.analysis.ShowAlterStmt.AlterType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
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
public class CancelAlterStmtTest {

    private Analyzer analyzer;
    private Catalog catalog;

    private ConnectContext ctx;

    private PaloAuth auth;

    @Before
    public void setUp() {
        auth = new PaloAuth();

        ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");

        catalog = AccessTestUtil.fetchAdminCatalog();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        PowerMock.mockStatic(ConnectContext.class);
        EasyMock.expect(ConnectContext.get()).andReturn(ctx).anyTimes();
        PowerMock.replay(ConnectContext.class);

        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testDb").anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn("testUser").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.replay(analyzer);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        // cancel alter column
        CancelAlterTableStmt stmt = new CancelAlterTableStmt(AlterType.COLUMN, new TableName(null, "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("CANCEL ALTER COLUMN FROM `testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals(AlterType.COLUMN, stmt.getAlterType());
        Assert.assertEquals("testTbl", stmt.getTableName());

        stmt = new CancelAlterTableStmt(AlterType.ROLLUP, new TableName(null, "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("CANCEL ALTER ROLLUP FROM `testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals(AlterType.ROLLUP, stmt.getAlterType());
    }
}
