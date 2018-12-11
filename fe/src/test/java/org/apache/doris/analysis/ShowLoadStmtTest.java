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
import org.apache.doris.system.SystemInfoService;

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
@PrepareForTest(Catalog.class)
public class ShowLoadStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    private SystemInfoService systemInfoService;


    @Before
    public void setUp() {
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.replay(systemInfoService);

        catalog = AccessTestUtil.fetchAdminCatalog();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testCluster:testDb").anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn("testCluster:testUser").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.replay(analyzer);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        ShowAlterStmt stmt = new ShowAlterStmt(AlterType.COLUMN, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW ALTER COLUMN FROM `testCluster:testDb`", stmt.toString());

        stmt = new ShowAlterStmt(AlterType.ROLLUP, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW ALTER ROLLUP FROM `testCluster:testDb`", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(analyzer);

        ShowAlterStmt stmt = new ShowAlterStmt(AlterType.ROLLUP, null);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
