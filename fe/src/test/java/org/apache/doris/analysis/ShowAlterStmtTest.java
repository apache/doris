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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.validation.constraints.Null;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class ShowAlterStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;
    private SystemInfoService systemInfo;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();
        systemInfo = new SystemInfoService();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfo).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testDb").anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn("testUser").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(analyzer);
    }

    @Test
    public void testAlterStmt1() throws UserException, AnalysisException {
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.COLUMN,null, null,
                null,null);
        stmt.analyzeSyntax(analyzer);
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `testDb`", stmt.toString());
    }
    
    @Test
    public void testAlterStmt2() throws UserException, AnalysisException {
        SlotRef slotRef = new SlotRef(null, "TableName");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.COLUMN, null, binaryPredicate, null,
                new LimitElement(1,2));
        stmt.analyzeSyntax(analyzer);
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `testDb` WHERE `TableName` = \'abc\' LIMIT 1, 2",
                stmt.toString());
    }
    
    @Test
    public void testAlterStmt3() throws UserException, AnalysisException {
        SlotRef slotRef = new SlotRef(null, "CreateTime");
        StringLiteral stringLiteral = new StringLiteral("2019-12-04");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.COLUMN, null, binaryPredicate, null, null);
        stmt.analyzeSyntax(analyzer);
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `testDb` WHERE `CreateTime` = \'2019-12-04 00:00:00\'",
                stmt.toString());
    }
}
