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

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class ShowAlterStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("testDb").anyTimes();
        EasyMock.expect(analyzer.getQualifiedUser()).andReturn("testUser").anyTimes();
        EasyMock.expect(analyzer.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(analyzer);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb`", stmt.toString());

        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        stmt = new ShowLoadStmt(null, binaryPredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb` WHERE `label` = \'abc\' LIMIT 10", stmt.toString());

        LikePredicate likePredicate = new LikePredicate(org.apache.doris.analysis.LikePredicate.Operator.LIKE,
                                                        slotRef, stringLiteral);
        stmt = new ShowLoadStmt(null, likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb` WHERE `label` LIKE \'abc\' LIMIT 10", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(analyzer);

        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
