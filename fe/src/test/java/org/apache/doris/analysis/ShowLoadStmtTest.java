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

import mockit.Expectations;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowLoadStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    private SystemInfoService systemInfoService;

    FakeCatalog fakeCatalog;

    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();

        systemInfoService = AccessTestUtil.fetchSystemInfoService();
        FakeCatalog.setSystemInfo(systemInfoService);

        catalog = AccessTestUtil.fetchAdminCatalog();
        FakeCatalog.setCatalog(catalog);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testCluster:testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:testUser";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";

                analyzer.getCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testCluster:testDb`", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
            }
        };

        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testWhere() throws UserException, AnalysisException {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testCluster:testDb`", stmt.toString());

        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        stmt = new ShowLoadStmt(null, binaryPredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testCluster:testDb` WHERE `label` = \'abc\' LIMIT 10", stmt.toString());

        LikePredicate likePredicate = new LikePredicate(org.apache.doris.analysis.LikePredicate.Operator.LIKE,
                                                        slotRef, stringLiteral);
        stmt = new ShowLoadStmt(null, likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testCluster:testDb` WHERE `label` LIKE \'abc\' LIMIT 10", stmt.toString());
    }
}
