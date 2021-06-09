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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CancelLoadStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    FakeCatalog fakeCatalog;

    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();

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
        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("doris_test_label");

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        CancelLoadStmt stmt = new CancelLoadStmt(null, binaryPredicate);
        stmt.analyze(analyzer);
        Assert.assertTrue(stmt.isAccurateMatch());
        Assert.assertEquals("CANCEL LOAD FROM testCluster:testDb WHERE `label` = 'doris_test_label'", stmt.toString());

        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        stmt = new CancelLoadStmt(null, likePredicate);
        stmt.analyze(analyzer);
        Assert.assertFalse(stmt.isAccurateMatch());
        Assert.assertEquals("CANCEL LOAD FROM testCluster:testDb WHERE `label` LIKE 'doris_test_label'", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("doris_test_label");
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

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        CancelLoadStmt stmt = new CancelLoadStmt(null, binaryPredicate);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
