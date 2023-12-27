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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;

import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowLoadStmtTest {
    private Analyzer analyzer;
    private Env env;

    private SystemInfoService systemInfoService;

    FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();

        systemInfoService = AccessTestUtil.fetchSystemInfoService();
        FakeEnv.setSystemInfo(systemInfoService);

        env = AccessTestUtil.fetchAdminCatalog();
        FakeEnv.setEnv(env);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                analyzer.getEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb`", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "";
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
        Assert.assertEquals("SHOW LOAD FROM `testDb`", stmt.toString());

        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        stmt = new ShowLoadStmt(null, binaryPredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb` WHERE `label` = \'abc\' LIMIT 10", stmt.toString());

        StringLiteral stringLiteralLike = new StringLiteral("ab%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteralLike);

        stmt = new ShowLoadStmt(null, likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb` WHERE `label` LIKE \'ab%\' LIMIT 10", stmt.toString());

        BinaryPredicate statePredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "state"), new StringLiteral("PENDING"));
        stmt = new ShowLoadStmt(null, statePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW LOAD FROM `testDb` WHERE `state` = \'PENDING\' LIMIT 10", stmt.toString());
    }

    @Test
    public void testInvalidWhereClause() throws AnalysisException {
        //test:  WHERE label="abc" AND label LIKE "def";  --> AnalysisException
        SlotRef slotRef1 = new SlotRef(null, "label");
        StringLiteral stringLiteral1 = new StringLiteral("abc");
        BinaryPredicate binaryPredicate1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef1, stringLiteral1);

        SlotRef slotRef2 = new SlotRef(null, "label");
        StringLiteral stringLiteral2 = new StringLiteral("def");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef2, stringLiteral2);

        CompoundPredicate compoundPredicate1 = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate1, likePredicate);
        ShowLoadStmt stmt1 = new ShowLoadStmt(null, compoundPredicate1, null, null);

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "column names on both sides of operator AND should be diffrent",
                () -> stmt1.analyze(analyzer));

        //test: WHERE state="abc" AND state="def";  --> AnalysisException
        SlotRef slotRef3 = new SlotRef(null, "state");
        StringLiteral stringLiteral3 = new StringLiteral("abc");
        BinaryPredicate binaryPredicate3 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef3, stringLiteral3);

        SlotRef slotRef4 = new SlotRef(null, "state");
        StringLiteral stringLiteral4 = new StringLiteral("def");
        BinaryPredicate binaryPredicate4 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef4, stringLiteral4);

        CompoundPredicate compoundPredicate2 = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate3, binaryPredicate4);
        ShowLoadStmt stmt2 = new ShowLoadStmt(null, compoundPredicate2, null, null);

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "column names on both sides of operator AND should be diffrent",
                () -> stmt2.analyze(analyzer));


    }
}
