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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowExportStmtTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws UserException {
        ShowExportStmt stmt = new ShowExportStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW EXPORT FROM `testDb`", stmt.toString());
    }

    @Test
    public void testWhere() throws UserException {
        ShowExportStmt stmt = new ShowExportStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW EXPORT FROM `testDb`", stmt.toString());

        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);

        stmt = new ShowExportStmt(null, binaryPredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW EXPORT FROM `testDb` WHERE `label` = 'abc' LIMIT 10", stmt.toString());
        Assert.assertFalse(stmt.isLabelUseLike());

        StringLiteral stringLiteralLike = new StringLiteral("ab%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteralLike);

        stmt = new ShowExportStmt(null, likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW EXPORT FROM `testDb` WHERE `label` LIKE 'ab%' LIMIT 10", stmt.toString());
        Assert.assertTrue(stmt.isLabelUseLike());

        BinaryPredicate statePredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "state"), new StringLiteral("PENDING"));
        stmt = new ShowExportStmt(null, statePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW EXPORT FROM `testDb` WHERE `state` = 'PENDING' LIMIT 10", stmt.toString());
    }

    @Test
    public void testInvalidWhereClause() {
        //test:  WHERE label="abc" AND id = 1;  --> AnalysisException
        SlotRef slotRef1 = new SlotRef(null, "label");
        StringLiteral stringLiteral1 = new StringLiteral("abc");
        BinaryPredicate binaryPredicate1 = new BinaryPredicate(Operator.EQ, slotRef1, stringLiteral1);

        SlotRef slotRef2 = new SlotRef(null, "id");
        IntLiteral intLiteral2 = new IntLiteral(1);
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef2, intLiteral2);

        CompoundPredicate compoundPredicate1 = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate1, likePredicate);
        ShowExportStmt stmt1 = new ShowExportStmt(null, compoundPredicate1, null, null);

        ExceptionChecker.expectThrows(AnalysisException.class, () -> stmt1.analyze(analyzer));

    }
}
