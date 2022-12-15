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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CancelExportStmtTest extends TestWithFeService {

    private Analyzer analyzer;
    private String dbName = "testDb";
    private String tblName = "table1";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + tblName + "\n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        analyzer = new Analyzer(connectContext.getEnv(), connectContext);
    }

    @Test
    public void testNormal() throws UserException {
        SlotRef labelSlotRef = new SlotRef(null, "label");
        StringLiteral labelStringLiteral = new StringLiteral("doris_test_label");

        SlotRef stateSlotRef = new SlotRef(null, "state");

        BinaryPredicate labelBinaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRef,
                labelStringLiteral);
        CancelExportStmt stmt = new CancelExportStmt(null, labelBinaryPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM default_cluster:testDb WHERE `label` = 'doris_test_label'",
                stmt.toString());

        SlotRef labelSlotRefUpper = new SlotRef(null, "LABEL");
        BinaryPredicate labelBinaryPredicateUpper = new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRefUpper,
                labelStringLiteral);
        CancelExportStmt stmtUpper = new CancelExportStmt(null, labelBinaryPredicateUpper);
        stmtUpper.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM default_cluster:testDb WHERE `LABEL` = 'doris_test_label'",
                stmtUpper.toString());

        StringLiteral stateStringLiteral = new StringLiteral("PENDING");
        BinaryPredicate stateBinaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef,
                stateStringLiteral);
        stmt = new CancelExportStmt(null, stateBinaryPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM default_cluster:testDb WHERE `state` = 'PENDING'", stmt.toString());

        LikePredicate labelLikePredicate = new LikePredicate(LikePredicate.Operator.LIKE, labelSlotRef,
                labelStringLiteral);
        stmt = new CancelExportStmt(null, labelLikePredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM default_cluster:testDb WHERE `label` LIKE 'doris_test_label'",
                stmt.toString());

        CompoundPredicate compoundAndPredicate = new CompoundPredicate(Operator.AND, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelExportStmt(null, compoundAndPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL EXPORT FROM default_cluster:testDb WHERE `label` = 'doris_test_label' AND `state` = 'PENDING'",
                stmt.toString());

        CompoundPredicate compoundOrPredicate = new CompoundPredicate(Operator.OR, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelExportStmt(null, compoundOrPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL EXPORT FROM default_cluster:testDb WHERE `label` = 'doris_test_label' OR `state` = 'PENDING'",
                stmt.toString());
    }

    @Test
    public void testError() {
        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral = new StringLiteral("FINISHED");

        LikePredicate stateLikePredicate =
                new LikePredicate(LikePredicate.Operator.LIKE, stateSlotRef, stateStringLiteral);
        CancelExportStmt stmt = new CancelExportStmt(null, stateLikePredicate);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Only label can use like",
                () -> stmt.analyze(analyzer));
    }
}
