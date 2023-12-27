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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.InsertLoadJob;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CancelLoadStmtTest extends TestWithFeService {

    private Analyzer analyzer;
    private String dbName = "testDb";
    private String tblName = "table1";
    private UserIdentity userInfo = new UserIdentity("root", "localhost");

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
        CancelLoadStmt stmt = new CancelLoadStmt(null, labelBinaryPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL LOAD FROM testDb WHERE `label` = 'doris_test_label'",
                stmt.toString());

        SlotRef labelSlotRefUpper = new SlotRef(null, "LABEL");
        BinaryPredicate labelBinaryPredicateUpper = new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRefUpper,
                labelStringLiteral);
        CancelLoadStmt stmtUpper = new CancelLoadStmt(null, labelBinaryPredicateUpper);
        stmtUpper.analyze(analyzer);
        Assertions.assertEquals("CANCEL LOAD FROM testDb WHERE `LABEL` = 'doris_test_label'",
                stmtUpper.toString());

        StringLiteral stateStringLiteral = new StringLiteral("LOADING");
        BinaryPredicate stateBinaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef,
                stateStringLiteral);
        stmt = new CancelLoadStmt(null, stateBinaryPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL LOAD FROM testDb WHERE `state` = 'LOADING'", stmt.toString());

        LikePredicate labelLikePredicate = new LikePredicate(LikePredicate.Operator.LIKE, labelSlotRef,
                labelStringLiteral);
        stmt = new CancelLoadStmt(null, labelLikePredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL LOAD FROM testDb WHERE `label` LIKE 'doris_test_label'",
                stmt.toString());

        CompoundPredicate compoundAndPredicate = new CompoundPredicate(Operator.AND, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelLoadStmt(null, compoundAndPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL LOAD FROM testDb WHERE `label` = 'doris_test_label' AND `state` = 'LOADING'",
                stmt.toString());

        CompoundPredicate compoundOrPredicate = new CompoundPredicate(Operator.OR, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelLoadStmt(null, compoundOrPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL LOAD FROM testDb WHERE `label` = 'doris_test_label' OR `state` = 'LOADING'",
                stmt.toString());

        // test match
        List<LoadJob> loadJobs = new ArrayList<>();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("testDb");
        long dbId = db.getId();
        Table tbl = db.getTableNullable(tblName);
        long tblId = tbl.getId();
        InsertLoadJob insertLoadJob1 = new InsertLoadJob("doris_test_label", 1L, dbId, tblId, 0, "", "", userInfo);
        loadJobs.add(insertLoadJob1);
        InsertLoadJob insertLoadJob2 = new InsertLoadJob("doris_test_label_1", 2L, dbId, tblId, 0, "", "", userInfo);
        loadJobs.add(insertLoadJob2);
        InsertLoadJob insertLoadJob3 = new InsertLoadJob("doris_test_label_2", 3L, dbId, tblId, 0, "", "", userInfo);
        loadJobs.add(insertLoadJob3);
        // label
        stmt = new CancelLoadStmt(null, labelBinaryPredicate);
        stmt.analyze(analyzer);
        List<LoadJob> matchLoadJobs = new ArrayList<>();
        LoadManager.addNeedCancelLoadJob(stmt, loadJobs, matchLoadJobs);
        Assertions.assertEquals(1, matchLoadJobs.size());
        // state
        matchLoadJobs.clear();
        stmt = new CancelLoadStmt(null, stateBinaryPredicate);
        stmt.analyze(analyzer);
        LoadManager.addNeedCancelLoadJob(stmt, loadJobs, matchLoadJobs);
        Assertions.assertEquals(0, matchLoadJobs.size());
        // or
        matchLoadJobs.clear();
        stmt = new CancelLoadStmt(null, compoundOrPredicate);
        stmt.analyze(analyzer);
        LoadManager.addNeedCancelLoadJob(stmt, loadJobs, matchLoadJobs);
        Assertions.assertEquals(1, matchLoadJobs.size());
        // and
        matchLoadJobs.clear();
        stmt = new CancelLoadStmt(null, compoundAndPredicate);
        stmt.analyze(analyzer);
        LoadManager.addNeedCancelLoadJob(stmt, loadJobs, matchLoadJobs);
        Assertions.assertEquals(0, matchLoadJobs.size());
    }

    @Test
    public void testError() {
        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral = new StringLiteral("FINISHED");

        LikePredicate stateLikePredicate =
                new LikePredicate(LikePredicate.Operator.LIKE, stateSlotRef, stateStringLiteral);
        CancelLoadStmt stmt = new CancelLoadStmt(null, stateLikePredicate);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Only label can use like",
                () -> stmt.analyze(analyzer));
    }
}
