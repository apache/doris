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
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Predicate;

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
        Assertions.assertEquals("CANCEL EXPORT FROM testDb WHERE (`label` = 'doris_test_label')",
                stmt.toString());

        SlotRef labelSlotRefUpper = new SlotRef(null, "LABEL");
        BinaryPredicate labelBinaryPredicateUpper = new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRefUpper,
                labelStringLiteral);
        CancelExportStmt stmtUpper = new CancelExportStmt(null, labelBinaryPredicateUpper);
        stmtUpper.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM testDb WHERE (`LABEL` = 'doris_test_label')",
                stmtUpper.toString());

        StringLiteral stateStringLiteral = new StringLiteral("PENDING");
        BinaryPredicate stateBinaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef,
                stateStringLiteral);
        stmt = new CancelExportStmt(null, stateBinaryPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM testDb WHERE (`state` = 'PENDING')", stmt.toString());

        LikePredicate labelLikePredicate = new LikePredicate(LikePredicate.Operator.LIKE, labelSlotRef,
                labelStringLiteral);
        stmt = new CancelExportStmt(null, labelLikePredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals("CANCEL EXPORT FROM testDb WHERE `label` LIKE 'doris_test_label'",
                stmt.toString());

        CompoundPredicate compoundAndPredicate = new CompoundPredicate(Operator.AND, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelExportStmt(null, compoundAndPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL EXPORT FROM testDb WHERE ((`label` = 'doris_test_label') AND (`state` = 'PENDING'))",
                stmt.toString());

        CompoundPredicate compoundOrPredicate = new CompoundPredicate(Operator.OR, labelBinaryPredicate,
                stateBinaryPredicate);
        stmt = new CancelExportStmt(null, compoundOrPredicate);
        stmt.analyze(analyzer);
        Assertions.assertEquals(
                "CANCEL EXPORT FROM testDb WHERE ((`label` = 'doris_test_label') OR (`state` = 'PENDING'))",
                stmt.toString());
    }

    @Test
    public void testError1() {
        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral = new StringLiteral("FINISHED");

        LikePredicate stateLikePredicate =
                new LikePredicate(LikePredicate.Operator.LIKE, stateSlotRef, stateStringLiteral);
        CancelExportStmt stmt = new CancelExportStmt(null, stateLikePredicate);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Only label can use like",
                () -> stmt.analyze(analyzer));
    }

    @Test
    public void testError2() {
        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral1 = new StringLiteral("EXPORTING");
        BinaryPredicate stateEqPredicate1 =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral1);

        StringLiteral stateStringLiteral2 = new StringLiteral("PENDING");
        BinaryPredicate stateEqPredicate2 =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral2);

        SlotRef labelSlotRef = new SlotRef(null, "label");
        StringLiteral labelStringLiteral1 = new StringLiteral("test_label");
        BinaryPredicate labelEqPredicate1 =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRef, labelStringLiteral1);

        CompoundPredicate compoundAndPredicate1 = new CompoundPredicate(Operator.AND, stateEqPredicate1,
                stateEqPredicate2);
        CompoundPredicate compoundAndPredicate2 = new CompoundPredicate(Operator.AND, compoundAndPredicate1,
                labelEqPredicate1);
        CompoundPredicate compoundAndPredicate3 = new CompoundPredicate(Operator.NOT, stateEqPredicate1, null);


        CancelExportStmt stmt1 = new CancelExportStmt(null, compoundAndPredicate2);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Current not support nested clause",
                () -> stmt1.analyze(analyzer));


        CancelExportStmt stmt2 = new CancelExportStmt(null, compoundAndPredicate3);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Current not support NOT operator",
                () -> stmt2.analyze(analyzer));
    }

    @Test
    public void testCancelJobFilter() throws UserException {
        List<ExportJob> exportJobList1 = Lists.newLinkedList();
        List<ExportJob> exportJobList2 = Lists.newLinkedList();
        ExportJob job1 = new ExportJob();
        ExportJob job2 = new ExportJob();
        ExportJob job3 = new ExportJob();
        ExportJob job4 = new ExportJob();


        try {
            Method setExportJobState = job1.getClass().getDeclaredMethod("setExportJobState",
                    ExportJobState.class);
            setExportJobState.setAccessible(true);
            setExportJobState.invoke(job2, ExportJobState.CANCELLED);
            setExportJobState.invoke(job3, ExportJobState.EXPORTING);

        } catch (Exception e) {
            throw new UserException(e);
        }

        exportJobList1.add(job1);
        exportJobList1.add(job2);
        exportJobList1.add(job3);
        exportJobList1.add(job4);
        exportJobList2.add(job1);
        exportJobList2.add(job2);

        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral = new StringLiteral("PENDING");
        BinaryPredicate stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        CancelExportStmt stmt = new CancelExportStmt(null, stateEqPredicate);
        stmt.analyze(analyzer);
        Predicate<ExportJob> filter = ExportMgr.buildCancelJobFilter(stmt);

        Assert.assertTrue(exportJobList1.stream().filter(filter).count() == 2);
        Assert.assertTrue(exportJobList2.stream().filter(filter).count() == 1);

        stateStringLiteral = new StringLiteral("EXPORTING");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        stmt = new CancelExportStmt(null, stateEqPredicate);
        stmt.analyze(analyzer);
        filter = ExportMgr.buildCancelJobFilter(stmt);

        Assert.assertTrue(exportJobList1.stream().filter(filter).count() == 1);

    }

    @Test
    public void testExportMgrCancelJob() throws UserException {
        FeConstants.runningUnitTest = true;
        ExportJob job1 = new ExportJob();
        job1.setId(1);
        job1.setLabel("label_job1");
        ExportJob job2 = new ExportJob();
        job2.setId(2);
        job2.setLabel("label_job2");
        ExportJob job3 = new ExportJob();
        job3.setId(3);
        job3.setLabel("label_job3");
        ExportJob job4 = new ExportJob();
        job4.setId(4);
        job4.setLabel("label_job4");

        try {
            Method setExportJobState = job1.getClass().getDeclaredMethod("setExportJobState",
                    ExportJobState.class);
            setExportJobState.setAccessible(true);
            // job1 is PENDING
            setExportJobState.invoke(job2, ExportJobState.EXPORTING);
            setExportJobState.invoke(job3, ExportJobState.FINISHED);
            setExportJobState.invoke(job4, ExportJobState.CANCELLED);
        } catch (Exception e) {
            throw new UserException(e);
        }

        ExportMgr exportMgr = new ExportMgr();
        exportMgr.unprotectAddJob(job1);
        exportMgr.unprotectAddJob(job2);
        exportMgr.unprotectAddJob(job3);
        exportMgr.unprotectAddJob(job4);


        // cancel export job where state = "PENDING"
        Assert.assertTrue(job1.getState() == ExportJobState.PENDING);
        SlotRef stateSlotRef = new SlotRef(null, "state");
        StringLiteral stateStringLiteral = new StringLiteral("PENDING");
        BinaryPredicate stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        CancelExportStmt stmt = new CancelExportStmt(null, stateEqPredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job1.getState() == ExportJobState.CANCELLED);

        // cancel export job where state = "EXPORTING"
        Assert.assertTrue(job2.getState() == ExportJobState.EXPORTING);
        stateStringLiteral = new StringLiteral("EXPORTING");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        stmt = new CancelExportStmt(null, stateEqPredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job2.getState() == ExportJobState.CANCELLED);

        // cancel export job where state = "FINISHED"
        Assert.assertTrue(job3.getState() == ExportJobState.FINISHED);
        stateStringLiteral = new StringLiteral("FINISHED");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        stmt = new CancelExportStmt(null, stateEqPredicate);
        try {
            stmt.analyze(analyzer);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Only support PENDING/EXPORTING"));
        }

        // cancel export job where state = "CANCELLED"
        Assert.assertTrue(job4.getState() == ExportJobState.CANCELLED);
        stateStringLiteral = new StringLiteral("CANCELLED");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        stmt = new CancelExportStmt(null, stateEqPredicate);
        try {
            stmt.analyze(analyzer);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Only support PENDING/EXPORTING"));
        }

        ExportJob job5 = new ExportJob();
        job5.setId(5);
        job5.setLabel("label_job5");
        exportMgr.unprotectAddJob(job5);

        ExportJob job6 = new ExportJob();
        job6.setId(6);
        job6.setLabel("label_job6");
        exportMgr.unprotectAddJob(job6);

        ExportJob job7 = new ExportJob();
        job7.setId(7);
        job7.setLabel("label_job7");
        exportMgr.unprotectAddJob(job7);

        ExportJob job8 = new ExportJob();
        job8.setId(8);
        job8.setLabel("label_job8");
        exportMgr.unprotectAddJob(job8);

        // cancel export job where label = "label_job5"
        Assert.assertTrue(job5.getState() == ExportJobState.PENDING);
        SlotRef labelSlotRef = new SlotRef(null, "label");
        StringLiteral labelStringLiteral = new StringLiteral("label_job5");
        BinaryPredicate labelEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRef, labelStringLiteral);
        stmt = new CancelExportStmt(null, labelEqPredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job5.getState() == ExportJobState.CANCELLED);

        // cancel export job where label like "job6"
        Assert.assertTrue(job6.getState() == ExportJobState.PENDING);
        labelSlotRef = new SlotRef(null, "label");
        labelStringLiteral = new StringLiteral("%job6");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, labelSlotRef, labelStringLiteral);
        stmt = new CancelExportStmt(null, likePredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job6.getState() == ExportJobState.CANCELLED);

        // cancel export job where label = "label_job7" AND STATE = "PENDING"
        Assert.assertTrue(job7.getState() == ExportJobState.PENDING);
        labelSlotRef = new SlotRef(null, "label");
        labelStringLiteral = new StringLiteral("label_job7");
        labelEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, labelSlotRef, labelStringLiteral);
        stateStringLiteral = new StringLiteral("PENDING");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        CompoundPredicate andCompoundPredicate = new CompoundPredicate(Operator.AND, labelEqPredicate, stateEqPredicate);
        stmt = new CancelExportStmt(null, andCompoundPredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job7.getState() == ExportJobState.CANCELLED);

        // cancel export job where label like "%job8" OR STATE = "PENDING"
        Assert.assertTrue(job8.getState() == ExportJobState.PENDING);
        labelSlotRef = new SlotRef(null, "label");
        labelStringLiteral = new StringLiteral("%job8");
        likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, labelSlotRef, labelStringLiteral);
        stateStringLiteral = new StringLiteral("PENDING");
        stateEqPredicate =
                new BinaryPredicate(BinaryPredicate.Operator.EQ, stateSlotRef, stateStringLiteral);
        CompoundPredicate orCompoundPredicate = new CompoundPredicate(Operator.OR, likePredicate, stateEqPredicate);
        stmt = new CancelExportStmt(null, orCompoundPredicate);
        stmt.analyze(analyzer);
        exportMgr.cancelExportJob(stmt);
        Assert.assertTrue(job8.getState() == ExportJobState.CANCELLED);
    }
}
