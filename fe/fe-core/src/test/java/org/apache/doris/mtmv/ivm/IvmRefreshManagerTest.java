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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IvmRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls() {
        MTMV mtmv = mockMtmv();
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, new ConnectContext()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, null));
    }

    @Test
    public void testManagerRejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshManager(null));
    }

    @Test
    public void testManagerReturnsSuccessForEmptyBundles() {
        MTMV mtmv = mockMtmv();
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        IvmRefreshResult result = manager.doRefresh(mtmv);
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerExecutesBundles() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));
        IvmRefreshResult result = manager.doRefresh(mtmv);
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
    }

    @Test
    public void testManagerThrowsHardFailureOnExecutorFailure() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> manager.doRefresh(mtmv));

        Assertions.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED,
                exception.getFailureReason());
        Assertions.assertTrue(executor.executeCalled);
    }

    @Test
    public void testManagerKeepsKnownExecutionFailureReason() {
        assertKnownExecutionFailureReason(IvmFailureReason.MIN_MAX_BOUNDARY_HIT,
                IvmFailureClassifier.MIN_MAX_BOUNDARY_MSG_PREFIX + ": deleted row may be current MIN value");
        assertKnownExecutionFailureReason(IvmFailureReason.NON_DETERMINISTIC_ROW_ID,
                IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX + " in INNER_JOIN");
    }

    @Test
    public void testManagerReturnsBinlogNotEnabledFallbackOnIvmException() {
        MTMV mtmv = mockMtmv();
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.throwBinlogNotEnabledOnAnalyze = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED, result.getFailureReason());
        Assertions.assertTrue(result.getDetailMessage().contains("no_binlog"));
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails() {
        MTMV mtmv = mockMtmv();
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, null, Collections.emptyList());
        manager.throwOnBuild = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFailureReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsIvmExceptionFailureReasonWhenAnalyzeFails() {
        MTMV mtmv = mockMtmv();
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.throwIvmExceptionOnAnalyze = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.AGG_UNSUPPORTED, result.getFailureReason());
        Assertions.assertTrue(result.getDetailMessage().contains("unsupported aggregate"));
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testValidatePlanSignaturePassesWhenMatched() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setPlanSignature("abc");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        MTMVAnalyzeQueryInfo queryInfo = newQueryInfo(new IvmPlanSignature("canonical", "abc"));
        TestIvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());

        manager.validatePlanSignature(mtmv, queryInfo);
    }

    @Test
    public void testValidatePlanSignatureFailsWhenMissing() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        IvmPlanSignature currentSignature = new IvmPlanSignature("canonical", "abc");
        MTMVAnalyzeQueryInfo queryInfo = newQueryInfo(currentSignature);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> manager.validatePlanSignature(mtmv, queryInfo));

        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("storedSignature=null"));
    }

    @Test
    public void testValidatePlanSignatureFailsWhenMismatched() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setPlanSignature("old");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        IvmPlanSignature currentSignature = new IvmPlanSignature("canonical", "new");
        MTMVAnalyzeQueryInfo queryInfo = newQueryInfo(currentSignature);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> manager.validatePlanSignature(mtmv, queryInfo));

        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("storedSignature=old"));
        Assertions.assertTrue(exception.getMessage().contains("currentSignature=new"));
    }

    @Test
    public void testPlanSignatureMismatchFallbackCarriesCurrentSignature() {
        MTMV mtmv = mockMtmv();
        IvmPlanSignature currentSignature = new IvmPlanSignature("canonical", "new");
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setPlanSignature("old");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList()) {
            @Override
            List<Command> analyzeDeltaCommands(IvmRefreshContext ctx) {
                validatePlanSignature(mtmv, newQueryInfo(currentSignature));
                return Collections.emptyList();
            }
        };

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, result.getFailureReason());
        Assertions.assertSame(currentSignature, result.getCurrentPlanSignature());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsBinlogBrokenBeforeNereidsFlow() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setBinlogBroken(true);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.BINLOG_BROKEN, result.getFailureReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerPrecheckDoesNotConsultExcludedTriggerTables() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        // Empty bundles → success (no-op, all base tables up to date)
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(executor.executeCalled);
        Mockito.verify(mtmv, Mockito.never()).getExcludedTriggerTables();
    }

    @Test
    public void testManagerPrecheckPassesWithoutStreamCheck() {
        // checkStreamSupport is currently disabled (stream/binlog not ready),
        // so precheck only checks binlogBroken and runningIvmRefresh.
        // With both false the precheck passes and the manager proceeds to analyze,
        // which returns empty bundles → success (no-op).
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerPassesHealthyPrecheckAndExecutes() {
        // With checkStreamSupport disabled, precheck only verifies binlogBroken.
        // No relation/table mocking is needed.
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
    }

    @Test
    public void testPrecheckReturnsPreviousRunIncompleteWhenFlagSet() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setRunningIvmRefresh(true);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.PREVIOUS_RUN_INCOMPLETE, result.getFailureReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testRunningIvmRefreshFlagSetBeforeExecutionAndClearedAfter() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));

        // Before refresh: flag should be false
        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());

        IvmRefreshResult result = manager.doRefresh(mtmv);

        // After successful refresh: flag should be cleared
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());
        // Two editlog writes: first sets flag=true, second clears flag=false
        Assertions.assertEquals(2, manager.persistCalls.size());
        Assertions.assertTrue(manager.persistCalls.get(0), "first persist should have runningIvmRefresh=true");
        Assertions.assertFalse(manager.persistCalls.get(1), "second persist should have runningIvmRefresh=false");
    }

    @Test
    public void testRunningIvmRefreshFlagLeftSetOnExecutionFailure() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> manager.doRefresh(mtmv));

        // Execution failed: flag should remain true
        Assertions.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED,
                exception.getFailureReason());
        Assertions.assertTrue(ivmInfo.isRunningIvmRefresh());
        // Only one editlog write: the one that set the flag=true before execution
        Assertions.assertEquals(1, manager.persistCalls.size());
        Assertions.assertTrue(manager.persistCalls.get(0), "persist should have runningIvmRefresh=true");
    }

    @Test
    public void testEmptyBundlesDoNotWriteEditlog() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(executor.executeCalled);
        // No editlog writes for empty bundles (no-op)
        Assertions.assertEquals(0, manager.persistCalls.size());
    }

    private void assertKnownExecutionFailureReason(IvmFailureReason expectedReason, String detail) {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.failureMessage = detail;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(cmd, mtmv));

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> manager.doRefresh(mtmv));

        Assertions.assertEquals(expectedReason, exception.getFailureReason());
        Assertions.assertTrue(executor.executeCalled);
    }

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext());
    }

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getName()).thenReturn("mv");
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("db");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(new IvmInfo());
        return mtmv;
    }

    private static MTMVAnalyzeQueryInfo newQueryInfo(IvmPlanSignature signature) {
        MTMVAnalyzeQueryInfo queryInfo = new MTMVAnalyzeQueryInfo(
                Collections.emptyList(), null, null, Collections.emptyMap());
        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.setPlanSignature(signature);
        queryInfo.setIvmNormalizeResult(normalizeResult);
        return queryInfo;
    }

    private static List<Command> makeCommands(Command cmd, MTMV mtmv) {
        return Collections.singletonList(cmd);
    }

    private static class TestDeltaExecutor extends IvmDeltaExecutor {
        private boolean executeCalled;
        private boolean throwOnExecute;
        private String failureMessage;

        @Override
        public void execute(IvmRefreshContext context, List<Command> commands,
                int exprIdStart) {
            executeCalled = true;
            if (throwOnExecute || failureMessage != null) {
                String message = failureMessage != null ? failureMessage : "executor failed";
                throw new RuntimeException(message);
            }
        }
    }

    private static class TestIvmRefreshManager extends IvmRefreshManager {
        private final IvmRefreshContext context;
        private final List<Command> commands;
        private boolean throwOnBuild;
        private boolean throwIvmExceptionOnAnalyze;
        private boolean throwBinlogNotEnabledOnAnalyze;
        private boolean useSuperPrecheck;
        /** Snapshots of runningIvmRefresh at each persistIvmInfo call. */
        private final List<Boolean> persistCalls = new ArrayList<>();

        private TestIvmRefreshManager(IvmDeltaExecutor deltaExecutor,
                IvmRefreshContext context, List<Command> commands) {
            super(deltaExecutor);
            this.context = context;
            this.commands = commands;
        }

        @Override
        IvmRefreshResult precheck(MTMV mtmv) {
            if (useSuperPrecheck) {
                return super.precheck(mtmv);
            }
            return IvmRefreshResult.success();
        }

        @Override
        IvmRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
            if (throwOnBuild) {
                throw new AnalysisException("build context failed");
            }
            return context;
        }

        @Override
        List<Command> analyzeDeltaCommands(IvmRefreshContext ctx) {
            if (throwIvmExceptionOnAnalyze) {
                throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED, "unsupported aggregate");
            }
            if (throwBinlogNotEnabledOnAnalyze) {
                throw new IvmException(IvmFailureReason.BINLOG_NOT_ENABLED,
                        "binlog is not enabled for table: no_binlog");
            }
            return commands;
        }

        @Override
        void persistIvmInfo(MTMV mtmv, IvmInfo ivmInfo) {
            // Snapshot the flag value at each call (not the mutable object ref)
            persistCalls.add(ivmInfo.isRunningIvmRefresh());
        }
    }
}
