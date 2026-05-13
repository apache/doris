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
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IvmRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls() {
        MTMV mtmv = mockMtmv();
        ConnectContext connectContext = new ConnectContext();

        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, connectContext));
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
        // Empty bundles means all base tables are up to date — no-op success.
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
        Command deltaWriteCommand = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<Command> commands = makeCommands(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), commands);

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
        Assertions.assertEquals(commands, executor.lastCommands);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure() {
        MTMV mtmv = mockMtmv();
        Command deltaWriteCommand = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(deltaWriteCommand, mtmv));

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, result.getFailureReason());
        Assertions.assertTrue(executor.executeCalled);
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
        // so precheck only checks binlogBroken.  With binlogBroken=false the
        // precheck passes and the manager proceeds to analyze, which returns
        // empty bundles → success (no-op, all base tables up to date).
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
        Command deltaWriteCommand = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<Command> commands = makeCommands(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), commands);
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
        Command deltaWriteCommand = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        IvmStreamRef streamRef = new IvmStreamRef(5L);
        streamRef.setLatestTso(10L);
        Map<BaseTableInfo, IvmStreamRef> streams = new HashMap<>();
        streams.put(baseTableInfo, streamRef);
        ivmInfo.setBaseTableStreams(streams);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<Command> commands = makeCommands(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), commands);

        // Before refresh: flag should be false
        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());

        IvmRefreshResult result = manager.doRefresh(mtmv);

        // After successful refresh: flag should be cleared, consumedTso advanced
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());
        Assertions.assertEquals(10L, streamRef.getConsumedTso());
        // Two editlog writes: first sets flag=true, second clears flag=false + advances TSO
        Assertions.assertEquals(2, manager.persistCalls.size());
        Assertions.assertTrue(manager.persistCalls.get(0), "first persist should have runningIvmRefresh=true");
        Assertions.assertFalse(manager.persistCalls.get(1), "second persist should have runningIvmRefresh=false");
    }

    @Test
    public void testRunningIvmRefreshFlagLeftSetOnExecutionFailure() {
        MTMV mtmv = mockMtmv();
        Command deltaWriteCommand = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        IvmStreamRef streamRef = new IvmStreamRef(5L);
        streamRef.setLatestTso(10L);
        Map<BaseTableInfo, IvmStreamRef> streams = new HashMap<>();
        streams.put(baseTableInfo, streamRef);
        ivmInfo.setBaseTableStreams(streams);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeCommands(deltaWriteCommand, mtmv));

        IvmRefreshResult result = manager.doRefresh(mtmv);

        // Execution failed: flag should remain true, consumedTso NOT advanced
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(ivmInfo.isRunningIvmRefresh());
        Assertions.assertEquals(5L, streamRef.getConsumedTso());
        // Only one editlog write: the one that set the flag=true before execution
        Assertions.assertEquals(1, manager.persistCalls.size());
        Assertions.assertTrue(manager.persistCalls.get(0), "persist should have runningIvmRefresh=true");
    }

    @Test
    public void testEmptyBundlesDoNotWriteRunningIvmRefreshEditlog() {
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

    @Test
    public void testEnsureBaseTableStreamsInitializedPopulatesEmptyStreams() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Assertions.assertTrue(ivmInfo.getBaseTableStreams().isEmpty());
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        BaseTableInfo bt1 = Mockito.mock(BaseTableInfo.class);
        BaseTableInfo bt2 = Mockito.mock(BaseTableInfo.class);
        Set<BaseTableInfo> baseTables = Sets.newHashSet(bt1, bt2);
        MTMVRelation relation = new MTMVRelation(baseTables, baseTables, baseTables, null, null);
        Mockito.when(mtmv.getRelation()).thenReturn(relation);

        IvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());
        manager.ensureBaseTableStreamsInitialized(mtmv);

        Map<BaseTableInfo, IvmStreamRef> streams = ivmInfo.getBaseTableStreams();
        Assertions.assertEquals(2, streams.size());
        for (IvmStreamRef ref : streams.values()) {
            Assertions.assertEquals(0, ref.getConsumedTso());
            Assertions.assertEquals(0, ref.getLatestTso());
        }
    }

    @Test
    public void testEnsureBaseTableStreamsInitializedSkipsIfAlreadyPopulated() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        BaseTableInfo existingBt = Mockito.mock(BaseTableInfo.class);
        Map<BaseTableInfo, IvmStreamRef> existingStreams = new HashMap<>();
        existingStreams.put(existingBt, new IvmStreamRef(99L));
        ivmInfo.setBaseTableStreams(existingStreams);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        IvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());
        manager.ensureBaseTableStreamsInitialized(mtmv);

        // Should not overwrite existing streams
        Assertions.assertEquals(1, ivmInfo.getBaseTableStreams().size());
        Assertions.assertEquals(99L, ivmInfo.getBaseTableStreams().get(existingBt).getConsumedTso());
    }

    @Test
    public void testEnsureBaseTableStreamsInitializedHandlesNullRelation() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        Mockito.when(mtmv.getRelation()).thenReturn(null);

        IvmRefreshManager manager = new TestIvmRefreshManager(
                new TestDeltaExecutor(), newContext(mtmv), Collections.emptyList());
        manager.ensureBaseTableStreamsInitialized(mtmv);

        // Should remain empty — no relation to initialize from
        Assertions.assertTrue(ivmInfo.getBaseTableStreams().isEmpty());
    }

    @Test
    public void testResetIvmStateAfterFullRefreshInitializesMissingStreams() {
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setRunningIvmRefresh(true);
        BaseTableInfo bt1 = Mockito.mock(BaseTableInfo.class);
        BaseTableInfo bt2 = Mockito.mock(BaseTableInfo.class);
        Map<BaseTableInfo, Long> capturedTsos = new HashMap<>();
        capturedTsos.put(bt1, 3L);
        capturedTsos.put(bt2, 5L);

        IvmRefreshManager.resetIvmStateAfterFullRefresh(ivmInfo, capturedTsos);

        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());
        Assertions.assertEquals(2, ivmInfo.getBaseTableStreams().size());
        Assertions.assertEquals(3L, ivmInfo.getBaseTableStreams().get(bt1).getConsumedTso());
        Assertions.assertEquals(3L, ivmInfo.getBaseTableStreams().get(bt1).getLatestTso());
        Assertions.assertEquals(5L, ivmInfo.getBaseTableStreams().get(bt2).getConsumedTso());
        Assertions.assertEquals(5L, ivmInfo.getBaseTableStreams().get(bt2).getLatestTso());
    }

    @Test
    public void testClearRunningIvmRefreshAfterFullRefreshKeepsStreams() {
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setRunningIvmRefresh(true);
        BaseTableInfo baseTable = Mockito.mock(BaseTableInfo.class);
        IvmStreamRef streamRef = new IvmStreamRef();
        streamRef.setConsumedTso(10L);
        streamRef.setLatestTso(20L);
        ivmInfo.getBaseTableStreams().put(baseTable, streamRef);

        IvmRefreshManager.clearRunningIvmRefreshAfterFullRefresh(ivmInfo);

        Assertions.assertFalse(ivmInfo.isRunningIvmRefresh());
        Assertions.assertSame(streamRef, ivmInfo.getBaseTableStreams().get(baseTable));
        Assertions.assertEquals(10L, streamRef.getConsumedTso());
        Assertions.assertEquals(20L, streamRef.getLatestTso());
    }

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext());
    }

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getName()).thenReturn("mv");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(new IvmInfo());
        return mtmv;
    }

    private static List<Command> makeCommands(Command deltaWriteCommand, MTMV mtmv) {
        return Collections.singletonList(deltaWriteCommand);
    }

    private static class TestDeltaExecutor extends IvmDeltaExecutor {
        private boolean executeCalled;
        private boolean throwOnExecute;
        private List<Command> lastCommands;

        @Override
        public void execute(IvmRefreshContext context, List<Command> commands,
                int exprIdStart) throws AnalysisException {
            executeCalled = true;
            lastCommands = commands;
            if (throwOnExecute) {
                throw new AnalysisException("executor failed");
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
