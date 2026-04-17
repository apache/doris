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
        org.apache.doris.mtmv.MTMVRefreshContext mtmvRefreshContext = new org.apache.doris.mtmv.MTMVRefreshContext(mtmv);

        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, connectContext, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, null, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, connectContext, null));
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
        List<IvmDeltaCommandBundle> bundles = makeBundles(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), bundles);

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
        Assertions.assertEquals(bundles, executor.lastBundles);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure() {
        MTMV mtmv = mockMtmv();
        Command deltaWriteCommand = Mockito.mock(Command.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeBundles(deltaWriteCommand, mtmv));

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFallbackReason.INCREMENTAL_EXECUTION_FAILED, result.getFallbackReason());
        Assertions.assertTrue(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails() {
        MTMV mtmv = mockMtmv();
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, null, Collections.emptyList());
        manager.throwOnBuild = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFallbackReason());
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
        Assertions.assertEquals(IvmFallbackReason.BINLOG_BROKEN, result.getFallbackReason());
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
        List<IvmDeltaCommandBundle> bundles = makeBundles(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), bundles);
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
        Assertions.assertEquals(IvmFallbackReason.PREVIOUS_RUN_INCOMPLETE, result.getFallbackReason());
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
        List<IvmDeltaCommandBundle> bundles = makeBundles(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), bundles);

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
                newContext(mtmv), makeBundles(deltaWriteCommand, mtmv));

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

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext(), new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
    }

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getName()).thenReturn("mv");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(new IvmInfo());
        return mtmv;
    }

    private static List<IvmDeltaCommandBundle> makeBundles(Command deltaWriteCommand, MTMV mtmv) {
        return Collections.singletonList(new IvmDeltaCommandBundle(deltaWriteCommand));
    }

    private static class TestDeltaExecutor extends IvmDeltaExecutor {
        private boolean executeCalled;
        private boolean throwOnExecute;
        private List<IvmDeltaCommandBundle> lastBundles;

        @Override
        public void execute(IvmRefreshContext context, List<IvmDeltaCommandBundle> bundles,
                int exprIdStart) throws AnalysisException {
            executeCalled = true;
            lastBundles = bundles;
            if (throwOnExecute) {
                throw new AnalysisException("executor failed");
            }
        }
    }

    private static class TestIvmRefreshManager extends IvmRefreshManager {
        private final IvmRefreshContext context;
        private final List<IvmDeltaCommandBundle> bundles;
        private boolean throwOnBuild;
        private boolean useSuperPrecheck;
        /** Snapshots of runningIvmRefresh at each persistIvmInfo call. */
        private final List<Boolean> persistCalls = new ArrayList<>();

        private TestIvmRefreshManager(IvmDeltaExecutor deltaExecutor,
                IvmRefreshContext context, List<IvmDeltaCommandBundle> bundles) {
            super(deltaExecutor);
            this.context = context;
            this.bundles = bundles;
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
        List<IvmDeltaCommandBundle> analyzeDeltaCommandBundles(IvmRefreshContext ctx) {
            return bundles;
        }

        @Override
        void persistIvmInfo(MTMV mtmv, IvmInfo ivmInfo) {
            // Snapshot the flag value at each call (not the mutable object ref)
            persistCalls.add(ivmInfo.isRunningIvmRefresh());
        }
    }
}
