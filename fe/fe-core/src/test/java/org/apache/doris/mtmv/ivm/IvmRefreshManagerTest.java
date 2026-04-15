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
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class IvmRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls() {
        MTMV mtmv = Mockito.mock(MTMV.class);
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
    public void testManagerReturnsNoBundlesFallback() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFallbackReason.PLAN_PATTERN_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerExecutesBundles() {
        MTMV mtmv = Mockito.mock(MTMV.class);
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
        MTMV mtmv = Mockito.mock(MTMV.class);
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
        MTMV mtmv = Mockito.mock(MTMV.class);
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
        MTMV mtmv = Mockito.mock(MTMV.class);
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
    public void testManagerPrecheckPassesWithoutStreamCheck() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        // checkStreamSupport is currently disabled (stream/binlog not ready),
        // so precheck only checks binlogBroken.  With binlogBroken=false the
        // precheck passes and the manager proceeds to analyze, which returns
        // empty bundles → PLAN_PATTERN_UNSUPPORTED.
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFallbackReason.PLAN_PATTERN_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerPassesHealthyPrecheckAndExecutes() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Command deltaWriteCommand = Mockito.mock(Command.class);
        // With checkStreamSupport disabled, precheck only verifies binlogBroken.
        // No relation/table mocking is needed.
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

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext(), new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
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
    }
}
