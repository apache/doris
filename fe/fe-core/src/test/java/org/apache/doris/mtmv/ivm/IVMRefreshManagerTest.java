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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class IVMRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls(@Mocked MTMV mtmv) {
        ConnectContext connectContext = new ConnectContext();
        org.apache.doris.mtmv.MTMVRefreshContext mtmvRefreshContext = new org.apache.doris.mtmv.MTMVRefreshContext(mtmv);

        Assertions.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(null, connectContext, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(mtmv, null, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(mtmv, connectContext, null));
    }

    @Test
    public void testCapabilityResultFactories() {
        IVMCapabilityResult ok = IVMCapabilityResult.ok();
        IVMCapabilityResult unsupported = IVMCapabilityResult.unsupported(FallbackReason.STREAM_UNSUPPORTED,
                "stream is unsupported");

        Assertions.assertTrue(ok.isIncremental());
        Assertions.assertNull(ok.getFallbackReason());
        Assertions.assertFalse(unsupported.isIncremental());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, unsupported.getFallbackReason());
        Assertions.assertEquals("stream is unsupported", unsupported.getDetailMessage());
        Assertions.assertTrue(unsupported.toString().contains("STREAM_UNSUPPORTED"));
    }

    @Test
    public void testManagerRejectsNulls() {
        IVMCapabilityChecker checker = (context, bundles) -> IVMCapabilityResult.ok();
        IVMDeltaExecutor executor = (context, bundles) -> { };

        Assertions.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(null, executor));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(checker, null));
    }

    @Test
    public void testManagerReturnsNoBundlesFallback(@Mocked MTMV mtmv) {
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor,
                newContext(mtmv), Collections.emptyList());

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.PLAN_PATTERN_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertEquals(0, checker.callCount);
        Assertions.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsCapabilityFallback(@Mocked MTMV mtmv, @Mocked LogicalPlan deltaWritePlan) {
        TestCapabilityChecker checker = new TestCapabilityChecker(
                IVMCapabilityResult.unsupported(FallbackReason.STREAM_UNSUPPORTED, "unsupported"));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<DeltaPlanBundle> bundles = makeBundles(deltaWritePlan, mtmv);
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor, newContext(mtmv), bundles);

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertEquals(1, checker.callCount);
        Assertions.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerExecutesBundles(@Mocked MTMV mtmv, @Mocked LogicalPlan deltaWritePlan) {
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<DeltaPlanBundle> bundles = makeBundles(deltaWritePlan, mtmv);
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor, newContext(mtmv), bundles);

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertEquals(1, checker.callCount);
        Assertions.assertEquals(1, executor.callCount);
        Assertions.assertEquals(bundles, executor.lastBundles);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure(@Mocked MTMV mtmv,
            @Mocked LogicalPlan deltaWritePlan) {
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor,
                newContext(mtmv), makeBundles(deltaWritePlan, mtmv));

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.INCREMENTAL_EXECUTION_FAILED, result.getFallbackReason());
        Assertions.assertEquals(1, executor.callCount);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails(@Mocked MTMV mtmv) {
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor, null, Collections.emptyList());
        manager.throwOnBuild = true;

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertEquals(0, checker.callCount);
        Assertions.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsBinlogBrokenBeforeNereidsFlow(@Mocked MTMV mtmv) {
        IVMInfo ivmInfo = new IVMInfo();
        ivmInfo.setBinlogBroken(true);
        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
            }
        };

        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.BINLOG_BROKEN, result.getFallbackReason());
        Assertions.assertEquals(0, checker.callCount);
        Assertions.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsStreamUnsupportedWithoutBinding(@Mocked MTMV mtmv,
            @Mocked MTMVRelation relation, @Mocked OlapTable olapTable) {
        IVMInfo ivmInfo = new IVMInfo();
        new Expectations() {
            {
                olapTable.getId();
                result = 1L;
                olapTable.getName();
                result = "t1";
                olapTable.getDBName();
                result = "db1";
                mtmv.getIvmInfo();
                result = ivmInfo;
                minTimes = 1;
                mtmv.getRelation();
                result = relation;
                relation.getBaseTablesOneLevelAndFromView();
                result = Sets.newHashSet(new BaseTableInfo(olapTable, 2L));
            }
        };

        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertEquals(0, checker.callCount);
        Assertions.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerPassesHealthyPrecheckAndExecutes(@Mocked MTMV mtmv,
            @Mocked MTMVRelation relation, @Mocked OlapTable olapTable, @Mocked LogicalPlan deltaWritePlan) {
        IVMInfo ivmInfo = new IVMInfo();
        new Expectations() {
            {
                olapTable.getId();
                result = 1L;
                olapTable.getName();
                result = "t1";
                olapTable.getDBName();
                result = "db1";
            }
        };
        BaseTableInfo baseTableInfo = new BaseTableInfo(olapTable, 2L);
        ivmInfo.setBaseTableStreams(new HashMap<>());
        ivmInfo.getBaseTableStreams().put(baseTableInfo, new IVMStreamRef(StreamType.OLAP, null, null));
        new MockUp<MTMVUtil>() {
            @Mock
            public TableIf getTable(BaseTableInfo input) {
                return olapTable;
            }
        };
        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
                minTimes = 1;
                mtmv.getRelation();
                result = relation;
                relation.getBaseTablesOneLevelAndFromView();
                result = Sets.newHashSet(baseTableInfo);
            }
        };

        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<DeltaPlanBundle> bundles = makeBundles(deltaWritePlan, mtmv);
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, executor, newContext(mtmv), bundles);
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertEquals(1, checker.callCount);
        Assertions.assertEquals(1, executor.callCount);
    }

    private static IVMRefreshContext newContext(MTMV mtmv) {
        return new IVMRefreshContext(mtmv, new ConnectContext(), new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
    }

    private static List<DeltaPlanBundle> makeBundles(LogicalPlan deltaWritePlan, MTMV mtmv) {
        return Collections.singletonList(new DeltaPlanBundle(new BaseTableInfo(mtmv, 0L), deltaWritePlan));
    }

    private static class TestCapabilityChecker implements IVMCapabilityChecker {
        private final IVMCapabilityResult result;
        private int callCount;

        private TestCapabilityChecker(IVMCapabilityResult result) {
            this.result = result;
        }

        @Override
        public IVMCapabilityResult check(IVMRefreshContext context, List<DeltaPlanBundle> bundles) {
            callCount++;
            return result;
        }
    }

    private static class TestDeltaExecutor implements IVMDeltaExecutor {
        private int callCount;
        private boolean throwOnExecute;
        private List<DeltaPlanBundle> lastBundles;

        @Override
        public void execute(IVMRefreshContext context, List<DeltaPlanBundle> bundles) throws AnalysisException {
            callCount++;
            lastBundles = bundles;
            if (throwOnExecute) {
                throw new AnalysisException("executor failed");
            }
        }
    }

    private static class TestIVMRefreshManager extends IVMRefreshManager {
        private final IVMRefreshContext context;
        private final List<DeltaPlanBundle> bundles;
        private boolean throwOnBuild;
        private boolean useSuperPrecheck;

        private TestIVMRefreshManager(IVMCapabilityChecker capabilityChecker, IVMDeltaExecutor deltaExecutor,
                IVMRefreshContext context, List<DeltaPlanBundle> bundles) {
            super(capabilityChecker, deltaExecutor);
            this.context = context;
            this.bundles = bundles;
        }

        @Override
        IVMRefreshResult precheck(MTMV mtmv) {
            if (useSuperPrecheck) {
                return super.precheck(mtmv);
            }
            return IVMRefreshResult.success();
        }

        @Override
        IVMRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
            if (throwOnBuild) {
                throw new AnalysisException("build context failed");
            }
            return context;
        }

        @Override
        List<DeltaPlanBundle> analyzeDeltaBundles(IVMRefreshContext ctx) {
            return bundles;
        }
    }
}
