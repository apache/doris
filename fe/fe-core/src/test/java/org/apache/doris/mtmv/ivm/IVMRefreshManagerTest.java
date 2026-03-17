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
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.Mocked;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class IVMRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls(@Mocked MTMV mtmv) {
        ConnectContext connectContext = new ConnectContext();
        org.apache.doris.mtmv.MTMVRefreshContext mtmvRefreshContext = new org.apache.doris.mtmv.MTMVRefreshContext(mtmv);

        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(null, connectContext, mtmvRefreshContext));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(mtmv, null, mtmvRefreshContext));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(mtmv, connectContext, null));
    }

    @Test
    public void testCapabilityResultFactories() {
        IVMCapabilityResult ok = IVMCapabilityResult.ok();
        IVMCapabilityResult unsupported = IVMCapabilityResult.unsupported(FallbackReason.STREAM_UNSUPPORTED,
                "stream is unsupported");

        Assert.assertTrue(ok.isIncremental());
        Assert.assertNull(ok.getFallbackReason());
        Assert.assertFalse(unsupported.isIncremental());
        Assert.assertEquals(FallbackReason.STREAM_UNSUPPORTED, unsupported.getFallbackReason());
        Assert.assertEquals("stream is unsupported", unsupported.getDetailMessage());
        Assert.assertTrue(unsupported.toString().contains("STREAM_UNSUPPORTED"));
    }

    @Test
    public void testPlanAnalysisFactories() {
        IVMPlanAnalysis valid = IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY);
        IVMPlanAnalysis invalid = IVMPlanAnalysis.unsupported("unsupported");

        Assert.assertTrue(valid.isValid());
        Assert.assertFalse(valid.isInvalid());
        Assert.assertEquals(IVMPlanPattern.SCAN_ONLY, valid.getPattern());
        Assert.assertThrows(IllegalArgumentException.class, valid::getUnsupportedReason);

        Assert.assertFalse(invalid.isValid());
        Assert.assertTrue(invalid.isInvalid());
        Assert.assertEquals("unsupported", invalid.getUnsupportedReason());
        Assert.assertThrows(IllegalArgumentException.class, invalid::getPattern);
    }

    @Test
    public void testManagerRejectsNulls() {
        IVMPlanAnalyzer analyzer = context -> IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY);
        IVMCapabilityChecker checker = (context, analysis) -> IVMCapabilityResult.ok();
        IVMDeltaPlannerDispatcher planner = (context, analysis) ->
                Collections.singletonList(new DeltaPlanBundle("delta"));
        IVMDeltaExecutor executor = (context, bundles) -> { };

        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(null, analyzer, planner, executor));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(checker, null, planner, executor));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(checker, analyzer, null, executor));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshManager(checker, analyzer, planner, null));
    }

    @Test
    public void testManagerReturnsPlanPatternUnsupported(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.unsupported("unsupported"));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.PLAN_PATTERN_UNSUPPORTED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, manager.buildContextCallCount);
        Assert.assertSame(mtmv, manager.lastMtmv);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(0, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsCapabilityFallback(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(
                IVMCapabilityResult.unsupported(FallbackReason.STREAM_UNSUPPORTED, "unsupported"));
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(1, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerExecutesPlannedBundles(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.INNER_JOIN));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        List<DeltaPlanBundle> bundles = Arrays.asList(new DeltaPlanBundle("delta-1"),
                new DeltaPlanBundle("delta-2"));
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(bundles);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        IVMRefreshContext context = newContext(mtmv);
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor, context);

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertTrue(result.isSuccess());
        Assert.assertNull(result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(1, checker.callCount);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(1, executor.callCount);
        Assert.assertEquals(context, planner.lastContext);
        Assert.assertEquals(IVMPlanPattern.INNER_JOIN, planner.lastAnalysis.getPattern());
        Assert.assertEquals(bundles, executor.lastBundles);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnPlannerFailure(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        planner.throwOnPlan = true;
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.INCREMENTAL_EXECUTION_FAILED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.UNION_ALL_ROOT));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.INCREMENTAL_EXECUTION_FAILED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(1, executor.callCount);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails(@Mocked MTMV mtmv) {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor, null);
        manager.throwOnBuild = true;

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(0, analyzer.callCount);
        Assert.assertEquals(0, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
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

        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.BINLOG_BROKEN, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(0, manager.buildContextCallCount);
        Assert.assertEquals(0, analyzer.callCount);
        Assert.assertEquals(0, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
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
            }
        };
        BaseTableInfo baseTableInfo = new BaseTableInfo(olapTable, 2L);
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

        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(0, manager.buildContextCallCount);
        Assert.assertEquals(0, analyzer.callCount);
        Assert.assertEquals(0, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerPassesHealthyIvmBinlogPrecheck(@Mocked MTMV mtmv,
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

        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIVMRefreshManager manager = new TestIVMRefreshManager(checker, analyzer, planner, executor,
                newContext(mtmv));
        manager.useSuperPrecheck = true;

        IVMRefreshResult result = manager.ivmRefresh(mtmv);

        Assert.assertTrue(result.isSuccess());
        Assert.assertNull(result.getFallbackReason());
        Assert.assertEquals(1, manager.precheckCallCount);
        Assert.assertEquals(1, manager.buildContextCallCount);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(1, checker.callCount);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(1, executor.callCount);
    }

    private static IVMRefreshContext newContext(MTMV mtmv) {
        return new IVMRefreshContext(mtmv, new ConnectContext(), new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
    }

    private static class TestPlanAnalyzer implements IVMPlanAnalyzer {
        private final IVMPlanAnalysis result;
        private int callCount;

        private TestPlanAnalyzer(IVMPlanAnalysis result) {
            this.result = result;
        }

        @Override
        public IVMPlanAnalysis analyze(IVMRefreshContext context) {
            callCount++;
            return result;
        }
    }

    private static class TestCapabilityChecker implements IVMCapabilityChecker {
        private final IVMCapabilityResult result;
        private int callCount;

        private TestCapabilityChecker(IVMCapabilityResult result) {
            this.result = result;
        }

        @Override
        public IVMCapabilityResult check(IVMRefreshContext context, IVMPlanAnalysis analysis) {
            callCount++;
            return result;
        }
    }

    private static class TestDeltaPlannerDispatcher implements IVMDeltaPlannerDispatcher {
        private final List<DeltaPlanBundle> result;
        private int callCount;
        private boolean throwOnPlan;
        private IVMRefreshContext lastContext;
        private IVMPlanAnalysis lastAnalysis;

        private TestDeltaPlannerDispatcher(List<DeltaPlanBundle> result) {
            this.result = new ArrayList<>(result);
        }

        @Override
        public List<DeltaPlanBundle> plan(IVMRefreshContext context, IVMPlanAnalysis analysis)
                throws AnalysisException {
            callCount++;
            lastContext = context;
            lastAnalysis = analysis;
            if (throwOnPlan) {
                throw new AnalysisException("planner failed");
            }
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
        private int buildContextCallCount;
        private int precheckCallCount;
        private boolean throwOnBuild;
        private boolean useSuperPrecheck;
        private MTMV lastMtmv;

        private TestIVMRefreshManager(IVMCapabilityChecker capabilityChecker, IVMPlanAnalyzer planAnalyzer,
                IVMDeltaPlannerDispatcher deltaPlannerDispatcher, IVMDeltaExecutor deltaExecutor,
                IVMRefreshContext context) {
            super(capabilityChecker, planAnalyzer, deltaPlannerDispatcher, deltaExecutor);
            this.context = context;
        }

        @Override
        IVMRefreshResult precheck(MTMV mtmv) {
            precheckCallCount++;
            if (useSuperPrecheck) {
                return super.precheck(mtmv);
            }
            return IVMRefreshResult.success();
        }

        @Override
        IVMRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
            buildContextCallCount++;
            lastMtmv = mtmv;
            if (throwOnBuild) {
                throw new AnalysisException("build context failed");
            }
            return context;
        }
    }
}
