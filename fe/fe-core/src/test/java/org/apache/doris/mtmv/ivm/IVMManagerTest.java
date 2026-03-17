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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.info.TableNameInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class IVMManagerTest {

    @Test
    public void testRefreshContextRejectsNulls() {
        TableNameInfo mvName = new TableNameInfo("internal", "db1", "mv1");
        Set<TableNameInfo> changedBaseTables = new LinkedHashSet<>();
        changedBaseTables.add(new TableNameInfo("internal", "db1", "t1"));

        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(null, changedBaseTables));
        Assert.assertThrows(NullPointerException.class,
                () -> new IVMRefreshContext(mvName, null));
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
    public void testManagerRejectsNulls() {
        IVMPlanAnalyzer analyzer = context -> IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY);
        IVMCapabilityChecker checker = (context, analysis) -> IVMCapabilityResult.ok();
        IVMDeltaPlannerDispatcher planner = (context, analysis) ->
                Collections.singletonList(new DeltaPlanBundle("delta"));
        IVMDeltaExecutor executor = (context, bundles) -> { };

        Assert.assertThrows(NullPointerException.class, () -> new IVMManager(null, analyzer, planner, executor));
        Assert.assertThrows(NullPointerException.class, () -> new IVMManager(checker, null, planner, executor));
        Assert.assertThrows(NullPointerException.class, () -> new IVMManager(checker, analyzer, null, executor));
        Assert.assertThrows(NullPointerException.class, () -> new IVMManager(checker, analyzer, planner, null));
    }

    @Test
    public void testManagerReturnsPlanPatternUnsupported() {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.unsupported("unsupported"));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        IVMManager manager = new IVMManager(checker, analyzer, planner, executor);

        Optional<FallbackReason> result = manager.ivmRefresh(newContext());

        Assert.assertEquals(Optional.of(FallbackReason.PLAN_PATTERN_UNSUPPORTED), result);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(0, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsCapabilityFallback() {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(
                IVMCapabilityResult.unsupported(FallbackReason.STREAM_UNSUPPORTED, "unsupported"));
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        IVMManager manager = new IVMManager(checker, analyzer, planner, executor);

        Optional<FallbackReason> result = manager.ivmRefresh(newContext());

        Assert.assertEquals(Optional.of(FallbackReason.STREAM_UNSUPPORTED), result);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(1, checker.callCount);
        Assert.assertEquals(0, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerExecutesPlannedBundles() {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.INNER_JOIN));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        List<DeltaPlanBundle> bundles = Arrays.asList(new DeltaPlanBundle("delta-1"),
                new DeltaPlanBundle("delta-2"));
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(bundles);
        TestDeltaExecutor executor = new TestDeltaExecutor();
        IVMManager manager = new IVMManager(checker, analyzer, planner, executor);
        IVMRefreshContext context = newContext();

        Optional<FallbackReason> result = manager.ivmRefresh(context);

        Assert.assertEquals(Optional.empty(), result);
        Assert.assertEquals(1, analyzer.callCount);
        Assert.assertEquals(1, checker.callCount);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(1, executor.callCount);
        Assert.assertEquals(context, planner.lastContext);
        Assert.assertEquals(IVMPlanPattern.INNER_JOIN, planner.lastAnalysis.getPattern());
        Assert.assertEquals(bundles, executor.lastBundles);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnPlannerFailure() {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.SCAN_ONLY));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        planner.throwOnPlan = true;
        TestDeltaExecutor executor = new TestDeltaExecutor();
        IVMManager manager = new IVMManager(checker, analyzer, planner, executor);

        Optional<FallbackReason> result = manager.ivmRefresh(newContext());

        Assert.assertEquals(Optional.of(FallbackReason.INCREMENTAL_EXECUTION_FAILED), result);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(0, executor.callCount);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure() {
        TestPlanAnalyzer analyzer = new TestPlanAnalyzer(IVMPlanAnalysis.of(IVMPlanPattern.UNION_ALL_ROOT));
        TestCapabilityChecker checker = new TestCapabilityChecker(IVMCapabilityResult.ok());
        TestDeltaPlannerDispatcher planner = new TestDeltaPlannerDispatcher(
                Collections.singletonList(new DeltaPlanBundle("delta")));
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        IVMManager manager = new IVMManager(checker, analyzer, planner, executor);

        Optional<FallbackReason> result = manager.ivmRefresh(newContext());

        Assert.assertEquals(Optional.of(FallbackReason.INCREMENTAL_EXECUTION_FAILED), result);
        Assert.assertEquals(1, planner.callCount);
        Assert.assertEquals(1, executor.callCount);
    }

    private static IVMRefreshContext newContext() {
        Set<TableNameInfo> changedBaseTables = new LinkedHashSet<>();
        changedBaseTables.add(new TableNameInfo("internal", "db1", "t1"));
        return new IVMRefreshContext(new TableNameInfo("internal", "db1", "mv1"), changedBaseTables);
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
}
