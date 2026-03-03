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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link UnassignedShuffleJob}, specifically the
 * degreeOfParallelism logic that limits instance count when query cache is enabled.
 */
public class UnassignedShuffleJobTest {

    private ConnectContext connectContext;
    private SessionVariable sessionVariable;
    private StatementContext statementContext;
    private PlanFragment fragment;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> connectContextMockedStatic;
    private AtomicLong instanceIdCounter;

    @BeforeEach
    public void setUp() {
        sessionVariable = Mockito.mock(SessionVariable.class);
        connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);

        // nextInstanceId() is called from buildInstances; provide unique IDs
        instanceIdCounter = new AtomicLong(0);
        Mockito.when(connectContext.nextInstanceId()).thenAnswer(
                invocation -> new TUniqueId(0, instanceIdCounter.incrementAndGet()));

        // Mock static ConnectContext.get() for SystemInfoService.getBackendsNumber
        connectContextMockedStatic = Mockito.mockStatic(ConnectContext.class);
        connectContextMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getConnectContext()).thenReturn(connectContext);

        fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.useSerialSource(Mockito.any())).thenReturn(false);

        // Mock Env.getCurrentSystemInfo()
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getBackendsNumber(false)).thenReturn(3);

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getClusterInfo()).thenReturn(systemInfoService);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        envMockedStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (connectContextMockedStatic != null) {
            connectContextMockedStatic.close();
        }
    }

    /**
     * Helper: create a mock AssignedJob whose unassignedJob().getFragment() has the given queryCacheParam.
     */
    private AssignedJob createMockAssignedJob(TQueryCacheParam queryCacheParam, DistributedPlanWorker worker) {
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        childFragment.queryCacheParam = queryCacheParam;

        UnassignedJob childUnassignedJob = Mockito.mock(UnassignedJob.class);
        Mockito.when(childUnassignedJob.getFragment()).thenReturn(childFragment);

        AssignedJob assignedJob = Mockito.mock(AssignedJob.class);
        Mockito.when(assignedJob.unassignedJob()).thenReturn(childUnassignedJob);
        Mockito.when(assignedJob.getAssignedWorker()).thenReturn(worker);
        return assignedJob;
    }

    private DistributedPlanWorker createMockWorker(long id) {
        DistributedPlanWorker worker = Mockito.mock(DistributedPlanWorker.class);
        Mockito.when(worker.id()).thenReturn(id);
        return worker;
    }

    private DistributeContext createDistributeContext(boolean isLoadJob) {
        DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
        return new DistributeContext(workerManager, isLoadJob);
    }

    /**
     * Create a mock UnassignedJob whose getAllChildrenTypes() returns an empty BitSet,
     * which is required by AbstractTreeNode constructor to avoid NPE.
     */
    private UnassignedJob createMockUnassignedJob() {
        UnassignedJob mockJob = Mockito.mock(UnassignedJob.class);
        Mockito.when(mockJob.getAllChildrenTypes()).thenReturn(new BitSet());
        return mockJob;
    }

    // ======================== Tests for degreeOfParallelism ========================

    /**
     * Test: When no query cache is enabled (queryCacheParam is null), degreeOfParallelism
     * should return the exchangeInstanceParallel value (no limiting).
     */
    @Test
    public void testDegreeOfParallelismWithoutQueryCache() {
        // exchangeInstanceParallel = -1 means not set
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        // Build inputJobs with no query cache
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        // Add 20 child instances with no query cache
        for (int i = 0; i < 20; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(null, worker));
        }

        // Access protected method via a testable subclass
        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(20, inputJobs);

        // Without query cache, should return -1 (not limited)
        Assertions.assertEquals(-1, result);
    }

    /**
     * Test: When query cache is enabled and exchangeInstanceParallel is not set,
     * degreeOfParallelism should limit to min(childInstanceNum, parallelExecInstanceNum * backendNum).
     */
    @Test
    public void testDegreeOfParallelismWithQueryCacheLimitsInstances() {
        // exchangeInstanceParallel not set
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);
        // parallelExecInstanceNum = 8, backendNum = 3 => maxInstanceNum = 24
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(8);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        // 100 child instances with query cache enabled
        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 100; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(100, inputJobs);

        // Should be min(100, 8 * 3) = 24
        Assertions.assertEquals(24, result);
    }

    /**
     * Test: When query cache is enabled and childInstanceNum is smaller than maxInstanceNum,
     * degreeOfParallelism should return childInstanceNum.
     */
    @Test
    public void testDegreeOfParallelismWithQueryCacheChildSmallerThanMax() {
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);
        // parallelExecInstanceNum = 8, backendNum = 3 => maxInstanceNum = 24
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(8);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        // Only 10 child instances (< 24), all with query cache
        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 10; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(10, inputJobs);

        // Should be min(10, 24) = 10
        Assertions.assertEquals(10, result);
    }

    /**
     * Test: When query cache is enabled AND exchangeInstanceParallel is set to a small value,
     * degreeOfParallelism should take the minimum of all three.
     */
    @Test
    public void testDegreeOfParallelismWithQueryCacheAndExchangeParallel() {
        // exchangeInstanceParallel = 5
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(5);
        // parallelExecInstanceNum = 8, backendNum = 3 => maxInstanceNum = 24
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(8);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 100; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(100, inputJobs);

        // Should be min(5, min(100, 24)) = min(5, 24) = 5
        Assertions.assertEquals(5, result);
    }

    /**
     * Test: When query cache is enabled AND exchangeInstanceParallel is set to a large value,
     * the query cache limit should still apply.
     */
    @Test
    public void testDegreeOfParallelismWithQueryCacheAndLargeExchangeParallel() {
        // exchangeInstanceParallel = 50 (larger than maxInstanceNum)
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(50);
        // parallelExecInstanceNum = 4, backendNum = 3 => maxInstanceNum = 12
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(4);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 100; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(100, inputJobs);

        // Should be min(50, min(100, 12)) = min(50, 12) = 12
        Assertions.assertEquals(12, result);
    }

    /**
     * Test: Mixed input jobs - some with query cache, some without.
     * If ANY child job has queryCacheParam, the limiting should apply.
     */
    @Test
    public void testDegreeOfParallelismWithMixedQueryCacheJobs() {
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(8);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        DistributedPlanWorker worker = createMockWorker(1);

        // Mix: some with cache, some without
        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 50; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }
        for (int i = 0; i < 50; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(null, worker));
        }

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                statementContext, fragment, exchangeToChildJob);
        int result = testableJob.testDegreeOfParallelism(100, inputJobs);

        // Query cache detected => min(100, 8*3) = 24
        Assertions.assertEquals(24, result);
    }

    /**
     * Test: When connectContext is null, degreeOfParallelism should return -1.
     */
    @Test
    public void testDegreeOfParallelismWithNullConnectContext() {
        StatementContext nullCtxStatement = Mockito.mock(StatementContext.class);
        Mockito.when(nullCtxStatement.getConnectContext()).thenReturn(null);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        TestableUnassignedShuffleJob testableJob = new TestableUnassignedShuffleJob(
                nullCtxStatement, fragment, exchangeToChildJob);

        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        inputJobs.put(exchangeNode, createMockAssignedJob(new TQueryCacheParam(), createMockWorker(1)));

        int result = testableJob.testDegreeOfParallelism(1, inputJobs);
        Assertions.assertEquals(-1, result);
    }

    // ======================== Tests for computeAssignedJobs ========================

    /**
     * Test: When query cache limits instances below the child fragment size,
     * computeAssignedJobs should produce the limited number of instances.
     * This exercises the if-branch where expectInstanceNum < biggestParallelChildFragment.size().
     */
    @Test
    public void testComputeAssignedJobsWithQueryCacheLimitsInstanceCount() {
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);
        // parallelExecInstanceNum = 2, backendNum = 3 => maxInstanceNum = 6
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(2);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        exchangeToChildJob.put(exchangeNode, createMockUnassignedJob());

        UnassignedShuffleJob job = new UnassignedShuffleJob(statementContext, fragment, exchangeToChildJob);

        // Create 20 child assigned jobs with query cache
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 20; i++) {
            DistributedPlanWorker worker = createMockWorker(i % 3 + 1);
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        DistributeContext distributeContext = createDistributeContext(false);

        List<AssignedJob> result = job.computeAssignedJobs(distributeContext, inputJobs);

        // Should be limited to min(20, 2*3) = 6 instances
        Assertions.assertEquals(6, result.size());
    }

    /**
     * Test: When query cache is NOT enabled, computeAssignedJobs should keep
     * the same instance count as the biggest parallel child fragment.
     */
    @Test
    public void testComputeAssignedJobsWithoutQueryCacheKeepsChildCount() {
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        exchangeToChildJob.put(exchangeNode, createMockUnassignedJob());

        UnassignedShuffleJob job = new UnassignedShuffleJob(statementContext, fragment, exchangeToChildJob);

        // Create 15 child assigned jobs WITHOUT query cache
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        for (int i = 0; i < 15; i++) {
            DistributedPlanWorker worker = createMockWorker(i % 3 + 1);
            inputJobs.put(exchangeNode, createMockAssignedJob(null, worker));
        }

        DistributeContext distributeContext = createDistributeContext(false);

        List<AssignedJob> result = job.computeAssignedJobs(distributeContext, inputJobs);

        // Without query cache, degreeOfParallelism returns -1, so instance count = child count = 15
        Assertions.assertEquals(15, result.size());
    }

    /**
     * Test: When query cache limits instances AND expectInstanceNum < biggestParallelChildFragment.size(),
     * the if-branch is taken (instances are shuffled to distinct workers).
     */
    @Test
    public void testComputeAssignedJobsWithQueryCacheTakesIfBranch() {
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(-1);
        // parallelExecInstanceNum = 1, backendNum = 3 => maxInstanceNum = 3
        Mockito.when(sessionVariable.getParallelExecInstanceNum()).thenReturn(1);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        exchangeToChildJob.put(exchangeNode, createMockUnassignedJob());

        UnassignedShuffleJob job = new UnassignedShuffleJob(statementContext, fragment, exchangeToChildJob);

        // Create 20 child assigned jobs with query cache, spread across 5 workers
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        TQueryCacheParam cacheParam = new TQueryCacheParam();
        for (int i = 0; i < 20; i++) {
            DistributedPlanWorker worker = createMockWorker(i % 5 + 1);
            inputJobs.put(exchangeNode, createMockAssignedJob(cacheParam, worker));
        }

        DistributeContext distributeContext = createDistributeContext(false);

        List<AssignedJob> result = job.computeAssignedJobs(distributeContext, inputJobs);

        // Should be limited to min(20, 1*3) = 3 instances (< 20, so if-branch is taken)
        Assertions.assertEquals(3, result.size());
    }

    /**
     * Regression test: When query cache is NOT enabled and exchangeInstanceParallel is set to a value
     * LARGER than the child fragment count, computeAssignedJobs must NOT expand the instance count.
     * Previously the else-branch used "expectInstanceNum > 0 ? expectInstanceNum : childSize" which
     * would wrongly inflate instances beyond the child count.
     */
    @Test
    public void testComputeAssignedJobsNoQueryCacheDoesNotExpandInstances() {
        // exchangeInstanceParallel = 100, much larger than the 5 child instances
        Mockito.when(sessionVariable.getExchangeInstanceParallel()).thenReturn(100);

        ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        exchangeToChildJob.put(exchangeNode, createMockUnassignedJob());

        UnassignedShuffleJob job = new UnassignedShuffleJob(statementContext, fragment, exchangeToChildJob);

        // 5 child jobs, NO query cache
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        for (int i = 0; i < 5; i++) {
            inputJobs.put(exchangeNode, createMockAssignedJob(null, createMockWorker(i + 1)));
        }

        DistributeContext distributeContext = createDistributeContext(false);
        List<AssignedJob> result = job.computeAssignedJobs(distributeContext, inputJobs);

        // Must stay at child count (5), NOT expand to 100
        Assertions.assertEquals(5, result.size());
    }

    /**
     * Testable subclass that exposes the protected degreeOfParallelism method.
     */
    private static class TestableUnassignedShuffleJob extends UnassignedShuffleJob {
        public TestableUnassignedShuffleJob(
                StatementContext statementContext, PlanFragment fragment,
                ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
            super(statementContext, fragment, exchangeToChildJob);
        }

        public int testDegreeOfParallelism(
                int childInstanceNum, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
            return degreeOfParallelism(childInstanceNum, inputJobs);
        }
    }
}
