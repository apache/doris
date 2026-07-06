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

package org.apache.doris.cloud.cache;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.cloud.CloudWarmUpJob.SyncEvent;
import org.apache.doris.cloud.CloudWarmUpJob.SyncMode;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Triple;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CacheHotspotManagerTest {
    private CacheHotspotManager cacheHotspotManager;
    private CloudSystemInfoService cloudSystemInfoService;
    private boolean originalRunningUnitTest;
    private AtomicLong nextJobId;
    private Env env;
    private EditLog editLog;
    private MockedStatic<Env> envMockedStatic;

    @Before
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        nextJobId = new AtomicLong(1000L);
        env = Mockito.mock(Env.class);
        editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getNextId()).thenAnswer(invocation -> nextJobId.getAndIncrement());
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        cloudSystemInfoService = new CloudSystemInfoService();
        cacheHotspotManager = new CacheHotspotManager(cloudSystemInfoService);
    }

    @After
    public void tearDown() {
        envMockedStatic.close();
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    @Test
    public void testWarmUpNewClusterByTable() {
        cloudSystemInfoService = new CloudSystemInfoService();
        // Use mock with CALLS_REAL_METHODS to avoid package-private access issues
        // (CacheHotspotManager's helper methods are package-private)
        cacheHotspotManager = Mockito.mock(CacheHotspotManager.class, invocation -> {
            String methodName = invocation.getMethod().getName();
            switch (methodName) {
                case "getFileCacheCapacity":
                    return 100L;
                case "getPartitionsFromTriple": {
                    List<Partition> partitions = new ArrayList<>();
                    Partition spyPartition = Mockito.spy(new Partition(1, "p1", null, null));
                    Mockito.doReturn(10000000L).when(spyPartition).getDataSize(Mockito.anyBoolean());
                    List<MaterializedIndex> list = new ArrayList<>();
                    list.add(new MaterializedIndex());
                    Mockito.doReturn(list).when(spyPartition)
                            .getMaterializedIndices(Mockito.any(IndexExtState.class));
                    partitions.add(spyPartition);
                    return partitions;
                }
                case "getBackendsFromCluster": {
                    String dstClusterName = (String) invocation.getArgument(0);
                    List<Backend> backends = new ArrayList<>();
                    backends.add(new Backend(11, dstClusterName, 0));
                    return backends;
                }
                case "getTabletsFromIndexs": {
                    List<Tablet> list = new ArrayList<>();
                    list.add(new CloudTablet(1001L));
                    return list;
                }
                case "getTabletIdsFromBe": {
                    Set<Long> tabletIds = new HashSet<>();
                    tabletIds.add(1001L);
                    return tabletIds;
                }
                default:
                    return invocation.callRealMethod();
            }
        });

        long jobId = 1L;
        String dstClusterName = "test_cluster";
        List<Triple<String, String, String>> tables = new ArrayList<>();
        tables.add(Triple.of("test_db", "test_table", ""));

        Map<Long, List<Tablet>> result = cacheHotspotManager.warmUpNewClusterByTable(
                jobId, dstClusterName, tables, true);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1001L, result.get(11L).get(0).getId());

        RuntimeException exception = Assert.assertThrows(RuntimeException.class, () ->
                cacheHotspotManager.warmUpNewClusterByTable(jobId, dstClusterName, tables, false));
        Assert.assertEquals("The cluster " + dstClusterName + " cache size is not enough", exception.getMessage());
    }

    @Test
    public void testCreateTableOnceJobRejectsPendingDuplicateOrderDifference() throws AnalysisException {
        long firstJobId = cacheHotspotManager.createJob(newTableStmt("dst", false,
                Triple.of("db1", "tbl1", ""),
                Triple.of("db2", "tbl2", "p1")));
        AnalysisException exception = Assert.assertThrows(AnalysisException.class, () ->
                cacheHotspotManager.createJob(newTableStmt("dst", false,
                Triple.of("db2", "tbl2", "p1"),
                Triple.of("db1", "tbl1", ""))));

        Assert.assertTrue(exception.getMessage().contains("already has a pending job"));
        Assert.assertTrue(exception.getMessage().contains("job id: " + firstJobId));
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
        Mockito.verify(env, Mockito.times(1)).getNextId();
        Mockito.verify(editLog, Mockito.times(1)).logModifyCloudWarmUpJob(Mockito.any(CloudWarmUpJob.class));
    }

    @Test
    public void testCreateTableOnceJobRejectsPendingDuplicateTableEntries() throws AnalysisException {
        long firstJobId = cacheHotspotManager.createJob(newTableStmt("dst", false,
                Triple.of("db1", "tbl1", ""),
                Triple.of("db1", "tbl1", "")));
        AnalysisException exception = Assert.assertThrows(AnalysisException.class, () ->
                cacheHotspotManager.createJob(newTableStmt("dst", false, Triple.of("db1", "tbl1", ""))));

        Assert.assertTrue(exception.getMessage().contains("already has a pending job"));
        Assert.assertTrue(exception.getMessage().contains("job id: " + firstJobId));
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateTableOnceJobDoesNotDedupDifferentForce() throws AnalysisException {
        long forceFalseJobId = cacheHotspotManager.createJob(newTableStmt("dst", false,
                Triple.of("db1", "tbl1", "")));
        long forceTrueJobId = cacheHotspotManager.createJob(newTableStmt("dst", true,
                Triple.of("db1", "tbl1", "")));

        Assert.assertNotEquals(forceFalseJobId, forceTrueJobId);
        Assert.assertEquals(2, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateClusterOnceJobDedupesPendingJob() throws AnalysisException {
        long firstJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));
        long reusedJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));

        Assert.assertEquals(firstJobId, reusedJobId);
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateClusterOnceJobDedupesRegardlessOfForceFlag() throws AnalysisException {
        long firstJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));
        long reusedJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", true));

        Assert.assertEquals(firstJobId, reusedJobId);
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateClusterOnceJobAllowsNewPendingWhenOnlyRunningExists() throws AnalysisException {
        CloudWarmUpJob runningJob = newClusterJob(10L, "src", "dst", SyncMode.ONCE, JobState.RUNNING, 100L);
        cacheHotspotManager.addCloudWarmUpJob(runningJob);

        long newJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));

        Assert.assertNotEquals(runningJob.getJobId(), newJobId);
        Assert.assertEquals(2, cacheHotspotManager.getCloudWarmUpJobs().size());
        Assert.assertEquals(JobState.PENDING, cacheHotspotManager.getCloudWarmUpJob(newJobId).getJobState());
    }

    @Test
    public void testCreateClusterOnceJobReusesPendingWhenRunningAndPendingExist() throws AnalysisException {
        CloudWarmUpJob runningJob = newClusterJob(10L, "src", "dst", SyncMode.ONCE, JobState.RUNNING, 100L);
        CloudWarmUpJob pendingJob = newClusterJob(11L, "src", "dst", SyncMode.ONCE, JobState.PENDING, 200L);
        cacheHotspotManager.addCloudWarmUpJob(runningJob);
        cacheHotspotManager.addCloudWarmUpJob(pendingJob);

        long reusedJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));

        Assert.assertEquals(pendingJob.getJobId(), reusedJobId);
        Assert.assertEquals(2, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateOnceJobIgnoresFinishedHistory() throws AnalysisException {
        CloudWarmUpJob finishedJob = newClusterJob(10L, "src", "dst", SyncMode.ONCE, JobState.FINISHED, 100L);
        cacheHotspotManager.addCloudWarmUpJob(finishedJob);

        long newJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));

        Assert.assertNotEquals(finishedJob.getJobId(), newJobId);
        Assert.assertEquals(2, cacheHotspotManager.getCloudWarmUpJobs().size());
        Assert.assertEquals(JobState.PENDING, cacheHotspotManager.getCloudWarmUpJob(newJobId).getJobState());
    }

    @Test
    public void testCreateClusterOnceJobReusesOldestHistoricalPendingDuplicateAfterReplay() throws Exception {
        CloudWarmUpJob newerPendingJob = newClusterJob(20L, "src", "dst", SyncMode.ONCE, JobState.PENDING, 200L);
        CloudWarmUpJob olderPendingJob = newClusterJob(30L, "src", "dst", SyncMode.ONCE, JobState.PENDING, 100L);
        cacheHotspotManager.replayCloudWarmUpJob(newerPendingJob);
        cacheHotspotManager.replayCloudWarmUpJob(olderPendingJob);

        long reusedJobId = cacheHotspotManager.createJob(newClusterStmt("dst", "src", false));

        Assert.assertEquals(olderPendingJob.getJobId(), reusedJobId);
        Assert.assertEquals(2, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateTableOnceJobRemovesLockEntryWhenCreateFails() throws Exception {
        boolean previousRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        cacheHotspotManager = new CacheHotspotManager(cloudSystemInfoService) {
            @Override
            public Map<Long, List<Tablet>> warmUpNewClusterByTable(long jobId, String dstClusterName,
                    List<Triple<String, String, String>> tables, boolean isForce) {
                throw new RuntimeException("mock create failure");
            }
        };

        try {
            RuntimeException exception = Assert.assertThrows(RuntimeException.class, () ->
                    cacheHotspotManager.createJob(newTableStmt("dst", false, Triple.of("db1", "tbl1", ""))));

            Assert.assertEquals("mock create failure", exception.getMessage());
            Assert.assertEquals(0, getOncePendingCreateLockCount());
            Assert.assertEquals(0, cacheHotspotManager.getCloudWarmUpJobs().size());
        } finally {
            FeConstants.runningUnitTest = previousRunningUnitTest;
        }
    }

    @Test
    public void testConcurrentCreateClusterOnceJobReleasesRefCountedLockAfterWaiterCompletes() throws Exception {
        CountDownLatch firstCreateEntered = new CountDownLatch(1);
        CountDownLatch allowFirstCreateToContinue = new CountDownLatch(1);
        AtomicInteger getNextIdCalls = new AtomicInteger();
        Mockito.when(env.getNextId()).thenAnswer(invocation -> {
            if (getNextIdCalls.incrementAndGet() == 1) {
                firstCreateEntered.countDown();
                Assert.assertTrue(allowFirstCreateToContinue.await(5, TimeUnit.SECONDS));
            }
            return nextJobId.getAndIncrement();
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Long> firstCreate = executor.submit(() -> createJobWithThreadLocalEnv(
                    newClusterStmt("dst", "src", false)));
            Assert.assertTrue(firstCreateEntered.await(5, TimeUnit.SECONDS));

            Future<Long> secondCreate = executor.submit(() -> createJobWithThreadLocalEnv(
                    newClusterStmt("dst", "src", false)));
            waitForOncePendingCreateLockRefCount(2, 5000L);

            allowFirstCreateToContinue.countDown();

            long firstJobId = firstCreate.get(5, TimeUnit.SECONDS);
            long secondJobId = secondCreate.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(firstJobId, secondJobId);
            Assert.assertEquals(1, getNextIdCalls.get());
            Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
            Assert.assertEquals(0, getOncePendingCreateLockCount());
        } finally {
            allowFirstCreateToContinue.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCreatePeriodicJobUnaffected() throws AnalysisException {
        WarmUpClusterCommand periodicStmt = newClusterStmt("dst", "src", false, periodicProperties(60));
        long firstJobId = cacheHotspotManager.createJob(periodicStmt);
        AnalysisException exception = Assert.assertThrows(AnalysisException.class, () ->
                cacheHotspotManager.createJob(newClusterStmt("dst", "src", false, periodicProperties(60))));

        Assert.assertEquals(1000L, firstJobId);
        Assert.assertTrue(exception.getMessage().contains("already has a runnable job"));
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testCreateEventDrivenJobUnaffected() throws AnalysisException {
        WarmUpClusterCommand eventDrivenStmt = newClusterStmt("dst", "src", false, eventDrivenProperties("load"));
        long firstJobId = cacheHotspotManager.createJob(eventDrivenStmt);
        AnalysisException exception = Assert.assertThrows(AnalysisException.class, () ->
                cacheHotspotManager.createJob(newClusterStmt("dst", "src", false, eventDrivenProperties("load"))));

        Assert.assertEquals(1000L, firstJobId);
        Assert.assertTrue(exception.getMessage().contains("already has a runnable job"));
        Assert.assertEquals(1, cacheHotspotManager.getCloudWarmUpJobs().size());
    }

    @Test
    public void testTryRegisterRunningJobLogsBlockedResult() {
        CloudWarmUpJob runningJob = newClusterJob(10L, "src_a", "dst_a", SyncMode.ONCE, JobState.RUNNING, 100L);
        CloudWarmUpJob blockedJob = newClusterJob(11L, "src_b", "dst_a", SyncMode.ONCE, JobState.PENDING, 200L);

        RecordingAppender appender = new RecordingAppender("warmup-lock-register-log-test");
        Logger logger = (Logger) LogManager.getLogger(CacheHotspotManager.class);
        appender.start();
        logger.addAppender(appender);
        try {
            Assert.assertTrue(cacheHotspotManager.tryRegisterRunningJob(runningJob));
            Assert.assertFalse(cacheHotspotManager.tryRegisterRunningJob(blockedJob));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }

        String logs = appender.messagesAsString();
        Assert.assertTrue(logs, logs.contains("warmup-lock register"));
        Assert.assertTrue(logs, logs.contains("jobId=11"));
        Assert.assertTrue(logs, logs.contains("existingJobId=10"));
        Assert.assertTrue(logs, logs.contains("registerResult=blocked"));
    }

    private WarmUpClusterCommand newTableStmt(String dstClusterName, boolean force,
            Triple<String, String, String>... tables) {
        WarmUpClusterCommand stmt = new WarmUpClusterCommand(new ArrayList<>(),
                null, dstClusterName, force, true);
        for (Triple<String, String, String> table : tables) {
            stmt.getTables().add(table);
        }
        return stmt;
    }

    private WarmUpClusterCommand newClusterStmt(String dstClusterName, String srcClusterName, boolean force) {
        return newClusterStmt(dstClusterName, srcClusterName, force, new HashMap<>());
    }

    private WarmUpClusterCommand newClusterStmt(String dstClusterName, String srcClusterName,
            boolean force, Map<String, String> properties) {
        return new WarmUpClusterCommand(null, srcClusterName, dstClusterName, force, false, properties);
    }

    private Map<String, String> periodicProperties(long syncIntervalSec) {
        Map<String, String> properties = new HashMap<>();
        properties.put("sync_mode", "periodic");
        properties.put("sync_interval_sec", String.valueOf(syncIntervalSec));
        return properties;
    }

    private Map<String, String> eventDrivenProperties(String syncEvent) {
        Map<String, String> properties = new HashMap<>();
        properties.put("sync_mode", "event_driven");
        properties.put("sync_event", syncEvent);
        return properties;
    }

    private CloudWarmUpJob newClusterJob(long jobId, String srcClusterName, String dstClusterName,
            SyncMode syncMode, JobState jobState, long createTimeMs) {
        CloudWarmUpJob.Builder builder = new CloudWarmUpJob.Builder()
                .setJobId(jobId)
                .setSrcClusterName(srcClusterName)
                .setDstClusterName(dstClusterName)
                .setJobType(JobType.CLUSTER)
                .setSyncMode(syncMode);
        if (syncMode == SyncMode.PERIODIC) {
            builder.setSyncInterval(60L);
        } else if (syncMode == SyncMode.EVENT_DRIVEN) {
            builder.setSyncEvent(SyncEvent.LOAD);
        }
        CloudWarmUpJob job = builder.build();
        job.setJobState(jobState);
        job.setCreateTimeMs(createTimeMs);
        return job;
    }

    private int getOncePendingCreateLockCount() throws Exception {
        return getOncePendingCreateLocks().size();
    }

    private long createJobWithThreadLocalEnv(WarmUpClusterCommand command) throws AnalysisException {
        try (MockedStatic<Env> threadLocalEnvMock = Mockito.mockStatic(Env.class)) {
            threadLocalEnvMock.when(Env::getCurrentEnv).thenReturn(env);
            return cacheHotspotManager.createJob(command);
        }
    }

    private int getOnlyOncePendingCreateLockRefCount() throws Exception {
        Map<?, ?> locks = getOncePendingCreateLocks();
        Assert.assertEquals(1, locks.size());
        Object lockEntry = locks.values().iterator().next();
        Field refCountField = lockEntry.getClass().getDeclaredField("refCount");
        refCountField.setAccessible(true);
        return refCountField.getInt(lockEntry);
    }

    private Map<?, ?> getOncePendingCreateLocks() throws Exception {
        Field locksField = CacheHotspotManager.class.getDeclaredField("oncePendingCreateLocks");
        locksField.setAccessible(true);
        return (Map<?, ?>) locksField.get(cacheHotspotManager);
    }

    private void waitForOncePendingCreateLockRefCount(int expectedRefCount, long timeoutMs) throws Exception {
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadlineMs) {
            if (getOncePendingCreateLockCount() == 1
                    && getOnlyOncePendingCreateLockRefCount() == expectedRefCount) {
                return;
            }
            Thread.sleep(10L);
        }
        Assert.fail("Timed out waiting for once pending create lock ref count " + expectedRefCount);
    }

    private static class RecordingAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        RecordingAppender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        String messagesAsString() {
            return String.join("\n", messages);
        }
    }
}
