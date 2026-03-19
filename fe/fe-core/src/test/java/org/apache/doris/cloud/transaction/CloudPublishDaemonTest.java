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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CalcDeleteBitmapTask;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudPublishDaemonTest {

    private CommittedTxnManager committedTxnManager;
    private CloudPublishDaemon daemon;
    private static final long DB_ID = 10001;
    private static final long TABLE_ID = 10002;
    private static final long PARTITION_ID = 10003;
    private static final long TABLET_ID_1 = 10004;
    private static final long TABLET_ID_2 = 10005;
    private static final long BACKEND_ID_1 = 20001;
    private static final long BACKEND_ID_2 = 20002;
    private static final long TXN_ID_1 = 30001;
    private static final long TXN_ID_2 = 30002;
    private static final long COMMIT_VERSION = 2;

    @Before
    public void setUp() {
        Config.cloud_unique_id = "cloud_unique_id_test";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        Config.cloud_publish_interval_ms = 100;
        Config.cloud_publish_thread_pool_size = 4;
        committedTxnManager = new CommittedTxnManager();

        // Mock TabletInvertedIndex to resolve tablet -> partition
        LocalTabletInvertedIndex mockIndex = new LocalTabletInvertedIndex();
        // Add tablet meta for test tablets
        TabletMeta meta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, 0, 0, TStorageMedium.HDD);
        mockIndex.addTablet(TABLET_ID_1, meta);
        mockIndex.addTablet(TABLET_ID_2, meta);

        new MockUp<Env>() {
            @Mock
            public TabletInvertedIndex getCurrentInvertedIndex() {
                return mockIndex;
            }
        };

        // Mock AgentTaskExecutor to be a no-op (don't actually send to BE)
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                // no-op in test
            }
        };

        daemon = new CloudPublishDaemon(committedTxnManager);
    }

    private CommittedTxnEntry buildEntry(long txnId, long backendId1, long tabletId1) {
        Map<Long, Long> partitionVersions = Maps.newHashMap();
        partitionVersions.put(PARTITION_ID, COMMIT_VERSION);
        List<TabletCommitInfo> commitInfos = Lists.newArrayList(
                new TabletCommitInfo(tabletId1, backendId1));
        return new CommittedTxnEntry(txnId, DB_ID, TABLE_ID,
                partitionVersions, commitInfos, null);
    }

    private CommittedTxnEntry buildEntryTwoBEs(long txnId) {
        Map<Long, Long> partitionVersions = Maps.newHashMap();
        partitionVersions.put(PARTITION_ID, COMMIT_VERSION);
        List<TabletCommitInfo> commitInfos = Lists.newArrayList(
                new TabletCommitInfo(TABLET_ID_1, BACKEND_ID_1),
                new TabletCommitInfo(TABLET_ID_2, BACKEND_ID_2));
        return new CommittedTxnEntry(txnId, DB_ID, TABLE_ID,
                partitionVersions, commitInfos, null);
    }

    /**
     * Test: dispatch creates CalcDeleteBitmapTask per-BE and adds to tracking map.
     */
    @Test
    public void testDispatchCalcDeleteBitmapTasks() {
        CommittedTxnEntry entry = buildEntryTwoBEs(TXN_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Run one scheduling cycle
        daemon.runAfterCatalogReady();

        // Verify tasks were created for both backends
        Map<Long, CalcDeleteBitmapTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertNotNull("tasks should be created", tasks);
        Assert.assertEquals("should have 2 BE tasks", 2, tasks.size());
        Assert.assertTrue("should have task for BE1", tasks.containsKey(BACKEND_ID_1));
        Assert.assertTrue("should have task for BE2", tasks.containsKey(BACKEND_ID_2));

        // Tasks should not be finished yet
        Assert.assertFalse(tasks.get(BACKEND_ID_1).isFinished());
        Assert.assertFalse(tasks.get(BACKEND_ID_2).isFinished());

        // Txn should still be in committed set
        Assert.assertTrue(committedTxnManager.contains(TXN_ID_1));
    }

    /**
     * Test: tasks not re-dispatched if already sent.
     */
    @Test
    public void testNoDoubleDispatch() {
        CommittedTxnEntry entry = buildEntry(TXN_ID_1, BACKEND_ID_1, TABLET_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        daemon.runAfterCatalogReady();
        Map<Long, CalcDeleteBitmapTask> firstTasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertNotNull(firstTasks);

        // Run again - should not create new tasks
        daemon.runAfterCatalogReady();
        Map<Long, CalcDeleteBitmapTask> sameTasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertSame("should be same task objects", firstTasks, sameTasks);
    }

    /**
     * Test: when not all tasks finished, lightweight publish is NOT triggered.
     */
    @Test
    public void testNoPublishWhenTasksNotFinished() {
        // Mock commitTxn to track if it's called
        AtomicInteger commitTxnCalls = new AtomicInteger(0);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                commitTxnCalls.incrementAndGet();
                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .build();
            }
        };

        CommittedTxnEntry entry = buildEntryTwoBEs(TXN_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Dispatch tasks
        daemon.runAfterCatalogReady();

        // Mark only one BE task as finished
        Map<Long, CalcDeleteBitmapTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        tasks.get(BACKEND_ID_1).setIsFinished(true);

        // Run again - should NOT trigger lightweight publish
        daemon.runAfterCatalogReady();

        // Give thread pool a moment
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        Assert.assertEquals("commitTxn should not be called", 0, commitTxnCalls.get());
        Assert.assertTrue("txn should still be in committed set", committedTxnManager.contains(TXN_ID_1));
    }

    /**
     * Test: full flow - dispatch, all tasks finish, lightweight publish succeeds.
     */
    @Test
    public void testFullPublishFlow() throws Exception {
        // Mock commitTxn for lightweight publish
        CountDownLatch publishCalled = new CountDownLatch(1);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                // Verify is_lightweight_publish is set
                Assert.assertTrue("should be lightweight publish",
                        request.getIsLightweightPublish());
                publishCalled.countDown();
                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .build();
            }
        };

        CommittedTxnEntry entry = buildEntryTwoBEs(TXN_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Phase 1: dispatch
        daemon.runAfterCatalogReady();
        Map<Long, CalcDeleteBitmapTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertNotNull(tasks);

        // Simulate BE completing all tasks
        tasks.get(BACKEND_ID_1).setIsFinished(true);
        tasks.get(BACKEND_ID_2).setIsFinished(true);

        // Phase 2: detect all finished, trigger lightweight publish
        daemon.runAfterCatalogReady();

        // Wait for async lightweight publish
        boolean called = publishCalled.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("lightweight publish should be called", called);

        // Wait a bit for cleanup
        Thread.sleep(200);

        // Verify cleanup
        Assert.assertFalse("txn should be removed from committed set",
                committedTxnManager.contains(TXN_ID_1));
        Assert.assertNull("txn should be removed from tracking",
                daemon.getTxnCalcTasks().get(TXN_ID_1));

        // Verify import thread was notified
        Assert.assertTrue("publish should be marked succeeded", entry.isPublishSucceeded());
    }

    /**
     * Test: lightweight publish fails, retries on next cycle.
     */
    @Test
    public void testLightweightPublishRetry() throws Exception {
        AtomicInteger commitTxnCalls = new AtomicInteger(0);
        CountDownLatch secondCall = new CountDownLatch(1);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                int call = commitTxnCalls.incrementAndGet();
                if (call == 1) {
                    // First call fails
                    return CommitTxnResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                    .setCode(MetaServiceCode.KV_TXN_CONFLICT)
                                    .setMsg("conflict"))
                            .build();
                }
                // Second call succeeds
                secondCall.countDown();
                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .build();
            }
        };

        CommittedTxnEntry entry = buildEntry(TXN_ID_1, BACKEND_ID_1, TABLET_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Dispatch and mark finished
        daemon.runAfterCatalogReady();
        daemon.getTxnCalcTasks().get(TXN_ID_1).get(BACKEND_ID_1).setIsFinished(true);

        // First attempt - lightweight publish fails
        daemon.runAfterCatalogReady();
        Thread.sleep(300);

        // Txn should still be in committed set (publish failed)
        Assert.assertTrue("txn should still be committed after failed publish",
                committedTxnManager.contains(TXN_ID_1));

        // Second attempt - should retry and succeed
        daemon.runAfterCatalogReady();
        boolean ok = secondCall.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("second publish call should happen", ok);

        Thread.sleep(200);
        Assert.assertFalse("txn should be removed after successful publish",
                committedTxnManager.contains(TXN_ID_1));
    }

    /**
     * Test: CalcDeleteBitmapTask.isAsyncPublish() returns true when latch is null.
     */
    @Test
    public void testCalcDeleteBitmapTaskAsyncMode() {
        // Async mode: latch = null
        CalcDeleteBitmapTask asyncTask = new CalcDeleteBitmapTask(
                BACKEND_ID_1, TXN_ID_1, DB_ID, Lists.newArrayList(), TXN_ID_1, null);
        Assert.assertTrue("should be async publish", asyncTask.isAsyncPublish());
        Assert.assertFalse("should not be finished initially", asyncTask.isFinished());

        // Sync mode: latch != null
        org.apache.doris.common.MarkedCountDownLatch<Long, Long> latch =
                new org.apache.doris.common.MarkedCountDownLatch<>(1);
        CalcDeleteBitmapTask syncTask = new CalcDeleteBitmapTask(
                BACKEND_ID_1, TXN_ID_1, DB_ID, Lists.newArrayList(), TXN_ID_1, latch);
        Assert.assertFalse("should not be async publish", syncTask.isAsyncPublish());
    }

    /**
     * Test: import thread awaitPublish is woken up after publish completes.
     */
    @Test
    public void testImportThreadWakeup() throws Exception {
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .build();
            }
        };

        CommittedTxnEntry entry = buildEntry(TXN_ID_1, BACKEND_ID_1, TABLET_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Simulate import thread waiting
        CountDownLatch importDone = new CountDownLatch(1);
        Thread importThread = new Thread(() -> {
            try {
                boolean completed = entry.awaitPublish(10000);
                Assert.assertTrue("publish should complete", completed);
                Assert.assertTrue("publish should succeed", entry.isPublishSucceeded());
                importDone.countDown();
            } catch (InterruptedException e) {
                Assert.fail("should not be interrupted");
            }
        });
        importThread.start();

        // Dispatch and finish
        daemon.runAfterCatalogReady();
        daemon.getTxnCalcTasks().get(TXN_ID_1).get(BACKEND_ID_1).setIsFinished(true);
        daemon.runAfterCatalogReady();

        boolean ok = importDone.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("import thread should be woken up", ok);
    }

    /**
     * Test: multiple txns are processed independently.
     */
    @Test
    public void testMultipleTxns() throws Exception {
        AtomicInteger publishCount = new AtomicInteger(0);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                publishCount.incrementAndGet();
                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .build();
            }
        };

        CommittedTxnEntry entry1 = buildEntry(TXN_ID_1, BACKEND_ID_1, TABLET_ID_1);
        CommittedTxnEntry entry2 = buildEntry(TXN_ID_2, BACKEND_ID_2, TABLET_ID_2);
        committedTxnManager.addCommittedTxn(entry1);
        committedTxnManager.addCommittedTxn(entry2);

        // Dispatch both
        daemon.runAfterCatalogReady();
        Assert.assertNotNull(daemon.getTxnCalcTasks().get(TXN_ID_1));
        Assert.assertNotNull(daemon.getTxnCalcTasks().get(TXN_ID_2));

        // Finish only txn1
        daemon.getTxnCalcTasks().get(TXN_ID_1).get(BACKEND_ID_1).setIsFinished(true);
        daemon.runAfterCatalogReady();
        Thread.sleep(300);

        // txn1 should be published, txn2 still pending
        Assert.assertFalse("txn1 should be removed", committedTxnManager.contains(TXN_ID_1));
        Assert.assertTrue("txn2 should still be committed", committedTxnManager.contains(TXN_ID_2));

        // Finish txn2
        daemon.getTxnCalcTasks().get(TXN_ID_2).get(BACKEND_ID_2).setIsFinished(true);
        daemon.runAfterCatalogReady();
        Thread.sleep(300);

        Assert.assertFalse("txn2 should be removed", committedTxnManager.contains(TXN_ID_2));
    }
}
