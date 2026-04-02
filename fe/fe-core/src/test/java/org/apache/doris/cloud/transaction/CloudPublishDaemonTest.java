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

import org.apache.doris.catalog.CloudTabletStatMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.event.Event;
import org.apache.doris.event.EventProcessor;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CalcDeleteBitmapAsyncPublishTask;
import org.apache.doris.thrift.TCalcDeleteBitmapAsyncPublishRequest;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TxnStateCallbackFactory;

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
import java.util.concurrent.atomic.AtomicReference;

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

        // Create TxnStateCallbackFactory for CloudPublishDaemon
        TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();

        daemon = new CloudPublishDaemon(committedTxnManager, callbackFactory);
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
     * Test: dispatch creates CalcDeleteBitmapAsyncPublishTask per-BE and adds to tracking map.
     */
    @Test
    public void testDispatchCalcDeleteBitmapAsyncPublishTasks() {
        CommittedTxnEntry entry = buildEntryTwoBEs(TXN_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Run one scheduling cycle
        daemon.runAfterCatalogReady();

        // Verify tasks were created for both backends
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertNotNull("tasks should be created", tasks);
        Assert.assertEquals("should have 2 BE tasks", 2, tasks.size());
        Assert.assertTrue("should have task for BE1", tasks.containsKey(BACKEND_ID_1));
        Assert.assertTrue("should have task for BE2", tasks.containsKey(BACKEND_ID_2));

        TCalcDeleteBitmapAsyncPublishRequest req = tasks.get(BACKEND_ID_1).toThrift();
        TCalcDeleteBitmapPartitionInfo partitionInfo = req.getPartitions().get(0);
        Assert.assertTrue("db id should be propagated", partitionInfo.isSetDbId());
        Assert.assertEquals(DB_ID, partitionInfo.getDbId());
        Assert.assertTrue("table id should be propagated", partitionInfo.isSetTableId());
        Assert.assertEquals(TABLE_ID, partitionInfo.getTableId());
        Assert.assertTrue("index ids should be propagated", partitionInfo.isSetIndexIds());
        Assert.assertEquals(1, partitionInfo.getIndexIdsSize());
        Assert.assertEquals(0L, partitionInfo.getIndexIds().get(0).longValue());

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
        Map<Long, CalcDeleteBitmapAsyncPublishTask> firstTasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        Assert.assertNotNull(firstTasks);

        // Run again - should not create new tasks
        daemon.runAfterCatalogReady();
        Map<Long, CalcDeleteBitmapAsyncPublishTask> sameTasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
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
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        tasks.get(BACKEND_ID_1).setIsFinished(true);

        // Run again - should NOT trigger lightweight publish
        daemon.runAfterCatalogReady();

        // Give thread pool a moment
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
            // ignored
        }

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
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
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
        // Set retry times to 1 so that KV_TXN_CONFLICT will exhaust retries immediately
        int originalRetryTimes = Config.meta_service_rpc_retry_times;
        Config.meta_service_rpc_retry_times = 1;

        try {
            AtomicInteger commitTxnCalls = new AtomicInteger(0);
            CountDownLatch secondCall = new CountDownLatch(1);
            new MockUp<MetaServiceProxy>() {
                @Mock
                public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                    int call = commitTxnCalls.incrementAndGet();
                    if (call == 1) {
                        // First call fails with non-conflict error (won't be retried)
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
        } finally {
            Config.meta_service_rpc_retry_times = originalRetryTimes;
        }
    }

    /**
     * Test: CalcDeleteBitmapAsyncPublishTask basic behavior.
     */
    @Test
    public void testCalcDeleteBitmapAsyncPublishTaskAsyncMode() {
        // Async publish task
        CalcDeleteBitmapAsyncPublishTask asyncTask = new CalcDeleteBitmapAsyncPublishTask(
                BACKEND_ID_1, TXN_ID_1, DB_ID, Lists.newArrayList(), TXN_ID_1);
        Assert.assertFalse("should not be finished initially", asyncTask.isFinished());
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

    /**
     * Test: lightweight publish passes tablet ids from CommittedTxnEntry to afterCommitCommon,
     * ensuring post-publish operations (updateVersion, addActiveTablets, etc.) are triggered.
     */
    @Test
    public void testPublishPassesTabletIdsToAfterCommitCommon() throws Exception {
        // Track tabletIds passed to CloudTabletStatMgr.addActiveTablets
        AtomicReference<List<Long>> capturedTabletIds = new AtomicReference<>(null);

        new MockUp<CloudTabletStatMgr>() {
            @Mock
            public CloudTabletStatMgr getInstance() {
                return new CloudTabletStatMgr();
            }

            @Mock
            public void addActiveTablets(List<Long> tabletIds) {
                capturedTabletIds.set(tabletIds);
            }
        };

        // Mock AnalysisManager and EventProcessor as no-ops
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateUpdatedRows(Map<Long, Long> updatedRows) {
            }

            @Mock
            public void setNewPartitionLoaded(List<Long> tableIds) {
            }
        };
        new MockUp<EventProcessor>() {
            @Mock
            public boolean processEvent(Event event) {
                return true;
            }
        };
        new MockUp<Env>() {
            @Mock
            public TabletInvertedIndex getCurrentInvertedIndex() {
                LocalTabletInvertedIndex mockIndex = new LocalTabletInvertedIndex();
                TabletMeta meta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, 0, 0, TStorageMedium.HDD);
                mockIndex.addTablet(TABLET_ID_1, meta);
                mockIndex.addTablet(TABLET_ID_2, meta);
                return mockIndex;
            }

            @Mock
            public AnalysisManager getAnalysisManager() {
                return new AnalysisManager();
            }

            @Mock
            public EventProcessor getEventProcessor() {
                return new EventProcessor();
            }
        };

        CountDownLatch publishCalled = new CountDownLatch(1);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                publishCalled.countDown();
                TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(DB_ID);
                txnInfoBuilder.setTxnId(TXN_ID_1);
                txnInfoBuilder.addTableIds(TABLE_ID);

                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(txnInfoBuilder.build())
                        .build();
            }
        };

        // Build entry with two tablets on two BEs
        CommittedTxnEntry entry = buildEntryTwoBEs(TXN_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Dispatch and finish tasks
        daemon.runAfterCatalogReady();
        Map<Long, CalcDeleteBitmapAsyncPublishTask> tasks = daemon.getTxnCalcTasks().get(TXN_ID_1);
        tasks.get(BACKEND_ID_1).setIsFinished(true);
        tasks.get(BACKEND_ID_2).setIsFinished(true);

        // Trigger lightweight publish
        daemon.runAfterCatalogReady();
        boolean called = publishCalled.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("lightweight publish should be called", called);
        Thread.sleep(200);

        // Verify tabletIds were passed to addActiveTablets
        Assert.assertNotNull("addActiveTablets should be called with tabletIds",
                capturedTabletIds.get());
        Assert.assertEquals("should have 2 tablet ids", 2, capturedTabletIds.get().size());
        Assert.assertTrue("should contain TABLET_ID_1",
                capturedTabletIds.get().contains(TABLET_ID_1));
        Assert.assertTrue("should contain TABLET_ID_2",
                capturedTabletIds.get().contains(TABLET_ID_2));
    }

    /**
     * Test: lightweight publish response with row count data triggers AnalysisManager update.
     */
    @Test
    public void testPublishUpdatesRowCount() throws Exception {
        long updatedRowCount = 500;
        AtomicReference<Map<Long, Long>> capturedUpdatedRows = new AtomicReference<>(null);

        new MockUp<AnalysisManager>() {
            @Mock
            public void updateUpdatedRows(Map<Long, Long> updatedRows) {
                capturedUpdatedRows.set(updatedRows);
            }

            @Mock
            public void setNewPartitionLoaded(List<Long> tableIds) {
            }
        };
        new MockUp<EventProcessor>() {
            @Mock
            public boolean processEvent(Event event) {
                return true;
            }
        };
        new MockUp<CloudTabletStatMgr>() {
            @Mock
            public CloudTabletStatMgr getInstance() {
                return new CloudTabletStatMgr();
            }

            @Mock
            public void addActiveTablets(List<Long> tabletIds) {
            }
        };
        new MockUp<Env>() {
            @Mock
            public TabletInvertedIndex getCurrentInvertedIndex() {
                LocalTabletInvertedIndex mockIndex = new LocalTabletInvertedIndex();
                TabletMeta meta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, 0, 0, TStorageMedium.HDD);
                mockIndex.addTablet(TABLET_ID_1, meta);
                return mockIndex;
            }

            @Mock
            public AnalysisManager getAnalysisManager() {
                return new AnalysisManager();
            }

            @Mock
            public EventProcessor getEventProcessor() {
                return new EventProcessor();
            }

            @Mock
            public InternalCatalog getInternalCatalog() {
                return new InternalCatalog();
            }
        };

        CountDownLatch publishCalled = new CountDownLatch(1);
        new MockUp<MetaServiceProxy>() {
            @Mock
            public CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                publishCalled.countDown();
                TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(DB_ID);
                txnInfoBuilder.setTxnId(TXN_ID_1);
                txnInfoBuilder.addTableIds(TABLE_ID);

                Cloud.TableStatsPB.Builder statsBuilder = Cloud.TableStatsPB.newBuilder();
                statsBuilder.setTableId(TABLE_ID);
                statsBuilder.setUpdatedRowCount(updatedRowCount);

                return CommitTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(txnInfoBuilder.build())
                        .addTableStats(statsBuilder.build())
                        .build();
            }
        };

        CommittedTxnEntry entry = buildEntry(TXN_ID_1, BACKEND_ID_1, TABLET_ID_1);
        committedTxnManager.addCommittedTxn(entry);

        // Dispatch and finish
        daemon.runAfterCatalogReady();
        daemon.getTxnCalcTasks().get(TXN_ID_1).get(BACKEND_ID_1).setIsFinished(true);
        daemon.runAfterCatalogReady();

        boolean called = publishCalled.await(5, TimeUnit.SECONDS);
        Assert.assertTrue("lightweight publish should be called", called);
        Thread.sleep(2000);

        // Verify row count was updated
        Assert.assertNotNull("updateUpdatedRows should be called", capturedUpdatedRows.get());
        Assert.assertEquals("should have 1 table entry", 1, capturedUpdatedRows.get().size());
        Assert.assertEquals("row count should match",
                Long.valueOf(updatedRowCount), capturedUpdatedRows.get().get(TABLE_ID));
    }
}
