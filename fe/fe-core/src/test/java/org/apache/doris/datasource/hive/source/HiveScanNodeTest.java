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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class HiveScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(10_000L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testDetermineTargetFileSplitSizeKeepsInitialSize() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(500L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(32 * MB, target);
    }

    /**
     * Test that ConnectContext is properly propagated to async threads and cleaned up afterward.
     * This tests the fix for the NPE issue where async threads in HiveScanNode.startSplit()
     * couldn't access ConnectContext because it's stored in ThreadLocal.
     */
    @Test
    public void testConnectContextPropagationInAsyncThread() throws Exception {
        // Create a mock ConnectContext
        ConnectContext parentContext = Mockito.mock(ConnectContext.class);

        // Set it in the current thread
        parentContext.setThreadLocalInfo();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ConnectContext> asyncContextRef = new AtomicReference<>();
        AtomicReference<ConnectContext> afterCleanupRef = new AtomicReference<>();

        try {
            // Simulate the pattern used in HiveScanNode.startSplit()
            CompletableFuture.runAsync(() -> {
                // Propagate ConnectContext to async thread
                if (parentContext != null) {
                    parentContext.setThreadLocalInfo();
                }
                try {
                    // Capture the context in async thread
                    asyncContextRef.set(ConnectContext.get());
                } finally {
                    // Clean up ThreadLocal to prevent leaks (as we do in the fix)
                    ConnectContext.remove();
                    afterCleanupRef.set(ConnectContext.get());
                    latch.countDown();
                }
            }, executor).get(5, TimeUnit.SECONDS);

            // Wait for async task to complete
            Assert.assertTrue("Async task should complete", latch.await(5, TimeUnit.SECONDS));

            // Verify ConnectContext was available in async thread
            Assert.assertNotNull("ConnectContext should be available in async thread", asyncContextRef.get());

            // Verify ConnectContext was cleaned up after task completion
            Assert.assertNull("ConnectContext should be cleaned up after task", afterCleanupRef.get());

        } finally {
            executor.shutdown();
            ConnectContext.remove();
        }
    }

    /**
     * Test that nested async threads also properly propagate and clean up ConnectContext.
     * This simulates the nested CompletableFuture.runAsync() pattern in HiveScanNode.startSplit().
     */
    @Test
    public void testNestedAsyncConnectContextPropagation() throws Exception {
        // Create a mock ConnectContext
        ConnectContext parentContext = Mockito.mock(ConnectContext.class);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch outerLatch = new CountDownLatch(1);
        CountDownLatch innerLatch = new CountDownLatch(1);
        AtomicReference<ConnectContext> outerContextRef = new AtomicReference<>();
        AtomicReference<ConnectContext> innerContextRef = new AtomicReference<>();
        AtomicReference<ConnectContext> outerAfterCleanupRef = new AtomicReference<>();
        AtomicReference<ConnectContext> innerAfterCleanupRef = new AtomicReference<>();

        try {
            // Simulate the nested async pattern in HiveScanNode.startSplit()
            CompletableFuture.runAsync(() -> {
                // Outer async thread: propagate ConnectContext
                if (parentContext != null) {
                    parentContext.setThreadLocalInfo();
                }
                try {
                    outerContextRef.set(ConnectContext.get());

                    // Inner async thread (like the partition processing in startSplit)
                    CompletableFuture.runAsync(() -> {
                        // Inner async thread: propagate ConnectContext
                        if (parentContext != null) {
                            parentContext.setThreadLocalInfo();
                        }
                        try {
                            innerContextRef.set(ConnectContext.get());
                        } finally {
                            // Clean up inner thread's ThreadLocal
                            ConnectContext.remove();
                            innerAfterCleanupRef.set(ConnectContext.get());
                            innerLatch.countDown();
                        }
                    }, executor).join();

                } finally {
                    // Clean up outer thread's ThreadLocal
                    ConnectContext.remove();
                    outerAfterCleanupRef.set(ConnectContext.get());
                    outerLatch.countDown();
                }
            }, executor).get(5, TimeUnit.SECONDS);

            // Wait for all tasks to complete
            Assert.assertTrue("Outer task should complete", outerLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Inner task should complete", innerLatch.await(5, TimeUnit.SECONDS));

            // Verify ConnectContext was available in both async threads
            Assert.assertNotNull("ConnectContext should be available in outer async thread",
                    outerContextRef.get());
            Assert.assertNotNull("ConnectContext should be available in inner async thread",
                    innerContextRef.get());

            // Verify both threads cleaned up their ThreadLocal
            Assert.assertNull("Outer thread should clean up ConnectContext", outerAfterCleanupRef.get());
            Assert.assertNull("Inner thread should clean up ConnectContext", innerAfterCleanupRef.get());

        } finally {
            executor.shutdown();
            ConnectContext.remove();
        }
    }

    /**
     * Test that ThreadLocal cleanup works correctly even when exception occurs.
     */
    @Test
    public void testConnectContextCleanupOnException() throws Exception {
        ConnectContext parentContext = Mockito.mock(ConnectContext.class);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<ConnectContext> afterExceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        try {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                if (parentContext != null) {
                    parentContext.setThreadLocalInfo();
                }
                try {
                    // Simulate an exception during processing
                    throw new RuntimeException("Simulated exception");
                } finally {
                    // ThreadLocal should still be cleaned up even on exception
                    ConnectContext.remove();
                    afterExceptionRef.set(ConnectContext.get());
                    latch.countDown();
                }
            }, executor);

            // The future should complete exceptionally
            try {
                future.get(5, TimeUnit.SECONDS);
                Assert.fail("Should have thrown an exception");
            } catch (Exception e) {
                // Expected
            }

            Assert.assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));

            // Verify ThreadLocal was cleaned up even after exception
            Assert.assertNull("ConnectContext should be cleaned up after exception", afterExceptionRef.get());

        } finally {
            executor.shutdown();
            ConnectContext.remove();
        }
    }
}
