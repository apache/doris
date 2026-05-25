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

package org.apache.doris.nereids.lineage;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link LineageEventProcessor} SPI-based plugin management
 * and event processing.
 */
public class LineageEventProcessorTest {
    private static final int SINGLE_LATCH_COUNT = 1;
    private static final int MULTI_EVENT_COUNT = 10;
    private static final long WORKER_WAIT_TIMEOUT_SECONDS = 10L;
    private static final long TEST_CONTEXT_TIMESTAMP_MS = 1000L;
    private static final long TEST_CONTEXT_DURATION_MS = 50L;

    // ==================== hasActivePlugins / refreshPlugins ====================

    @Test
    public void testHasActivePluginsDefaultEmpty() {
        LineageEventProcessor processor = new LineageEventProcessor();
        Assertions.assertFalse(processor.hasActivePlugins(),
                "Newly created processor should have no active plugins");
    }

    @Test
    public void testInitPluginsWithOnePlugin() {
        LineageEventProcessor processor = new LineageEventProcessor();
        LineagePlugin mockPlugin = new NoOpLineagePlugin("test-plugin");

        processor.initPlugins(Collections.singletonList(mockPlugin));
        Assertions.assertTrue(processor.hasActivePlugins(),
                "Should have active plugins after refresh with one plugin");
    }

    @Test
    public void testInitPluginsWithNull() {
        LineageEventProcessor processor = new LineageEventProcessor();
        processor.initPlugins(Collections.singletonList(new NoOpLineagePlugin("p1")));
        Assertions.assertTrue(processor.hasActivePlugins());

        processor.initPlugins(null);
        Assertions.assertFalse(processor.hasActivePlugins(),
                "Refreshing with null should clear plugins");
    }

    @Test
    public void testInitPluginsWithEmptyList() {
        LineageEventProcessor processor = new LineageEventProcessor();
        processor.initPlugins(Collections.singletonList(new NoOpLineagePlugin("p1")));
        Assertions.assertTrue(processor.hasActivePlugins());

        processor.initPlugins(Collections.emptyList());
        Assertions.assertFalse(processor.hasActivePlugins(),
                "Refreshing with empty list should clear plugins");
    }

    @Test
    public void testInitPluginsWithMultiplePlugins() {
        LineageEventProcessor processor = new LineageEventProcessor();
        List<LineagePlugin> plugins = Arrays.asList(
                new NoOpLineagePlugin("plugin-a"),
                new NoOpLineagePlugin("plugin-b"),
                new NoOpLineagePlugin("plugin-c")
        );
        processor.initPlugins(plugins);
        Assertions.assertTrue(processor.hasActivePlugins());
    }

    @Test
    public void testHasActivePluginsAllFilteredOut() {
        LineageEventProcessor processor = new LineageEventProcessor();
        LineagePlugin filterPlugin = new TrackingLineagePlugin("filter-out",
                false, new AtomicInteger(), new ArrayList<>(), new CountDownLatch(SINGLE_LATCH_COUNT));
        processor.initPlugins(Collections.singletonList(filterPlugin));
        Assertions.assertFalse(processor.hasActivePlugins(),
                "Should be false when all plugins are filtered out");
    }

    @Test
    public void testHasActivePluginsWhenFilterThrows() {
        LineageEventProcessor processor = new LineageEventProcessor();
        processor.initPlugins(Collections.singletonList(new EventFilterExceptionLineagePlugin("boom")));
        Assertions.assertFalse(processor.hasActivePlugins(),
                "Should be false when plugin filter throws exception");
    }

    // ==================== submitLineageEvent ====================

    @Test
    public void testSubmitLineageEventNull() {
        LineageEventProcessor processor = new LineageEventProcessor();
        Assertions.assertFalse(processor.submitLineageEvent(null),
                "Null event should not be accepted");
    }

    @Test
    public void testSubmitLineageEventSuccess() {
        LineageEventProcessor processor = new LineageEventProcessor();
        LineageInfo lineageInfo = createLineageInfo("q1");
        Assertions.assertTrue(processor.submitLineageEvent(lineageInfo),
                "Valid lineage event should be accepted");
    }

    @Test
    public void testSubmitLineageEventMultiple() {
        LineageEventProcessor processor = new LineageEventProcessor();
        for (int i = 0; i < MULTI_EVENT_COUNT; i++) {
            Assertions.assertTrue(processor.submitLineageEvent(createLineageInfo("q" + i)),
                    "Event #" + i + " should be accepted");
        }
    }

    // ==================== Worker event dispatch ====================

    @Test
    public void testWorkerDispatchesToPlugin() throws Exception {
        LineageEventProcessor processor = new LineageEventProcessor();
        CountDownLatch latch = new CountDownLatch(SINGLE_LATCH_COUNT);
        AtomicInteger execCount = new AtomicInteger(0);
        List<String> processedQueryIds = Collections.synchronizedList(new ArrayList<>());

        LineagePlugin trackingPlugin = new TrackingLineagePlugin("tracker",
                true, execCount, processedQueryIds, latch);

        processor.initPlugins(Collections.singletonList(trackingPlugin));

        // Start processor (without SPI discovery, since we already set plugins via refreshPlugins)
        Thread workerThread = new Thread(processor.new Worker(), "test-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        // Submit event
        LineageInfo event = createLineageInfo("test-query-1");
        Assertions.assertTrue(processor.submitLineageEvent(event));

        // Wait for worker to process
        boolean completed = latch.await(WORKER_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(completed, "Worker should process event within timeout");
        Assertions.assertEquals(SINGLE_LATCH_COUNT, execCount.get(), "Plugin exec should be called once");
        Assertions.assertEquals("test-query-1", processedQueryIds.get(0));
    }

    @Test
    public void testWorkerSkipsPluginWhenEventFilterReturnsFalse() throws Exception {
        LineageEventProcessor processor = new LineageEventProcessor();
        CountDownLatch filterLatch = new CountDownLatch(SINGLE_LATCH_COUNT);
        AtomicInteger execCount = new AtomicInteger(0);

        // Plugin that filters out events (eventFilter returns false)
        LineagePlugin filterPlugin = new TrackingLineagePlugin("filter-out",
                false, execCount, new ArrayList<>(), filterLatch);

        // A second plugin that accepts events to verify worker is running
        CountDownLatch acceptLatch = new CountDownLatch(SINGLE_LATCH_COUNT);
        AtomicInteger acceptExecCount = new AtomicInteger(0);
        LineagePlugin acceptPlugin = new TrackingLineagePlugin("accept",
                true, acceptExecCount, new ArrayList<>(), acceptLatch);

        processor.initPlugins(Arrays.asList(filterPlugin, acceptPlugin));

        Thread workerThread = new Thread(processor.new Worker(), "test-worker-filter");
        workerThread.setDaemon(true);
        workerThread.start();

        processor.submitLineageEvent(createLineageInfo("q-filter-test"));
        boolean completed = acceptLatch.await(WORKER_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(completed, "Accept plugin should process the event");
        Assertions.assertEquals(0, execCount.get(),
                "Filter plugin exec should NOT be called when eventFilter returns false");
        Assertions.assertEquals(SINGLE_LATCH_COUNT, acceptExecCount.get(),
                "Accept plugin exec should be called once");
    }

    @Test
    public void testWorkerHandlesPluginException() throws Exception {
        LineageEventProcessor processor = new LineageEventProcessor();
        CountDownLatch latch = new CountDownLatch(SINGLE_LATCH_COUNT);
        AtomicInteger goodExecCount = new AtomicInteger(0);

        // Bad plugin that throws exception
        LineagePlugin badPlugin = new ExceptionLineagePlugin("bad-plugin");

        // Good plugin after the bad one to verify processing continues
        LineagePlugin goodPlugin = new TrackingLineagePlugin("good-plugin",
                true, goodExecCount, new ArrayList<>(), latch);

        processor.initPlugins(Arrays.asList(badPlugin, goodPlugin));

        Thread workerThread = new Thread(processor.new Worker(), "test-worker-exception");
        workerThread.setDaemon(true);
        workerThread.start();

        processor.submitLineageEvent(createLineageInfo("q-exception-test"));
        boolean completed = latch.await(WORKER_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(completed,
                "Good plugin should still process event even if bad plugin throws");
        Assertions.assertEquals(SINGLE_LATCH_COUNT, goodExecCount.get(),
                "Good plugin exec should be called despite bad plugin exception");
    }

    // ==================== LineagePluginFactory SPI contract ====================

    @Test
    public void testLineagePluginFactoryContract() {
        LineagePluginFactory factory = new TestLineagePluginFactory();
        Assertions.assertEquals("test", factory.name());
        LineagePlugin plugin = factory.create();
        Assertions.assertNotNull(plugin);
        Assertions.assertEquals("test", plugin.name());
    }

    @Test
    public void testLineagePluginFactoryCreateWithContext() {
        LineagePluginFactory factory = new TestLineagePluginFactory();
        LineagePlugin plugin = factory.create(new PluginContext(Collections.singletonMap("plugin.name", "test")));
        Assertions.assertNotNull(plugin);
        Assertions.assertEquals("test", plugin.name());
    }

    // ==================== LineagePlugin interface contract ====================

    @Test
    public void testLineagePluginInitializeAndClose() throws Exception {
        NoOpLineagePlugin plugin = new NoOpLineagePlugin("lifecycle-test");
        Assertions.assertFalse(plugin.isInitialized());

        plugin.initialize(new PluginContext(Collections.singletonMap("key1", "val1")));
        Assertions.assertTrue(plugin.isInitialized());

        plugin.close();
        Assertions.assertTrue(plugin.isClosed());
    }

    // ==================== Helpers ====================

    private static LineageInfo createLineageInfo(String queryId) {
        LineageInfo info = new LineageInfo();
        LineageContext context = new LineageContext(InsertIntoTableCommand.class, queryId,
                "insert into tgt select 1", "user1", "test_db",
                TEST_CONTEXT_TIMESTAMP_MS, TEST_CONTEXT_DURATION_MS);
        context.setState("OK");
        info.setContext(context);
        return info;
    }

    // ==================== Test doubles ====================

    /**
     * A simple no-op plugin for testing basic operations.
     */
    static class NoOpLineagePlugin implements LineagePlugin {
        private final String pluginName;
        private boolean initialized = false;
        private boolean closed = false;

        NoOpLineagePlugin(String name) {
            this.pluginName = name;
        }

        @Override
        public String name() {
            return pluginName;
        }

        @Override
        public void initialize(PluginContext context) {
            this.initialized = true;
        }

        @Override
        public boolean eventFilter() {
            return true;
        }

        @Override
        public boolean exec(LineageInfo lineageInfo) {
            return true;
        }

        @Override
        public void close() {
            this.closed = true;
        }

        boolean isInitialized() {
            return initialized;
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * A plugin that tracks exec() calls for verification.
     */
    static class TrackingLineagePlugin implements LineagePlugin {
        private final String pluginName;
        private final boolean filterResult;
        private final AtomicInteger execCounter;
        private final List<String> processedIds;
        private final CountDownLatch latch;

        TrackingLineagePlugin(String name, boolean filterResult, AtomicInteger execCounter,
                              List<String> processedIds, CountDownLatch latch) {
            this.pluginName = name;
            this.filterResult = filterResult;
            this.execCounter = execCounter;
            this.processedIds = processedIds;
            this.latch = latch;
        }

        @Override
        public String name() {
            return pluginName;
        }

        @Override
        public void initialize(PluginContext context) {
        }

        @Override
        public boolean eventFilter() {
            return filterResult;
        }

        @Override
        public boolean exec(LineageInfo lineageInfo) {
            execCounter.incrementAndGet();
            if (lineageInfo != null && lineageInfo.getContext() != null) {
                processedIds.add(lineageInfo.getContext().getQueryId());
            }
            latch.countDown();
            return true;
        }

        @Override
        public void close() {
        }
    }

    /**
     * A plugin that always throws an exception in exec().
     */
    static class ExceptionLineagePlugin implements LineagePlugin {
        private final String pluginName;

        ExceptionLineagePlugin(String name) {
            this.pluginName = name;
        }

        @Override
        public String name() {
            return pluginName;
        }

        @Override
        public void initialize(PluginContext context) {
        }

        @Override
        public boolean eventFilter() {
            return true;
        }

        @Override
        public boolean exec(LineageInfo lineageInfo) {
            throw new RuntimeException("Simulated plugin failure");
        }

        @Override
        public void close() {
        }
    }

    /**
     * A plugin that throws exception in eventFilter.
     */
    static class EventFilterExceptionLineagePlugin implements LineagePlugin {
        private final String pluginName;

        EventFilterExceptionLineagePlugin(String name) {
            this.pluginName = name;
        }

        @Override
        public String name() {
            return pluginName;
        }

        @Override
        public void initialize(PluginContext context) {
        }

        @Override
        public boolean eventFilter() {
            throw new RuntimeException("Simulated filter failure");
        }

        @Override
        public boolean exec(LineageInfo lineageInfo) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    /**
     * Test factory for testing the factory SPI contract.
     */
    static class TestLineagePluginFactory implements LineagePluginFactory {
        @Override
        public String name() {
            return "test";
        }

        @Override
        public LineagePlugin create() {
            return new NoOpLineagePlugin("test");
        }
    }
}
