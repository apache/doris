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

package org.apache.doris.metric;

import com.clearspring.analytics.util.Lists;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.BufferPool;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;
import com.codahale.metrics.Histogram;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonMetricVisitor extends MetricVisitor {
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_DIRECT_BUFFER_POOL_SIZE_BYTES = "jvm_direct_buffer_pool_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String METRIC = "metric";
    private static final String QUANTILE = "quantile";
    private static final String MS = "ms";
    private static final String TYPE = "type";
    private static final String STATUS = "status";
    private static final String NOUNIT = "nounit";
    private static final String NODE_INFO = "node_info";

    private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private List<MetricJson> metricJsons;

    public JsonMetricVisitor(String prefix) {
        super(prefix);
        metricJsons = Lists.newArrayList();
    }

    static class MetricJson<T> {
        private Map<String, String> tags;
        private String unit;
        private T value;

        public MetricJson(Map<String, String> tags, String unit, T value) {
            this.tags = tags;
            this.unit = unit;
            this.value = value;
        }
    }

    @Override
    public void setMetricNumber(int metricNumber) {
    }

    @Override
    public void visitJvm(StringBuilder sb, JvmStats jvmStats) {
        // heap
        addJvm(JVM_HEAP_SIZE_BYTES, "max", jvmStats.getMem().getHeapMax().getBytes());
        addJvm(JVM_HEAP_SIZE_BYTES, "committed", jvmStats.getMem().getHeapCommitted().getBytes());
        addJvm(JVM_HEAP_SIZE_BYTES, "used", jvmStats.getMem().getHeapUsed().getBytes());
        // non heap
        addJvm(JVM_NON_HEAP_SIZE_BYTES, "committed", jvmStats.getMem().getNonHeapCommitted().getBytes());
        addJvm(JVM_NON_HEAP_SIZE_BYTES, "used", jvmStats.getMem().getNonHeapUsed().getBytes());

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                addJvm(JVM_YOUNG_SIZE_BYTES, "used", memPool.getUsed().getBytes());
                addJvm(JVM_YOUNG_SIZE_BYTES, "peak_used", memPool.getPeakUsed().getBytes());
                addJvm(JVM_YOUNG_SIZE_BYTES, "max", memPool.getMax().getBytes());
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                addJvm(JVM_OLD_SIZE_BYTES, "used", memPool.getUsed().getBytes());
                addJvm(JVM_OLD_SIZE_BYTES, "peak_used", memPool.getPeakUsed().getBytes());
                addJvm(JVM_OLD_SIZE_BYTES, "max", memPool.getMax().getBytes());
            }
        }

        // direct buffer pool
        Iterator<BufferPool> poolIter = jvmStats.getBufferPools().iterator();
        while (poolIter.hasNext()) {
            BufferPool pool = poolIter.next();
            if (pool.getName().equalsIgnoreCase("direct")) {
                addJvm(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES, "count", pool.getCount());
                addJvm(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES, "used", pool.getUsed().getBytes());
                addJvm(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES, "capacity", pool.getTotalCapacity().getBytes());
            }
        }

        // gc
        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            if (gc.getName().equalsIgnoreCase("young")) {
                addJvm(JVM_YOUNG_GC, "count", gc.getCollectionCount());
                addJvm(JVM_YOUNG_GC, "time", gc.getCollectionTime().getMillis());
            } else if (gc.getName().equalsIgnoreCase("old")) {
                addJvm(JVM_OLD_GC, "count", gc.getCollectionCount());
                addJvm(JVM_OLD_GC, "time", gc.getCollectionTime().getMillis());
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        addJvm(JVM_THREAD, "count", threads.getCount());
        addJvm(JVM_THREAD, "peak_count", threads.getPeakCount());
        addJvm(JVM_THREAD, "new_count", threads.getThreadsNewCount());
        addJvm(JVM_THREAD, "runnable_count", threads.getThreadsRunnableCount());
        addJvm(JVM_THREAD, "blocked_count", threads.getThreadsBlockedCount());
        addJvm(JVM_THREAD, "waiting_count", threads.getThreadsWaitingCount());
        addJvm(JVM_THREAD, "timed_waiting_count", threads.getThreadsTimedWaitingCount());
        addJvm(JVM_THREAD, "terminated_count", threads.getThreadsTerminatedCount());

        // Return metric JSON data in getNodeInfo().
    }

    private void addJvm(String metric, String type, long value) {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(METRIC, metric);
        tags.put(TYPE, type);
        addMetricJson(tags, NOUNIT, value);
    }

    @Override
    public void visit(StringBuilder sb, @SuppressWarnings("rawtypes") Metric metric) {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(METRIC, metric.getName());
        List<MetricLabel> labels = metric.getLabels();
        for (MetricLabel metricLabel : labels) {
            tags.put(metricLabel.getKey(), metricLabel.getValue());
        }
        MetricJson metricJson = new MetricJson(tags, metric.getUnit().name(), metric.getValue());
        metricJsons.add(metricJson);

        // Return metric JSON data in getNodeInfo().
    }

    @Override
    public void visitHistogram(StringBuilder sb, String name, Histogram histogram) {
        String theName = name.replaceAll("\\.", "_");
        Snapshot snapshot = histogram.getSnapshot();
        addHistogramQuantile(theName, "0.75", snapshot.get75thPercentile());
        addHistogramQuantile(theName, "0.95", snapshot.get95thPercentile());
        addHistogramQuantile(theName, "0.98", snapshot.get98thPercentile());
        addHistogramQuantile(theName, "0.99", snapshot.get99thPercentile());
        addHistogramQuantile(theName, "0.999", snapshot.get999thPercentile());

        Map<String, String> tags1 = Maps.newHashMap();
        tags1.put(METRIC, theName.concat("_sum"));
        addMetricJson(tags1, MS, histogram.getCount() * snapshot.getMean());

        Map<String, String> tags2 = Maps.newHashMap();
        tags2.put(METRIC, theName.concat("_count"));
        addMetricJson(tags2, MS, histogram.getCount());

        // Return metric JSON data in getNodeInfo().
    }

    private void addHistogramQuantile(String metric, String quantile, double value) {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(METRIC, metric);
        tags.put(QUANTILE, quantile);
        addMetricJson(tags, MS, value);
    }

    @Override
    public void getNodeInfo(StringBuilder sb) {
        addNodeInfo(NODE_INFO, "fe_node_num", "total",
                Catalog.getCurrentCatalog().getFrontends(null).size());
        addNodeInfo(NODE_INFO, "be_node_num", "total",
                Catalog.getCurrentSystemInfo().getBackendIds(false).size());
        addNodeInfo(NODE_INFO, "be_node_num", "alive",
                Catalog.getCurrentSystemInfo().getBackendIds(true).size());
        addNodeInfo(NODE_INFO, "be_node_num", "decommissioned",
                Catalog.getCurrentSystemInfo().getDecommissionedBackendIds().size());
        addNodeInfo(NODE_INFO, "broker_node_num", "dead",
                Catalog.getCurrentCatalog().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive).count());
        if (Catalog.getCurrentCatalog().isMaster()) {
            Map<String, String> tags = Maps.newHashMap();
            tags.put(METRIC, NODE_INFO);
            tags.put(STATUS, "is_master");
            addMetricJson(tags, NOUNIT, 1);
        }

        sb.append(gson.toJson(metricJsons));
    }

    private void addNodeInfo(String metric, String type, String status, long value) {
        Map<String, String> tags = Maps.newHashMap();
        tags.put(METRIC, metric);
        tags.put(TYPE, type);
        tags.put(STATUS, status);
        addMetricJson(tags, NOUNIT, value);
    }

    private void addMetricJson(Map<String, String> tags, String unit, double value) {
        metricJsons.add(new MetricJson(tags, unit, value));
    }

    private void addMetricJson(Map<String, String> tags, String unit, long value) {
        metricJsons.add(new MetricJson(tags, unit, value));
    }
}