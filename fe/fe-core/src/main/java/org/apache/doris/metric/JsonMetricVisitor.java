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

import org.apache.doris.catalog.Env;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JsonMetricVisitor extends MetricVisitor {
    private int metricOrdinal = 0;
    private int histogramOrdinal = 0;
    private boolean closed = false;

    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String JVM_GC = "jvm_gc";

    public JsonMetricVisitor() {
        super();
        sb.append("[\n");
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, null, "max", "bytes", jvmStats.getMem().getHeapMax().getBytes());
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, null, "committed", "bytes",
                jvmStats.getMem().getHeapCommitted().getBytes());
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, null, "used", "bytes", jvmStats.getMem().getHeapUsed().getBytes());

        // non heap
        setJvmJsonMetric(sb, JVM_NON_HEAP_SIZE_BYTES, null, "committed", "bytes",
                jvmStats.getMem().getNonHeapCommitted().getBytes());
        setJvmJsonMetric(sb, JVM_NON_HEAP_SIZE_BYTES, null, "used", "bytes",
                jvmStats.getMem().getNonHeapUsed().getBytes());

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, null, "used", "bytes", memPool.getUsed().getBytes());
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, null, "peak_used", "bytes",
                        memPool.getPeakUsed().getBytes());
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, null, "max", "bytes", memPool.getMax().getBytes());
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, null, "used", "bytes", memPool.getUsed().getBytes());
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, null, "peak_used", "bytes", memPool.getPeakUsed().getBytes());
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, null, "max", "bytes", memPool.getMax().getBytes());
            }
        }

        // gc
        for (GarbageCollector gc : jvmStats.getGc()) {
            setJvmJsonMetric(sb, JVM_GC, gc.getName() + " Count", "count", "nounit", gc.getCollectionCount());
            setJvmJsonMetric(sb, JVM_GC, gc.getName() + " Time", "time", "milliseconds",
                    gc.getCollectionTime().getMillis());
        }

        // threads
        Threads threads = jvmStats.getThreads();
        setJvmJsonMetric(sb, JVM_THREAD, null, "count", "nounit", threads.getCount());
        setJvmJsonMetric(sb, JVM_THREAD, null, "peak_count", "nounit", threads.getPeakCount());
        setJvmJsonMetric(sb, JVM_THREAD, null, "new_count", "nounit", threads.getThreadsNewCount());
        setJvmJsonMetric(sb, JVM_THREAD, null,  "runnable_count", "nounit", threads.getThreadsRunnableCount());
        setJvmJsonMetric(sb, JVM_THREAD, null,  "blocked_count", "nounit", threads.getThreadsBlockedCount());
        setJvmJsonMetric(sb, JVM_THREAD, null,  "waiting_count", "nounit", threads.getThreadsWaitingCount());
        setJvmJsonMetric(sb, JVM_THREAD, null, "timed_waiting_count", "nounit", threads.getThreadsTimedWaitingCount());
        setJvmJsonMetric(sb, JVM_THREAD, null, "terminated_count", "nounit", threads.getThreadsTerminatedCount());
    }

    private void setJvmJsonMetric(StringBuilder sb, String metric, String name, String type, String unit, long value) {
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(metric).append("\"");
        if (name != null) {
            sb.append(",\n");
            sb.append("\t\t\"name\":\"").append(name).append("\"\n");
        }
        if (type != null) {
            sb.append(",\n");
            sb.append("\t\t\"type\":\"").append(type).append("\"\n");
        }
        sb.append("\n\t},\n");
        sb.append("\t\"unit\":\"").append(unit).append("\",\n");
        sb.append("\t\"value\":").append(value).append("\n}");
        sb.append(",\n");
    }

    @Override
    public void visit(String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        if (metricOrdinal++ != 0) {
            sb.append(",\n");
        }
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(prefix).append(metric.getName()).append("\"");

        // name
        @SuppressWarnings("unchecked") List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append(",\n");
            int i = 0;
            for (MetricLabel label : labels) {
                if (i++ > 0) {
                    sb.append(",\n");
                }
                sb.append("\t\t\"").append(label.getKey()).append("\":\"").append(label.getValue()).append("\"");
            }
        }
        sb.append("\n\t},\n");
        sb.append("\t\"unit\":\"").append(metric.getUnit().name().toLowerCase()).append("\",\n");

        // value
        sb.append("\t\"value\":").append(metric.getValue().toString()).append("\n}");
    }

    @Override
    public void visitHistogram(String prefix, String name, Histogram histogram) {
        if (histogramOrdinal++ == 0) {
            sb.append(",\n");
        }

        // part.part.part.k1=v1.k2=v2
        List<String> names = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        for (String part : name.split("\\.")) {
            String[] kv = part.split("=");
            if (kv.length == 1) {
                names.add(kv[0]);
            } else if (kv.length == 2) {
                tags.add(String.format("\"%s\":\"%s\"", kv[0], kv[1]));
            }
        }
        final String fullName = prefix + String.join("_", names);
        Snapshot snapshot = histogram.getSnapshot();
        setHistogramJsonMetric(sb, fullName, "\"quantile\":\"0.75\"", tags, snapshot.get75thPercentile());
        setHistogramJsonMetric(sb, fullName, "\"quantile\":\"0.95\"", tags, snapshot.get95thPercentile());
        setHistogramJsonMetric(sb, fullName, "\"quantile\":\"0.98\"", tags, snapshot.get98thPercentile());
        setHistogramJsonMetric(sb, fullName, "\"quantile\":\"0.99\"", tags, snapshot.get99thPercentile());
        setHistogramJsonMetric(sb, fullName, "\"quantile\":\"0.999\"", tags, snapshot.get999thPercentile());
        setHistogramJsonMetric(sb, fullName.concat("_sum"), null, null,
                histogram.getCount() * snapshot.getMean());
        setHistogramJsonMetric(sb, fullName.concat("_count"), null, null, histogram.getCount());
    }

    private void setHistogramJsonMetric(StringBuilder sb, String metric, String quantile,
                                        List<String> tags, double value) {
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(metric).append("\"");
        if (quantile != null) {
            sb.append(",\n");
            sb.append("\t\t").append(quantile).append("\n");
        }
        if (tags != null) {
            for (String tag : tags) {
                sb.append(",\n");
                sb.append("\t\t").append(tag).append("\n");
            }
        }
        sb.append("\n\t},\n");
        sb.append("\t\"unit\":\"").append("ms").append("\",\n");
        sb.append("\t\"value\":").append(value).append("\n}");
        sb.append(",\n");
    }

    @Override
    public void visitNodeInfo() {
        if (Env.getCurrentEnv().isMaster()) {
            setNodeInfo(sb, "node_info", "is_master", null, 1, false);
        }
        setNodeInfo(sb, "node_info", "fe_node_num", "total",
                Env.getCurrentEnv().getFrontends(null).size(), false);
        setNodeInfo(sb, "node_info", "be_node_num", "total",
                Env.getCurrentSystemInfo().getAllBackendIds(false).size(), false);
        setNodeInfo(sb, "node_info", "be_node_num", "alive",
                Env.getCurrentSystemInfo().getAllBackendIds(true).size(), false);
        setNodeInfo(sb, "node_info", "be_node_num", "decommissioned",
                Env.getCurrentSystemInfo().getDecommissionedBackendIds().size(), false);
        setNodeInfo(sb, "node_info", "be_node_num", "dead",
                Env.getCurrentEnv().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive).count(), true);
    }

    @Override
    public void visitCloudTableStats() {
        return;
    }

    private void setNodeInfo(StringBuilder sb, String metric, String type,
                             String status, long value, boolean lastMetric) {
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(metric).append("\"");
        if (type != null) {
            sb.append(",\n");
            sb.append("\t\t\"type\":\"").append(type).append("\"");
        }
        if (status != null) {
            sb.append(",\n");
            sb.append("\t\t\"state\":\"").append(status).append("\"\n");
        }
        sb.append("\n\t},\n");
        sb.append("\t\"unit\":\"").append("nounit").append("\",\n");
        sb.append("\t\"value\":").append(value).append("\n}");
        if (!lastMetric) {
            sb.append(",\n");
        }
    }

    @Override
    public String finish() {
        if (!closed) {
            sb.append("\n]");
            closed = true;
        }
        return sb.toString();
    }
}
