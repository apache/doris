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

import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Histogram;

import java.util.Iterator;
import java.util.List;

public class JsonMetricVisitor extends MetricVisitor {
    private int ordinal = 0;
    private boolean closed = false;

    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    public JsonMetricVisitor() {
        super();
        sb.append("[\n");
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, "max", "bytes", jvmStats.getMem().getHeapMax().getBytes());
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, "committed", "bytes",
                jvmStats.getMem().getHeapCommitted().getBytes());
        setJvmJsonMetric(sb, JVM_HEAP_SIZE_BYTES, "used", "bytes", jvmStats.getMem().getHeapUsed().getBytes());

        // non heap
        setJvmJsonMetric(sb, JVM_NON_HEAP_SIZE_BYTES, "committed", "bytes",
                jvmStats.getMem().getNonHeapCommitted().getBytes());
        setJvmJsonMetric(sb, JVM_NON_HEAP_SIZE_BYTES, "used", "bytes", jvmStats.getMem().getNonHeapUsed().getBytes());

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, "used", "bytes", memPool.getUsed().getBytes());
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, "peak_used", "bytes", memPool.getPeakUsed().getBytes());
                setJvmJsonMetric(sb, JVM_YOUNG_SIZE_BYTES, "max", "bytes", memPool.getMax().getBytes());
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, "used", "bytes", memPool.getUsed().getBytes());
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, "peak_used", "bytes", memPool.getPeakUsed().getBytes());
                setJvmJsonMetric(sb, JVM_OLD_SIZE_BYTES, "max", "bytes", memPool.getMax().getBytes());
            }
        }

        // gc
        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            if (gc.getName().equalsIgnoreCase("young")) {
                setJvmJsonMetric(sb, JVM_YOUNG_GC, "count", "nounit", gc.getCollectionCount());
                setJvmJsonMetric(sb, JVM_YOUNG_GC, "time", "milliseconds", gc.getCollectionTime().getMillis());
            } else if (gc.getName().equalsIgnoreCase("old")) {
                setJvmJsonMetric(sb, JVM_OLD_GC, "count", "nounit", gc.getCollectionCount());
                setJvmJsonMetric(sb, JVM_OLD_GC, "time", "milliseconds", gc.getCollectionTime().getMillis());
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        setJvmJsonMetric(sb, JVM_THREAD, "count", "nounit", threads.getCount());
        setJvmJsonMetric(sb, JVM_THREAD, "peak_count", "nounit", threads.getPeakCount());
        setJvmJsonMetric(sb, JVM_THREAD, "new_count", "nounit", threads.getThreadsNewCount());
        setJvmJsonMetric(sb, JVM_THREAD, "runnable_count", "nounit", threads.getThreadsRunnableCount());
        setJvmJsonMetric(sb, JVM_THREAD, "blocked_count", "nounit", threads.getThreadsBlockedCount());
        setJvmJsonMetric(sb, JVM_THREAD, "waiting_count", "nounit", threads.getThreadsWaitingCount());
        setJvmJsonMetric(sb, JVM_THREAD, "timed_waiting_count", "nounit", threads.getThreadsTimedWaitingCount());
        setJvmJsonMetric(sb, JVM_THREAD, "terminated_count", "nounit", threads.getThreadsTerminatedCount());
    }

    private void setJvmJsonMetric(StringBuilder sb, String metric, String type, String unit, long value) {
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(metric).append("\"");
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
        if (ordinal++ != 0) {
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
        return;
    }

    @Override
    public void getNodeInfo() {
        return;
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
