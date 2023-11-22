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
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Like this:
 * # HELP doris_fe_job_load_broker_cost_ms doris_fe_job_load_broker_cost_ms
 * # TYPE doris_fe_job_load_broker_cost_ms gauge
 * doris_fe_job{job="load", type="mini", state="pending"} 0
 */
public class PrometheusMetricVisitor extends MetricVisitor {
    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String HELP = "# HELP ";
    private static final String TYPE = "# TYPE ";

    private Set<String> metricNames = new HashSet();

    public PrometheusMetricVisitor() {
        super();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        sb.append(Joiner.on(" ").join(HELP, JVM_HEAP_SIZE_BYTES, "jvm heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"max\"} ").append(jvmStats.getMem().getHeapMax().getBytes())
                .append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getHeapUsed().getBytes())
                .append("\n");
        // non heap
        sb.append(Joiner.on(" ").join(HELP, JVM_NON_HEAP_SIZE_BYTES, "jvm non heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_NON_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getNonHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"used\"} ")
                .append(jvmStats.getMem().getNonHeapUsed().getBytes()).append("\n");

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_SIZE_BYTES, "jvm young mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"max\"} ")
                        .append(memPool.getMax().getBytes()).append("\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_SIZE_BYTES, "jvm old mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"peak_used\"} ")
                        .append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"max\"} "
                ).append(memPool.getMax().getBytes()).append("\n");
            }
        }

        // gc
        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            if (gc.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_GC, "jvm young gc stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_GC, "gauge\n"));
                sb.append(JVM_YOUNG_GC).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
                sb.append(JVM_YOUNG_GC).append("{type=\"time\"} ")
                        .append(gc.getCollectionTime().getMillis()).append("\n");
            } else if (gc.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_GC, "jvm old gc stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_GC, "gauge\n"));
                sb.append(JVM_OLD_GC).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
                sb.append(JVM_OLD_GC).append("{type=\"time\"} ")
                        .append(gc.getCollectionTime().getMillis()).append("\n");
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(HELP, JVM_THREAD, "jvm thread stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_THREAD, "gauge\n"));
        sb.append(JVM_THREAD).append("{type=\"count\"} ")
                .append(threads.getCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"peak_count\"} ")
                .append(threads.getPeakCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"new_count\"} ")
                .append(threads.getThreadsNewCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"runnable_count\"} ")
                .append(threads.getThreadsRunnableCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"blocked_count\"} ")
                .append(threads.getThreadsBlockedCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"waiting_count\"} ")
                .append(threads.getThreadsWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"timed_waiting_count\"} ")
                .append(threads.getThreadsTimedWaitingCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"terminated_count\"} ")
                .append(threads.getThreadsTerminatedCount()).append("\n");
        return;
    }

    @Override
    public void visit(String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        // title
        final String fullName = prefix + metric.getName();
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append(metric.getDescription()).append("\n");
            sb.append(TYPE).append(fullName).append(" ").append(metric.getType().name().toLowerCase()).append("\n");
            metricNames.add(fullName);
        }
        sb.append(fullName);

        // name
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append("{");
            List<String> labelStrs = labels.stream().map(l -> l.getKey() + "=\"" + l.getValue()
                    + "\"").collect(Collectors.toList());
            sb.append(Joiner.on(", ").join(labelStrs));
            sb.append("}");
        }

        // value
        sb.append(" ").append(metric.getValue().toString()).append("\n");
    }

    @Override
    public void visitHistogram(String prefix, String name, Histogram histogram) {
        // part.part.part.k1=v1.k2=v2
        List<String> names = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        for (String part : name.split("\\.")) {
            String[] kv = part.split("=");
            if (kv.length == 1) {
                names.add(kv[0]);
            } else if (kv.length == 2) {
                tags.add(String.format("%s=\"%s\"", kv[0], kv[1]));
            }
        }
        final String fullName = prefix + String.join("_", names);
        final String fullTag = String.join(",", tags);
        // we should define metric name only once
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append("\n");
            sb.append(TYPE).append(fullName).append(" ").append("summary\n");
            metricNames.add(fullName);
        }
        String delimiter = tags.isEmpty() ? "" : ",";
        Snapshot snapshot = histogram.getSnapshot();
        sb.append(fullName).append("{quantile=\"0.75\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get75thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.95\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get95thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.98\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get98thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.99\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get99thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.999\"").append(delimiter).append(fullTag).append("} ")
            .append(snapshot.get999thPercentile()).append("\n");
        sb.append(fullName).append("_sum {").append(fullTag).append("} ")
                .append(histogram.getCount() * snapshot.getMean()).append("\n");
        sb.append(fullName).append("_count {").append(fullTag).append("} ")
                .append(histogram.getCount()).append("\n");
    }

    @Override
    public void getNodeInfo() {
        final String NODE_INFO = "node_info";
        sb.append(Joiner.on(" ").join(TYPE, NODE_INFO, "gauge\n"));
        sb.append(NODE_INFO).append("{type=\"fe_node_num\", state=\"total\"} ")
                .append(Env.getCurrentEnv().getFrontends(null).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"total\"} ")
                .append(Env.getCurrentSystemInfo().getAllBackendIds(false).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"alive\"} ")
                .append(Env.getCurrentSystemInfo().getAllBackendIds(true).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"decommissioned\"} ")
                .append(Env.getCurrentSystemInfo().getDecommissionedBackendIds().size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"broker_node_num\", state=\"dead\"} ").append(
                Env.getCurrentEnv().getBrokerMgr().getAllBrokers()
                        .stream().filter(b -> !b.isAlive).count()).append("\n");

        // only master FE has this metrics, to help the Grafana knows who is the master
        if (Env.getCurrentEnv().isMaster()) {
            sb.append(NODE_INFO).append("{type=\"is_master\"} ").append(1).append("\n");
        }
        return;
    }
}
