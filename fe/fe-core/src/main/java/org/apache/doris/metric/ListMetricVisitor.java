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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Like this:
 * [
 *      ['FE', 'METRIC_NAME', 'METRIC_TYPE', 'METRIC_VALUE', 'TAG'],
 *      []
 * ]
 */
public class ListMetricVisitor extends MetricVisitor {
    private static final Logger logger = LogManager.getLogger(ListMetricVisitor.class);

    private List<List<String>> metricsList;
    private String localHostAddr;
    private StringBuilder strConcat;

    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String JVM_GC = "jvm_gc";
    private static final String TYPE_GAUGE = "gauge";
    private static final String TYPE_COUNTER = "counter";
    private static final String TYPE_HISTOGRAM = "histogram";

    public ListMetricVisitor(List<List<String>> metricsList, String localHostAddr) {
        super();
        // List[ List[FE, METRIC_NAME, METRIC_TYPE, METRIC_VALUE, TAG] ]
        this.metricsList = metricsList;
        this.localHostAddr = localHostAddr;
        this.strConcat = new StringBuilder();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        this.metricsList.add(newGaugeSubList(JVM_HEAP_SIZE_BYTES,
                jvmStats.getMem().getHeapMax().getBytes(), "{\"type\"=\"max\"}"));
        this.metricsList.add(newGaugeSubList(JVM_HEAP_SIZE_BYTES,
                jvmStats.getMem().getHeapCommitted().getBytes(), "{\"type\"=\"committed\"}"));
        this.metricsList.add(newGaugeSubList(JVM_HEAP_SIZE_BYTES,
                jvmStats.getMem().getHeapUsed().getBytes(), "{\"type\"=\"used\"}"));

        // non heap
        this.metricsList.add(newGaugeSubList(JVM_NON_HEAP_SIZE_BYTES,
                jvmStats.getMem().getNonHeapCommitted().getBytes(), "{\"type\"=\"committed\"}"));
        this.metricsList.add(newGaugeSubList(JVM_NON_HEAP_SIZE_BYTES,
                jvmStats.getMem().getNonHeapUsed().getBytes(), "{\"type\"=\"used\"}"));

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                this.metricsList.add(newGaugeSubList(JVM_YOUNG_SIZE_BYTES,
                        memPool.getUsed().getBytes(), "{\"type\"=\"used\"}"));
                this.metricsList.add(newGaugeSubList(JVM_YOUNG_SIZE_BYTES,
                        memPool.getPeakUsed().getBytes(), "{\"type\"=\"peak_used\"}"));
                this.metricsList.add(newGaugeSubList(JVM_YOUNG_SIZE_BYTES,
                        memPool.getMax().getBytes(), "{\"type\"=\"max\"}"));
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                this.metricsList.add(newGaugeSubList(JVM_OLD_SIZE_BYTES,
                        memPool.getUsed().getBytes(), "{\"type\"=\"used\"}"));
                this.metricsList.add(newGaugeSubList(JVM_OLD_SIZE_BYTES,
                        memPool.getPeakUsed().getBytes(), "{\"type\"=\"peak_used\"}"));
                this.metricsList.add(newGaugeSubList(JVM_OLD_SIZE_BYTES,
                        memPool.getMax().getBytes(), "{\"type\"=\"max\"}"));
            }
        }

        // gc
        for (GarbageCollector gc : jvmStats.getGc()) {
            this.metricsList.add(newGaugeSubList(JVM_GC,
                    gc.getCollectionCount(), "{\"name\"=\"" + gc.getName() + "\",\"type\"=\"count\"}"));
            this.metricsList.add(newGaugeSubList(JVM_GC,
                    gc.getCollectionTime().getMillis(), "{\"name\"=\"" + gc.getName() + "\",\"type\"=\"time\"}"));
        }

        // threads
        Threads threads = jvmStats.getThreads();
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getCount(), "\"type\"=\"count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getPeakCount(), "\"type\"=\"peak_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsNewCount(), "\"type\"=\"new_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsRunnableCount(), "\"type\"=\"runnable_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsBlockedCount(), "\"type\"=\"blocked_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsWaitingCount(), "\"type\"=\"waiting_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsTimedWaitingCount(), "\"type\"=\"timed_waiting_count\"}"));
        this.metricsList.add(newGaugeSubList(JVM_THREAD,
                threads.getThreadsTerminatedCount(), "\"type\"=\"terminated_count\"}"));
    }

    @Override
    public void visit(String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        // title
        final String fullName = prefix + metric.getName();
        // name
        strConcat.setLength(0);
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            strConcat.append("{");
            List<String> labelStrs = labels.stream().map(l -> "\"" + l.getKey() + "\"" + "=\"" + l.getValue()
                    + "\"").collect(Collectors.toList());
            strConcat.append(Joiner.on(", ").join(labelStrs));
            strConcat.append("}");
        }
        // value
        List<String> metricStr =
                newSubListByType(fullName, metric.getValue().toString(), strConcat.toString(), metric.getType());
        if (metricStr != null) {
            this.metricsList.add(metricStr);
        }
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
                tags.add(String.format("\"%s\"=\"%s\"", kv[0], kv[1]));
            }
        }
        final String fullName = prefix + String.join("_", names);
        final String fullTag = String.join(",", tags);
        String delimiter = tags.isEmpty() ? "" : ",";
        Snapshot snapshot = histogram.getSnapshot();

        this.metricsList.add(newHistogramSubList(fullName,
                snapshot.get75thPercentile(), "{\"quantile\"=\"0.75\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName,
                snapshot.get95thPercentile(), "{\"quantile\"=\"0.95\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName,
                snapshot.get98thPercentile(), "{\"quantile\"=\"0.98\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName,
                snapshot.get99thPercentile(), "{\"quantile\"=\"0.99\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName,
                snapshot.get999thPercentile(), "{\"quantile\"=\"0.999\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName + "_sum",
                histogram.getCount() * snapshot.getMean(),
                "{\"quantile\"=\"0.75\"" + delimiter + fullTag + "}"));
        this.metricsList.add(newHistogramSubList(fullName + "_count",
                histogram.getCount(), "{\"quantile\"=\"0.75\"" + delimiter + fullTag + "}"));
    }

    @Override
    public void visitNodeInfo() {
        final String NODE_INFO = "node_info";

        this.metricsList.add(newGaugeSubList(NODE_INFO, Env.getCurrentEnv().getFrontends(null).size(),
                "{\"type\"=\"fe_node_num\", \"state\"=\"total\"}"));
        this.metricsList.add(newGaugeSubList(NODE_INFO, Env.getCurrentSystemInfo().getAllBackendIds(false).size(),
                "{\"type\"=\"be_node_num\", \"state\"=\"total\"}"));
        this.metricsList.add(newGaugeSubList(NODE_INFO, Env.getCurrentSystemInfo().getAllBackendIds(true).size(),
                "{\"type\"=\"be_node_num\", \"state\"=\"alive\"}"));
        this.metricsList.add(newGaugeSubList(NODE_INFO, Env.getCurrentSystemInfo().getDecommissionedBackendIds().size(),
                "{\"type\"=\"be_node_num\", \"state\"=\"decommissioned\"}"));
        this.metricsList.add(newGaugeSubList(NODE_INFO,
                Env.getCurrentEnv().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive).count(),
                "{\"type\"=\"broker_node_num\", \"state\"=\"dead\"}"));

        // only master FE has this metrics
        if (Env.getCurrentEnv().isMaster()) {
            this.metricsList.add(newGaugeSubList(NODE_INFO, 1,
                    "{\"type\"=\"is_master\"}"));
        }
    }

    @Override
    public void visitCloudTableStats() {
    }

    @Override
    public void visitWorkloadGroup() {
        try {
            String counterTitle = "doris_workload_group_query_detail";
            Map<String, List<String>> workloadGroupMap = Env.getCurrentEnv().getWorkloadGroupMgr()
                    .getWorkloadGroupQueryDetail();
            for (Map.Entry<String, List<String>> entry : workloadGroupMap.entrySet()) {
                String name = entry.getKey();
                List<String> valList = entry.getValue();
                this.metricsList.add(newCounterSubList(counterTitle, valList.get(0),
                        String.format("{\"name\"=\"%s\", \"type\"=\"%s\"}", name, "running_query_num")));
                this.metricsList.add(newCounterSubList(counterTitle, valList.get(1),
                        String.format("{\"name\"=\"%s\", \"type\"=\"%s\"}", name, "waiting_query_num")));
            }
        } catch (Exception e) {
            logger.warn("error happens when get workload group query detail ", e);
        }
    }

    private List<String> newGaugeSubList(String name, Object value, String tag) {
        return newSubList(name, TYPE_GAUGE, String.valueOf(value), tag);
    }

    private List<String> newCounterSubList(String name, Object value, String tag) {
        return newSubList(name, TYPE_COUNTER, String.valueOf(value), tag);
    }

    private List<String> newHistogramSubList(String name, Object value, String tag) {
        return newSubList(name, TYPE_HISTOGRAM, String.valueOf(value), tag);
    }

    private List<String> newSubListByType(String name, Object value, String tag, Metric.MetricType metricType) {
        if (Metric.MetricType.GAUGE.equals(metricType)) {
            return newGaugeSubList(name, value, tag);
        } else if (Metric.MetricType.COUNTER.equals(metricType)) {
            return newCounterSubList(name, value, tag);
        } else {
            return null;
        }
    }

    private List<String> newSubList(String name, String type, String value, String tag) {
        // List[FE, METRIC_NAME, METRIC_TYPE, METRIC_VALUE, TAG]
        List<String> subList = new ArrayList<>(Collections.nCopies(5, ""));
        subList.set(0, this.localHostAddr);
        subList.set(1, name);
        subList.set(2, type);
        subList.set(3, value);
        subList.set(4, tag);
        return subList;
    }

}
