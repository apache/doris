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
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

/*
 * SimpleCoreMetricVisitor only show some core metrics of FE, with format:
 *      name type value
 * eg:
 *      jvm_young_used_percent DOUBLE 70.1
 *      query_latency_ms_75 LONG 2
 */
public class SimpleCoreMetricVisitor extends MetricVisitor {

    private static final String TYPE_LONG = "LONG";
    private static final String TYPE_DOUBLE = "DOUBLE";

    public static final String JVM_YOUNG_USED_PERCENT = "jvm_young_used_percent";
    public static final String JVM_OLD_USED_PERCENT = "jvm_old_used_percent";
    public static final String JVM_THREAD = "jvm_thread";

    public static final String MAX_JOURMAL_ID = "max_journal_id";
    public static final String CONNECTION_TOTAL = "connection_total";
    public static final String QUERY_LATENCY_MS = "query_latency_ms";

    public static final String QUERY_PER_SECOND = "qps";
    public static final String REQUEST_PER_SECOND = "rps";
    public static final String QUERY_ERR_RATE = "query_err_rate";

    public static final String MAX_TABLET_COMPACTION_SCORE = "max_tablet_compaction_score";

    private static final Map<String, String> CORE_METRICS = Maps.newHashMap();

    static {
        CORE_METRICS.put(MAX_JOURMAL_ID, TYPE_LONG);
        CORE_METRICS.put(CONNECTION_TOTAL, TYPE_LONG);
        CORE_METRICS.put(QUERY_LATENCY_MS, TYPE_LONG);
        CORE_METRICS.put(QUERY_PER_SECOND, TYPE_DOUBLE);
        CORE_METRICS.put(REQUEST_PER_SECOND, TYPE_DOUBLE);
        CORE_METRICS.put(QUERY_ERR_RATE, TYPE_DOUBLE);
        CORE_METRICS.put(MAX_TABLET_COMPACTION_SCORE, TYPE_LONG);
    }

    public SimpleCoreMetricVisitor() {
        super();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                long used = memPool.getUsed().getBytes();
                long max = memPool.getMax().getBytes();
                String percent = String.format("%.1f", (double) used / (max + 1) * 100);
                sb.append(Joiner.on(" ").join(JVM_YOUNG_USED_PERCENT, TYPE_DOUBLE, percent)).append("\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                long used = memPool.getUsed().getBytes();
                long max = memPool.getMax().getBytes();
                String percent = String.format("%.1f", (double) used / (max + 1) * 100);
                sb.append(Joiner.on(" ").join(JVM_OLD_USED_PERCENT, TYPE_DOUBLE, percent)).append("\n");
            }
        }

        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(JVM_THREAD, TYPE_LONG, threads.getCount())).append("\n");
        return;
    }

    @Override
    public void visit(String prefix, Metric metric) {
        if (!CORE_METRICS.containsKey(metric.getName())) {
            return;
        }

        if (CORE_METRICS.get(metric.getName()).equals(TYPE_DOUBLE)) {
            sb.append(Joiner.on(" ").join(prefix + metric.getName(), TYPE_DOUBLE,
                    String.format("%.2f", Double.valueOf(metric.getValue().toString())))).append("\n");
        } else {
            sb.append(Joiner.on(" ")
                    .join(prefix + metric.getName(), CORE_METRICS.get(metric.getName()), metric.getValue().toString()))
                    .append("\n");
        }
    }

    @Override
    public void visitHistogram(String prefix, String name, Histogram histogram) {
        if (!CORE_METRICS.containsKey(name)) {
            return;
        }
        Snapshot snapshot = histogram.getSnapshot();
        sb.append(Joiner.on(" ").join(prefix + name + "_75", CORE_METRICS.get(name),
                String.format("%.0f", snapshot.get75thPercentile()))).append("\n");
        sb.append(Joiner.on(" ").join(prefix + name + "_95", CORE_METRICS.get(name),
                String.format("%.0f", snapshot.get95thPercentile()))).append("\n");
        sb.append(Joiner.on(" ").join(prefix + name + "_99", CORE_METRICS.get(name),
                String.format("%.0f", snapshot.get99thPercentile()))).append("\n");
    }

    @Override
    public void getNodeInfo() {
        long feDeadNum = Env.getCurrentEnv().getFrontends(null).stream().filter(f -> !f.isAlive()).count();
        long beDeadNum = Env.getCurrentSystemInfo().getIdToBackend().values().stream().filter(b -> !b.isAlive())
                .count();
        long brokerDeadNum = Env.getCurrentEnv().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive)
                .count();
        sb.append("doris_fe_frontend_dead_num").append(" ").append(feDeadNum).append("\n");
        sb.append("doris_fe_backend_dead_num").append(" ").append(beDeadNum).append("\n");
        sb.append("doris_fe_broker_dead_num").append(" ").append(brokerDeadNum).append("\n");
    }
}
