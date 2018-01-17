// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.metric;

import com.baidu.palo.alter.Alter;
import com.baidu.palo.alter.AlterJob.JobType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadJob.EtlJobType;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.monitor.jvm.JvmService;
import com.baidu.palo.monitor.jvm.JvmStats;
import com.baidu.palo.monitor.jvm.JvmStats.BufferPool;
import com.baidu.palo.monitor.jvm.JvmStats.GarbageCollector;
import com.baidu.palo.monitor.jvm.JvmStats.MemoryPool;
import com.baidu.palo.monitor.jvm.JvmStats.Threads;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

public final class MetricRepo {
    private static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    private static final String HELP = "# HELP";
    private static final String TYPE = "# TYPE";
    
    private static boolean isInit = false;

    public enum MetricType {
        COUNTER,
        HISTOGRAM,
        METER,
        TIMER
    }

    // ================= all custom metrics ====================
    // 0. jvm
    public static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    public static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";

    public static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    public static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";

    public static final String JVM_DIRECT_BUFFER_POOL_SIZE_BYTES = "jvm_direct_buffer_pool_size_bytes";
    public static final String JVM_YOUNG_GC = "jvm_young_gc";
    public static final String JVM_OLD_GC = "jvm_old_gc";

    public static final String JVM_THREAD = "jvm_thread";

    // 1. gauge
    // load job
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_ALL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_PENDING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_ETL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_LOADING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_QUORUM;
    public static Gauge<Integer> GAUGE_JOB_LOAD_HADOOP_CANCELLED;

    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_ALL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_PENDING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_ETL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_LOADING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_QUORUM;
    public static Gauge<Integer> GAUGE_JOB_LOAD_MINI_CANCELLED;

    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_ALL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_PENDING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_ETL;
    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_LOADING;
    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_QUORUM;
    public static Gauge<Integer> GAUGE_JOB_LOAD_BROKER_CANCELLED;

    // rollup job
    public static Gauge<Integer> GAUGE_JOB_ROLLUP_ALL;
    public static Gauge<Integer> GAUGE_JOB_ROLLUP_RUNNING;
    
    // schema change job
    public static Gauge<Integer> GAUGE_JOB_SCHEMA_ALL;
    public static Gauge<Integer> GAUGE_JOB_SCHEMA_RUNNING;

    // 2. meter
    // query
    public static Meter METER_QUERY;
    public static Meter METER_REQUEST;
    // load job
    public static Meter METER_LOAD_ADD;
    public static Meter METER_LOAD_FINISHED;

    // 3. histograms
    // query latency
    public static Histogram HISTO_QUERY_LATENCY;
    // load job
    public static Histogram HISTO_LOAD_MINI_COST;
    public static Histogram HISTO_LOAD_MINI_ETL_COST;
    public static Histogram HISTO_LOAD_MINI_LOADING_COST;
    public static Histogram HISTO_LOAD_HADOOP_COST;
    public static Histogram HISTO_LOAD_HADOOP_ETL_COST;
    public static Histogram HISTO_LOAD_HADOOP_LOADING_COST;
    public static Histogram HISTO_LOAD_BROKER_COST;
    public static Histogram HISTO_LOAD_BROKER_ETL_COST;
    public static Histogram HISTO_LOAD_BROKER_LOADING_COST;

    // 4. counter
    public static Counter COUNTER_REQUEST_ALL;
    public static Counter COUNTER_QUERY_ALL;
    public static Counter COUNTER_QUERY_ERR;
    // connection
    public static Counter COUNTER_CONNECTIONS;

    private static final String PALO_NODE_INFO = "palo_node_info";
    private static final String PALO_FE_JOB = "palo_fe_job";

    // ================ end all custom metrics ====================

    public static synchronized void init() {
        if (isInit) {
            return;
        }
        isInit = true;

        // 1. gauge
        // load jobs
        GAUGE_JOB_LOAD_HADOOP_ALL = registerLoadGauge(EtlJobType.HADOOP, null);
        GAUGE_JOB_LOAD_HADOOP_PENDING = registerLoadGauge(EtlJobType.HADOOP, JobState.PENDING);
        GAUGE_JOB_LOAD_HADOOP_ETL = registerLoadGauge(EtlJobType.HADOOP, JobState.ETL);
        GAUGE_JOB_LOAD_HADOOP_LOADING = registerLoadGauge(EtlJobType.HADOOP, JobState.LOADING);
        GAUGE_JOB_LOAD_HADOOP_QUORUM = registerLoadGauge(EtlJobType.HADOOP, JobState.QUORUM_FINISHED);
        GAUGE_JOB_LOAD_HADOOP_CANCELLED = registerLoadGauge(EtlJobType.HADOOP, JobState.CANCELLED);
        GAUGE_JOB_LOAD_MINI_ALL = registerLoadGauge(EtlJobType.MINI, null);
        GAUGE_JOB_LOAD_MINI_PENDING = registerLoadGauge(EtlJobType.MINI, JobState.PENDING);
        GAUGE_JOB_LOAD_MINI_ETL = registerLoadGauge(EtlJobType.MINI, JobState.ETL);
        GAUGE_JOB_LOAD_MINI_LOADING = registerLoadGauge(EtlJobType.MINI, JobState.LOADING);
        GAUGE_JOB_LOAD_MINI_QUORUM = registerLoadGauge(EtlJobType.MINI, JobState.QUORUM_FINISHED);
        GAUGE_JOB_LOAD_MINI_CANCELLED = registerLoadGauge(EtlJobType.MINI, JobState.CANCELLED);
        GAUGE_JOB_LOAD_BROKER_ALL = registerLoadGauge(EtlJobType.BROKER, null);
        GAUGE_JOB_LOAD_BROKER_PENDING = registerLoadGauge(EtlJobType.BROKER, JobState.PENDING);
        GAUGE_JOB_LOAD_BROKER_ETL = registerLoadGauge(EtlJobType.BROKER, JobState.ETL);
        GAUGE_JOB_LOAD_BROKER_LOADING = registerLoadGauge(EtlJobType.BROKER, JobState.LOADING);
        GAUGE_JOB_LOAD_BROKER_QUORUM = registerLoadGauge(EtlJobType.BROKER, JobState.QUORUM_FINISHED);
        GAUGE_JOB_LOAD_BROKER_CANCELLED = registerLoadGauge(EtlJobType.BROKER, JobState.CANCELLED);

        // rollup
        GAUGE_JOB_ROLLUP_ALL = registerAlterGauge(JobType.ROLLUP, null);
        GAUGE_JOB_ROLLUP_RUNNING = registerAlterGauge(JobType.ROLLUP,
                                                      com.baidu.palo.alter.AlterJob.JobState.RUNNING);

        // schema change
        GAUGE_JOB_SCHEMA_ALL = registerAlterGauge(JobType.SCHEMA_CHANGE, null);
        GAUGE_JOB_SCHEMA_RUNNING = registerAlterGauge(JobType.SCHEMA_CHANGE,
                                                      com.baidu.palo.alter.AlterJob.JobState.RUNNING);

        // 2. meter
        METER_QUERY = registerMeter("palo", "fe", "request", "query", "rate");
        METER_REQUEST = registerMeter("palo", "fe", "request", "total", "rate");
        METER_LOAD_ADD = registerMeter("palo", "fe", "job", "load", "submit", "rate");
        METER_LOAD_FINISHED = registerMeter("palo", "fe", "job", "load", "finished", "rate");

        // 3. histogram
        HISTO_QUERY_LATENCY = registerHistogram("palo", "fe", "query", "latency", "ms");

        HISTO_LOAD_MINI_COST = registerHistogram("palo", "fe", "job", "load", "mini", "cost", "ms");
        HISTO_LOAD_MINI_ETL_COST = registerHistogram("palo", "fe", "job", "load", "mini", "etl", "cost", "ms");
        HISTO_LOAD_MINI_LOADING_COST = registerHistogram("palo", "fe", "job", "load", "mini", "etl", "cost", "ms");

        HISTO_LOAD_HADOOP_COST = registerHistogram("palo", "fe", "job", "load", "hadoop", "cost", "ms");
        HISTO_LOAD_HADOOP_ETL_COST = registerHistogram("palo", "fe", "job", "load", "hadoop", "etl", "cost", "ms");
        HISTO_LOAD_HADOOP_LOADING_COST = registerHistogram("palo", "fe", "job", "load", "hadoop", "etl", "cost", "ms");

        HISTO_LOAD_BROKER_COST = registerHistogram("palo", "fe", "job", "load", "broker", "cost", "ms");
        HISTO_LOAD_BROKER_ETL_COST = registerHistogram("palo", "fe", "job", "load", "broker", "etl", "cost", "ms");
        HISTO_LOAD_BROKER_LOADING_COST = registerHistogram("palo", "fe", "job", "load", "broker", "etl", "cost", "ms");

        // 4. counter
        COUNTER_REQUEST_ALL = registerCounter("palo", "fe", "request", "total");
        COUNTER_QUERY_ALL = registerCounter("palo", "fe", "query", "total");
        COUNTER_QUERY_ERR = registerCounter("palo", "fe", "query", "err", "total");
        COUNTER_CONNECTIONS = registerCounter("palo", "fe", "connection", "total");
    }

    private static Gauge<Integer> registerLoadGauge(EtlJobType type, JobState state) {
        String name = MetricRegistry.name("palo", "fe", "job", "load", type.name().toLowerCase(),
                                          state == null ? "total" : state.name().toLowerCase());
        METRIC_REGISTER.register(name, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                if (!Catalog.getInstance().isMaster()) {
                    return -1;
                }
                Load load = Catalog.getInstance().getLoadInstance();
                return load.getLoadJobNumByTypeAndState(type, state);
            }
        });
        return METRIC_REGISTER.gauge(name, null);
    }

    private static Gauge<Integer> registerAlterGauge(JobType type, com.baidu.palo.alter.AlterJob.JobState state) {
        String name = MetricRegistry.name("palo", "fe", "job", "alter", type.name().toLowerCase(),
                                          state == null ? "ALL" : state.name().toLowerCase());

        if (type == JobType.ROLLUP) {
            METRIC_REGISTER.register(name, new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    if (!Catalog.getInstance().isMaster()) {
                        return -1;
                    }
                    Alter alter = Catalog.getInstance().getAlterInstance();
                    return alter.getRollupHandler().getAlterJobNumByState(state);
                }
            });
        } else if (type == JobType.SCHEMA_CHANGE) {
            METRIC_REGISTER.register(name, new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    if (!Catalog.getInstance().isMaster()) {
                        return -1;
                    }
                    Alter alter = Catalog.getInstance().getAlterInstance();
                    return alter.getSchemaChangeHandler().getAlterJobNumByState(state);
                }
            });
        }

        return METRIC_REGISTER.gauge(name, null);
    }

    private static Counter registerCounter(String name, String... subname) {
        return METRIC_REGISTER.counter(MetricRegistry.name(name, subname));
    }

    private static Meter registerMeter(String name, String... subname) {
        return METRIC_REGISTER.meter(MetricRegistry.name(name, subname));
    }

    private static Histogram registerHistogram(String name, String... subname) {
        return METRIC_REGISTER.histogram(MetricRegistry.name(name, subname));
    }

    public static String getJsonStr() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(METRIC_REGISTER.getMetrics());
        } catch (Exception e) {
            return "";
        }
    }
    
    public static String getPlainText() {
        // long curTime = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        String role = Catalog.getInstance().getRole().name();

        // 0. jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        // heap
        sb.append(Joiner.on(" ").join(HELP, JVM_HEAP_SIZE_BYTES, "jvm heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"max\"} ").append(jvmStats.getMem().getHeapMax().getBytes()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"committed\"} ").append(jvmStats.getMem().getHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getHeapUsed().getBytes()).append("\n");
        // non heap
        sb.append(Joiner.on(" ").join(HELP, JVM_NON_HEAP_SIZE_BYTES, "jvm non heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_NON_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"committed\"} ").append(jvmStats.getMem().getNonHeapCommitted().getBytes()).append("\n");
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getNonHeapUsed().getBytes()).append("\n");

        // mem pool
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("young")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_YOUNG_SIZE_BYTES, "jvm young mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_YOUNG_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"used\"} ").append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"peak_used\"} ").append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_YOUNG_SIZE_BYTES).append("{type=\"max\"} ").append(memPool.getMax().getBytes()).append("\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_SIZE_BYTES, "jvm old mem pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"used\"} ").append(memPool.getUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"peak_used\"} ").append(memPool.getPeakUsed().getBytes()).append("\n");
                sb.append(JVM_OLD_SIZE_BYTES).append("{type=\"max\"} ").append(memPool.getMax().getBytes()).append("\n");
            }
        }

        // direct buffer pool
        Iterator<BufferPool> poolIter = jvmStats.getBufferPools().iterator();
        while (poolIter.hasNext()) {
            BufferPool pool = poolIter.next();
            if (pool.getName().equalsIgnoreCase("direct")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_DIRECT_BUFFER_POOL_SIZE_BYTES,
                                              "jvm direct buffer pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_DIRECT_BUFFER_POOL_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"count\"} ").append(pool.getCount()).append("\n");
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"used\"} ").append(pool.getUsed().getBytes()).append("\n");
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"capacity\"} ").append(pool.getTotalCapacity().getBytes()).append("\n");
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
                sb.append(JVM_YOUNG_GC).append("{type=\"time\"} ").append(gc.getCollectionTime().getMillis()).append("\n");
            } else if (gc.getName().equalsIgnoreCase("old")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_OLD_GC, "jvm old gc stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_OLD_GC, "gauge\n"));
                sb.append(JVM_OLD_GC).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
                sb.append(JVM_OLD_GC).append("{type=\"time\"} ").append(gc.getCollectionTime().getMillis()).append("\n");
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(HELP, JVM_THREAD, "jvm thread stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_THREAD, "gauge\n"));
        sb.append(JVM_THREAD).append("{type=\"count\"} ").append(threads.getCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"peak_count\"} ").append(threads.getPeakCount()).append("\n");

        // 1. counter
        SortedMap<String, Counter> counters = METRIC_REGISTER.getCounters();
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            String name = entry.getKey().replaceAll("\\.", "_");
            Counter counter = entry.getValue();
            sb.append(Joiner.on(" ").join(HELP, name, name, "\n"));
            sb.append(Joiner.on(" ").join(TYPE, name, "counter", "\n"));
            sb.append(Joiner.on(" ").join(name, counter.getCount())).append("\n");
        }
        
        // 2. gauge
        SortedMap<String, Gauge> gauges = METRIC_REGISTER.getGauges();
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            String name = entry.getKey().replaceAll("\\.", "_");
            Gauge<Integer> gauge = entry.getValue();
            if (!name.startsWith("palo_fe_job")) {
                sb.append(Joiner.on(" ").join(HELP, name, name, "\n"));
                sb.append(Joiner.on(" ").join(TYPE, name, "gauge", "\n"));
                sb.append(Joiner.on(" ").join(name, gauge.getValue())).append("\n");
            }
        }
        
        // load jobs
        sb.append(Joiner.on(" ").join(TYPE, "palo_fe_job", "gauge", "\n"));
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"total\"} ").append(GAUGE_JOB_LOAD_HADOOP_ALL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"pending\"} ").append(GAUGE_JOB_LOAD_HADOOP_PENDING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"etl\"} ").append(GAUGE_JOB_LOAD_HADOOP_ETL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"loading\"} ").append(GAUGE_JOB_LOAD_HADOOP_LOADING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"quorum\"} ").append(GAUGE_JOB_LOAD_HADOOP_QUORUM.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"hadoop\", state=\"cancelled\"} ").append(GAUGE_JOB_LOAD_HADOOP_CANCELLED.getValue()).append("\n");
        
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"total\"} ").append(GAUGE_JOB_LOAD_BROKER_ALL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"pending\"} ").append(GAUGE_JOB_LOAD_BROKER_PENDING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"etl\"} ").append(GAUGE_JOB_LOAD_BROKER_ETL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"loading\"} ").append(GAUGE_JOB_LOAD_BROKER_LOADING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"quorum\"} ").append(GAUGE_JOB_LOAD_BROKER_QUORUM.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"broker\", state=\"cancelled\"} ").append(GAUGE_JOB_LOAD_BROKER_CANCELLED.getValue()).append("\n");

        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"total\"} ").append(GAUGE_JOB_LOAD_MINI_ALL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"pending\"} ").append(GAUGE_JOB_LOAD_MINI_PENDING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"etl\"} ").append(GAUGE_JOB_LOAD_MINI_ETL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"loading\"} ").append(GAUGE_JOB_LOAD_MINI_LOADING.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"quorum\"} ").append(GAUGE_JOB_LOAD_MINI_QUORUM.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"load\", type=\"mini\", state=\"cancelled\"} ").append(GAUGE_JOB_LOAD_MINI_CANCELLED.getValue()).append("\n");

        sb.append(PALO_FE_JOB).append("{job=\"alter\", type=\"rollup\", state=\"total\"} ").append(GAUGE_JOB_ROLLUP_ALL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"alter\", type=\"rollup\", state=\"running\"} ").append(GAUGE_JOB_ROLLUP_RUNNING.getValue()).append("\n");

        sb.append(PALO_FE_JOB).append("{job=\"alter\", type=\"schema\", state=\"total\"} ").append(GAUGE_JOB_SCHEMA_ALL.getValue()).append("\n");
        sb.append(PALO_FE_JOB).append("{job=\"alter\", type=\"schema\", state=\"running\"} ").append(GAUGE_JOB_SCHEMA_RUNNING.getValue()).append("\n");

        // 3. histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            String name = entry.getKey().replaceAll("\\.", "_");
            Histogram histogram = entry.getValue();
            sb.append(Joiner.on(" ").join(HELP, name, name, "\n"));
            sb.append(Joiner.on(" ").join(TYPE, name, "summary", "\n"));
            Snapshot snapshot = histogram.getSnapshot();
            sb.append(name).append("{quantile=\"0.75\"} ").append(snapshot.get75thPercentile()).append("\n");
            sb.append(name).append("{quantile=\"0.95\"} ").append(snapshot.get95thPercentile()).append("\n");
            sb.append(name).append("{quantile=\"0.98\"} ").append(snapshot.get98thPercentile()).append("\n");
            sb.append(name).append("{quantile=\"0.99\"} ").append(snapshot.get99thPercentile()).append("\n");
            sb.append(name).append("{quantile=\"0.999\"} ").append(snapshot.get999thPercentile()).append("\n");
            sb.append(name).append("_sum ").append(histogram.getCount() * snapshot.getMean()).append("\n");
            sb.append(name).append("_count ").append(histogram.getCount()).append("\n");
        }

        // 4. meter
        SortedMap<String, Meter> meters = METRIC_REGISTER.getMeters();
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            String name = entry.getKey().replaceAll("\\.", "_");
            Meter meter = entry.getValue();
            sb.append(Joiner.on(" ").join(HELP, name, name, "\n"));
            sb.append(Joiner.on(" ").join(TYPE, name, "gauge", "\n"));
            sb.append(Joiner.on(" ").join(name, meter.getMeanRate())).append("\n");
        }
        
        // 5. master info
        if (Catalog.getInstance().isMaster()) {
            sb.append(Joiner.on(" ").join(TYPE, PALO_NODE_INFO, "gauge", "\n"));
            sb.append(PALO_NODE_INFO).append("{type=\"fe_node_num\", state=\"total\"} ")
                    .append(Catalog.getInstance().getFrontends(null).size()).append("\n");
            sb.append(PALO_NODE_INFO).append("{type=\"be_node_num\", state=\"total\"} ")
                    .append(Catalog.getCurrentSystemInfo().getBackendIds(false).size()).append("\n");
            sb.append(PALO_NODE_INFO).append("{type=\"be_node_num\", state=\"alive\"} ")
                    .append(Catalog.getCurrentSystemInfo().getBackendIds(true).size()).append("\n");
            sb.append(PALO_NODE_INFO).append("{type=\"be_node_num\", state=\"decommissioned\"} ")
                    .append(Catalog.getCurrentSystemInfo().getDecommissionedBackendIds().size()).append("\n");
        }

        return sb.toString();
    }
}
