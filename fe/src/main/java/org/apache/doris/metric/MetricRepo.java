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

import org.apache.doris.alter.Alter;
import org.apache.doris.alter.AlterJob.JobType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.persist.EditLog;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.SortedMap;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    private static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    private static final DorisMetricRegistry PALO_METRIC_REGISTER = new DorisMetricRegistry();
    
    public static AtomicBoolean isInit = new AtomicBoolean(false);

    public static LongCounterMetric COUNTER_REQUEST_ALL;
    public static LongCounterMetric COUNTER_QUERY_ALL;
    public static LongCounterMetric COUNTER_QUERY_ERR;
    public static LongCounterMetric COUNTER_LOAD_ADD;
    public static LongCounterMetric COUNTER_LOAD_FINISHED;
    public static LongCounterMetric COUNTER_EDIT_LOG_WRITE;
    public static LongCounterMetric COUNTER_EDIT_LOG_READ;
    public static LongCounterMetric COUNTER_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_IMAGE_WRITE;
    public static LongCounterMetric COUNTER_IMAGE_PUSH;
    public static LongCounterMetric COUNTER_TXN_REJECT;
    public static LongCounterMetric COUNTER_TXN_BEGIN;
    public static LongCounterMetric COUNTER_TXN_FAILED;
    public static LongCounterMetric COUNTER_TXN_SUCCESS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ROWS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_RECEIVED_BYTES;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ERROR_ROWS;

    public static Histogram HISTO_QUERY_LATENCY;
    public static Histogram HISTO_EDIT_LOG_WRITE_LATENCY;

    // following metrics will be updated by metric calculator
    public static GaugeMetricImpl<Double> GAUGE_QUERY_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_REQUEST_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ERR_RATE;

    private static Timer metricTimer = new Timer();
    private static MetricCalculator metricCalculator = new MetricCalculator();

    public static synchronized void init() {
        if (isInit.get()) {
            return;
        }

        // 1. gauge
        // load jobs
        LoadManager loadManger = Catalog.getCurrentCatalog().getLoadManager();
        for (EtlJobType jobType : EtlJobType.values()) {
            for (JobState state : JobState.values()) {
                GaugeMetric<Long> gauge = (GaugeMetric<Long>) new GaugeMetric<Long>("job",
                        "job statistics") {
                    @Override
                    public Long getValue() {
                        if (!Catalog.getInstance().isMaster()) {
                            return 0L;
                        }
                        return loadManger.getLoadJobNum(state, jobType);
                    }
                };
                gauge.addLabel(new MetricLabel("job", "load"))
                    .addLabel(new MetricLabel("type", jobType.name()))
                    .addLabel(new MetricLabel("state", state.name()));
                PALO_METRIC_REGISTER.addPaloMetrics(gauge);
            }
        }

        // running alter job
        Alter alter = Catalog.getInstance().getAlterInstance();
        for (JobType jobType : JobType.values()) {
            if (jobType != JobType.SCHEMA_CHANGE && jobType != JobType.ROLLUP) {
                continue;
            }
            
            GaugeMetric<Long> gauge = (GaugeMetric<Long>) new GaugeMetric<Long>("job",
                    "job statistics") {
                @Override
                public Long getValue() {
                    if (!Catalog.getInstance().isMaster()) {
                        return 0L;
                    }
                    if (jobType == JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler().getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    } else {
                        return alter.getRollupHandler().getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    }
                }
            };
            gauge.addLabel(new MetricLabel("job", "alter"))
                .addLabel(new MetricLabel("type", jobType.name()))
                .addLabel(new MetricLabel("state", "running"));
            PALO_METRIC_REGISTER.addPaloMetrics(gauge);
        }

        // capacity
        generateTabletNumMetrics();

        // connections
        GaugeMetric<Integer> conections = (GaugeMetric<Integer>) new GaugeMetric<Integer>(
                "connection_total", "total connections") {
            @Override
            public Integer getValue() {
                return ExecuteEnv.getInstance().getScheduler().getConnectionNum();
            }
        };
        PALO_METRIC_REGISTER.addPaloMetrics(conections);

        // journal id
        GaugeMetric<Long> maxJournalId = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "max_journal_id", "max journal id of this frontends") {
            @Override
            public Long getValue() {
                EditLog editLog = Catalog.getInstance().getEditLog();
                if (editLog == null) {
                    return -1L;
                }
                return editLog.getMaxJournalId();
            }
        };
        PALO_METRIC_REGISTER.addPaloMetrics(maxJournalId);

        // scheduled tablet num
        GaugeMetric<Long> scheduledTabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "scheduled_tablet_num", "number of tablets being scheduled") {
            @Override
            public Long getValue() {
                if (!Catalog.getInstance().isMaster()) {
                    return 0L;
                }
                return (long) Catalog.getCurrentCatalog().getTabletScheduler().getTotalNum();
            }
        };
        PALO_METRIC_REGISTER.addPaloMetrics(scheduledTabletNum);

        // qps, rps and error rate
        // these metrics should be set an init value, in case that metric calculator is not running
        GAUGE_QUERY_PER_SECOND = new GaugeMetricImpl<>("qps", "query per second");
        GAUGE_QUERY_PER_SECOND.setValue(0.0);
        PALO_METRIC_REGISTER.addPaloMetrics(GAUGE_QUERY_PER_SECOND);
        GAUGE_REQUEST_PER_SECOND = new GaugeMetricImpl<>("rps", "request per second");
        GAUGE_REQUEST_PER_SECOND.setValue(0.0);
        PALO_METRIC_REGISTER.addPaloMetrics(GAUGE_REQUEST_PER_SECOND);
        GAUGE_QUERY_ERR_RATE = new GaugeMetricImpl<>("query_err_rate", "query_error_rate");
        PALO_METRIC_REGISTER.addPaloMetrics(GAUGE_QUERY_ERR_RATE);
        GAUGE_QUERY_ERR_RATE.setValue(0.0);

        // 2. counter
        COUNTER_REQUEST_ALL = new LongCounterMetric("request_total", "total request");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_REQUEST_ALL);
        COUNTER_QUERY_ALL = new LongCounterMetric("query_total", "total query");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_QUERY_ALL);
        COUNTER_QUERY_ERR = new LongCounterMetric("query_err", "total error query");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_QUERY_ERR);
        COUNTER_LOAD_ADD = new LongCounterMetric("load_add", "total laod submit");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_LOAD_ADD);
        COUNTER_LOAD_FINISHED = new LongCounterMetric("load_finished", "total laod finished");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_LOAD_FINISHED);
        COUNTER_EDIT_LOG_WRITE = new LongCounterMetric("edit_log_write", "counter of edit log write into bdbje");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_EDIT_LOG_WRITE);
        COUNTER_EDIT_LOG_READ = new LongCounterMetric("edit_log_read", "counter of edit log read from bdbje");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_EDIT_LOG_READ);
        COUNTER_EDIT_LOG_SIZE_BYTES = new LongCounterMetric("edit_log_size_bytes", "size of edit log");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_EDIT_LOG_SIZE_BYTES);
        COUNTER_IMAGE_WRITE = new LongCounterMetric("image_write", "counter of image generated");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_IMAGE_WRITE);
        COUNTER_IMAGE_PUSH = new LongCounterMetric("image_push",
                "counter of image succeeded in pushing to other frontends");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_IMAGE_PUSH);

        COUNTER_TXN_REJECT = new LongCounterMetric("txn_reject", "counter of rejected transactions");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_TXN_REJECT);
        COUNTER_TXN_BEGIN = new LongCounterMetric("txn_begin", "counter of begining transactions");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_TXN_BEGIN);
        COUNTER_TXN_SUCCESS = new LongCounterMetric("txn_success", "counter of success transactions");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_TXN_SUCCESS);
        COUNTER_TXN_FAILED = new LongCounterMetric("txn_failed", "counter of failed transactions");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_TXN_FAILED);

        COUNTER_ROUTINE_LOAD_ROWS = new LongCounterMetric("routine_load_rows", "total rows of routine load");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_ROUTINE_LOAD_ROWS);
        COUNTER_ROUTINE_LOAD_RECEIVED_BYTES = new LongCounterMetric("routine_load_receive_bytes",
                "total received bytes of routine load");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_ROUTINE_LOAD_RECEIVED_BYTES);
        COUNTER_ROUTINE_LOAD_ERROR_ROWS = new LongCounterMetric("routine_load_error_rows",
                "total error rows of routine load");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_ROUTINE_LOAD_ERROR_ROWS);

        // 3. histogram
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("query", "latency", "ms"));
        HISTO_EDIT_LOG_WRITE_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("editlog", "write", "latency", "ms"));

        isInit.set(true);

        if (Config.enable_metric_calculator) {
            metricTimer.scheduleAtFixedRate(metricCalculator, 0, 15 * 1000 /* 15s */);
        }
    }

    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateTabletNumMetrics() {
        final String TABLET_NUM = "tablet_num";
        // remove all previous 'tablet_num' metric
        PALO_METRIC_REGISTER.removeMetrics(TABLET_NUM);

        LOG.info("begin to generate capacity metrics");
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

        for (Long beId : infoService.getBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            // tablet number of each backends
            GaugeMetric<Long> tabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(TABLET_NUM,
                    "tablet number") {
                @Override
                public Long getValue() {
                    if (!Catalog.getInstance().isMaster()) {
                        return 0L;
                    }
                    return (long) invertedIndex.getTabletNumByBackendId(beId);
                }
            };

            tabletNum.addLabel(new MetricLabel("backend", be.getHost() + ":" + be.getHeartbeatPort()));
            PALO_METRIC_REGISTER.addPaloMetrics(tabletNum);
        } // end for backends
    }

    public static synchronized String getMetric(MetricVisitor visitor) {
        if (!isInit.get()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        // jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        visitor.visitJvm(sb, jvmStats);

        // doris metrics
        for (Metric metric : PALO_METRIC_REGISTER.getPaloMetrics()) {
            visitor.visit(sb, metric);
        }

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(sb, entry.getKey(), entry.getValue());
        }
        
        // master info
        if (Catalog.getInstance().isMaster()) {
            visitor.getNodeInfo(sb);
        }

        return sb.toString();
    }

    public static void addMetric(Metric<?> metric) {
        init();
        PALO_METRIC_REGISTER.addPaloMetrics(metric);
    }
}

