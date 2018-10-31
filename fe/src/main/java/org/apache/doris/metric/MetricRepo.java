// Modifications copyright (C) 2018, Baidu.com, Inc.
// Copyright 2018 The Apache Software Foundation

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
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob.EtlJobType;
import org.apache.doris.load.LoadJob.JobState;
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
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    private static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    private static final PaloMetricRegistry PALO_METRIC_REGISTER = new PaloMetricRegistry();
    
    public static AtomicBoolean isInit = new AtomicBoolean(false);

    public static PaloLongCounterMetric COUNTER_REQUEST_ALL;
    public static PaloLongCounterMetric COUNTER_QUERY_ALL;
    public static PaloLongCounterMetric COUNTER_QUERY_ERR;
    public static PaloLongCounterMetric COUNTER_LOAD_ADD;
    public static PaloLongCounterMetric COUNTER_LOAD_FINISHED;
    public static PaloLongCounterMetric COUNTER_EDIT_LOG_WRITE;
    public static PaloLongCounterMetric COUNTER_EDIT_LOG_READ;
    public static PaloLongCounterMetric COUNTER_IMAGE_WRITE;
    public static PaloLongCounterMetric COUNTER_IMAGE_PUSH;
    public static Histogram HISTO_QUERY_LATENCY;

    public static synchronized void init() {
        if (isInit.get()) {
            return;
        }

        // 1. gauge
        // load jobs
        Load load = Catalog.getInstance().getLoadInstance();
        for (EtlJobType jobType : EtlJobType.values()) {
            for (JobState state : JobState.values()) {
                PaloGaugeMetric<Integer> gauge = (PaloGaugeMetric<Integer>) new PaloGaugeMetric<Integer>("job",
                        "job statistics") {
                    @Override
                    public Integer getValue() {
                        if (!Catalog.getInstance().isMaster()) {
                            return 0;
                        }
                        return load.getLoadJobNumByTypeAndState(jobType, state);
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
            
            PaloGaugeMetric<Integer> gauge = (PaloGaugeMetric<Integer>) new PaloGaugeMetric<Integer>("job",
                    "job statistics") {
                @Override
                public Integer getValue() {
                    if (!Catalog.getInstance().isMaster()) {
                        return 0;
                    }
                    if (jobType == JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler().getAlterJobNumByState(org.apache.doris.alter.AlterJob.JobState.RUNNING);
                    } else {
                        return alter.getRollupHandler().getAlterJobNumByState(org.apache.doris.alter.AlterJob.JobState.RUNNING);
                    }
                }
            };
            gauge.addLabel(new MetricLabel("job", "alter"))
                .addLabel(new MetricLabel("type", jobType.name()))
                .addLabel(new MetricLabel("state", "running"));
            PALO_METRIC_REGISTER.addPaloMetrics(gauge);
        }

        // capacity
        generateCapacityMetrics();

        // connections
        PaloGaugeMetric<Integer> conections = (PaloGaugeMetric<Integer>) new PaloGaugeMetric<Integer>(
                "connection_total", "total connections") {
            @Override
            public Integer getValue() {
                return ExecuteEnv.getInstance().getScheduler().getConnectionNum();
            }
        };
        PALO_METRIC_REGISTER.addPaloMetrics(conections);

        // journal id
        PaloGaugeMetric<Long> maxJournalId = (PaloGaugeMetric<Long>) new PaloGaugeMetric<Long>(
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

        // 2. counter
        COUNTER_REQUEST_ALL = new PaloLongCounterMetric("request_total", "total request");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_REQUEST_ALL);
        COUNTER_QUERY_ALL = new PaloLongCounterMetric("query_total", "total query");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_QUERY_ALL);
        COUNTER_QUERY_ERR = new PaloLongCounterMetric("query_err", "total error query");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_QUERY_ERR);
        COUNTER_LOAD_ADD = new PaloLongCounterMetric("load_add", "total laod submit");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_LOAD_ADD);
        COUNTER_LOAD_FINISHED = new PaloLongCounterMetric("load_finished", "total laod finished");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_LOAD_FINISHED);
        COUNTER_EDIT_LOG_WRITE = new PaloLongCounterMetric("edit_log_write", "counter of edit log write into bdbje");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_EDIT_LOG_WRITE);
        COUNTER_EDIT_LOG_READ = new PaloLongCounterMetric("edit_log_read", "counter of edit log read from bdbje");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_EDIT_LOG_READ);
        COUNTER_IMAGE_WRITE = new PaloLongCounterMetric("image_write", "counter of image generated");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_IMAGE_WRITE);
        COUNTER_IMAGE_PUSH = new PaloLongCounterMetric("image_push",
                "counter of image succeeded in pushing to other frontends");
        PALO_METRIC_REGISTER.addPaloMetrics(COUNTER_IMAGE_PUSH);

        // 3. histogram
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("query", "latency", "ms"));

        isInit.set(true);
        ;
    }

    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateCapacityMetrics() {
        final String CAPACITY = "capacity";
        // remove all previous 'capacity' metric
        PALO_METRIC_REGISTER.removeMetrics(CAPACITY);

        LOG.info("begin to generate capacity metrics");
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        for (Long beId : infoService.getBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            LOG.debug("get backend: {}", be);
            for (DiskInfo diskInfo : be.getDisks().values()) {
                LOG.debug("get disk: {}", diskInfo);
                PaloGaugeMetric<Long> total = (PaloGaugeMetric<Long>) new PaloGaugeMetric<Long>(CAPACITY,
                        "disk capacity") {
                    @Override
                    public Long getValue() {
                        if (!Catalog.getInstance().isMaster()) {
                            return 0L;
                        }
                        return diskInfo.getTotalCapacityB();
                    }
                };
                total.addLabel(new MetricLabel("backend", be.getHost() + ":" + be.getHttpPort()))
                        .addLabel(new MetricLabel("path", diskInfo.getRootPath()))
                        .addLabel(new MetricLabel("type", "total"));
                PALO_METRIC_REGISTER.addPaloMetrics(total);
                
                PaloGaugeMetric<Long> used = (PaloGaugeMetric<Long>) new PaloGaugeMetric<Long>(CAPACITY,
                        "disk capacity") {
                    @Override
                    public Long getValue() {
                        if (!Catalog.getInstance().isMaster()) {
                            return 0L;
                        }
                        return diskInfo.getDataUsedCapacityB();
                    }
                };

                used.addLabel(new MetricLabel("backend", be.getHost() + ":" + be.getHttpPort()))
                        .addLabel(new MetricLabel("path", diskInfo.getRootPath()))
                        .addLabel(new MetricLabel("type", "used"));
                PALO_METRIC_REGISTER.addPaloMetrics(used);
            }
        }
    }

    public static synchronized String getMetric(PaloMetricVisitor visitor) {
        if (!isInit.get()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        // jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        sb.append(visitor.visitJvm(jvmStats)).append("\n");

        // palo metrics
        for (PaloMetric metric : PALO_METRIC_REGISTER.getPaloMetrics()) {
            sb.append(visitor.visit(metric)).append("\n");
        }

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            sb.append(visitor.visitHistogram(entry.getKey(), entry.getValue())).append("\n");
        }
        
        // master info
        if (Catalog.getInstance().isMaster()) {
            sb.append(visitor.getPaloNodeInfo()).append("\n");
        }

        return sb.toString();
    }
}

