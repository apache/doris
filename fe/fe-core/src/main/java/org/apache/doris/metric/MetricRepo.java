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
import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.TransactionStatus;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    // METRIC_REGISTER is only used for histogram metrics
    public static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    public static final DorisMetricRegistry DORIS_METRIC_REGISTER = new DorisMetricRegistry();

    public static volatile boolean isInit = false;
    public static final SystemMetrics SYSTEM_METRICS = new SystemMetrics();

    public static final String TABLET_NUM = "tablet_num";
    public static final String TABLET_MAX_COMPACTION_SCORE = "tablet_max_compaction_score";

    public static LongCounterMetric COUNTER_REQUEST_ALL;
    public static LongCounterMetric COUNTER_QUERY_ALL;
    public static LongCounterMetric COUNTER_QUERY_ERR;
    public static LongCounterMetric COUNTER_QUERY_TABLE;
    public static LongCounterMetric COUNTER_QUERY_OLAP_TABLE;
    public static LongCounterMetric COUNTER_QUERY_HIVE_TABLE;
    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_ALL;
    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_ERR;
    public static Histogram HISTO_QUERY_LATENCY;
    public static AutoMappedMetric<Histogram> USER_HISTO_QUERY_LATENCY;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> USER_GAUGE_QUERY_INSTANCE_NUM;
    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_INSTANCE_BEGIN;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_ALL;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_FAILED;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_SIZE;

    public static LongCounterMetric COUNTER_CACHE_ADDED_SQL;
    public static LongCounterMetric COUNTER_CACHE_ADDED_PARTITION;
    public static LongCounterMetric COUNTER_CACHE_HIT_SQL;
    public static LongCounterMetric COUNTER_CACHE_HIT_PARTITION;

    public static LongCounterMetric COUNTER_EDIT_LOG_WRITE;
    public static LongCounterMetric COUNTER_EDIT_LOG_READ;
    public static LongCounterMetric COUNTER_EDIT_LOG_CURRENT;
    public static LongCounterMetric COUNTER_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_EDIT_LOG_CLEAN_SUCCESS;
    public static LongCounterMetric COUNTER_EDIT_LOG_CLEAN_FAILED;
    public static Histogram HISTO_EDIT_LOG_WRITE_LATENCY;

    public static LongCounterMetric COUNTER_IMAGE_WRITE_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_WRITE_FAILED;
    public static LongCounterMetric COUNTER_IMAGE_PUSH_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_PUSH_FAILED;
    public static LongCounterMetric COUNTER_IMAGE_CLEAN_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_CLEAN_FAILED;

    public static LongCounterMetric COUNTER_TXN_REJECT;
    public static LongCounterMetric COUNTER_TXN_BEGIN;
    public static LongCounterMetric COUNTER_TXN_FAILED;
    public static LongCounterMetric COUNTER_TXN_SUCCESS;
    public static Histogram HISTO_TXN_EXEC_LATENCY;
    public static Histogram HISTO_TXN_PUBLISH_LATENCY;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> DB_GAUGE_TXN_NUM;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> DB_GAUGE_PUBLISH_TXN_NUM;

    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ROWS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_RECEIVED_BYTES;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ERROR_ROWS;
    public static LongCounterMetric COUNTER_HIT_SQL_BLOCK_RULE;

    public static AutoMappedMetric<LongCounterMetric> THRIFT_COUNTER_RPC_ALL;
    public static AutoMappedMetric<LongCounterMetric> THRIFT_COUNTER_RPC_LATENCY;

    // following metrics will be updated by metric calculator
    public static GaugeMetricImpl<Double> GAUGE_QUERY_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_REQUEST_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ERR_RATE;
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLET_COMPACTION_SCORE;

    private static ScheduledThreadPoolExecutor metricTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "metric-timer-pool", true);
    private static MetricCalculator metricCalculator = new MetricCalculator();

    // init() should only be called after catalog is contructed.
    public static synchronized void init() {
        if (isInit) {
            return;
        }

        // load jobs
        LoadManager loadManger = Env.getCurrentEnv().getLoadManager();
        for (EtlJobType jobType : EtlJobType.values()) {
            if (jobType == EtlJobType.UNKNOWN) {
                continue;
            }
            for (JobState state : JobState.values()) {
                GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "job statistics") {
                    @Override
                    public Long getValue() {
                        if (!Env.getCurrentEnv().isMaster()) {
                            return 0L;
                        }
                        return loadManger.getLoadJobNum(state, jobType);
                    }
                };
                gauge.addLabel(new MetricLabel("job", "load")).addLabel(new MetricLabel("type", jobType.name()))
                        .addLabel(new MetricLabel("state", state.name()));
                DORIS_METRIC_REGISTER.addMetrics(gauge);
            }
        }

        //  routine load jobs
        RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
        for (RoutineLoadJob.JobState jobState : RoutineLoadJob.JobState.values()) {
            GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "routine load job statistics") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    Set<RoutineLoadJob.JobState> states = Sets.newHashSet();
                    states.add(jobState);
                    List<RoutineLoadJob> jobs = routineLoadManager.getRoutineLoadJobByState(states);
                    return Long.valueOf(jobs.size());
                }
            };
            gauge.addLabel(new MetricLabel("job", "load")).addLabel(new MetricLabel("type", "ROUTINE_LOAD"))
                    .addLabel(new MetricLabel("state", jobState.name()));
            DORIS_METRIC_REGISTER.addMetrics(gauge);
        }

        // running alter job
        Alter alter = Env.getCurrentEnv().getAlterInstance();
        for (JobType jobType : JobType.values()) {
            if (jobType != JobType.SCHEMA_CHANGE && jobType != JobType.ROLLUP) {
                continue;
            }

            GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "job statistics") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    if (jobType == JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler()
                                .getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    } else {
                        return alter.getMaterializedViewHandler().getAlterJobV2Num(
                                org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    }
                }
            };
            gauge.addLabel(new MetricLabel("job", "alter")).addLabel(new MetricLabel("type", jobType.name()))
                    .addLabel(new MetricLabel("state", "running"));
            DORIS_METRIC_REGISTER.addMetrics(gauge);
        }

        // capacity
        generateBackendsTabletMetrics();

        // connections
        GaugeMetric<Integer> connections = new GaugeMetric<Integer>("connection_total", MetricUnit.CONNECTIONS,
                "total connections") {
            @Override
            public Integer getValue() {
                return ExecuteEnv.getInstance().getScheduler().getConnectionNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(connections);

        // journal id
        GaugeMetric<Long> maxJournalId = new GaugeMetric<Long>("max_journal_id", MetricUnit.NOUNIT,
                "max journal id of this frontends") {
            @Override
            public Long getValue() {
                EditLog editLog = Env.getCurrentEnv().getEditLog();
                if (editLog == null) {
                    return -1L;
                }
                return editLog.getMaxJournalId();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(maxJournalId);

        // scheduled tablet num
        GaugeMetric<Long> scheduledTabletNum = new GaugeMetric<Long>("scheduled_tablet_num", MetricUnit.NOUNIT,
                "number of tablets being scheduled") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return (long) Env.getCurrentEnv().getTabletScheduler().getTotalNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(scheduledTabletNum);

        // txn status
        for (TransactionStatus status : TransactionStatus.values()) {
            GaugeMetric<Long> gauge = new GaugeMetric<Long>("txn_status", MetricUnit.NOUNIT, "txn statistics") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return Env.getCurrentGlobalTransactionMgr().getTxnNumByStatus(status);
                }
            };
            gauge.addLabel(new MetricLabel("type", status.name().toLowerCase()));
            DORIS_METRIC_REGISTER.addMetrics(gauge);
        }

        // qps, rps and error rate
        // these metrics should be set an init value, in case that metric calculator is not running
        GAUGE_QUERY_PER_SECOND = new GaugeMetricImpl<>("qps", MetricUnit.NOUNIT, "query per second");
        GAUGE_QUERY_PER_SECOND.setValue(0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_QUERY_PER_SECOND);
        GAUGE_REQUEST_PER_SECOND = new GaugeMetricImpl<>("rps", MetricUnit.NOUNIT, "request per second");
        GAUGE_REQUEST_PER_SECOND.setValue(0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_REQUEST_PER_SECOND);
        GAUGE_QUERY_ERR_RATE = new GaugeMetricImpl<>("query_err_rate", MetricUnit.NOUNIT, "query error rate");
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_QUERY_ERR_RATE);
        GAUGE_QUERY_ERR_RATE.setValue(0.0);
        GAUGE_MAX_TABLET_COMPACTION_SCORE = new GaugeMetricImpl<>("max_tablet_compaction_score", MetricUnit.NOUNIT,
                "max tablet compaction score of all backends");
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MAX_TABLET_COMPACTION_SCORE);
        GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(0L);

        // query
        COUNTER_REQUEST_ALL = new LongCounterMetric("request_total", MetricUnit.REQUESTS, "total request");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_REQUEST_ALL);
        COUNTER_QUERY_ALL = new LongCounterMetric("query_total", MetricUnit.REQUESTS, "total query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_ALL);
        COUNTER_QUERY_ERR = new LongCounterMetric("query_err", MetricUnit.REQUESTS, "total error query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_ERR);
        COUNTER_QUERY_TABLE = new LongCounterMetric("query_table", MetricUnit.REQUESTS, "total query from table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_TABLE);
        COUNTER_QUERY_OLAP_TABLE = new LongCounterMetric("query_olap_table", MetricUnit.REQUESTS,
                "total query from olap table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_OLAP_TABLE);
        COUNTER_QUERY_HIVE_TABLE = new LongCounterMetric("query_hive_table", MetricUnit.REQUESTS,
                "total query from hive table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_HIVE_TABLE);
        USER_COUNTER_QUERY_ALL = new AutoMappedMetric<>(name -> {
            LongCounterMetric userCountQueryAll  = new LongCounterMetric("query_total", MetricUnit.REQUESTS,
                    "total query for single user");
            userCountQueryAll.addLabel(new MetricLabel("user", name));
            DORIS_METRIC_REGISTER.addMetrics(userCountQueryAll);
            return userCountQueryAll;
        });
        USER_COUNTER_QUERY_ERR = new AutoMappedMetric<>(name -> {
            LongCounterMetric userCountQueryErr  = new LongCounterMetric("query_err", MetricUnit.REQUESTS,
                    "total error query for single user");
            userCountQueryErr.addLabel(new MetricLabel("user", name));
            DORIS_METRIC_REGISTER.addMetrics(userCountQueryErr);
            return userCountQueryErr;
        });
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("query", "latency", "ms"));
        USER_HISTO_QUERY_LATENCY = new AutoMappedMetric<>(name -> {
            String metricName = MetricRegistry.name("query", "latency", "ms", "user=" + name);
            return METRIC_REGISTER.histogram(metricName);
        });
        USER_COUNTER_QUERY_INSTANCE_BEGIN = addLabeledMetrics("user", () ->
                new LongCounterMetric("query_instance_begin", MetricUnit.NOUNIT,
                "number of query instance begin"));
        USER_GAUGE_QUERY_INSTANCE_NUM = addLabeledMetrics("user", () ->
                new GaugeMetricImpl<>("query_instance_num", MetricUnit.NOUNIT,
                "number of running query instances of current user"));
        GaugeMetric<Long> queryInstanceNum = new GaugeMetric<Long>("query_instance_num",
                MetricUnit.NOUNIT, "number of query instances of all current users") {
            @Override
            public Long getValue() {
                QeProcessorImpl qe = ((QeProcessorImpl) QeProcessorImpl.INSTANCE);
                long totalInstanceNum = 0;
                for (Map.Entry<String, Integer> e : qe.getInstancesNumPerUser().entrySet()) {
                    long value = e.getValue() == null ? 0L : e.getValue().longValue();
                    totalInstanceNum += value;
                    USER_GAUGE_QUERY_INSTANCE_NUM.getOrAdd(e.getKey()).setValue(value);
                }
                return totalInstanceNum;
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(queryInstanceNum);
        BE_COUNTER_QUERY_RPC_ALL = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_total", MetricUnit.NOUNIT, ""));
        BE_COUNTER_QUERY_RPC_FAILED = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_failed", MetricUnit.NOUNIT, ""));
        BE_COUNTER_QUERY_RPC_SIZE = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_size", MetricUnit.BYTES, ""));

        // cache
        COUNTER_CACHE_ADDED_SQL = new LongCounterMetric("cache_added", MetricUnit.REQUESTS,
                "Number of SQL mode cache added");
        COUNTER_CACHE_ADDED_SQL.addLabel(new MetricLabel("type", "sql"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_ADDED_SQL);
        COUNTER_CACHE_ADDED_PARTITION = new LongCounterMetric("cache_added", MetricUnit.REQUESTS,
                "Number of Partition mode cache added");
        COUNTER_CACHE_ADDED_PARTITION.addLabel(new MetricLabel("type", "partition"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_ADDED_PARTITION);
        COUNTER_CACHE_HIT_SQL = new LongCounterMetric("cache_hit", MetricUnit.REQUESTS,
                "total hits query by sql model");
        COUNTER_CACHE_HIT_SQL.addLabel(new MetricLabel("type", "sql"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_HIT_SQL);
        COUNTER_CACHE_HIT_PARTITION = new LongCounterMetric("cache_hit", MetricUnit.REQUESTS,
                "total hits query by partition model");
        COUNTER_CACHE_HIT_PARTITION.addLabel(new MetricLabel("type", "partition"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_HIT_PARTITION);

        // edit log
        COUNTER_EDIT_LOG_WRITE = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of edit log write into bdbje");
        COUNTER_EDIT_LOG_WRITE.addLabel(new MetricLabel("type", "write"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_WRITE);
        COUNTER_EDIT_LOG_READ = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of edit log read from bdbje");
        COUNTER_EDIT_LOG_READ.addLabel(new MetricLabel("type", "read"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_READ);
        COUNTER_EDIT_LOG_CURRENT = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of current edit log in bdbje");
        COUNTER_EDIT_LOG_CURRENT.addLabel(new MetricLabel("type", "current"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CURRENT);
        COUNTER_EDIT_LOG_SIZE_BYTES = new LongCounterMetric("edit_log", MetricUnit.BYTES,
                "size of accumulated edit log");
        COUNTER_EDIT_LOG_SIZE_BYTES.addLabel(new MetricLabel("type", "accumulated_bytes"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_SIZE_BYTES);
        COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES = new LongCounterMetric("edit_log", MetricUnit.BYTES,
                "size of current edit log");
        COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES.addLabel(new MetricLabel("type", "current_bytes"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES);
        HISTO_EDIT_LOG_WRITE_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("editlog", "write", "latency", "ms"));

        // edit log clean
        COUNTER_EDIT_LOG_CLEAN_SUCCESS = new LongCounterMetric("edit_log_clean", MetricUnit.OPERATIONS,
            "counter of edit log succeed in cleaning");
        COUNTER_EDIT_LOG_CLEAN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CLEAN_SUCCESS);
        COUNTER_EDIT_LOG_CLEAN_FAILED = new LongCounterMetric("edit_log_clean", MetricUnit.OPERATIONS,
            "counter of edit log failed to clean");
        COUNTER_EDIT_LOG_CLEAN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CLEAN_FAILED);

        // image generate
        COUNTER_IMAGE_WRITE_SUCCESS = new LongCounterMetric("image_write", MetricUnit.OPERATIONS,
                "counter of image succeed in write");
        COUNTER_IMAGE_WRITE_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_WRITE_SUCCESS);
        COUNTER_IMAGE_WRITE_FAILED = new LongCounterMetric("image_write", MetricUnit.OPERATIONS,
                "counter of image failed to write");
        COUNTER_IMAGE_WRITE_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_WRITE_FAILED);

        COUNTER_IMAGE_PUSH_SUCCESS = new LongCounterMetric("image_push", MetricUnit.OPERATIONS,
                "counter of image succeeded in pushing to other frontends");
        COUNTER_IMAGE_PUSH_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_PUSH_SUCCESS);
        COUNTER_IMAGE_PUSH_FAILED = new LongCounterMetric("image_push", MetricUnit.OPERATIONS,
                "counter of image failed to other frontends");
        COUNTER_IMAGE_PUSH_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_PUSH_FAILED);

        // image clean
        COUNTER_IMAGE_CLEAN_SUCCESS = new LongCounterMetric("image_clean", MetricUnit.OPERATIONS,
                "counter of image succeeded in cleaning");
        COUNTER_IMAGE_CLEAN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_CLEAN_SUCCESS);
        COUNTER_IMAGE_CLEAN_FAILED = new LongCounterMetric("image_clean", MetricUnit.OPERATIONS,
                "counter of image failed to clean");
        COUNTER_IMAGE_CLEAN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_CLEAN_FAILED);

        // txn
        COUNTER_TXN_REJECT = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of rejected transactions");
        COUNTER_TXN_REJECT.addLabel(new MetricLabel("type", "reject"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_REJECT);
        COUNTER_TXN_BEGIN = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of beginning transactions");
        COUNTER_TXN_BEGIN.addLabel(new MetricLabel("type", "begin"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_BEGIN);
        COUNTER_TXN_SUCCESS = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of success transactions");
        COUNTER_TXN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_SUCCESS);
        COUNTER_TXN_FAILED = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of failed transactions");
        COUNTER_TXN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_FAILED);
        HISTO_TXN_EXEC_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("txn", "exec", "latency", "ms"));
        HISTO_TXN_PUBLISH_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("txn", "publish", "latency", "ms"));
        GaugeMetric<Long> txnNum = new GaugeMetric<Long>("txn_num", MetricUnit.NOUNIT,
                "number of running transactions") {
            @Override
            public Long getValue() {
                return Env.getCurrentGlobalTransactionMgr().getAllRunningTxnNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(txnNum);
        DB_GAUGE_TXN_NUM = addLabeledMetrics("db", () ->
                new GaugeMetricImpl<>("txn_num", MetricUnit.NOUNIT, "number of running transactions"));
        GaugeMetric<Long> publishTxnNum = new GaugeMetric<Long>("publish_txn_num", MetricUnit.NOUNIT,
                "number of publish transactions") {
            @Override
            public Long getValue() {
                return Env.getCurrentGlobalTransactionMgr().getAllPublishTxnNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(publishTxnNum);
        DB_GAUGE_PUBLISH_TXN_NUM = addLabeledMetrics("db",
                () -> new GaugeMetricImpl<>("publish_txn_num", MetricUnit.NOUNIT, "number of publish transactions"));
        COUNTER_ROUTINE_LOAD_ROWS = new LongCounterMetric("routine_load_rows", MetricUnit.ROWS,
                "total rows of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_ROWS);
        COUNTER_ROUTINE_LOAD_RECEIVED_BYTES = new LongCounterMetric("routine_load_receive_bytes", MetricUnit.BYTES,
                "total received bytes of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_RECEIVED_BYTES);
        COUNTER_ROUTINE_LOAD_ERROR_ROWS = new LongCounterMetric("routine_load_error_rows", MetricUnit.ROWS,
                "total error rows of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_ERROR_ROWS);

        COUNTER_HIT_SQL_BLOCK_RULE = new LongCounterMetric("counter_hit_sql_block_rule", MetricUnit.ROWS,
                "total hit sql block rule query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_HIT_SQL_BLOCK_RULE);

        THRIFT_COUNTER_RPC_ALL = addLabeledMetrics("method", () ->
                new LongCounterMetric("thrift_rpc_total", MetricUnit.NOUNIT, ""));
        THRIFT_COUNTER_RPC_LATENCY = addLabeledMetrics("method", () ->
                new LongCounterMetric("thrift_rpc_latency_ms", MetricUnit.MILLISECONDS, ""));

        // init system metrics
        initSystemMetrics();

        updateMetrics();
        isInit = true;

        if (Config.enable_metric_calculator) {
            metricTimer.scheduleAtFixedRate(metricCalculator, 0, 15 * 1000L, TimeUnit.MILLISECONDS);
        }
    }

    private static void initSystemMetrics() {
        // TCP retransSegs
        GaugeMetric<Long> tcpRetransSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "All TCP packets retransmitted") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpRetransSegs;
            }
        };
        tcpRetransSegs.addLabel(new MetricLabel("name", "tcp_retrans_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tcpRetransSegs);

        // TCP inErrs
        GaugeMetric<Long> tpcInErrs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all problematic TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInErrs;
            }
        };
        tpcInErrs.addLabel(new MetricLabel("name", "tcp_in_errs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcInErrs);

        // TCP inSegs
        GaugeMetric<Long> tpcInSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInSegs;
            }
        };
        tpcInSegs.addLabel(new MetricLabel("name", "tcp_in_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcInSegs);

        // TCP outSegs
        GaugeMetric<Long> tpcOutSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets send with RST") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpOutSegs;
            }
        };
        tpcOutSegs.addLabel(new MetricLabel("name", "tcp_out_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcOutSegs);

        // Memory Total
        GaugeMetric<Long> memTotal = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "Total usable memory") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memTotal;
            }
        };
        memTotal.addLabel(new MetricLabel("name", "memory_total"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memTotal);

        // Memory Free
        GaugeMetric<Long> memFree = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "The amount of physical memory not used by the system") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memFree;
            }
        };
        memFree.addLabel(new MetricLabel("name", "memory_free"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memFree);

        // Memory Total
        GaugeMetric<Long> memAvailable = (GaugeMetric<Long>) new GaugeMetric<Long>("meminfo", MetricUnit.BYTES,
                "An estimate of how much memory is available for starting new applications, without swapping") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memAvailable;
            }
        };
        memAvailable.addLabel(new MetricLabel("name", "memory_available"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memAvailable);

        // Buffers
        GaugeMetric<Long> buffers = (GaugeMetric<Long>) new GaugeMetric<Long>("meminfo", MetricUnit.BYTES,
                "Memory in buffer cache, so relatively temporary storage for raw disk blocks") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.buffers;
            }
        };
        buffers.addLabel(new MetricLabel("name", "buffers"));
        DORIS_METRIC_REGISTER.addSystemMetrics(buffers);

        // Cached
        GaugeMetric<Long> cached = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "Memory in the pagecache (Diskcache and Shared Memory)") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.cached;
            }
        };
        cached.addLabel(new MetricLabel("name", "cached"));
        DORIS_METRIC_REGISTER.addSystemMetrics(cached);
    }

    // to generate the metrics related to tablets of each backends
    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateBackendsTabletMetrics() {
        // remove all previous 'tablet' metric
        DORIS_METRIC_REGISTER.removeMetrics(TABLET_NUM);
        DORIS_METRIC_REGISTER.removeMetrics(TABLET_MAX_COMPACTION_SCORE);

        SystemInfoService infoService = Env.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();

        for (Long beId : infoService.getAllBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            // tablet number of each backends
            GaugeMetric<Long> tabletNum = new GaugeMetric<Long>(TABLET_NUM, MetricUnit.NOUNIT, "tablet number") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return (long) invertedIndex.getTabletNumByBackendId(beId);
                }
            };
            tabletNum.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            DORIS_METRIC_REGISTER.addMetrics(tabletNum);

            // max compaction score of tablets on each backends
            GaugeMetric<Long> tabletMaxCompactionScore = new GaugeMetric<Long>(TABLET_MAX_COMPACTION_SCORE,
                    MetricUnit.NOUNIT, "tablet max compaction score") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return be.getTabletMaxCompactionScore();
                }
            };
            tabletMaxCompactionScore.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            DORIS_METRIC_REGISTER.addMetrics(tabletMaxCompactionScore);

        } // end for backends
    }

    public static synchronized String getMetric(MetricVisitor visitor) {
        if (!isInit) {
            return "";
        }

        // update the metrics first
        updateMetrics();

        // jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        visitor.visitJvm(jvmStats);

        // doris metrics and system metrics.
        DORIS_METRIC_REGISTER.accept(visitor);

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(MetricVisitor.FE_PREFIX, entry.getKey(), entry.getValue());
        }

        // node info
        visitor.getNodeInfo();

        return visitor.finish();
    }

    public static <M extends Metric<?>> AutoMappedMetric<M> addLabeledMetrics(String label, Supplier<M> metric) {
        return new AutoMappedMetric<>(value -> {
            M m = metric.get();
            m.addLabel(new MetricLabel(label, value));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(m);
            return m;
        });
    }

    // update some metrics to make a ready to be visited
    private static void updateMetrics() {
        SYSTEM_METRICS.update();
    }

    public static synchronized List<Metric> getMetricsByName(String name) {
        return DORIS_METRIC_REGISTER.getMetricsByName(name);
    }
}
