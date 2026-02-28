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

import org.apache.doris.common.Config;
import org.apache.doris.metric.Metric.MetricUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

public class CloudMetrics {
    protected static String CLOUD_CLUSTER_DELIMITER = "@delimiter#";
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_REQUEST_ALL_COUNTER;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_QUERY_ALL_COUNTER;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_QUERY_ERR_COUNTER;

    protected static AutoMappedMetric<GaugeMetricImpl<Double>> CLUSTER_REQUEST_PER_SECOND_GAUGE;
    protected static AutoMappedMetric<GaugeMetricImpl<Double>> CLUSTER_QUERY_PER_SECOND_GAUGE;
    protected static AutoMappedMetric<GaugeMetricImpl<Double>> CLUSTER_QUERY_ERR_RATE_GAUGE;

    protected static AutoMappedMetric<Histogram> CLUSTER_QUERY_LATENCY_HISTO;

    protected static AutoMappedMetric<GaugeMetricImpl<Integer>> CLUSTER_BACKEND_ALIVE;
    protected static AutoMappedMetric<GaugeMetricImpl<Integer>> CLUSTER_BACKEND_ALIVE_TOTAL;

    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_WARM_UP_JOB_EXEC_COUNT;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_WARM_UP_JOB_REQUESTED_TABLETS;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_WARM_UP_JOB_FINISHED_TABLETS;

    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_WARM_UP_JOB_LATEST_START_TIME;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_WARM_UP_JOB_LAST_FINISH_TIME;

    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_CLOUD_PARTITION_BALANCE_NUM;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_CLOUD_TABLE_BALANCE_NUM;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_CLOUD_GLOBAL_BALANCE_NUM;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_CLOUD_SMOOTH_UPGRADE_BALANCE_NUM;
    protected static AutoMappedMetric<LongCounterMetric> CLUSTER_CLOUD_WARM_UP_CACHE_BALANCE_NUM;

    // Per-method meta-service RPC metrics
    public static AutoMappedMetric<LongCounterMetric> META_SERVICE_RPC_TOTAL;
    public static AutoMappedMetric<LongCounterMetric> META_SERVICE_RPC_FAILED;
    public static AutoMappedMetric<LongCounterMetric> META_SERVICE_RPC_RETRY;
    public static AutoMappedMetric<GaugeMetricImpl<Double>> META_SERVICE_RPC_PER_SECOND;
    public static AutoMappedMetric<Histogram> META_SERVICE_RPC_LATENCY;

    // Aggregate meta-service metrics
    public static LongCounterMetric META_SERVICE_RPC_ALL_TOTAL;
    public static LongCounterMetric META_SERVICE_RPC_ALL_FAILED;
    public static LongCounterMetric META_SERVICE_RPC_ALL_RETRY;
    public static GaugeMetricImpl<Double> META_SERVICE_RPC_ALL_PER_SECOND;

    protected static void init() {
        if (Config.isNotCloudMode()) {
            return;
        }
        CLUSTER_REQUEST_ALL_COUNTER = new AutoMappedMetric<>(name -> new LongCounterMetric("request_total",
            MetricUnit.REQUESTS, "total request"));

        CLUSTER_QUERY_ALL_COUNTER = new AutoMappedMetric<>(name -> new LongCounterMetric("query_total",
            MetricUnit.REQUESTS, "total query"));

        CLUSTER_QUERY_ERR_COUNTER = new AutoMappedMetric<>(name -> new LongCounterMetric("query_err",
            MetricUnit.REQUESTS, "total error query"));

        CLUSTER_REQUEST_PER_SECOND_GAUGE = new AutoMappedMetric<>(name -> new GaugeMetricImpl<Double>("rps",
            MetricUnit.NOUNIT, "request per second", 0.0));

        CLUSTER_QUERY_PER_SECOND_GAUGE = new AutoMappedMetric<>(name -> new GaugeMetricImpl<Double>("qps",
            MetricUnit.NOUNIT, "query per second", 0.0));

        CLUSTER_QUERY_ERR_RATE_GAUGE = new AutoMappedMetric<>(name -> new GaugeMetricImpl<Double>("query_err_rate",
            MetricUnit.NOUNIT, "query error rate", 0.0));

        CLUSTER_BACKEND_ALIVE = new AutoMappedMetric<>(name -> new GaugeMetricImpl<Integer>("backend_alive",
            MetricUnit.NOUNIT, "backend alive or not", 0));

        CLUSTER_BACKEND_ALIVE_TOTAL = new AutoMappedMetric<>(name -> new GaugeMetricImpl<Integer>("backend_alive_total",
            MetricUnit.NOUNIT, "backend alive num in cluster", 0));

        CLUSTER_QUERY_LATENCY_HISTO = new AutoMappedMetric<>(key -> {
            String[] values = key.split(CLOUD_CLUSTER_DELIMITER);
            String clusterId = values[0];
            String clusterName = values[1];
            String metricName = MetricRegistry.name("query", "latency", "ms", "cluster_id="
                    + clusterId, "cluster_name=" + clusterName);
            return MetricRepo.METRIC_REGISTER.histogram(metricName);
        });

        CLUSTER_WARM_UP_JOB_EXEC_COUNT = new AutoMappedMetric<>(name -> new LongCounterMetric(
                "file_cache_warm_up_job_exec_count", MetricUnit.NOUNIT, "warm up job execution count"));
        CLUSTER_WARM_UP_JOB_LATEST_START_TIME = new AutoMappedMetric<>(name -> new LongCounterMetric(
                "file_cache_warm_up_job_latest_start_time", MetricUnit.MILLISECONDS,
                "the latest start time (ms, epoch time) of the warm up job"));
        CLUSTER_WARM_UP_JOB_LAST_FINISH_TIME = new AutoMappedMetric<>(name -> new LongCounterMetric(
                "file_cache_warm_up_job_last_finish_time", MetricUnit.MILLISECONDS,
                "the last finish time (ms, epoch time) of the warm up job"));

        CLUSTER_WARM_UP_JOB_REQUESTED_TABLETS = new AutoMappedMetric<>(
                name -> new LongCounterMetric("file_cache_warm_up_job_requested_tablets",
                        MetricUnit.NOUNIT, "warm up job requested tablets"));

        CLUSTER_WARM_UP_JOB_FINISHED_TABLETS = new AutoMappedMetric<>(
                name -> new LongCounterMetric("file_cache_warm_up_job_finished_tablets",
                        MetricUnit.NOUNIT, "warm up job finished tablets"));

        CLUSTER_CLOUD_PARTITION_BALANCE_NUM = new AutoMappedMetric<>(name -> new LongCounterMetric(
            "cloud_partition_balance_num", MetricUnit.NOUNIT,
            "current cluster cloud partition balance sync edit log number"));

        CLUSTER_CLOUD_TABLE_BALANCE_NUM = new AutoMappedMetric<>(name -> new LongCounterMetric(
            "cloud_table_balance_num", MetricUnit.NOUNIT,
            "current cluster cloud table balance sync edit log number"));

        CLUSTER_CLOUD_GLOBAL_BALANCE_NUM = new AutoMappedMetric<>(name -> new LongCounterMetric(
            "cloud_global_balance_num", MetricUnit.NOUNIT,
            "current cluster cloud be balance sync edit log number"));

        CLUSTER_CLOUD_SMOOTH_UPGRADE_BALANCE_NUM = new AutoMappedMetric<>(name -> new LongCounterMetric(
            "cloud_smooth_upgrade_balance_num", MetricUnit.NOUNIT,
            "current cluster cloud smooth upgrade sync edit log number"));

        CLUSTER_CLOUD_WARM_UP_CACHE_BALANCE_NUM = new AutoMappedMetric<>(name -> new LongCounterMetric(
            "cloud_warm_up_balance_num", MetricUnit.NOUNIT,
            "current cluster cloud warm up cache sync edit log number"));

        // Per-method meta-service RPC metrics
        META_SERVICE_RPC_TOTAL = MetricRepo.addLabeledMetrics("method", () ->
            new LongCounterMetric("meta_service_rpc_total", MetricUnit.NOUNIT,
                "total meta service RPC calls"));
        META_SERVICE_RPC_FAILED = MetricRepo.addLabeledMetrics("method", () ->
            new LongCounterMetric("meta_service_rpc_failed", MetricUnit.NOUNIT,
                "failed meta service RPC calls"));
        META_SERVICE_RPC_RETRY = MetricRepo.addLabeledMetrics("method", () ->
            new LongCounterMetric("meta_service_rpc_retry", MetricUnit.NOUNIT,
                "meta service RPC retry attempts"));
        META_SERVICE_RPC_PER_SECOND = new AutoMappedMetric<>(methodName -> {
            GaugeMetricImpl<Double> gauge = new GaugeMetricImpl<>("meta_service_rpc_per_second",
                    MetricUnit.NOUNIT, "meta service RPC requests per second", 0.0);
            gauge.addLabel(new MetricLabel("method", methodName));
            return gauge;
        });
        META_SERVICE_RPC_LATENCY = new AutoMappedMetric<>(methodName -> {
            String metricName = MetricRegistry.name("meta_service", "rpc", "latency", "ms",
                    "method=" + methodName);
            return MetricRepo.METRIC_REGISTER.histogram(metricName);
        });

        // Aggregate meta-service metrics
        META_SERVICE_RPC_ALL_TOTAL = new LongCounterMetric("meta_service_rpc_all_total",
            MetricUnit.NOUNIT, "total meta service RPC calls across all methods");
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(META_SERVICE_RPC_ALL_TOTAL);
        META_SERVICE_RPC_ALL_FAILED = new LongCounterMetric("meta_service_rpc_all_failed",
            MetricUnit.NOUNIT, "total failed meta service RPC calls");
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(META_SERVICE_RPC_ALL_FAILED);
        META_SERVICE_RPC_ALL_RETRY = new LongCounterMetric("meta_service_rpc_all_retry",
            MetricUnit.NOUNIT, "total meta service RPC retry attempts");
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(META_SERVICE_RPC_ALL_RETRY);
        META_SERVICE_RPC_ALL_PER_SECOND = new GaugeMetricImpl<>("meta_service_rpc_all_per_second",
            MetricUnit.NOUNIT, "meta service RPC requests per second (all methods)", 0.0);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(META_SERVICE_RPC_ALL_PER_SECOND);
    }
}
