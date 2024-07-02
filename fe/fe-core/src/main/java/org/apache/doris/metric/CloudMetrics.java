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
    }
}
