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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

/*
 * MetricCalculator will collect and calculate some certain metrics at a fix rate,
 * such QPS, and save the result for users to get.
 */
public class MetricCalculator extends TimerTask {
    private long lastTs = -1;
    private long lastQueryCounter = -1;
    private long lastRequestCounter = -1;
    private long lastQueryErrCounter = -1;

    private Map<String, Long> clusterLastRequestCounter = new HashMap<>();
    private Map<String, Long> clusterLastQueryCounter = new HashMap<>();
    private Map<String, Long> clusterLastQueryErrCounter = new HashMap<>();

    @Override
    public void run() {
        update();
    }

    private void update() {
        long currentTs = System.currentTimeMillis();
        if (lastTs == -1) {
            lastTs = currentTs;
            lastQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
            lastRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
            lastQueryErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
            initCloudMetrics();
            return;
        }

        long interval = (currentTs - lastTs) / 1000 + 1;

        // qps
        long currentQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
        double qps = (double) (currentQueryCounter - lastQueryCounter) / interval;
        MetricRepo.GAUGE_QUERY_PER_SECOND.setValue(qps < 0 ? 0.0 : qps);
        lastQueryCounter = currentQueryCounter;

        // rps
        long currentRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
        double rps = (double) (currentRequestCounter - lastRequestCounter) / interval;
        MetricRepo.GAUGE_REQUEST_PER_SECOND.setValue(rps < 0 ? 0.0 : rps);
        lastRequestCounter = currentRequestCounter;

        // err rate
        long currentErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
        double errRate = (double) (currentErrCounter - lastQueryErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ERR_RATE.setValue(errRate < 0 ? 0.0 : errRate);
        lastQueryErrCounter = currentErrCounter;

        updateCloudMetrics(interval);
        lastTs = currentTs;

        // max tablet compaction score of all backends
        long maxCompactionScore = 0;
        List<Metric> compactionScoreMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_MAX_COMPACTION_SCORE);
        for (Metric metric : compactionScoreMetrics) {
            if (((GaugeMetric<Long>) metric).getValue() > maxCompactionScore) {
                maxCompactionScore = ((GaugeMetric<Long>) metric).getValue();
            }
        }
        MetricRepo.GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(maxCompactionScore);
    }

    private void initCloudMetrics() {
        if (!Config.isCloudMode()) {
            return;
        }
        Map<String, LongCounterMetric> requsetAllMetrics = CloudMetrics.CLUSTER_REQUEST_ALL_COUNTER.getMetrics();
        if (requsetAllMetrics != null) {
            requsetAllMetrics.forEach((clusterId, metric) -> {
                clusterLastRequestCounter.put(clusterId, metric.getValue());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
            });
        }

        Map<String, LongCounterMetric> queryAllMetrics = CloudMetrics.CLUSTER_QUERY_ALL_COUNTER.getMetrics();
        if (queryAllMetrics != null) {
            queryAllMetrics.forEach((clusterId, metric) -> {
                clusterLastQueryCounter.put(clusterId, metric.getValue());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
            });
        }

        Map<String, LongCounterMetric> queryErrMetrics = CloudMetrics.CLUSTER_QUERY_ERR_COUNTER.getMetrics();
        if (queryErrMetrics != null) {
            queryErrMetrics.forEach((clusterId, metric) -> {
                clusterLastQueryErrCounter.put(clusterId, metric.getValue());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
            });
        }
    }

    private void updateCloudMetrics(long interval) {
        if (!Config.isCloudMode()) {
            return;
        }

        Map<String, LongCounterMetric> requsetAllMetrics = CloudMetrics.CLUSTER_REQUEST_ALL_COUNTER.getMetrics();
        if (requsetAllMetrics != null) {
            requsetAllMetrics.forEach((clusterId, metric) -> {
                double rps = (double) (metric.getValue() - clusterLastRequestCounter.getOrDefault(clusterId, 0L))
                        / interval;
                rps = Double.max(rps, 0);
                MetricRepo.updateClusterRequestPerSecond(clusterId, rps,  metric.getLabels());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
                clusterLastRequestCounter.put(clusterId, metric.getValue());
            });
        }

        Map<String, LongCounterMetric> queryAllMetrics = CloudMetrics.CLUSTER_QUERY_ALL_COUNTER.getMetrics();
        if (queryAllMetrics != null) {
            queryAllMetrics.forEach((clusterId, metric) -> {
                double rps = (double) (metric.getValue() - clusterLastQueryCounter.getOrDefault(clusterId, 0L))
                        / interval;
                rps = Double.max(rps, 0);
                MetricRepo.updateClusterQueryPerSecond(clusterId, rps,  metric.getLabels());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
                clusterLastQueryCounter.put(clusterId, metric.getValue());
            });
        }

        Map<String, LongCounterMetric> queryErrMetrics = CloudMetrics.CLUSTER_QUERY_ERR_COUNTER.getMetrics();
        if (queryErrMetrics != null) {
            queryErrMetrics.forEach((clusterId, metric) -> {
                double rps = (double) (metric.getValue() - clusterLastQueryErrCounter.getOrDefault(clusterId, 0L))
                        / interval;
                rps = Double.max(rps, 0);
                MetricRepo.updateClusterQueryErrRate(clusterId, rps, metric.getLabels());
                MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
                clusterLastQueryErrCounter.put(clusterId, metric.getValue());
            });
        }
    }
}
