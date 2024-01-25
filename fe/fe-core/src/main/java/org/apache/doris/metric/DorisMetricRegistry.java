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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DorisMetricRegistry {
    ConcurrentHashMap<String, MetricList> metrics = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, MetricList> systemMetrics = new ConcurrentHashMap<>();

    public DorisMetricRegistry() {

    }

    public void addMetrics(Metric metric) {
        // No metric needs to be added to the Checkpoint thread.
        // And if you add a metric in Checkpoint thread, it will cause the metric to be added repeatedly,
        // and the Checkpoint Catalog may be saved incorrectly, resulting in FE memory leaks.
        if (!Env.isCheckpointThread()) {
            String labelId = computeLabelId(metric.getLabels());
            metrics.computeIfAbsent(metric.getName(), (k) -> new MetricList())
                    .addMetrics(labelId, metric);
        }
    }

    public void addSystemMetrics(Metric sysMetric) {
        if (!Env.isCheckpointThread()) {
            String labelId = computeLabelId(sysMetric.getLabels());
            systemMetrics.computeIfAbsent(sysMetric.getName(), (k) -> new MetricList())
                    .addMetrics(labelId, sysMetric);
        }
    }

    public void accept(MetricVisitor visitor) {
        final List<MetricList> metricsList = Lists.newArrayList();
        metrics.forEach((name, list) -> metricsList.add(list));
        final List<MetricList> sysMetricsList = Lists.newArrayList();
        systemMetrics.forEach((name, list) -> sysMetricsList.add(list));
        for (MetricList list : metricsList) {
            for (Metric metric : list.getMetrics()) {
                visitor.visit(MetricVisitor.FE_PREFIX, metric);
            }
        }
        for (MetricList list : sysMetricsList) {
            for (Metric metric : list.getMetrics()) {
                visitor.visit(MetricVisitor.SYS_PREFIX, metric);
            }
        }
    }

    // the metrics by metric name
    public List<Metric> getMetricsByName(String name) {
        MetricList list = metrics.get(name);
        if (list == null) {
            list = systemMetrics.get(name);
        }
        if (list == null) {
            return Lists.newArrayList();
        }
        return list.getMetrics();
    }

    public void removeMetrics(String name) {
        // Same reason as comment in addMetrics()
        if (!Env.isCheckpointThread()) {
            metrics.remove(name);
        }
    }

    public void removeMetricsByNameAndLabels(String name, List<MetricLabel> labels) {
        // Same reason as comment in addMetrics()
        if (!Env.isCheckpointThread()) {
            MetricList metricList = metrics.get(name);
            if (metricList != null) {
                String labelId = computeLabelId(labels);
                metricList.removeByLabelId(labelId);
            }
        }
    }

    private static String computeLabelId(List<MetricLabel> labels) {
        TreeMap<String, String> labelMap = new TreeMap<>();
        for (MetricLabel label : labels) {
            labelMap.put(label.getKey(), label.getValue().replace("\\", "\\\\").replace("\"", "\\\""));
        }
        return labelMap.entrySet()
                .stream()
                .map(e -> String.format("%s=\"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));
    }

    public static class MetricList {
        private final HashMap<String, Metric> metrics = new HashMap<>();

        private synchronized void addMetrics(String labelId, Metric metric) {
            metrics.put(labelId, metric);
        }

        private synchronized List<Metric> getMetrics() {
            return new ArrayList<>(metrics.values());
        }

        private synchronized void removeByLabelId(String labelId) {
            metrics.remove(labelId);
        }
    }
}
