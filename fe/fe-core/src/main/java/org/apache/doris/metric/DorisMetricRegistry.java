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

import org.apache.doris.catalog.Catalog;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class DorisMetricRegistry {

    private Collection<Metric> paloMetrics = new PriorityQueue<>(Comparator.comparing(Metric::getName));

    public DorisMetricRegistry() {

    }

    public synchronized void addPaloMetrics(Metric paloMetric) {
        // No metric needs to be added to the Checkpoint thread.
        // And if you add a metric in Checkpoint thread, it will cause the metric to be added repeatedly,
        // and the Checkpoint Catalog may be saved incorrectly, resulting in FE memory leaks.
        if (!Catalog.isCheckpointThread()) {
            paloMetrics.add(paloMetric);
        }
    }

    public synchronized List<Metric> getPaloMetrics() {
        return Lists.newArrayList(paloMetrics);
    }

    // the metrics by metric name
    public synchronized List<Metric> getPaloMetricsByName(String name) {
        return paloMetrics.stream().filter(m -> m.getName().equals(name)).collect(Collectors.toList());
    }

    public synchronized void removeMetrics(String name) {
        // Same reason as comment in addPaloMetrics()
        if (!Catalog.isCheckpointThread()) {
            paloMetrics = paloMetrics.stream().filter(m -> !(m.getName().equals(name))).collect(Collectors.toList());
        }
    }
}
