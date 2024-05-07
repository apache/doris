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

import com.google.common.collect.Lists;

import java.util.List;

public abstract class Metric<T> {
    public enum MetricType {
        GAUGE, COUNTER
    }

    public enum MetricUnit {
        NANOSECONDS,
        MICROSECONDS,
        MILLISECONDS,
        SECONDS,
        BYTES,
        ROWS,
        PERCENT,
        REQUESTS,
        OPERATIONS,
        BLOCKS,
        ROWSETS,
        CONNECTIONS,
        PACKETS,
        NOUNIT
    }

    protected String name;
    protected MetricType type;
    protected MetricUnit unit;
    protected List<MetricLabel> labels = Lists.newArrayList();
    protected String description;

    public Metric(String name, MetricType type, MetricUnit unit, String description) {
        this.name = name;
        this.type = type;
        this.unit = unit;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public MetricType getType() {
        return type;
    }

    public MetricUnit getUnit() {
        return unit;
    }

    public String getDescription() {
        return description;
    }

    public Metric<T> addLabel(MetricLabel label) {
        if (labels.contains(label)) {
            return this;
        }
        labels.add(label);
        return this;
    }

    public List<MetricLabel> getLabels() {
        return labels;
    }

    public Metric<T> setLabels(List<MetricLabel> newLabels) {
        this.labels = newLabels;
        return this;
    }

    public abstract T getValue();
}
