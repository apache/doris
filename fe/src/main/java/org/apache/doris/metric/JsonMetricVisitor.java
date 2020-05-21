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

import org.apache.doris.monitor.jvm.JvmStats;
import com.codahale.metrics.Histogram;
import java.util.List;

public class JsonMetricVisitor extends MetricVisitor {
    private int ordinal = 0;
    private int metricNumber = 0;

    public JsonMetricVisitor(String prefix) {
        super(prefix);
    }

    @Override
    public void setMetricNumber(int metricNumber) {
        this.metricNumber = metricNumber;
    }

    @Override
    public void visitJvm(StringBuilder sb, JvmStats jvmStats) {
        return;
    }

    @Override
    public void visit(StringBuilder sb, @SuppressWarnings("rawtypes") Metric metric) {
        if (ordinal++ == 0) {
            sb.append("[\n");
        }
        sb.append("{\n\t\"tags\":\n\t{\n");
        sb.append("\t\t\"metric\":\"").append(metric.getName()).append("\"");

        // name
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append(",\n");
            int i = 0;
            for (MetricLabel label : labels) {
                if (i++ > 0) {
                    sb.append(",\n");
                }
                sb.append("\t\t\"").append(label.getKey()).append("\":\"").append(label.getValue()).append("\"");
            }
        }
        sb.append("\n\t},\n");
        sb.append("\t\"unit\":\"").append(metric.getUnit().name().toLowerCase()).append( "\",\n");

        // value
        sb.append("\t\"value\":").append(metric.getValue().toString()).append("\n}");
        if (ordinal < metricNumber) {
            sb.append(",\n");
        } else {
            sb.append("\n]");
        }
        return;
    }

    @Override
    public void visitHistogram(StringBuilder sb, String name, Histogram histogram) {
        return;
    }

    @Override
    public void getNodeInfo(StringBuilder sb) {
        return;
    }
}

