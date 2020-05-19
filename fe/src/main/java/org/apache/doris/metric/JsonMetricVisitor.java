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

/*
 * Like this:
 * # HELP doris_fe_job_load_broker_cost_ms doris_fe_job_load_broker_cost_ms 
 * # TYPE doris_fe_job_load_broker_cost_ms gauge 
 * doris_fe_job{job="load", type="mini", state="pending"} 0
 */
public class JsonMetricVisitor extends MetricVisitor {
    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_DIRECT_BUFFER_POOL_SIZE_BYTES = "jvm_direct_buffer_pool_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String HELP = "# HELP ";
    private static final String TYPE = "# TYPE ";
    private int ordinal = 0;
    private int metric_number = 0;

    public JsonMetricVisitor(String prefix) {
        super(prefix);
    }

    @Override
    public void setMetricNumber(int metric_number) {
        this.metric_number = metric_number;
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
        if (ordinal < metric_number) {
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

