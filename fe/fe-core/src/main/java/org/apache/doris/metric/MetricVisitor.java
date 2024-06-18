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

/*
 * MetricVisitor will visit the metrics in metric repo and print them in StringBuilder
 */
public abstract class MetricVisitor {

    // for FE metrics
    public static final String FE_PREFIX = "doris_fe_";
    // for system metrics
    public static final String SYS_PREFIX = "system_";

    protected StringBuilder sb = new StringBuilder();

    public MetricVisitor() {
    }

    public abstract void visitJvm(JvmStats jvmStats);

    public abstract void visit(String prefix, Metric metric);

    public abstract void visitHistogram(String prefix, String name, Histogram histogram);

    public abstract void visitNodeInfo();

    public abstract void visitCloudTableStats();

    public String finish() {
        return sb.toString();
    }
}
