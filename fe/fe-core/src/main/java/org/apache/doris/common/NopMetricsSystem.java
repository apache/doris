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

package org.apache.doris.common;

import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;

/**
 * A no-op MetricsSystem implementation to prevent Hadoop metrics2 memory leak.
 *
 * Each Hadoop FileSystem instance registers metrics with the global DefaultMetricsSystem,
 * creating MetricsSourceAdapter and JMX MBeans that are never unregistered on close().
 * Since Doris FE does not consume Hadoop metrics, we replace the default with this no-op
 * to prevent unbounded accumulation of MetricCounterLong, MBeanAttributeInfo, etc.
 */
public class NopMetricsSystem extends MetricsSystem {

    @Override
    public MetricsSystem init(String prefix) {
        return this;
    }

    @Override
    public <T> T register(String name, String desc, T source) {
        return source;
    }

    @Override
    public void unregisterSource(String name) {}

    @Override
    public MetricsSource getSource(String name) {
        return null;
    }

    @Override
    public <T extends MetricsSink> T register(String name, String desc, T sink) {
        return sink;
    }

    @Override
    public void register(Callback callback) {}

    @Override
    public void publishMetricsNow() {}

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public String currentConfig() {
        return "";
    }
}
