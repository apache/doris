// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.util;

import io.dropwizard.metrics.Metric;
import io.dropwizard.metrics.MetricName;
import io.dropwizard.metrics.MetricRegistry;
import org.codehaus.jackson.map.ObjectMapper;

public class Metrics {
    private static final MetricRegistry METRICS = new MetricRegistry();
    
    public enum MetricType {
        COUNTER,
        HISTOGRAM,
        METER,
        TIMER
    }
    
    private Metrics() {
    }

    public static Metric getMetric(MetricType metricType, String name) {
        if (METRICS.getMetrics().containsKey(name)) {
            return METRICS.getMetrics().get(name);
        } else {
            switch (metricType) {
                case COUNTER:
                    return METRICS.counter(name);
                case HISTOGRAM:
                    return METRICS.histogram(name);
                case METER:
                    return METRICS.meter(name);
                case TIMER:
                    return METRICS.timer(name);
                default:
                    return null;
            }
        }
    }
    
    public static MetricName name(Class<?> klass, String... names) {
        return MetricRegistry.name(klass, names);
    }
    
    public static MetricName name(String name, String... names) {
        return MetricRegistry.name(name, names);
    }
    
    public static String getJsonStr() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(METRICS.getMetrics());
        } catch (Exception e) {
            return "";
        }
    }
    
//    public static void main(String[] args) throws InterruptedException, IOException {
//        System.out.println("Hello World!");
//        Counter counter = (Counter)Metrics.getMetric(Metrics.MetricType.COUNTER, "xxx");
//        counter.inc(1);
//        System.out.println(Metrics.getJsonStr());
//    }
}
