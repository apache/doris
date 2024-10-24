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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class AutoMappedMetric<M> {

    private final Map<String, M> nameToMetric = new ConcurrentHashMap<>();
    private final Function<String, M> metricSupplier;

    public AutoMappedMetric(Function<String, M> metricSupplier) {
        this.metricSupplier = metricSupplier;
    }

    public M getOrAdd(String name) {
        return nameToMetric.computeIfAbsent(name, metricSupplier);
    }

    public Map<String, M> getMetrics() {
        return nameToMetric;
    }

}
