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

import com.google.common.util.concurrent.AtomicDouble;

public class DoubleCounterMetric extends CounterMetric<Double> {

    public DoubleCounterMetric(String name, MetricUnit unit, String description) {
        super(name, unit, description);
    }

    private AtomicDouble value = new AtomicDouble(0.0);

    @Override
    public void increase(Double delta) {
        value.addAndGet(delta);
    }

    @Override
    public Double getValue() {
        return value.get();
    }
}
