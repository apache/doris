// Modifications copyright (C) 2018, Baidu.com, Inc.
// Copyright 2018 The Apache Software Foundation

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

public class PaloDoubleCounterMetric extends PaloCounterMetric<Double> {

    public PaloDoubleCounterMetric(String name, String description) {
        super(name, description);
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
