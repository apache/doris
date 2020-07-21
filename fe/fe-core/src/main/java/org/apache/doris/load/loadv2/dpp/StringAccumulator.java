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

package org.apache.doris.load.loadv2.dpp;

import org.apache.spark.util.AccumulatorV2;
import java.util.List;
import java.util.ArrayList;

// This class is a accumulator of string based on AccumulatorV2
// (https://spark.apache.org/docs/latest/api/java/org/apache/spark/util/AccumulatorV2.html).
// Spark does not provide string accumulator.
//
// This class is used to collect the invalid rows when doing etl.
public class StringAccumulator extends AccumulatorV2<String, String> {
    private List<String> strs = new ArrayList<>();

    @Override
    public boolean isZero() {
        return strs.isEmpty();
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        StringAccumulator newAccumulator = new StringAccumulator();
        newAccumulator.strs.addAll(this.strs);
        return newAccumulator;
    }

    @Override
    public void reset() {
        strs.clear();
    }

    @Override
    public void add(String v) {
        strs.add(v);
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        StringAccumulator o = (StringAccumulator)other;
        strs.addAll(o.strs);
    }

    @Override
    public String value() {
        return strs.toString();
    }
}