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

package org.apache.doris.statistics.model;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.List;
import java.util.Map;

/**
 * Builder for histogram
 */
public class HistogramBuilder {

    private Type dataType;

    private double sampleRate;

    private int numBuckets;

    private List<Bucket> buckets;

    private Map<Literal, Float> mcv;

    private List<Bucket> mcvBuckets;

    public HistogramBuilder() {
    }

    public HistogramBuilder(Histogram histogram) {
        this.dataType = histogram.dataType;
        this.sampleRate = histogram.sampleRate;
        this.buckets = histogram.buckets;
        this.mcv = histogram.mcv;
        this.mcvBuckets = histogram.mcvBuckets;
    }

    public HistogramBuilder setDataType(Type dataType) {
        this.dataType = dataType;
        return this;
    }

    public HistogramBuilder setSampleRate(double sampleRate) {
        this.sampleRate = sampleRate;
        return this;
    }

    public HistogramBuilder setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
        return this;
    }

    public HistogramBuilder setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
        return this;
    }

    public HistogramBuilder setMcv(Map<Literal, Float> mcv) {
        this.mcv = mcv;
        return this;
    }

    public HistogramBuilder setMcvBuckets(List<Bucket> mcvBuckets) {
        this.mcvBuckets = mcvBuckets;
        return this;
    }

    public Histogram build() {
        return new Histogram(dataType, sampleRate, numBuckets, buckets, mcv, mcvBuckets);
    }
}
