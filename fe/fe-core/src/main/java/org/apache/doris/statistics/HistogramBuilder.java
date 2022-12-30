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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Type;

import java.util.Comparator;
import java.util.List;

public class HistogramBuilder {
    private Type dataType;

    private int maxBucketNum;

    private int bucketNum;

    private double sampleRate;

    private List<Bucket> buckets;

    public HistogramBuilder() {
    }

    public HistogramBuilder(Histogram histogram) {
        this.dataType = histogram.dataType;
        this.maxBucketNum = histogram.maxBucketNum;
        this.bucketNum = histogram.bucketNum;
        this.sampleRate = histogram.sampleRate;
        this.buckets = histogram.buckets;
    }

    public HistogramBuilder setDataType(Type dataType) {
        this.dataType = dataType;
        return this;
    }

    public HistogramBuilder setMaxBucketNum(int maxBucketNum) {
        this.maxBucketNum = maxBucketNum;
        return this;
    }

    public HistogramBuilder setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
        return this;
    }

    public HistogramBuilder setSampleRate(double sampleRate) {
        if (sampleRate < 0 || sampleRate > 1.0) {
            this.sampleRate = 1.0;
        } else {
            this.sampleRate = sampleRate;
        }
        return this;
    }

    public HistogramBuilder setBuckets(List<Bucket> buckets) {
        buckets.sort(Comparator.comparing(Bucket::getLower));
        this.buckets = buckets;
        return this;
    }

    public Type getDataType() {
        return dataType;
    }

    public int getMaxBucketNum() {
        return maxBucketNum;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Histogram build() {
        return new Histogram(dataType, maxBucketNum, bucketNum, sampleRate, buckets);
    }
}
