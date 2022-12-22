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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    private Type dataType;

    private int maxBucketSize;
    private int bucketSize;
    private float sampleRate;

    private List<Bucket> buckets;

    public Histogram(Type dataType) {
        this.dataType = dataType;
    }

    public Type getDataType() {
        return dataType;
    }

    public void setDataType(Type dataType) {
        this.dataType = dataType;
    }

    public int getMaxBucketSize() {
        return maxBucketSize;
    }

    public void setMaxBucketSize(int maxBucketSize) {
        this.maxBucketSize = maxBucketSize;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public float getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(float sampleRate) {
        if (sampleRate < 0f || sampleRate > 1f) {
            this.sampleRate = 1f;
        } else {
            this.sampleRate = sampleRate;
        }
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public static Histogram defaultHistogram() {
        Type type = Type.fromPrimitiveType(PrimitiveType.INVALID_TYPE);
        List<Bucket> buckets = Lists.newArrayList();
        Histogram histogram = new Histogram(type);
        histogram.setMaxBucketSize(0);
        histogram.setBucketSize(0);
        histogram.setSampleRate(1.0f);
        histogram.setBuckets(buckets);
        return histogram;
    }

    /**
     * Histogram info is stored in an internal table in json format,
     * and Histogram obj can be obtained by this method.
     */
    public static Histogram deserializeFromJson(Type datatype, String json) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }

        try {
            Histogram histogram = new Histogram(datatype);
            JSONObject histogramJson = JSON.parseObject(json);

            List<Bucket> buckets = Lists.newArrayList();
            JSONArray jsonArray = histogramJson.getJSONArray("buckets");

            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject bucketJson = jsonArray.getJSONObject(i);
                Bucket bucket = new Bucket();
                bucket.lower = StatisticsUtil.readableValue(datatype, bucketJson.get("lower").toString());
                bucket.upper = StatisticsUtil.readableValue(datatype, bucketJson.get("upper").toString());
                bucket.count = bucketJson.getIntValue("count");
                bucket.preSum = bucketJson.getIntValue("pre_sum");
                bucket.ndv = bucketJson.getIntValue("ndv");
                buckets.add(bucket);
            }

            histogram.setBuckets(buckets);

            int maxBucketSize = histogramJson.getIntValue("max_bucket_size");
            histogram.setMaxBucketSize(maxBucketSize);

            int bucketSize = histogramJson.getIntValue("bucket_size");
            histogram.setBucketSize(bucketSize);

            float sampleRate = histogramJson.getFloatValue("sample_rate");
            histogram.setSampleRate(sampleRate);

            return histogram;
        } catch (Throwable e) {
            LOG.warn("deserialize from json error, input json string: {}", json, e);
        }

        return null;
    }

    /**
     * Convert to json format string
     */
    public static String serializeToJson(Histogram histogram) {
        if (histogram == null) {
            return "";
        }

        JSONObject histogramJson = new JSONObject();
        histogramJson.put("max_bucket_size", histogram.maxBucketSize);
        histogramJson.put("bucket_size", histogram.bucketSize);
        histogramJson.put("sample_rate", histogram.sampleRate);

        JSONArray bucketsJsonArray = new JSONArray();
        histogramJson.put("buckets", bucketsJsonArray);

        if (histogram.buckets != null) {
            for (Bucket bucket : histogram.buckets) {
                JSONObject bucketJson = new JSONObject();
                bucketJson.put("count", bucket.count);
                bucketJson.put("pre_sum", bucket.preSum);
                bucketJson.put("ndv", bucket.ndv);
                bucketJson.put("upper", bucket.upper.getStringValue());
                bucketJson.put("lower", bucket.lower.getStringValue());
                bucketsJsonArray.add(bucketJson);
            }
        }

        return histogramJson.toJSONString();
    }
}
