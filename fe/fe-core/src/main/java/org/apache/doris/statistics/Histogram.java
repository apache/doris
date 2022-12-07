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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
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
        // sort here for quick lookup
        buckets.sort(Comparator.comparing(Bucket::getLower));
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
            LOG.error("deserialize from json error.", e);
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

        for (Bucket bucket : histogram.buckets) {
            JSONObject bucketJson = new JSONObject();
            bucketJson.put("count", bucket.count);
            bucketJson.put("pre_sum", bucket.preSum);
            bucketJson.put("ndv", bucket.ndv);
            bucketJson.put("upper", bucket.upper.getStringValue());
            bucketJson.put("lower", bucket.lower.getStringValue());
            bucketsJsonArray.add(bucketJson);
        }

        return histogramJson.toJSONString();
    }

    /**
     * Given a value, return the bucket to which it belongs,
     * return null if not found.
     */
    public Bucket findBucket(LiteralExpr key) {
        if (!buckets.isEmpty()) {
            int left = 0;
            int right = buckets.size() - 1;
            if (key.compareTo(buckets.get(right).upper) > 0) {
                return null;
            }

            while (left < right) {
                int mid = left + (right - left) / 2;
                if (key.compareTo(buckets.get(mid).upper) > 0) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            return buckets.get(right);
        }

        return null;
    }

    /**
     * Given a range, return the number of elements contained in the range.
     * Calculate the range count based on the sampling ratio.
     */
    public long rangeCount(LiteralExpr lower, boolean isIncludeLower, LiteralExpr upper, boolean isIncludeUpper) {
        try {
            double count = rangeCountIgnoreSampleRate(lower, isIncludeLower, upper, isIncludeUpper);
            return (long) Math.max((count) / sampleRate, 0);
        } catch (Throwable e) {
            LOG.warn("Failed to get the number of elements in the histogram range: + " + e);
        }
        return 0;
    }

    /**
     * Given a range, return the number of elements contained in the range.
     */
    private int rangeCountIgnoreSampleRate(LiteralExpr lower, boolean isIncludeLower,
                                           LiteralExpr upper, boolean isIncludeUpper) throws AnalysisException {
        if (buckets.isEmpty()) {
            return 0;
        }

        if (lower != null && upper == null) {
            if (isIncludeLower) {
                return greatEqualCount(lower);
            } else {
                return greatCount(lower);
            }
        }

        if (lower == null && upper != null) {
            if (isIncludeUpper) {
                return lessEqualCount(upper);
            } else {
                return lessCount(upper);
            }
        }

        if (lower != null) {
            int cmp = lower.compareTo(upper);
            if (cmp > 0) {
                return 0;
            } else if (cmp == 0) {
                if (!isIncludeLower || !isIncludeUpper) {
                    return 0;
                } else {
                    Bucket bucket = findBucket(upper);
                    if (bucket == null) {
                        return 0;
                    } else {
                        return bucket.count / bucket.ndv;
                    }
                }
            }
            Bucket lowerBucket = findBucket(lower);
            if (lowerBucket == null) {
                return 0;
            }
            Bucket upperBucket = findBucket(upper);
            if (upperBucket == null) {
                return greatEqualCount(lower);
            }
            if (isIncludeLower && isIncludeUpper) {
                return totalCount() - lessCount(lower) - greatCount(upper);
            } else if (isIncludeLower) {
                return totalCount() - lessCount(lower) - greatEqualCount(upper);
            } else if (isIncludeUpper) {
                return totalCount() - lessEqualCount(lower) - greatCount(upper);
            } else {
                return totalCount() - lessEqualCount(lower) - greatEqualCount(upper);
            }
        }

        return totalCount();
    }

    private int totalCount() {
        if (buckets.isEmpty()) {
            return 0;
        } else {
            Bucket lastBucket = buckets.get(buckets.size() - 1);
            return lastBucket.preSum + lastBucket.count;
        }
    }

    private int lessCount(LiteralExpr key) throws AnalysisException {
        Bucket bucket = findBucket(key);
        if (bucket == null) {
            if (buckets.isEmpty()) {
                return 0;
            }
            if (key.compareTo(buckets.get(0).lower) < 0) {
                return 0;
            }
            if ((key.compareTo(buckets.get(buckets.size() - 1).upper)) > 0) {
                return totalCount();
            }
            return totalCount();
        } else {
            if (key.compareTo(bucket.lower) == 0) {
                return bucket.preSum;
            } else if (key.compareTo(bucket.upper) == 0) {
                return bucket.preSum + bucket.count - bucket.count / bucket.ndv;
            } else {
                Double min = StatisticsUtil.convertToDouble(dataType, bucket.lower.getStringValue());
                Double max = StatisticsUtil.convertToDouble(dataType, bucket.upper.getStringValue());
                Double v = StatisticsUtil.convertToDouble(dataType, key.getStringValue());
                if (v < min) {
                    v = min;
                }
                if (v > max) {
                    v = max;
                }
                int result = bucket.preSum;
                if (max > min) {
                    result += (v - min) * bucket.count / (max - min);
                    if (v > min) {
                        result -= bucket.count / bucket.ndv;
                        if (result < 0) {
                            result = 0;
                        }
                    }
                }
                return result;
            }
        }
    }

    private int lessEqualCount(LiteralExpr key) throws AnalysisException {
        int lessCount = lessCount(key);
        Bucket bucket = findBucket(key);
        if (bucket == null) {
            return lessCount;
        } else {
            if (key.compareTo(bucket.lower) < 0) {
                return lessCount;
            }
            return lessCount + bucket.count / bucket.ndv;
        }
    }

    private int greatCount(LiteralExpr key) throws AnalysisException {
        int lessEqualCount = lessEqualCount(key);
        return totalCount() - lessEqualCount;
    }

    private int greatEqualCount(LiteralExpr key) throws AnalysisException {
        int greatCount = greatCount(key);
        Bucket bucket = findBucket(key);
        if (bucket != null) {
            if (key.compareTo(bucket.lower) < 0) {
                return greatCount;
            }
            return greatCount + bucket.count / bucket.ndv;
        } else {
            return greatCount;
        }
    }
}
