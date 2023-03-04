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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    public final Type dataType;

    public final double sampleRate;

    public final int numBuckets;

    public final List<Bucket> buckets;

    public Histogram(Type dataType, int numBuckets, double sampleRate, List<Bucket> buckets) {
        this.dataType = dataType;
        this.numBuckets = numBuckets;
        this.sampleRate = sampleRate;
        this.buckets = buckets;
    }

    public static Histogram DEFAULT = new HistogramBuilder().setDataType(Type.INVALID).setNumBuckets(0)
            .setSampleRate(1.0).setBuckets(Lists.newArrayList()).build();

    // TODO: use thrift
    public static Histogram fromResultRow(ResultRow resultRow) {
        try {
            HistogramBuilder histogramBuilder = new HistogramBuilder();

            long catalogId = Long.parseLong(resultRow.getColumnValue("catalog_id"));
            long idxId = Long.parseLong(resultRow.getColumnValue("idx_id"));
            long dbId = Long.parseLong(resultRow.getColumnValue("db_id"));
            long tblId = Long.parseLong(resultRow.getColumnValue("tbl_id"));

            String colName = resultRow.getColumnValue("col_id");
            Column col = StatisticsUtil.findColumn(catalogId, dbId, tblId, idxId, colName);
            if (col == null) {
                LOG.warn("Failed to deserialize histogram statistics, ctlId: {} dbId: {}"
                        + "tblId: {} column: {} not exists", catalogId, dbId, tblId, colName);
                return Histogram.DEFAULT;
            }

            Type dataType = col.getType();
            histogramBuilder.setDataType(dataType);

            double sampleRate = Double.parseDouble(resultRow.getColumnValue("sample_rate"));
            histogramBuilder.setSampleRate(sampleRate);

            String json = resultRow.getColumnValue("buckets");
            JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();

            int bucketNum = jsonObj.get("num_buckets").getAsInt();
            histogramBuilder.setNumBuckets(bucketNum);

            List<Bucket> buckets = Lists.newArrayList();
            JsonArray jsonArray = jsonObj.getAsJsonArray("buckets");
            for (JsonElement element : jsonArray) {
                String bucketJson = element.toString();
                buckets.add(Bucket.deserializeFromJson(dataType, bucketJson));
            }
            histogramBuilder.setBuckets(buckets);

            return histogramBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to deserialize histogram statistics.", e);
            return Histogram.DEFAULT;
        }
    }

    /**
     * Histogram info is stored in an internal table in json format,
     * and Histogram obj can be obtained by this method.
     */
    public static Histogram deserializeFromJson(String json) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }

        try {
            HistogramBuilder histogramBuilder = new HistogramBuilder();

            JsonObject histogramJson = JsonParser.parseString(json).getAsJsonObject();
            String typeStr = histogramJson.get("data_type").getAsString();
            Type dataType = Type.fromPrimitiveType(PrimitiveType.valueOf(typeStr));
            histogramBuilder.setDataType(dataType);

            float sampleRate = histogramJson.get("sample_rate").getAsFloat();
            histogramBuilder.setSampleRate(sampleRate);

            int bucketSize = histogramJson.get("num_buckets").getAsInt();
            histogramBuilder.setNumBuckets(bucketSize);

            JsonArray jsonArray = histogramJson.getAsJsonArray("buckets");
            List<Bucket> buckets = Lists.newArrayList();

            for (JsonElement element : jsonArray) {
                String bucketJsonStr = element.toString();
                buckets.add(Bucket.deserializeFromJson(dataType, bucketJsonStr));
            }
            histogramBuilder.setBuckets(buckets);

            return histogramBuilder.build();
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

        JsonObject histogramJson = new JsonObject();

        histogramJson.addProperty("data_type", histogram.dataType.toString());
        histogramJson.addProperty("sample_rate", histogram.sampleRate);
        histogramJson.addProperty("num_buckets", histogram.numBuckets);

        JsonArray bucketsJsonArray = new JsonArray();
        histogram.buckets.stream().map(Bucket::serializeToJsonObj).forEach(bucketsJsonArray::add);
        histogramJson.add("buckets", bucketsJsonArray);

        return histogramJson.toString();
    }

    /**
     * Given a value, return the bucket to which it belongs,
     * return null if not found.
     */
    public Bucket findBucket(LiteralExpr key) {
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }

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
    private int rangeCountIgnoreSampleRate(LiteralExpr lower, boolean isIncludeLower, LiteralExpr upper,
            boolean isIncludeUpper) throws AnalysisException {
        if (buckets == null || buckets.isEmpty()) {
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
        if (buckets == null || buckets.isEmpty()) {
            return 0;
        }
        Bucket lastBucket = buckets.get(buckets.size() - 1);
        return lastBucket.preSum + lastBucket.count;
    }

    private int lessCount(LiteralExpr key) throws AnalysisException {
        Bucket bucket = findBucket(key);
        if (bucket == null) {
            if (buckets == null || buckets.isEmpty()) {
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
