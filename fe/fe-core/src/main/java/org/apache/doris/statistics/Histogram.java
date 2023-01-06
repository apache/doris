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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    public final Type dataType;

    public final int maxBucketNum;

    public final int bucketNum;

    public final double sampleRate;

    public final List<Bucket> buckets;

    public Histogram(Type dataType, int maxBucketNum, int bucketNum,
                     double sampleRate, List<Bucket> buckets) {
        this.dataType = dataType;
        this.maxBucketNum = maxBucketNum;
        this.bucketNum = bucketNum;
        this.sampleRate = sampleRate;
        this.buckets = buckets;
    }

    public static Histogram DEFAULT = new HistogramBuilder()
            .setDataType(Type.INVALID).setMaxBucketNum(1)
            .setBucketNum(0).setSampleRate(1.0).setBuckets(Lists.newArrayList()).build();

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
                                + "tblId: {} column: {} not exists",
                        catalogId, dbId, tblId, colName);
                return Histogram.DEFAULT;
            }

            double sampleRate = Double.parseDouble(resultRow.getColumnValue("sample_rate"));
            histogramBuilder.setSampleRate(sampleRate);
            histogramBuilder.setDataType(col.getType());

            String json = resultRow.getColumnValue("buckets");
            JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();

            int maxBucketNum = jsonObj.get("max_bucket_num").getAsInt();
            histogramBuilder.setMaxBucketNum(maxBucketNum);

            int bucketNum = jsonObj.get("bucket_num").getAsInt();
            histogramBuilder.setBucketNum(bucketNum);

            JsonArray jsonArray = jsonObj.getAsJsonArray("buckets");
            List<Bucket> buckets = Bucket.deserializeFromjson(col.getType(), jsonArray);
            histogramBuilder.setBuckets(buckets);

            return histogramBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Failed to deserialize histogram statistics.", e);
            return Histogram.DEFAULT;
        }
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
            HistogramBuilder histogramBuilder = new HistogramBuilder();

            histogramBuilder.setDataType(datatype);

            JsonObject histogramJson = JsonParser.parseString(json).getAsJsonObject();
            JsonArray jsonArray = histogramJson.getAsJsonArray("buckets");
            List<Bucket> buckets = Bucket.deserializeFromjson(datatype, jsonArray);

            histogramBuilder.setBuckets(buckets);

            int maxBucketSize = histogramJson.get("max_bucket_num").getAsInt();
            histogramBuilder.setMaxBucketNum(maxBucketSize);

            int bucketSize = histogramJson.get("bucket_num").getAsInt();
            histogramBuilder.setBucketNum(bucketSize);

            float sampleRate = histogramJson.get("sample_rate").getAsFloat();
            histogramBuilder.setSampleRate(sampleRate);

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

        histogramJson.addProperty("max_bucket_num", histogram.maxBucketNum);
        histogramJson.addProperty("bucket_num", histogram.bucketNum);
        histogramJson.addProperty("sample_rate", histogram.sampleRate);

        JsonArray bucketsJsonArray = new JsonArray();
        histogramJson.add("buckets", bucketsJsonArray);

        for (Bucket bucket : histogram.buckets) {
            JsonObject bucketJson = new JsonObject();
            bucketJson.addProperty("count", bucket.count);
            bucketJson.addProperty("pre_sum", bucket.preSum);
            bucketJson.addProperty("ndv", bucket.ndv);
            bucketJson.addProperty("upper", bucket.upper.getStringValue());
            bucketJson.addProperty("lower", bucket.lower.getStringValue());
            bucketsJsonArray.add(bucketJson);
        }

        return histogramJson.toString();
    }
}
