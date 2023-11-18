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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    public final Type dataType;

    public final double sampleRate;

    public final List<Bucket> buckets;

    public final int numBuckets;

    public Histogram(Type dataType, double sampleRate, int numBuckets, List<Bucket> buckets) {
        this.dataType = dataType;
        this.sampleRate = sampleRate;
        this.numBuckets = numBuckets;
        this.buckets = buckets;
    }

    public static Histogram UNKNOWN = new HistogramBuilder().setDataType(Type.NULL)
            .setSampleRate(0).setNumBuckets(0).setBuckets(Collections.emptyList())
            .build();

    // TODO: use thrift
    public static Histogram fromResultRow(ResultRow resultRow) {
        try {
            HistogramBuilder histogramBuilder = new HistogramBuilder();
            HistData histData = new HistData(resultRow);
            long catalogId = histData.statsId.catalogId;
            long idxId = histData.statsId.idxId;
            long dbId = histData.statsId.dbId;
            long tblId = histData.statsId.tblId;
            String colName = histData.statsId.colId;
            Column col = StatisticsUtil.findColumn(catalogId, dbId, tblId, idxId, colName);
            if (col == null) {
                LOG.warn("Failed to deserialize histogram statistics, ctlId: {} dbId: {}"
                        + "tblId: {} column: {} not exists", catalogId, dbId, tblId, colName);
                return null;
            }

            Type dataType = col.getType();
            histogramBuilder.setDataType(dataType);

            double sampleRate = histData.sampleRate;
            histogramBuilder.setSampleRate(sampleRate);

            String json = histData.buckets;
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
            return null;
        }
    }

    /**
     * Histogram info is stored in an internal table in json format,
     * and Histogram obj can be obtained by this method.
     */
    public static Histogram deserializeFromJson(String json) {
        if (Strings.isNullOrEmpty(json)) {
            return Histogram.UNKNOWN;
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

        return Histogram.UNKNOWN;
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
        histogramJson.addProperty("num_buckets", histogram.buckets.size());

        JsonArray bucketsJson = getBucketsJson(histogram.buckets);
        histogramJson.add("buckets", bucketsJson);

        return histogramJson.toString();
    }

    public static JsonArray getBucketsJson(List<Bucket> buckets) {
        if (buckets == null) {
            return null;
        }
        JsonArray bucketsJsonArray = new JsonArray();
        buckets.stream().map(Bucket::serializeToJsonObj).forEach(bucketsJsonArray::add);
        return bucketsJsonArray;
    }

    public double size() {
        if (CollectionUtils.isEmpty(buckets)) {
            return 0;
        }
        Bucket lastBucket = buckets.get(buckets.size() - 1);
        return lastBucket.preSum + lastBucket.count;
    }

    @Override
    public String toString() {
        return serializeToJson(this);
    }
}
