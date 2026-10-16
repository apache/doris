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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.statistics.repository.HistData;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    public final Type dataType;

    public final double sampleRate;

    public final List<Bucket> buckets;

    public final int numBuckets;

    // Optional mcv_histogram: top-N hot values + residual buckets (non-overlapping).
    public final Map<Literal, Float> mcv;
    public final List<Bucket> mcvBuckets;

    public Histogram(Type dataType, double sampleRate, int numBuckets, List<Bucket> buckets) {
        this(dataType, sampleRate, numBuckets, buckets, null, null);
    }

    public Histogram(Type dataType, double sampleRate, int numBuckets, List<Bucket> buckets,
            Map<Literal, Float> mcv, List<Bucket> mcvBuckets) {
        this.dataType = dataType;
        this.sampleRate = sampleRate;
        this.numBuckets = numBuckets;
        this.buckets = buckets;
        this.mcv = mcv == null ? Collections.emptyMap() : mcv;
        this.mcvBuckets = mcvBuckets == null ? Collections.emptyList() : mcvBuckets;
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

            if (jsonObj.has("mcv_histogram") && jsonObj.get("mcv_histogram").isJsonObject()) {
                JsonObject mcvObj = jsonObj.getAsJsonObject("mcv_histogram");
                Map<Literal, Float> mcv = StatisticsUtil.getHotValues(mcvObj.get("mcv").getAsString(), dataType);
                List<Bucket> mcvBuckets = Lists.newArrayList();
                for (JsonElement element : mcvObj.getAsJsonArray("buckets")) {
                    mcvBuckets.add(Bucket.deserializeFromJson(dataType, element.toString()));
                }
                if (mcv != null && !mcv.isEmpty()) {
                    histogramBuilder.setMcv(mcv).setMcvBuckets(mcvBuckets);
                }
            }

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

    /** Build a histogram from column hot values when stored histogram has no MCV section. */
    public static Histogram fromHotValues(ColumnStatistic colStats) {
        Map<Literal, Float> hotValues = StatisticsUtil.getHotValuesWithOriginalThreshold(colStats.hotValues,
                Math.max(1, colStats.ndv));
        if (hotValues == null) {
            return null;
        }
        if (colStats.histogram != null && !colStats.histogram.hasMcv()) {
            Histogram withoutHotValues = colStats.histogram.removeValues(hotValues.keySet());
            return new Histogram(colStats.histogram.dataType, 0, 0, Collections.emptyList(), hotValues,
                    withoutHotValues == null ? Collections.emptyList() : withoutHotValues.buckets);
        }
        double hotRatio = hotValues.values().stream().mapToDouble(r -> r).sum();
        List<Bucket> buckets = hotRatio >= 1 ? Collections.emptyList() : Lists.newArrayList(new Bucket(
                colStats.minValue, colStats.maxValue, 1 - hotRatio, 0, Math.max(1, colStats.ndv - hotValues.size())));
        Type dataType = colStats.minExpr != null ? colStats.minExpr.getType() : Type.NULL;
        return new Histogram(dataType, 0, 0, Collections.emptyList(), hotValues, buckets);
    }

    public boolean hasMcv() {
        return !mcv.isEmpty();
    }

    /** True if a multi-ndv bucket collapses to equal bounds (e.g. ints beyond 2^53). */
    public boolean hasCollapsedBuckets() {
        for (Bucket bucket : Iterables.concat(buckets, mcvBuckets)) {
            if (bucket.ndv > 1 && bucket.upper <= bucket.lower) {
                return true;
            }
        }
        return false;
    }

    /** Restrict histogram to [lower, upper]; open interval when {@code inclusive} is false. */
    public Histogram intersectRange(double lower, double upper, boolean inclusive) {
        DataType type = getDataType();
        if (!inclusive) {
            lower = nextValueAbove(lower, type);
            upper = nextValueBelow(upper, type);
        }
        return rebuild(intersectBuckets(buckets, lower, upper, type), mcvInRange(lower, upper),
                intersectBuckets(mcvBuckets, lower, upper, type));
    }

    private static double nextValueAbove(double value, DataType type) {
        return type.isIntegralType() ? value + 1 : Math.nextUp(value);
    }

    private static double nextValueBelow(double value, DataType type) {
        return type.isIntegralType() ? value - 1 : Math.nextDown(value);
    }

    private Map<Literal, Float> mcvInRange(double lower, double upper) {
        Map<Literal, Float> result = Maps.newLinkedHashMap();
        for (Map.Entry<Literal, Float> entry : mcv.entrySet()) {
            double value = entry.getKey().getDouble();
            if (value >= lower && value <= upper) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private static List<Bucket> intersectBuckets(List<Bucket> source, double lower, double upper, DataType type) {
        List<Bucket> result = Lists.newArrayList();
        for (Bucket bucket : source) {
            double newLower = Math.max(bucket.lower, lower);
            double newUpper = Math.min(bucket.upper, upper);
            if (newLower > newUpper) {
                continue;
            }
            double fraction = bucket.coveredFraction(newLower, newUpper, type);
            result.add(new Bucket(newLower, newUpper, bucket.count * fraction, 0, bucket.ndv * fraction));
        }
        return result;
    }

    /** Drop the given values from MCV/buckets. Null if nothing remains. */
    public Histogram removeValues(Collection<Literal> values) {
        Map<Literal, Float> newMcv = Maps.newLinkedHashMap(mcv);
        List<Bucket> newBuckets = copyBuckets(buckets);
        List<Bucket> newMcvBuckets = copyBuckets(mcvBuckets);
        for (Literal value : values) {
            removeValue(newBuckets, value.getDouble());
            Literal key = StatisticsUtil.findHotValueKey(newMcv, value);
            if (key != null) {
                newMcv.remove(key);
            } else {
                removeValue(newMcvBuckets, value.getDouble());
            }
        }
        return rebuild(newBuckets, newMcv, newMcvBuckets);
    }

    private static List<Bucket> copyBuckets(List<Bucket> source) {
        List<Bucket> result = Lists.newArrayList();
        for (Bucket bucket : source) {
            result.add(new Bucket(bucket.lower, bucket.upper, bucket.count, 0, bucket.ndv));
        }
        return result;
    }

    private static void removeValue(List<Bucket> buckets, double value) {
        for (int i = 0; i < buckets.size(); i++) {
            Bucket bucket = buckets.get(i);
            if (value < bucket.lower || value > bucket.upper) {
                continue;
            }
            if (bucket.lower == bucket.upper || bucket.ndv <= 1) {
                buckets.remove(i);
            } else {
                bucket.count -= bucket.count / bucket.ndv;
                bucket.ndv -= 1;
            }
            return;
        }
    }

    /** Keep only the given values as MCV. Null if none match. */
    public Histogram retainValues(Collection<Literal> values) {
        Map<Literal, Float> newMcv = Maps.newLinkedHashMap();
        for (Literal value : values) {
            double selectivity = getValueSelectivity(value);
            if (selectivity > 0) {
                Literal key = StatisticsUtil.findHotValueKey(mcv, value);
                newMcv.put(key != null ? key : value, (float) selectivity);
            }
        }
        return rebuild(Collections.emptyList(), newMcv, Collections.emptyList());
    }

    /** Row-share of values in [lower, upper] (open when {@code inclusive} is false). */
    public double getRangeSelectivity(double lower, double upper, boolean inclusive) {
        DataType type = getDataType();
        if (!inclusive) {
            lower = nextValueAbove(lower, type);
            upper = nextValueBelow(upper, type);
        }
        double selectivity = mcvInRange(lower, upper).values().stream().mapToDouble(r -> r).sum();
        double otherValuesCount = sumCount(getOtherValueBuckets());
        if (otherValuesCount > 0) {
            double keptCount = sumCount(intersectBuckets(getOtherValueBuckets(), lower, upper, type));
            selectivity += (1 - getHotRatio()) * keptCount / otherValuesCount;
        }
        return selectivity;
    }

    public double getValuesSelectivity(Collection<Literal> values) {
        double selectivity = 0;
        for (Literal value : values) {
            selectivity += getValueSelectivity(value);
        }
        return selectivity;
    }

    private double getValueSelectivity(Literal value) {
        Literal key = StatisticsUtil.findHotValueKey(mcv, value);
        if (key != null) {
            return mcv.get(key);
        }
        double otherValuesCount = sumCount(getOtherValueBuckets());
        if (otherValuesCount <= 0) {
            return 0;
        }
        double doubleValue = value.getDouble();
        for (Bucket bucket : getOtherValueBuckets()) {
            if (doubleValue >= bucket.lower && doubleValue <= bucket.upper) {
                return (1 - getHotRatio()) * bucket.count / otherValuesCount / Math.max(1, bucket.ndv);
            }
        }
        return 0;
    }

    public double getNdv() {
        return getOtherValueBuckets().stream().mapToDouble(b -> b.ndv).sum() + mcv.size();
    }

    public DataType getDataType() {
        return dataType == null || dataType.isNull() ? NullType.INSTANCE : DataType.fromCatalogType(dataType);
    }

    private double getHotRatio() {
        return mcv.values().stream().mapToDouble(r -> r).sum();
    }

    private static double sumCount(List<Bucket> buckets) {
        return buckets.stream().mapToDouble(b -> b.count).sum();
    }

    private List<Bucket> getOtherValueBuckets() {
        return hasMcv() ? mcvBuckets : buckets;
    }

    // Rebuild with renormalized MCV ratios; null if empty.
    private Histogram rebuild(List<Bucket> newBuckets, Map<Literal, Float> newMcv, List<Bucket> newMcvBuckets) {
        if (newMcv.isEmpty()) {
            List<Bucket> remaining = newBuckets.isEmpty() ? newMcvBuckets : newBuckets;
            return remaining.isEmpty() ? null : new Histogram(dataType, sampleRate, remaining.size(), remaining);
        }
        double otherValuesCount = sumCount(getOtherValueBuckets());
        double keptCount = sumCount(hasMcv() ? newMcvBuckets : newBuckets);
        double keptRatio = newMcv.values().stream().mapToDouble(r -> r).sum()
                + (otherValuesCount > 0 ? (1 - getHotRatio()) * keptCount / otherValuesCount : 0);
        if (keptRatio > 0) {
            newMcv.replaceAll((value, ratio) -> (float) (ratio / keptRatio));
        }
        return new Histogram(dataType, sampleRate, newBuckets.size(), newBuckets, newMcv, newMcvBuckets);
    }

    @Override
    public String toString() {
        return serializeToJson(this);
    }
}
