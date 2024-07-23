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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

class HistogramTest {
    private Histogram histogramUnderTest;

    @BeforeEach
    void setUp() throws Exception {
        String json = "{\"data_type\":\"DATETIME\",\"sample_rate\":1.0,\"num_buckets\":5,\"buckets\":"
                + "[{\"lower\":\"2022-09-21 17:30:29\",\"upper\":\"2022-09-21 22:30:29\","
                + "\"count\":9,\"pre_sum\":0,\"ndv\":1},"
                + "{\"lower\":\"2022-09-22 17:30:29\",\"upper\":\"2022-09-22 22:30:29\","
                + "\"count\":10,\"pre_sum\":9,\"ndv\":1},"
                + "{\"lower\":\"2022-09-23 17:30:29\",\"upper\":\"2022-09-23 22:30:29\","
                + "\"count\":9,\"pre_sum\":19,\"ndv\":1},"
                + "{\"lower\":\"2022-09-24 17:30:29\",\"upper\":\"2022-09-24 22:30:29\","
                + "\"count\":9,\"pre_sum\":28,\"ndv\":1},"
                + "{\"lower\":\"2022-09-25 17:30:29\",\"upper\":\"2022-09-25 22:30:29\","
                + "\"count\":9,\"pre_sum\":37,\"ndv\":1}]}";
        histogramUnderTest = Histogram.deserializeFromJson(json);
        if (histogramUnderTest == null) {
            Assertions.fail();
        }
    }

    @Test
    void testDeserializeFromJson() throws Exception {
        Type dataType = histogramUnderTest.dataType;
        Assertions.assertTrue(dataType.isDatetime());

        int numBuckets = histogramUnderTest.buckets.size();
        Assertions.assertEquals(5, numBuckets);

        double sampleRate = histogramUnderTest.sampleRate;
        Assertions.assertEquals(1.0, sampleRate);

        List<Bucket> buckets = histogramUnderTest.buckets;
        Assertions.assertEquals(5, buckets.size());

        LiteralExpr expectedLower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr expectedUpper = LiteralExpr.create("2022-09-21 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        boolean flag = false;

        for (Bucket bucket : buckets) {
            LiteralExpr lower = bucket.lowerExpr;
            LiteralExpr upper = bucket.upperExpr;
            if (expectedLower.equals(lower) && expectedUpper.equals(upper)) {
                flag = true;
                break;
            }
        }

        Assertions.assertTrue(flag);
    }

    @Test
    void testSerializeToJson() throws AnalysisException {
        String json = Histogram.serializeToJson(histogramUnderTest);
        JsonObject histogramJson = JsonParser.parseString(json).getAsJsonObject();

        String typeStr = histogramJson.get("data_type").getAsString();
        Assertions.assertEquals("datetime", typeStr);
        Type datatype = Type.fromPrimitiveType(PrimitiveType.valueOf(typeStr.toUpperCase()));
        Assertions.assertNotNull(datatype);

        int numBuckets = histogramJson.get("num_buckets").getAsInt();
        Assertions.assertEquals(5, numBuckets);

        float sampleRate = histogramJson.get("sample_rate").getAsFloat();
        Assertions.assertEquals(1.0, sampleRate);

        JsonArray jsonArray = histogramJson.get("buckets").getAsJsonArray();
        Assertions.assertEquals(5, jsonArray.size());

        // test first bucket
        LiteralExpr expectedLower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr expectedUpper = LiteralExpr.create("2022-09-21 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        boolean flag = false;

        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject bucketJson = jsonArray.get(i).getAsJsonObject();
            LiteralExpr lower = StatisticsUtil.readableValue(datatype,
                    bucketJson.get("lower_expr").getAsString());
            LiteralExpr upper = StatisticsUtil.readableValue(datatype,
                    bucketJson.get("upper_expr").getAsString());
            int count = bucketJson.get("count").getAsInt();
            int preSum = bucketJson.get("pre_sum").getAsInt();
            int ndv = bucketJson.get("ndv").getAsInt();
            if (expectedLower.equals(lower) && expectedUpper.equals(upper)
                    && count == 9 && preSum == 0 && ndv == 1) {
                flag = true;
                break;
            }
        }

        Assertions.assertTrue(flag);
    }
}
