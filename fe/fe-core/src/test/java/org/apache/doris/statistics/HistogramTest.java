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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

class HistogramTest {
    private final Type datatype = Type.fromPrimitiveType(PrimitiveType.DATETIME);
    private Histogram histogramUnderTest;

    @BeforeEach
    void setUp() throws Exception {
        String json = "{\"max_bucket_size\":128,\"bucket_size\":5,\"sample_rate\":1.0,\"buckets\":"
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
        histogramUnderTest = Histogram.deserializeFromJson(datatype, json);
        if (histogramUnderTest == null) {
            Assertions.fail();
        }
    }

    @Test
    void testFindBucket() throws Exception {
        // Setup
        LiteralExpr key1 = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr key2 = LiteralExpr.create("2022-09-23 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        // Run the test
        Bucket bucket1 = histogramUnderTest.findBucket(key1);
        Bucket bucket2 = histogramUnderTest.findBucket(key2);

        // Verify the results
        Assertions.assertEquals(1, bucket1.getNdv());
        Assertions.assertEquals(1, bucket2.getNdv());
        Assertions.assertEquals(9, bucket1.getCount());
        Assertions.assertEquals(9, bucket2.getCount());
        Assertions.assertEquals(0, bucket1.getPreSum());
        Assertions.assertEquals(19, bucket2.getPreSum());

        LiteralExpr lower1 = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr lower2 = LiteralExpr.create("2022-09-23 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper1 = LiteralExpr.create("2022-09-21 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper2 = LiteralExpr.create("2022-09-23 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        Assertions.assertEquals(lower1, bucket1.getLower());
        Assertions.assertEquals(lower2, bucket2.getLower());
        Assertions.assertEquals(upper1, bucket1.getUpper());
        Assertions.assertEquals(upper2, bucket2.getUpper());
    }

    @Test
    void testRangeCount() throws Exception {
        // Setup
        LiteralExpr lower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper = LiteralExpr.create("2022-09-23 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        // Run the test
        long count1 = histogramUnderTest.rangeCount(lower, true, upper, true);
        long count2 = histogramUnderTest.rangeCount(lower, true, upper, false);
        long count3 = histogramUnderTest.rangeCount(lower, false, upper, false);
        long count4 = histogramUnderTest.rangeCount(lower, false, upper, true);
        long count5 = histogramUnderTest.rangeCount(null, true, upper, true);
        long count6 = histogramUnderTest.rangeCount(lower, true, null, true);
        long count7 = histogramUnderTest.rangeCount(null, true, null, true);

        // Verify the results
        Assertions.assertEquals(28L, count1);
        Assertions.assertEquals(19L, count2);
        Assertions.assertEquals(10L, count3);
        Assertions.assertEquals(19L, count4);
        Assertions.assertEquals(28L, count5);
        Assertions.assertEquals(46L, count6);
        Assertions.assertEquals(46L, count7);
    }

    @Test
    void testDeserializeFromJson() throws Exception {
        Type dataType = histogramUnderTest.getDataType();
        Assertions.assertTrue(dataType.isDatetime());

        int maxBucketSize = histogramUnderTest.getMaxBucketSize();
        Assertions.assertEquals(128, maxBucketSize);

        int bucketSize = histogramUnderTest.getBucketSize();
        Assertions.assertEquals(5, bucketSize);

        float sampleRate = histogramUnderTest.getSampleRate();
        Assertions.assertEquals(1.0, sampleRate);

        List<Bucket> buckets = histogramUnderTest.getBuckets();
        Assertions.assertEquals(5, buckets.size());

        LiteralExpr lower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper = LiteralExpr.create("2022-09-25 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        long count = histogramUnderTest.rangeCount(lower, false, upper, false);
        Assertions.assertEquals(28, count);
    }

    @Test
    void testSerializeToJson() throws AnalysisException {
        String json = Histogram.serializeToJson(histogramUnderTest);
        JSONObject histogramJson = JSON.parseObject(json);

        int maxBucketSize = histogramJson.getIntValue("max_bucket_size");
        Assertions.assertEquals(128, maxBucketSize);

        int bucketSize = histogramJson.getIntValue("bucket_size");
        Assertions.assertEquals(5, bucketSize);

        float sampleRate = histogramJson.getFloat("sample_rate");
        Assertions.assertEquals(1.0, sampleRate);

        JSONArray jsonArray = histogramJson.getJSONArray("buckets");
        Assertions.assertEquals(5, jsonArray.size());

        // test first bucket
        LiteralExpr expectedLower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr expectedUpper = LiteralExpr.create("2022-09-21 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        boolean flag = false;

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject bucketJson = jsonArray.getJSONObject(i);
            assert datatype != null;
            LiteralExpr lower = StatisticsUtil.readableValue(datatype, bucketJson.get("lower").toString());
            LiteralExpr upper = StatisticsUtil.readableValue(datatype, bucketJson.get("upper").toString());
            int count = bucketJson.getIntValue("count");
            int preSum = bucketJson.getIntValue("pre_sum");
            int ndv = bucketJson.getIntValue("ndv");
            if (expectedLower.equals(lower) && expectedUpper.equals(upper) && count == 9 && preSum == 0 && ndv == 1) {
                flag = true;
                break;
            }
        }

        Assertions.assertTrue(flag);
    }
}
