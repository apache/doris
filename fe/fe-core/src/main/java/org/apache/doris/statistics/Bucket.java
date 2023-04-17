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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Bucket {
    public double lower;
    public double upper;
    public double count;
    public double preSum;
    public double ndv;

    // For display only.
    public LiteralExpr lowerExpr;
    public LiteralExpr upperExpr;

    public Bucket() {
    }

    public Bucket(double lower, double upper, double count, double preSum, double ndv) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.preSum = preSum;
        this.ndv = ndv;
    }

    public Bucket(double lower, double upper, double count, double preSum, double ndv,
            LiteralExpr lowerExpr, LiteralExpr upperExpr) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.preSum = preSum;
        this.ndv = ndv;
        this.lowerExpr = lowerExpr;
        this.upperExpr = upperExpr;
    }

    public static Bucket deserializeFromJson(Type datatype, String json) throws AnalysisException {
        Bucket bucket = new Bucket();
        JsonObject bucketJson = JsonParser.parseString(json).getAsJsonObject();
        bucket.lower = StatisticsUtil.convertToDouble(datatype, bucketJson.get("lower").getAsString());
        bucket.upper = StatisticsUtil.convertToDouble(datatype, bucketJson.get("upper").getAsString());
        bucket.count = bucketJson.get("count").getAsInt();
        bucket.preSum = bucketJson.get("pre_sum").getAsInt();
        bucket.ndv = bucketJson.get("ndv").getAsInt();

        // LowerExpr and upperExpr for display only.
        bucket.lowerExpr = StatisticsUtil.readableValue(datatype, bucketJson.get("lower").getAsString());
        bucket.upperExpr = StatisticsUtil.readableValue(datatype, bucketJson.get("upper").getAsString());

        return bucket;
    }

    public static JsonObject serializeToJsonObj(Bucket bucket) {
        if (bucket == null) {
            return null;
        }

        JsonObject bucketJson = new JsonObject();
        bucketJson.addProperty("lower_expr", bucket.lowerExpr.getStringValue());
        bucketJson.addProperty("upper_expr", bucket.upperExpr.getStringValue());
        bucketJson.addProperty("count", bucket.count);
        bucketJson.addProperty("pre_sum", bucket.preSum);
        bucketJson.addProperty("ndv", bucket.ndv);

        return bucketJson;
    }

    public static String serializeToJson(Bucket bucket) {
        return bucket == null ? "" : serializeToJsonObj(bucket).toString();
    }
}
