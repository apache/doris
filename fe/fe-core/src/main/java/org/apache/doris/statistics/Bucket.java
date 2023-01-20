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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;

public class Bucket {
    public LiteralExpr lower;
    public LiteralExpr upper;
    public int count;
    public int preSum;
    public int ndv;

    public LiteralExpr getLower() {
        return lower;
    }

    public void setLower(LiteralExpr lower) {
        this.lower = lower;
    }

    public LiteralExpr getUpper() {
        return upper;
    }

    public void setUpper(LiteralExpr upper) {
        this.upper = upper;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getPreSum() {
        return preSum;
    }

    public void setPreSum(int preSum) {
        this.preSum = preSum;
    }

    public int getNdv() {
        return ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    public static List<Bucket> deserializeFromjson(Type datatype, JsonArray jsonArray)
            throws AnalysisException {
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject bucketJson = jsonArray.get(i).getAsJsonObject();
            Bucket bucket = new Bucket();
            bucket.lower = StatisticsUtil.readableValue(datatype, bucketJson.get("lower").getAsString());
            bucket.upper = StatisticsUtil.readableValue(datatype, bucketJson.get("upper").getAsString());
            bucket.count = bucketJson.get("count").getAsInt();
            bucket.preSum = bucketJson.get("pre_sum").getAsInt();
            bucket.ndv = bucketJson.get("ndv").getAsInt();
            buckets.add(bucket);
        }
        return buckets;
    }
}
