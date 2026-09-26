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

package org.apache.doris.mtmv;

import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class MTMVPartitionInfoTest {

    @Test
    public void testUserSpecifiedExprDefaultValue() {
        // Test that userSpecifiedExpr defaults to true
        MTMVPartitionInfo info = new MTMVPartitionInfo(MTMVPartitionType.EXPR);
        Assert.assertTrue(info.isUserSpecifiedExpr());
    }

    @Test
    public void testUserSpecifiedExprSetterGetter() {
        MTMVPartitionInfo info = new MTMVPartitionInfo(MTMVPartitionType.EXPR);

        // Test setting to false
        info.setUserSpecifiedExpr(false);
        Assert.assertFalse(info.isUserSpecifiedExpr());

        // Test setting back to true
        info.setUserSpecifiedExpr(true);
        Assert.assertTrue(info.isUserSpecifiedExpr());
    }

    @Test
    public void testUserSpecifiedExprSerialization() {
        Gson gson = new Gson();

        // Test serialization with userSpecifiedExpr = false
        MTMVPartitionInfo info = new MTMVPartitionInfo(MTMVPartitionType.EXPR);
        info.setPartitionCol("month_dt");
        info.setUserSpecifiedExpr(false);

        String json = gson.toJson(info);
        Assert.assertTrue(json.contains("\"use\":false"));

        // Test deserialization
        MTMVPartitionInfo deserialized = gson.fromJson(json, MTMVPartitionInfo.class);
        Assert.assertEquals(MTMVPartitionType.EXPR, deserialized.getPartitionType());
        Assert.assertEquals("month_dt", deserialized.getPartitionCol());
        Assert.assertFalse(deserialized.isUserSpecifiedExpr());
    }

    @Test
    public void testUserSpecifiedExprBackwardCompatibility() {
        Gson gson = new Gson();

        // Simulate old persisted JSON without "use" field
        String oldJson = "{\"pt\":\"EXPR\",\"pc\":\"month_dt\",\"pi\":[]}";

        // Deserialize - should default to true for backward compatibility
        MTMVPartitionInfo deserialized = gson.fromJson(oldJson, MTMVPartitionInfo.class);
        Assert.assertEquals(MTMVPartitionType.EXPR, deserialized.getPartitionType());
        Assert.assertEquals("month_dt", deserialized.getPartitionCol());
        Assert.assertTrue(deserialized.isUserSpecifiedExpr());
    }
}
