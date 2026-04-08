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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for MTMVTaskContext Gson serialization/deserialization compatibility
 * across versions (upgrade and downgrade scenarios).
 */
public class MTMVTaskContextCompatTest {

    private static final Gson GSON = new Gson();

    // Forward compat: INCREMENTAL mode serialized → old version reads isComplete=false
    @Test
    public void testForwardCompatIncrementalCarriesIsCompleteFalse() {
        MTMVTaskContext ctx = new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL);
        String json = GSON.toJson(ctx);
        JsonObject obj = GSON.fromJson(json, JsonObject.class);
        Assert.assertFalse("Old version should see isComplete=false for INCREMENTAL",
                obj.get("isComplete").getAsBoolean());
        Assert.assertEquals("refreshMode field should be present",
                "INCREMENTAL", obj.get("refreshMode").getAsString());
    }

    // Forward compat: COMPLETE mode serialized → old version reads isComplete=true
    @Test
    public void testForwardCompatCompleteCarriesIsCompleteTrue() {
        MTMVTaskContext ctx = new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.COMPLETE);
        String json = GSON.toJson(ctx);
        JsonObject obj = GSON.fromJson(json, JsonObject.class);
        Assert.assertTrue("Old version should see isComplete=true for COMPLETE",
                obj.get("isComplete").getAsBoolean());
    }

    // Forward compat: PARTITIONS mode serialized → old version reads isComplete=false
    @Test
    public void testForwardCompatPartitionsCarriesIsCompleteFalse() {
        MTMVTaskContext ctx = new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS);
        String json = GSON.toJson(ctx);
        JsonObject obj = GSON.fromJson(json, JsonObject.class);
        Assert.assertFalse("Old version should see isComplete=false for PARTITIONS",
                obj.get("isComplete").getAsBoolean());
    }

    // Backward compat: old task JSON (no refreshMode) with isComplete=true → new code reads COMPLETE
    @Test
    public void testBackwardCompatOldTaskIsCompleteTrue() {
        String oldJson = "{\"triggerMode\":\"MANUAL\",\"isComplete\":true}";
        MTMVTaskContext ctx = GSON.fromJson(oldJson, MTMVTaskContext.class);
        Assert.assertTrue(ctx.isComplete());
        Assert.assertEquals(RefreshMode.COMPLETE, ctx.getRefreshMode());
    }

    // Backward compat: old task JSON (no refreshMode) with isComplete=false → new code reads AUTO
    @Test
    public void testBackwardCompatOldTaskIsCompleteFalse() {
        String oldJson = "{\"triggerMode\":\"MANUAL\",\"isComplete\":false}";
        MTMVTaskContext ctx = GSON.fromJson(oldJson, MTMVTaskContext.class);
        Assert.assertFalse(ctx.isComplete());
        Assert.assertEquals(RefreshMode.AUTO, ctx.getRefreshMode());
    }

    // Round-trip: serialize new → deserialize new preserves RefreshMode
    @Test
    public void testRoundTripPreservesRefreshMode() {
        for (RefreshMode mode : RefreshMode.values()) {
            MTMVTaskContext original = new MTMVTaskContext(
                    MTMVTaskTriggerMode.MANUAL, null, mode);
            String json = GSON.toJson(original);
            MTMVTaskContext restored = GSON.fromJson(json, MTMVTaskContext.class);
            Assert.assertEquals("Round-trip should preserve RefreshMode " + mode,
                    mode, restored.getRefreshMode());
        }
    }
}
