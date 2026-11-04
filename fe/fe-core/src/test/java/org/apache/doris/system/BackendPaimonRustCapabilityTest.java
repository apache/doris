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

package org.apache.doris.system;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TBackendInfo;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class BackendPaimonRustCapabilityTest {
    @Test
    public void testCapabilitySurvivesHeartbeatReplayAndBackendImage() {
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        Assert.assertFalse(backend.isPaimonRustReaderSupported());
        BackendHbResponse response = heartbeat();
        response.setPaimonRustReaderSupported(true);
        BackendHbResponse replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(response), BackendHbResponse.class);
        Assert.assertTrue(replay.isPaimonRustReaderSupported());
        Assert.assertTrue(backend.handleHbResponse(replay, true));
        Assert.assertTrue(backend.isPaimonRustReaderSupported());
        Backend restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(backend), Backend.class);
        Assert.assertTrue(restored.isPaimonRustReaderSupported());
    }

    @Test
    public void testLegacyHeartbeatClearsPreviouslyAdvertisedSupport() {
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        BackendHbResponse response = heartbeat();
        response.setPaimonRustReaderSupported(true);
        backend.handleHbResponse(response, false);
        Assert.assertTrue(backend.isPaimonRustReaderSupported());

        TBackendInfo legacy = new TBackendInfo();
        Assert.assertFalse(legacy.isSetSupportsPaimonRustReader());
        Assert.assertFalse(legacy.isSupportsPaimonRustReader());
        BackendHbResponse legacyResponse = heartbeat();
        Assert.assertFalse(legacyResponse.isPaimonRustReaderSupported());
        Assert.assertTrue(backend.handleHbResponse(legacyResponse, false));
        Assert.assertFalse(backend.isPaimonRustReaderSupported());

        JsonObject legacyJson = GsonUtils.GSON.toJsonTree(heartbeat()).getAsJsonObject();
        legacyJson.remove("supportsPaimonRustReader");
        BackendHbResponse replay = GsonUtils.GSON.fromJson(legacyJson, BackendHbResponse.class);
        Assert.assertFalse(replay.isPaimonRustReaderSupported());
        Assert.assertFalse(GsonUtils.GSON.fromJson("{}", Backend.class).isPaimonRustReaderSupported());
    }

    private BackendHbResponse heartbeat() {
        return new BackendHbResponse(1, 9060, 8040, 8060, 1000, 1000, "test-version", Tag.VALUE_MIX,
                0, 0, false, 8070);
    }
}
