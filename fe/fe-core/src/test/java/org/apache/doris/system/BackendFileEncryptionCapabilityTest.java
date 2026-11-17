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

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

class BackendFileEncryptionCapabilityTest {
    @Test
    void freshCapabilityIsRuntimeStateAndDoesNotSurviveAnImage() {
        Backend backend = backend();
        Assertions.assertEquals(0, backend.getNativeFileEncryptionVersion());
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), 1), false);
        Assertions.assertEquals(1, backend.getNativeFileEncryptionVersion());
        Backend restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(backend), Backend.class);
        Assertions.assertEquals(0, restored.getNativeFileEncryptionVersion());
    }

    @Test
    void absentLegacyCapabilityClearsPreviouslyVerifiedSupport() {
        Backend backend = backend();
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), 1), false);
        Assertions.assertEquals(1, backend.getNativeFileEncryptionVersion());
        JsonObject legacy = GsonUtils.GSON.toJsonTree(heartbeat(System.currentTimeMillis(), 1)).getAsJsonObject();
        legacy.remove("nativeFileEncryptionVersion");
        Assertions.assertTrue(backend.handleHbResponse(
                GsonUtils.GSON.fromJson(legacy, BackendHbResponse.class), false));
        Assertions.assertEquals(0, backend.getNativeFileEncryptionVersion());
        Assertions.assertTrue(backend.isAlive());
    }

    @Test
    void failedHeartbeatClearsCapabilityBeforeTheLivenessGraceExpires() {
        Backend backend = backend();
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), 1), false);
        Assertions.assertEquals(1, backend.getNativeFileEncryptionVersion());
        Assertions.assertTrue(backend.handleHbResponse(
                new BackendHbResponse(1, "127.0.0.1", backend.getLastUpdateMs(), "fixture failure"), false));
        Assertions.assertEquals(0, backend.getNativeFileEncryptionVersion());
    }

    @Test
    void historicalReplayCannotEnableExecutionButFreshBroadcastCan() {
        Backend backend = backend();
        long beforeProcessStart = ManagementFactory.getRuntimeMXBean().getStartTime() - 1;
        backend.handleHbResponse(heartbeat(beforeProcessStart, 1), true);
        Assertions.assertEquals(0, backend.getNativeFileEncryptionVersion());
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), 1), true);
        Assertions.assertEquals(1, backend.getNativeFileEncryptionVersion());
    }

    @Test
    void staleOrFutureHeartbeatsCannotEnableExecution() {
        for (long offset : new long[] {-TimeUnit.DAYS.toMillis(1), TimeUnit.DAYS.toMillis(1)}) {
            Backend backend = backend();
            backend.handleHbResponse(heartbeat(System.currentTimeMillis() + offset, 1), false);
            Assertions.assertEquals(0, backend.getNativeFileEncryptionVersion());
        }
    }

    private static Backend backend() {
        return new Backend(1, "127.0.0.1", 9050);
    }

    private static BackendHbResponse heartbeat(long time, int version) {
        BackendHbResponse response = new BackendHbResponse(1, 9060, 8040, 8060, time, time,
                "fixture-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        JsonObject json = GsonUtils.GSON.toJsonTree(response).getAsJsonObject();
        json.addProperty("nativeFileEncryptionVersion", version);
        return GsonUtils.GSON.fromJson(json, BackendHbResponse.class);
    }
}
