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

import org.apache.doris.common.Config;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.HeartbeatResponse.HbStatus;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    @Test
    void serializedRevocationKeepsLeaderAndFollowerAliveUntilTheSameFailureThreshold() throws IOException {
        long previous = Config.max_backend_heartbeat_failure_tolerance_count;
        Config.max_backend_heartbeat_failure_tolerance_count = 3;
        try {
            Backend leader = backend();
            Backend follower = backend();
            long time = System.currentTimeMillis();
            BackendHbResponse success = new BackendHbResponse(1, 9060, 8040, 8060, time, time - 1000,
                    "fixture-version", Tag.VALUE_COMPUTATION, 7, time - 500, true, 8070, 16384);
            success.setNativeFileEncryptionVersion(1);
            success.setPaimonRustReaderSupported(true);
            leader.handleHbResponse(success, false);
            follower.handleHbResponse(roundTrip(success), true);
            Assertions.assertEquals(1, follower.getNativeFileEncryptionVersion());
            for (int failure = 1; failure <= 3; failure++) {
                BackendHbResponse response = new BackendHbResponse(1, "127.0.0.1",
                        leader.getLastUpdateMs(), "fixture failure");
                boolean changed = leader.handleHbResponse(response, false);
                Assertions.assertEquals(failure != 2, changed);
                if (changed) {
                    BackendHbResponse journal = roundTrip(leader.heartbeatForJournal(response));
                    follower.handleHbResponse(journal, true);
                    Assertions.assertEquals(leader.isAlive(), follower.isAlive(),
                            "Capability revocation must not bypass the leader's heartbeat failure grace on replay");
                    Assertions.assertEquals(failure < 3 ? HbStatus.OK : HbStatus.BAD, journal.getStatus());
                    Assertions.assertEquals(0, journal.getNativeFileEncryptionVersion());
                }
                Assertions.assertEquals(HbStatus.BAD, response.getStatus(),
                        "The leader still needs the original failure for connection eviction and diagnostics");
                Assertions.assertEquals(failure < 3, leader.isAlive());
                Assertions.assertEquals(leader.isAlive(), follower.isAlive());
                Assertions.assertEquals(0, leader.getNativeFileEncryptionVersion());
                Assertions.assertEquals(0, follower.getNativeFileEncryptionVersion());
                // Preserve the existing journal fields; unannotated version/load metrics are not serialized.
                Assertions.assertEquals(leader.getLastUpdateMs(), follower.getLastUpdateMs());
                Assertions.assertEquals(leader.getLastStartTime(), follower.getLastStartTime());
                Assertions.assertEquals(leader.getBePort(), follower.getBePort());
                Assertions.assertEquals(leader.getHttpPort(), follower.getHttpPort());
                Assertions.assertEquals(leader.getBrpcPort(), follower.getBrpcPort());
                Assertions.assertEquals(leader.getArrowFlightSqlPort(), follower.getArrowFlightSqlPort());
                Assertions.assertEquals(leader.getBackendStatus().isShutdown, follower.getBackendStatus().isShutdown);
                Assertions.assertEquals(leader.getNodeRoleTag(), follower.getNodeRoleTag());
                Assertions.assertTrue(follower.isPaimonRustReaderSupported());
            }
            leader.handleHbResponse(success, false);
            follower.handleHbResponse(roundTrip(success), true);
            Assertions.assertTrue(leader.isAlive());
            Assertions.assertTrue(follower.isAlive());
            Assertions.assertEquals(1, follower.getNativeFileEncryptionVersion());
        } finally {
            Config.max_backend_heartbeat_failure_tolerance_count = previous;
        }
    }

    private static BackendHbResponse roundTrip(BackendHbResponse response) throws IOException {
        HbPackage journal = new HbPackage();
        journal.addHbResponse(response);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        journal.write(new DataOutputStream(bytes));
        return (BackendHbResponse) HbPackage.read(new DataInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))).getHbResults().get(0);
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
