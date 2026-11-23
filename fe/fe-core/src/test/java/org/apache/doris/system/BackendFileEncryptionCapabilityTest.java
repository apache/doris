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
        Assertions.assertFalse(backend.isNativeFileEncryptionSupported());
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), true), false);
        Assertions.assertTrue(backend.isNativeFileEncryptionSupported());
        Backend restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(backend), Backend.class);
        Assertions.assertFalse(restored.isNativeFileEncryptionSupported());
    }

    @Test
    void unavailableCapabilityClearsPreviouslyVerifiedSupport() {
        Backend backend = backend();
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), true), false);
        Assertions.assertTrue(backend.isNativeFileEncryptionSupported());
        Assertions.assertTrue(backend.handleHbResponse(heartbeat(System.currentTimeMillis(), false), false));
        Assertions.assertFalse(backend.isNativeFileEncryptionSupported());
        Assertions.assertTrue(backend.isAlive());
    }

    @Test
    void failedHeartbeatClearsCapabilityBeforeTheLivenessGraceExpires() {
        Backend backend = backend();
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), true), false);
        Assertions.assertTrue(backend.isNativeFileEncryptionSupported());
        Assertions.assertTrue(backend.handleHbResponse(
                new BackendHbResponse(1, "127.0.0.1", backend.getLastUpdateMs(), "fixture failure"), false));
        Assertions.assertFalse(backend.isNativeFileEncryptionSupported());
    }

    @Test
    void historicalReplayCannotEnableExecutionButFreshBroadcastCan() {
        Backend backend = backend();
        long beforeProcessStart = ManagementFactory.getRuntimeMXBean().getStartTime() - 1;
        backend.handleHbResponse(heartbeat(beforeProcessStart, true), true);
        Assertions.assertFalse(backend.isNativeFileEncryptionSupported());
        backend.handleHbResponse(heartbeat(System.currentTimeMillis(), true), true);
        Assertions.assertTrue(backend.isNativeFileEncryptionSupported());
    }

    @Test
    void staleOrFutureHeartbeatsCannotEnableExecution() {
        for (long offset : new long[] {-TimeUnit.DAYS.toMillis(1), TimeUnit.DAYS.toMillis(1)}) {
            Backend backend = backend();
            backend.handleHbResponse(heartbeat(System.currentTimeMillis() + offset, true), false);
            Assertions.assertFalse(backend.isNativeFileEncryptionSupported());
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
            success.setNativeFileEncryptionSupported(true);
            success.setPaimonRustReaderSupported(true);
            leader.handleHbResponse(success, false);
            follower.handleHbResponse(roundTrip(success), true);
            Assertions.assertTrue(follower.isNativeFileEncryptionSupported());
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
                    Assertions.assertFalse(journal.isNativeFileEncryptionSupported());
                }
                Assertions.assertEquals(HbStatus.BAD, response.getStatus(),
                        "The leader still needs the original failure for connection eviction and diagnostics");
                Assertions.assertEquals(failure < 3, leader.isAlive());
                Assertions.assertEquals(leader.isAlive(), follower.isAlive());
                Assertions.assertFalse(leader.isNativeFileEncryptionSupported());
                Assertions.assertFalse(follower.isNativeFileEncryptionSupported());
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
            Assertions.assertTrue(follower.isNativeFileEncryptionSupported());
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

    private static BackendHbResponse heartbeat(long time, boolean supported) {
        BackendHbResponse response = new BackendHbResponse(1, 9060, 8040, 8060, time, time,
                "fixture-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        response.setNativeFileEncryptionSupported(supported);
        return response;
    }
}
