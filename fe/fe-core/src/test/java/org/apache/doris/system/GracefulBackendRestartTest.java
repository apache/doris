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

import org.apache.doris.catalog.Env;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class GracefulBackendRestartTest {
    private SessionVariable sessionVariable;
    private MockedStatic<VariableMgr> variables;

    @BeforeEach
    public void setUp() {
        sessionVariable = new SessionVariable();
        variables = Mockito.mockStatic(VariableMgr.class);
        variables.when(VariableMgr::getDefaultSessionVariable).thenReturn(sessionVariable);
    }

    @AfterEach
    public void tearDown() {
        variables.close();
    }

    @Test
    public void testCheckpointReplayDoesNotInvalidateServingConnections() {
        Backend backend = new Backend(1, "be.test", 9050);
        backend.setLastStartTime(100);
        backend.setAlive(true);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::isCheckpointThread).thenReturn(true);
            backend.handleHbResponse(heartbeat(200), true);
            env.verify(Env::getCurrentEnv, Mockito.never());
            Assertions.assertEquals(200, backend.getLastStartTime());
            Assertions.assertTrue(backend.isAlive());
        }
    }

    @Test
    public void testHeartbeatReplayInvalidatesBeforePublishingNewEpoch() {
        TestBackend backend = new TestBackend();
        backend.setLastStartTime(100);
        backend.setAlive(true);
        backend.handleHbResponse(heartbeat(200), true);
        Assertions.assertEquals(1, backend.invalidations);
        Assertions.assertEquals(100, backend.epochWhenInvalidated);
        Assertions.assertEquals(200, backend.getLastStartTime());
        backend.handleHbResponse(heartbeat(200), true);
        Assertions.assertEquals(1, backend.invalidations);
    }

    @Test
    public void testRecoveryInvalidatesButFirstHeartbeatDoesNot() {
        TestBackend backend = new TestBackend();
        backend.handleHbResponse(heartbeat(100), false);
        Assertions.assertEquals(0, backend.invalidations);
        backend.setAlive(false);
        backend.handleHbResponse(heartbeat(100), false);
        Assertions.assertEquals(1, backend.invalidations);
        Assertions.assertTrue(backend.isAlive());
    }

    @Test
    public void testGracefulModeDoesNotDelayFirstStartupOrHealthyBackend() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend backend = new TestBackend();
        for (int i = 0; i < 6; i++) {
            BackendHbResponse response = heartbeat(100);
            backend.handleHbResponse(response, false);
            Assertions.assertTrue(response.isReadyForScheduling());
            Assertions.assertTrue(backend.isQueryAvailable());
        }
        Assertions.assertEquals(0, backend.invalidations);
    }

    @Test
    public void testRecoveryWaitsForFiveSuccessfulHeartbeats() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend backend = recoveringBackend();
        assertWaiting(backend, 100, 4);
        Assertions.assertEquals(0, backend.invalidations);
        BackendHbResponse response = heartbeat(100);
        backend.handleHbResponse(response, false);
        Assertions.assertTrue(response.isReadyForScheduling());
        Assertions.assertTrue(backend.isQueryAvailable());
        Assertions.assertEquals(1, backend.invalidations);
        backend.handleHbResponse(heartbeat(100), false);
        Assertions.assertTrue(backend.isQueryAvailable());
        Assertions.assertEquals(1, backend.invalidations);
    }

    @Test
    public void testAliveRestartPublishesWaitingStateForReplay() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend masterBackend = recoveringBackend();
        masterBackend.setAlive(true);
        TestBackend followerBackend = recoveringBackend();
        followerBackend.setAlive(true);
        for (int i = 0; i < 5; i++) {
            BackendHbResponse response = heartbeat(200);
            boolean changed = masterBackend.handleHbResponse(response, false);
            if (i == 0 || i == 4) {
                Assertions.assertTrue(changed, "publish both the waiting and ready transitions");
            }
            if (changed) {
                HbPackage heartbeatLog = new HbPackage();
                heartbeatLog.addHbResponse(response);
                HbPackage replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(heartbeatLog), HbPackage.class);
                followerBackend.handleHbResponse((BackendHbResponse) replay.getHbResults().get(0), true);
            }
            Assertions.assertEquals(i == 4, masterBackend.isAlive());
            Assertions.assertEquals(i == 4, followerBackend.isAlive());
            Assertions.assertEquals(i == 4 ? 200 : 100, followerBackend.getLastStartTime());
        }
        Assertions.assertEquals(1, masterBackend.invalidations);
        Assertions.assertEquals(1, followerBackend.invalidations);
        Assertions.assertEquals(100, followerBackend.epochWhenInvalidated);
    }

    @Test
    public void testFailureAndAnotherRestartResetConfirmation() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend backend = recoveringBackend();
        assertWaiting(backend, 100, 3);
        backend.handleHbResponse(new BackendHbResponse(1, "be.test", 0, "connection refused"), false);
        assertWaiting(backend, 100, 3);
        assertWaiting(backend, 200, 4);
        backend.handleHbResponse(heartbeat(200), false);
        Assertions.assertTrue(backend.isQueryAvailable());
        Assertions.assertEquals(200, backend.getLastStartTime());
    }

    @Test
    public void testShutdownHeartbeatResetsConfirmationAndRemainsExcluded() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend backend = recoveringBackend();
        assertWaiting(backend, 100, 3);
        backend.handleHbResponse(new BackendHbResponse(1, 9060, 8040, 8060,
                System.currentTimeMillis(), 100, "test", "mix", 0, 0, true, -1), false);
        Assertions.assertFalse(backend.isQueryAvailable());
        assertWaiting(backend, 200, 4);
        backend.handleHbResponse(heartbeat(200), false);
        Assertions.assertTrue(backend.isQueryAvailable());
    }

    @Test
    public void testDisablingGracefulModeReleasesWaitingBackend() {
        sessionVariable.enableGracefulShutdown = true;
        TestBackend backend = recoveringBackend();
        assertWaiting(backend, 100, 2);
        sessionVariable.enableGracefulShutdown = false;
        backend.handleHbResponse(heartbeat(100), false);
        Assertions.assertTrue(backend.isQueryAvailable());
        sessionVariable.enableGracefulShutdown = true;
        backend.setAlive(false);
        assertWaiting(backend, 100, 4);
        backend.handleHbResponse(heartbeat(100), false);
        Assertions.assertTrue(backend.isQueryAvailable());
    }

    @Test
    public void testLegacyReplayDoesNotStartLocalConfirmation() {
        sessionVariable.enableGracefulShutdown = true;
        JsonObject legacy = GsonUtils.GSON.toJsonTree(heartbeat(200)).getAsJsonObject();
        legacy.remove("readyForScheduling");
        BackendHbResponse replay = GsonUtils.GSON.fromJson(legacy, BackendHbResponse.class);
        Assertions.assertTrue(replay.isReadyForScheduling());
        TestBackend backend = recoveringBackend();
        backend.handleHbResponse(replay, true);
        Assertions.assertTrue(backend.isQueryAvailable());
    }

    private static TestBackend recoveringBackend() {
        TestBackend backend = new TestBackend();
        backend.setLastStartTime(100);
        backend.setAlive(false);
        return backend;
    }

    private static void assertWaiting(TestBackend backend, long epoch, int heartbeats) {
        for (int i = 0; i < heartbeats; i++) {
            BackendHbResponse response = heartbeat(epoch);
            backend.handleHbResponse(response, false);
            Assertions.assertFalse(response.isReadyForScheduling());
            Assertions.assertFalse(backend.isAlive());
            Assertions.assertFalse(backend.isQueryAvailable());
            Assertions.assertFalse(backend.isLoadAvailable());
            Assertions.assertFalse(backend.isScheduleAvailable());
        }
    }

    private static BackendHbResponse heartbeat(long epoch) {
        return new BackendHbResponse(1, 9060, 8040, 8060, System.currentTimeMillis(), epoch,
                "test", "mix", 0, 0, false, -1);
    }

    private static class TestBackend extends Backend {
        private int invalidations;
        private long epochWhenInvalidated;

        TestBackend() {
            super(1, "be.test", 9050);
        }

        @Override
        public void invalidateLocalConnections() {
            Assertions.assertFalse(isAlive(), "restart must not be scheduled before connection cleanup");
            invalidations++;
            epochWhenInvalidated = getLastStartTime();
        }
    }
}
