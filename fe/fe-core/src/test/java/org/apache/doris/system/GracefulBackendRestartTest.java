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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GracefulBackendRestartTest {
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
