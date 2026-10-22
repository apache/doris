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

public class BackendRemoteSpillBytesTest {
    private static BackendHbResponse okResponse(long beStartTime) {
        return new BackendHbResponse(1L, 9060, 8040, 8060, System.currentTimeMillis(), beStartTime,
                "version", "mix", 0, 0, false, 8070);
    }

    // The remote spill size comes from disk reports; a restarted BE no longer holds the spill of
    // its previous run, so the value is cleared when the heartbeat sees a new start time.
    @Test
    public void testRemoteSpillBytesClearedOnRestart() {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.handleHbResponse(okResponse(1000L), false);
        backend.setRemoteSpillBytes(64L);

        // Same process: kept.
        backend.handleHbResponse(okResponse(1000L), false);
        Assertions.assertEquals(64L, backend.getRemoteSpillBytes());

        // Restarted while alive: cleared.
        backend.handleHbResponse(okResponse(2000L), false);
        Assertions.assertEquals(0L, backend.getRemoteSpillBytes());

        // Restarted after being marked dead: cleared as well.
        backend.setRemoteSpillBytes(64L);
        backend.setAlive(false);
        backend.handleHbResponse(okResponse(3000L), false);
        Assertions.assertEquals(0L, backend.getRemoteSpillBytes());

        // Back to alive without a restart: kept.
        backend.setRemoteSpillBytes(64L);
        backend.setAlive(false);
        backend.handleHbResponse(okResponse(3000L), false);
        Assertions.assertEquals(64L, backend.getRemoteSpillBytes());
    }
}
