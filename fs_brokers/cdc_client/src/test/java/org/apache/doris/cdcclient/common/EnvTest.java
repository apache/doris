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

package org.apache.doris.cdcclient.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EnvTest {

    @AfterEach
    void resetTlsConfiguration() {
        Env env = Env.getCurrentEnv();
        env.setBackendHttpTlsEnabled(false);
        env.setBackendHttpTlsCaCertificatePath(null);
    }

    @Test
    void getReaderIfPresentReturnsNullForUnknownJob() {
        // An off-target releaseReader RPC must be a no-op, never create a reader -> peek returns null.
        assertNull(Env.getCurrentEnv().getReaderIfPresent("no-such-job-id"));
    }

    @Test
    void detachReaderIfOwnerReturnsNullForUnknownJob() {
        // Stale release for an unknown job (no lock/context) must be a no-op.
        assertNull(Env.getCurrentEnv().detachReaderIfOwner("no-such-job-id", "t1"));
    }

    @Test
    void internalHttpUrlUsesHttpWhenBackendTlsDisabled() {
        Env env = Env.getCurrentEnv();
        env.setBackendHttpPort(8040);
        env.setBackendHttpTlsEnabled(false);

        assertEquals("http://127.0.0.1:8040", env.getBackendInternalHttpUrl());
        assertEquals("http://fe-host:8030", env.getInternalHttpUrl("fe-host:8030"));
    }

    @Test
    void internalHttpUrlUsesHttpsWhenBackendTlsEnabled() {
        Env env = Env.getCurrentEnv();
        env.setBackendHttpPort(8040);
        env.setBackendHttpTlsEnabled(true);

        assertEquals("https://127.0.0.1:8040", env.getBackendInternalHttpUrl());
        assertEquals("https://fe-host:8030", env.getInternalHttpUrl("fe-host:8030"));
    }
}
