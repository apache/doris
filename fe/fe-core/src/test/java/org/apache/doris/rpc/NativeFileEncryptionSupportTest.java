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

package org.apache.doris.rpc;

import org.apache.doris.common.ThriftClientTransportProvider;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.tls.server.ServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class NativeFileEncryptionSupportTest {
    @Test
    void missingTransportOrBackendProofNeverGrantsExecution() {
        Backend current = backend(1, 1);
        Assertions.assertEquals(0, NativeFileEncryptionSupport.minimumVersion(false, List.of(current)));
        Assertions.assertEquals(0, NativeFileEncryptionSupport.minimumVersion(true, List.of()));
        Assertions.assertEquals(0, NativeFileEncryptionSupport.minimumVersion(true,
                List.of(current, backend(2, 0))));
        Assertions.assertEquals(1, NativeFileEncryptionSupport.minimumVersion(true,
                List.of(current, backend(2, 2))));
        current.handleHbResponse(new BackendHbResponse(1, "127.0.0.1", current.getLastUpdateMs(), "fixture"), false);
        Assertions.assertEquals(0, NativeFileEncryptionSupport.minimumVersion(true, List.of(current)));
    }

    @Test
    void unknownProvidersDoNotInheritOssConfigurationProof() {
        BackendServiceClientChannelProvider channels = (address, resolvedIp, executor) -> {
            throw new AssertionError("A capability check must not create a channel");
        };
        ThriftClientTransportProvider transports = (address, timeout, nonBlocking) -> {
            throw new AssertionError("A capability check must not open a transport");
        };
        ServerStarter listener = new ServerStarter() {
            @Override
            public void start() {
                throw new AssertionError("A capability check must not start a listener");
            }

            @Override
            public void stop() {
                throw new AssertionError("A capability check must not stop a listener");
            }
        };
        Assertions.assertFalse(channels.supportsPrivateFileKeys());
        Assertions.assertFalse(transports.supportsPrivateFileKeys());
        Assertions.assertFalse(listener.supportsPrivateFileKeys());
    }

    private static Backend backend(long id, int version) {
        Backend backend = new Backend(id, "127.0.0.1", 9050);
        long now = System.currentTimeMillis();
        BackendHbResponse heartbeat = new BackendHbResponse(id, 9060, 8040, 8060, now, now,
                "fixture-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        heartbeat.setNativeFileEncryptionVersion(version);
        backend.handleHbResponse(heartbeat, false);
        return backend;
    }
}
