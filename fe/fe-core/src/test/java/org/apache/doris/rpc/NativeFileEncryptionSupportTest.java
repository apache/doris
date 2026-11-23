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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThriftClientTransportProvider;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tls.PrivateRpcTls;
import org.apache.doris.tls.server.FeServerStarterFactory;
import org.apache.doris.tls.server.ServerStarter;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class NativeFileEncryptionSupportTest {
    @ParameterizedTest
    @ValueSource(strings = {"query", "load", "group"})
    void admissionIgnoresBackendsOutsideTheQueryCandidates(String exclusion) throws Exception {
        long previousTolerance = Config.max_backend_heartbeat_failure_tolerance_count;
        Config.max_backend_heartbeat_failure_tolerance_count = 3;
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> contexts = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<PrivateRpcTls> tls = Mockito.mockStatic(PrivateRpcTls.class);
                MockedStatic<FeServerStarterFactory> listener = Mockito.mockStatic(FeServerStarterFactory.class)) {
            SystemInfoService service = new SystemInfoService();
            Backend eligible = backend(1, true);
            Backend excluded = backend(2, true);
            eligible.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected"));
            excluded.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected"));
            service.addBackend(eligible);
            service.addBackend(excluded);
            excluded.handleHbResponse(new BackendHbResponse(2, excluded.getHost(),
                    excluded.getLastUpdateMs(), "fixture heartbeat failure"), false);
            Assertions.assertTrue(excluded.isAlive());
            Assertions.assertFalse(excluded.isNativeFileEncryptionSupported());
            switch (exclusion) {
                case "query":
                    excluded.setQueryDisabled(true);
                    break;
                case "load":
                    excluded.setLoadDisabled(true);
                    break;
                default:
                    excluded.setTagMap(Map.of(Tag.TYPE_LOCATION, "another"));
                    break;
            }
            Env currentEnv = Mockito.mock(Env.class);
            env.when(Env::getCurrentEnv).thenReturn(currentEnv);
            env.when(Env::getCurrentSystemInfo).thenReturn(service);
            ConnectContext context = Mockito.mock(ConnectContext.class);
            Mockito.when(context.getComputeGroup()).thenReturn(new ComputeGroup("selected", "selected", service));
            contexts.when(ConnectContext::get).thenReturn(context);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.BRPC)).thenReturn(true);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.THRIFT)).thenReturn(true);
            listener.when(FeServerStarterFactory::supportsPrivateFileKeys).thenReturn(true);

            Assertions.assertTrue(NativeFileEncryptionSupport.supported(eligible));
            Assertions.assertTrue(NativeFileEncryptionSupport.supported());
            excluded.setQueryDisabled(false);
            excluded.setLoadDisabled(false);
            excluded.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected"));
            Assertions.assertFalse(NativeFileEncryptionSupport.supported(),
                    "An eligible backend without a current capability must still refuse the query");
        } finally {
            Config.max_backend_heartbeat_failure_tolerance_count = previousTolerance;
        }
    }

    @Test
    void missingTransportOrBackendProofNeverGrantsExecution() {
        Backend current = backend(1, true);
        Assertions.assertFalse(NativeFileEncryptionSupport.allBackendsSupported(false, List.of(current)));
        Assertions.assertFalse(NativeFileEncryptionSupport.allBackendsSupported(true, List.of()));
        Assertions.assertFalse(NativeFileEncryptionSupport.allBackendsSupported(true,
                List.of(current, backend(2, false))));
        Assertions.assertTrue(NativeFileEncryptionSupport.allBackendsSupported(true,
                List.of(current, backend(2, true))));
        current.handleHbResponse(new BackendHbResponse(1, "127.0.0.1", current.getLastUpdateMs(), "fixture"), false);
        Assertions.assertFalse(NativeFileEncryptionSupport.allBackendsSupported(true, List.of(current)));
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

    private static Backend backend(long id, boolean supported) {
        Backend backend = new Backend(id, "127.0.0.1", 9050);
        long now = System.currentTimeMillis();
        BackendHbResponse heartbeat = new BackendHbResponse(id, 9060, 8040, 8060, now, now,
                "fixture-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        heartbeat.setNativeFileEncryptionSupported(supported);
        backend.handleHbResponse(heartbeat, false);
        return backend;
    }
}
