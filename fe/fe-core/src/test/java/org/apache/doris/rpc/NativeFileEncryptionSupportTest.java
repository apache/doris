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
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tls.PrivateRpcTls;
import org.apache.doris.tls.server.FeServerStarterFactory;
import org.apache.doris.tls.server.ServerStarter;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class NativeFileEncryptionSupportTest {
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void admissionAndScanReuseTheSelectedBackends(boolean admissionFirst, boolean capableFirst) throws Exception {
        boolean previousPrefer = Config.prefer_compute_node_for_external_table;
        int previousMinimum = Config.min_backend_num_for_external_table;
        Config.prefer_compute_node_for_external_table = true;
        Config.min_backend_num_for_external_table = 2;
        AtomicInteger selections = new AtomicInteger();
        Backend compute = backend(1, true);
        Backend capable = backend(2, true);
        Backend incapable = backend(3, false);
        compute.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected", Tag.TYPE_ROLE, Tag.VALUE_COMPUTATION));
        capable.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected"));
        incapable.setTagMap(Map.of(Tag.TYPE_LOCATION, "selected"));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> contexts = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<PrivateRpcTls> tls = Mockito.mockStatic(PrivateRpcTls.class);
                MockedStatic<FeServerStarterFactory> listener = Mockito.mockStatic(FeServerStarterFactory.class);
                MockedConstruction<BeSelectionPolicy> policies = Mockito.mockConstruction(BeSelectionPolicy.class,
                        (policy, construction) -> {
                            policy.preferredLocations = new ArrayList<>();
                            Mockito.when(policy.getCandidateBackends(Mockito.anyCollection())).thenAnswer(call -> {
                                Assertions.assertTrue(policy.preferComputeNode);
                                Assertions.assertEquals(2, policy.expectBeNum);
                                boolean capableSelection = (selections.getAndIncrement() % 2 == 0) == capableFirst;
                                return List.of(compute, capableSelection ? capable : incapable);
                            });
                        })) {
            SystemInfoService service = new SystemInfoService();
            service.addBackend(compute);
            service.addBackend(capable);
            service.addBackend(incapable);
            env.when(Env::getCurrentSystemInfo).thenReturn(service);
            ConnectContext context = Mockito.mock(ConnectContext.class);
            contexts.when(ConnectContext::get).thenReturn(context);
            ComputeGroup group = new ComputeGroup("selected", "selected", service);
            Mockito.when(context.getComputeGroup()).thenReturn(group);
            StatementContext statement = new StatementContext();
            Mockito.when(context.getStatementContext()).thenReturn(statement);
            SessionVariable session = Mockito.mock(SessionVariable.class);
            Mockito.when(session.getTimeZone()).thenReturn("UTC");
            Mockito.when(context.getSessionVariable()).thenReturn(session);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.BRPC)).thenReturn(true);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.THRIFT)).thenReturn(true);
            listener.when(FeServerStarterFactory::supportsPrivateFileKeys).thenReturn(true);
            FederationBackendPolicy scan = new FederationBackendPolicy();
            if (admissionFirst) {
                Assertions.assertEquals(capableFirst, NativeFileEncryptionSupport.supported());
                scan.init();
            } else {
                scan.init();
                Assertions.assertEquals(capableFirst, NativeFileEncryptionSupport.supported());
            }
            Assertions.assertEquals(Set.of(1L, capableFirst ? 2L : 3L),
                    scan.getBackends().stream().map(Backend::getId).collect(Collectors.toSet()));
            Assertions.assertEquals(1, selections.get(), "One statement must make one candidate selection");
            FederationBackendPolicy secondScan = new FederationBackendPolicy();
            secondScan.init();
            Assertions.assertEquals(List.copyOf(scan.getBackends()), List.copyOf(secondScan.getBackends()));
            Assertions.assertEquals(1, selections.get());
            if (capableFirst) {
                capable.handleHbResponse(new BackendHbResponse(capable.getId(), capable.getHost(),
                        capable.getLastUpdateMs(), "fixture heartbeat failure"), false);
                Assertions.assertFalse(NativeFileEncryptionSupport.supported(),
                        "Memoize membership, not a previously successful capability check");
                Assertions.assertEquals(1, selections.get());
            }

            // A new statement must not inherit the previous statement's selected subset or capability result.
            StatementContext followingStatement = new StatementContext();
            Mockito.when(context.getStatementContext()).thenReturn(followingStatement);
            Assertions.assertEquals(!capableFirst, NativeFileEncryptionSupport.supported());
            FederationBackendPolicy next = new FederationBackendPolicy();
            next.init();
            Assertions.assertEquals(Set.of(1L, capableFirst ? 3L : 2L),
                    next.getBackends().stream().map(Backend::getId).collect(Collectors.toSet()));
            Assertions.assertEquals(2, selections.get());

            FederationBackendPolicy preferred = new FederationBackendPolicy();
            preferred.init(List.of("127.0.0.1"));
            Assertions.assertEquals(3, selections.get(), "Preferred-location scans keep their own policy");
            Assertions.assertEquals(!capableFirst, NativeFileEncryptionSupport.supported());
            Assertions.assertEquals(3, selections.get());

            ComputeGroup anotherGroup = new ComputeGroup("another", "selected", service);
            Mockito.when(context.getComputeGroup()).thenReturn(anotherGroup);
            Assertions.assertEquals(!capableFirst, NativeFileEncryptionSupport.supported());
            Assertions.assertEquals(4, selections.get(), "Compute groups must not share a selected subset");
            Mockito.when(context.getComputeGroup()).thenReturn(group);
            Assertions.assertEquals(!capableFirst, NativeFileEncryptionSupport.supported());
            Assertions.assertEquals(4, selections.get());
            followingStatement.resetConnectorStatementScope();
            Assertions.assertFalse(NativeFileEncryptionSupport.supported());
            Assertions.assertEquals(5, selections.get(), "Resetting a planning attempt must discard the selection");
        } finally {
            Config.prefer_compute_node_for_external_table = previousPrefer;
            Config.min_backend_num_for_external_table = previousMinimum;
        }
    }

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
