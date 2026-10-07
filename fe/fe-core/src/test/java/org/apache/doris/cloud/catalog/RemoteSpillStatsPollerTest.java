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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RemoteSpillStatsPollerTest {
    private int savedMaxAge;
    private int savedPollInterval;

    @BeforeEach
    public void setUp() {
        savedMaxAge = Config.cloud_spill_stats_max_age_second;
        savedPollInterval = Config.cloud_spill_stats_poll_interval_second;
        Config.cloud_spill_stats_max_age_second = 300;
        Config.cloud_spill_stats_poll_interval_second = 60;
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_spill_stats_max_age_second = savedMaxAge;
        Config.cloud_spill_stats_poll_interval_second = savedPollInterval;
    }

    @Test
    public void testRemoteSpillBytesNotFetchedYet() {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, poller::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("not been polled"), e.getMessage());
    }

    @Test
    public void testRemoteSpillBytesFresh() throws AnalysisException {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.nanoTime());
        Assertions.assertEquals(12345L, poller.getRemoteSpillBytes());
        // Within the limit: still served.
        poller.setRemoteSpillStatsForTest(67L, System.nanoTime() - TimeUnit.SECONDS.toNanos(200));
        Assertions.assertEquals(67L, poller.getRemoteSpillBytes());
    }

    @Test
    public void testRemoteSpillBytesStale() {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.nanoTime() - TimeUnit.SECONDS.toNanos(301));
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, poller::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("stale"), e.getMessage());
        // The limit is mutable: raising it makes the same value acceptable again.
        Config.cloud_spill_stats_max_age_second = 600;
        Assertions.assertDoesNotThrow(poller::getRemoteSpillBytes);
    }

    @Test
    public void testMaxAgeCoversThreePollIntervals() {
        Assertions.assertEquals(300, RemoteSpillStatsPoller.maxAgeSecond());
        // A poll interval longer than a third of the max age cannot make every value stale.
        Config.cloud_spill_stats_poll_interval_second = 600;
        Assertions.assertEquals(1800, RemoteSpillStatsPoller.maxAgeSecond());
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.nanoTime() - TimeUnit.SECONDS.toNanos(700));
        Assertions.assertDoesNotThrow(poller::getRemoteSpillBytes);
    }

    @Test
    public void testPollSumsAliveBackendsOnly() throws Exception {
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Backend first = backend(1, true);
        Backend second = backend(2, true);
        Backend dead = backend(3, false);
        Mockito.when(systemInfo.getAllBackendsByAllCluster())
                .thenReturn(ImmutableMap.of(1L, first, 2L, second, 3L, dead));
        Mockito.when(proxy.getBeResourceAsync(Mockito.eq(first.getBrpcAddress()), Mockito.anyInt(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 17)));
        Mockito.when(proxy.getBeResourceAsync(Mockito.eq(second.getBrpcAddress()), Mockito.anyInt(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 25)));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> mockedProxy = Mockito.mockStatic(BackendServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
            poller.runAfterCatalogReady();
            Assertions.assertEquals(42L, poller.getRemoteSpillBytes());
            Mockito.verify(proxy, Mockito.never()).getBeResourceAsync(
                    Mockito.eq(dead.getBrpcAddress()), Mockito.anyInt(), Mockito.any());

            Mockito.when(first.isAlive()).thenReturn(false);
            Mockito.when(second.isAlive()).thenReturn(false);
            poller.runAfterCatalogReady();
            Assertions.assertEquals(0L, poller.getRemoteSpillBytes());
        }
    }

    @Test
    public void testFailedPollKeepsPreviousCompleteSum() throws Exception {
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Backend backend = backend(1, true);
        Backend other = backend(2, true);
        Mockito.when(systemInfo.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(1L, backend, 2L, other));
        TNetworkAddress address = backend.getBrpcAddress();
        TNetworkAddress otherAddress = other.getBrpcAddress();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> mockedProxy = Mockito.mockStatic(BackendServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(address), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 40)));
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(otherAddress), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 51)));
            poller.runAfterCatalogReady();
            Assertions.assertEquals(91L, poller.getRemoteSpillBytes());

            // A successful first BE must not replace the cached complete sum when another BE fails.
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(address), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 100)));
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(otherAddress), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(
                            InternalService.PGetBeResourceResponse.getDefaultInstance()));
            poller.runAfterCatalogReady();
            Assertions.assertEquals(91L, poller.getRemoteSpillBytes());

            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(otherAddress), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.OK, 51)));
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(address), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(response(TStatusCode.INTERNAL_ERROR, 1)));
            poller.runAfterCatalogReady();
            Assertions.assertEquals(91L, poller.getRemoteSpillBytes());

            CompletableFuture<InternalService.PGetBeResourceResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("RPC failed"));
            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(address), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(failed);
            poller.runAfterCatalogReady();
            Assertions.assertEquals(91L, poller.getRemoteSpillBytes());

            Mockito.when(proxy.getBeResourceAsync(Mockito.eq(address), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(null);
            poller.runAfterCatalogReady();
            Assertions.assertEquals(91L, poller.getRemoteSpillBytes());

            // A failed round must not renew freshness and hide an outdated billing value.
            poller.setRemoteSpillStatsForTest(91L, System.nanoTime() - TimeUnit.SECONDS.toNanos(301));
            poller.runAfterCatalogReady();
            Assertions.assertThrows(AnalysisException.class, poller::getRemoteSpillBytes);
        }
    }

    private static Backend backend(long id, boolean alive) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(id);
        Mockito.when(backend.isAlive()).thenReturn(alive);
        Mockito.when(backend.getBrpcAddress()).thenReturn(new TNetworkAddress("127.0.0.1", (int) (8000 + id)));
        return backend;
    }

    private static InternalService.PGetBeResourceResponse response(TStatusCode status, long bytes) {
        return InternalService.PGetBeResourceResponse.newBuilder()
                .setStatus(Types.PStatus.newBuilder().setStatusCode(status.getValue()))
                .setGlobalBeResourceUsage(InternalService.PGlobalResourceUsage.newBuilder()
                        .setRemoteSpillBytes(bytes))
                .build();
    }
}
