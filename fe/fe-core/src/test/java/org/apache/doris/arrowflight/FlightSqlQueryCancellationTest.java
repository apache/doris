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

package org.apache.doris.arrowflight;

import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlightSqlQueryCancellationTest {
    private static final TUniqueId QUERY = new TUniqueId(10L, 20L);
    private static final TUniqueId FIRST = new TUniqueId(10L, 21L);
    private static final TUniqueId SECOND = new TUniqueId(10L, 22L);
    private static final TNetworkAddress FINISHED_BE = new TNetworkAddress("127.0.0.1", 8001);
    private static final TNetworkAddress ACTIVE_BE = new TNetworkAddress("127.0.0.1", 8002);

    private static PCancelPlanFragmentResult response(TStatusCode status) {
        return PCancelPlanFragmentResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(status.getValue())).build();
    }

    @Test
    public void cancelReachesAllBackendsWithoutLiveCoordinatorOrResultFragment() throws Exception {
        FlightSqlQueryCancellation routes = new FlightSqlQueryCancellation(() -> 0L);
        // The selected endpoint and its coordinator have finished; only routing metadata survives.
        routes.register(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(FINISHED_BE, ACTIVE_BE), 120);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.eq(QUERY), Mockito.any()))
                .thenReturn(Futures.immediateFuture(response(TStatusCode.OK)));
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals(TStatusCode.OK, routes.cancel(FIRST).getStatusCode());
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(Mockito.eq(FINISHED_BE),
                    Mockito.eq(QUERY), Mockito.any());
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(Mockito.eq(ACTIVE_BE),
                    Mockito.eq(QUERY), Mockito.any());
        }
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(SECOND).getStatusCode());
    }

    @Test
    public void applicationErrorKeepsRouteForRetry() throws Exception {
        FlightSqlQueryCancellation routes = new FlightSqlQueryCancellation(() -> 0L);
        routes.register(QUERY, ImmutableList.of(FIRST), ImmutableList.of(ACTIVE_BE), 120);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.eq(QUERY), Mockito.any()))
                .thenReturn(Futures.immediateFuture(response(TStatusCode.CANCELLED)),
                        Futures.immediateFuture(response(TStatusCode.OK)));
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals(TStatusCode.CANCELLED, routes.cancel(FIRST).getStatusCode());
            Assertions.assertEquals(TStatusCode.OK, routes.cancel(FIRST).getStatusCode());
        }
    }

    @Test
    public void routesExpireAfterExecutionTimeout() {
        AtomicLong clock = new AtomicLong();
        FlightSqlQueryCancellation routes = new FlightSqlQueryCancellation(clock::get);
        routes.register(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(ACTIVE_BE), 120);
        clock.set(TimeUnit.SECONDS.toNanos(126));
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(FIRST).getStatusCode());
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(SECOND).getStatusCode());
    }

    @Test
    public void channelCleanupRemovesItsRoutes() {
        FlightSqlChannel channel = new FlightSqlChannel();
        try {
            channel.registerRemoteQuery(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(ACTIVE_BE), 120);
            channel.reset();
            Assertions.assertEquals(TStatusCode.NOT_FOUND,
                    FlightSqlQueryCancellation.INSTANCE.cancel(FIRST).getStatusCode());
            Assertions.assertEquals(TStatusCode.NOT_FOUND,
                    FlightSqlQueryCancellation.INSTANCE.cancel(SECOND).getStatusCode());
        } finally {
            channel.close();
        }
    }

    // The session drops a query's routes when its next request begins and when it is torn down.
    @Test
    public void nextRequestAndSessionTeardownRemoveRoutes() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.eq(QUERY), Mockito.any()))
                .thenReturn(Futures.immediateFuture(response(TStatusCode.OK)));
        ConnectContext ctx = ConnectContext.forFlight("flight-query-cancellation-peer");
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            adapter.getChannel().registerRemoteQuery(QUERY, ImmutableList.of(FIRST), ImmutableList.of(ACTIVE_BE), 120);
            adapter.beginRequest();
            Assertions.assertEquals(TStatusCode.NOT_FOUND,
                    FlightSqlQueryCancellation.INSTANCE.cancel(FIRST).getStatusCode());
            adapter.getChannel().registerRemoteQuery(QUERY, ImmutableList.of(SECOND), ImmutableList.of(ACTIVE_BE), 120);
            ctx.releaseProtocolSession();
            Assertions.assertEquals(TStatusCode.NOT_FOUND,
                    FlightSqlQueryCancellation.INSTANCE.cancel(SECOND).getStatusCode());
            // A route that stayed registered would have been cancelled through the mocked proxy.
            Mockito.verifyNoInteractions(proxy);
        }
    }
}
