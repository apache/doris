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
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
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

    private static BackendServiceProxy cancellingProxy(TUniqueId queryId) throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.eq(queryId), Mockito.any()))
                .thenReturn(Futures.immediateFuture(response(TStatusCode.OK)));
        return proxy;
    }

    private static FlightSqlEndpointsLocation endpoint(TUniqueId resultId) {
        return new FlightSqlEndpointsLocation(resultId, new TNetworkAddress("127.0.0.1", 8070),
                new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>());
    }

    @Test
    public void cancelReachesAllBackendsWithoutLiveCoordinatorOrResultFragment() throws Exception {
        FlightSqlQueryCancellation routes =
                new FlightSqlQueryCancellation(() -> 0L, FlightSqlQueryCancellation.MAX_ROUTED_RESULTS);
        // The selected endpoint and its coordinator have finished; only routing metadata survives.
        routes.register(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(FINISHED_BE, ACTIVE_BE), 120);
        BackendServiceProxy proxy = cancellingProxy(QUERY);
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals(TStatusCode.OK, routes.cancel(FIRST).getStatusCode());
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(Mockito.eq(FINISHED_BE),
                    Mockito.eq(QUERY), Mockito.any());
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(Mockito.eq(ACTIVE_BE),
                    Mockito.eq(QUERY), Mockito.any());
        }
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(SECOND).getStatusCode());
        Assertions.assertEquals(0, routes.routedResults());
    }

    @Test
    public void applicationErrorKeepsRouteForRetry() throws Exception {
        FlightSqlQueryCancellation routes =
                new FlightSqlQueryCancellation(() -> 0L, FlightSqlQueryCancellation.MAX_ROUTED_RESULTS);
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
        FlightSqlQueryCancellation routes =
                new FlightSqlQueryCancellation(clock::get, FlightSqlQueryCancellation.MAX_ROUTED_RESULTS);
        routes.register(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(ACTIVE_BE), 120);
        clock.set(TimeUnit.SECONDS.toNanos(126));
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(FIRST).getStatusCode());
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(SECOND).getStatusCode());
        // An expired route that no stream reported leaves at the next registration.
        routes.register(QUERY, ImmutableList.of(FIRST, SECOND), ImmutableList.of(ACTIVE_BE), 120);
        clock.set(TimeUnit.SECONDS.toNanos(252));
        routes.register(new TUniqueId(11L, 20L), ImmutableList.of(new TUniqueId(11L, 21L)),
                ImmutableList.of(ACTIVE_BE), 120);
        Assertions.assertEquals(1, routes.routedResults());
    }

    // The client reads a query's results from the BEs, so it can still hold a stream of the query open, and
    // close it early, after its session has started the next request or has been torn down.
    @Test
    public void routesOutliveTheSessionsNextRequestAndTeardown() throws Exception {
        TUniqueId query = new TUniqueId(30L, 40L);
        TUniqueId readAfterNextRequest = new TUniqueId(30L, 41L);
        TUniqueId readAfterTeardown = new TUniqueId(30L, 42L);
        Coordinator coord = Mockito.mock(Coordinator.class);
        Mockito.when(coord.getQueryId()).thenReturn(query);
        Mockito.when(coord.getBackendBrpcAddresses()).thenReturn(ImmutableList.of(ACTIVE_BE));
        Mockito.when(coord.getQueryOptions()).thenReturn(new TQueryOptions().setExecutionTimeout(120));
        BackendServiceProxy proxy = cancellingProxy(query);
        ConnectContext ctx = ConnectContext.forFlight("flight-query-cancellation-peer");
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            ctx.addFlightSqlEndpointsLocation(endpoint(readAfterNextRequest));
            FlightSqlQueryCancellation.INSTANCE.register(ctx, coord);
            adapter.beginRequest();
            Assertions.assertEquals(TStatusCode.OK,
                    FlightSqlQueryCancellation.INSTANCE.cancel(readAfterNextRequest).getStatusCode());
            ctx.addFlightSqlEndpointsLocation(endpoint(readAfterTeardown));
            FlightSqlQueryCancellation.INSTANCE.register(ctx, coord);
            ctx.releaseProtocolSession();
            Assertions.assertEquals(TStatusCode.OK,
                    FlightSqlQueryCancellation.INSTANCE.cancel(readAfterTeardown).getStatusCode());
            Mockito.verify(proxy, Mockito.times(2)).cancelPipelineXPlanFragmentAsync(Mockito.eq(ACTIVE_BE),
                    Mockito.eq(query), Mockito.any());
        }
    }

    // With routes outliving their sessions, the bound is what keeps the registry from growing with the query
    // rate. The oldest routes, the likeliest to belong to streams that ended, give way first, and as a whole.
    @Test
    public void boundDropsWholeRoutesOldestFirst() throws Exception {
        FlightSqlQueryCancellation routes = new FlightSqlQueryCancellation(() -> 0L, 3);
        TUniqueId oldQuery = new TUniqueId(50L, 1L);
        TUniqueId newQuery = new TUniqueId(51L, 1L);
        TUniqueId oldSecond = new TUniqueId(50L, 3L);
        TUniqueId newFirst = new TUniqueId(51L, 2L);
        routes.register(oldQuery, ImmutableList.of(new TUniqueId(50L, 2L), oldSecond),
                ImmutableList.of(ACTIVE_BE), 120);
        routes.register(newQuery, ImmutableList.of(newFirst, new TUniqueId(51L, 3L)),
                ImmutableList.of(ACTIVE_BE), 120);
        Assertions.assertEquals(2, routes.routedResults());
        Assertions.assertEquals(TStatusCode.NOT_FOUND, routes.cancel(oldSecond).getStatusCode());
        BackendServiceProxy proxy = cancellingProxy(newQuery);
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals(TStatusCode.OK, routes.cancel(newFirst).getStatusCode());
        }
    }
}
