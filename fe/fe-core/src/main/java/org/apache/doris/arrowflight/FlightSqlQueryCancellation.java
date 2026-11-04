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

import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.common.Status;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Routes the early close of a Flight result stream, which a BE reports through cancelFlightQuery, to every BE of
 * the stream's query. A route outlives the request and the session that registered it: the client reads the
 * results from the BEs, so it can still hold a stream of the query open after its session has started another
 * request or has been torn down, and close that stream early then. A route goes when its cancellation succeeds,
 * when its query's execution timeout (plus 5 s) has passed, and when newer routes push it out of the bounds:
 * {@link #MAX_ROUTED_RESULTS} result ids and {@link #MAX_ROUTED_BACKENDS} BE addresses. Those go oldest first, as
 * the oldest routes are the likeliest to belong to streams that have ended; a query whose route was pushed out
 * ends at its timeout, as every Flight query did before routes existed.
 */
public final class FlightSqlQueryCancellation {
    private static final Logger LOG = LogManager.getLogger(FlightSqlQueryCancellation.class);
    // A route costs about 270 bytes plus about 30 bytes per address of its query's BEs, and the result ids of a
    // query share one. Bounding both the result ids and the addresses keeps the registry within about 55 MB however
    // many Flight queries are registered within one execution timeout, and however many BEs each of them runs on.
    static final int MAX_ROUTED_RESULTS = 100_000;
    static final int MAX_ROUTED_BACKENDS = 1_000_000;
    public static final FlightSqlQueryCancellation INSTANCE =
            new FlightSqlQueryCancellation(System::nanoTime, MAX_ROUTED_RESULTS, MAX_ROUTED_BACKENDS);
    private final LongSupplier nanoClock;
    private final int resultCapacity;
    private final int backendCapacity;
    // In registration order, so the eldest entry belongs to the oldest route. Guarded by this.
    private final LinkedHashMap<TUniqueId, Route> results = new LinkedHashMap<>();
    // The addresses of the routes that some result id still maps to. Guarded by this.
    private long routedBackends;

    FlightSqlQueryCancellation(LongSupplier nanoClock, int resultCapacity, int backendCapacity) {
        this.nanoClock = nanoClock;
        this.resultCapacity = resultCapacity;
        this.backendCapacity = backendCapacity;
    }

    /**
     * Registers the route of a Flight query whose results stay on the BEs, from the result ids of the endpoints
     * the session collected for it, before its coordinator is closed or deferred. Nothing of the session is
     * kept, so neither its next request nor its teardown drops the route.
     */
    public void register(ConnectContext context, Coordinator coord) {
        register(coord.getQueryId(), context.getFlightSqlEndpointsLocations().stream()
                        .map(FlightSqlEndpointsLocation::getFinstId).distinct().collect(Collectors.toList()),
                coord.getBackendBrpcAddresses(), coord.getQueryOptions().getExecutionTimeout());
    }

    public void register(TUniqueId queryId, List<TUniqueId> resultIds,
            List<TNetworkAddress> backends, int timeoutSeconds) {
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("Flight query has no cancellation backends");
        }
        // A route leaves the registry, and releases its addresses, with its result ids.
        if (resultIds.isEmpty()) {
            throw new IllegalArgumentException("Flight query has no result ids");
        }
        // Keep only cancellation addresses, not the coordinator, scan state, or query queue slot.
        // Ordinary Flight coordinators are unregistered when GetFlightInfo returns.
        Route route = new Route(queryId.deepCopy(),
                resultIds.stream().map(TUniqueId::deepCopy).distinct().collect(Collectors.toList()),
                backends.stream().map(TNetworkAddress::deepCopy).distinct().collect(Collectors.toList()),
                nanoClock.getAsLong() + TimeUnit.SECONDS.toNanos(Math.max(0L, timeoutSeconds) + 5));
        synchronized (this) {
            for (TUniqueId resultId : route.resultIds) {
                Route previous = results.put(resultId, route);
                if (previous != null) {
                    unrouteOne(previous);
                }
            }
            route.routedIds = route.resultIds.size();
            routedBackends += route.backends.size();
            // Whole routes leave from the eldest end: the expired ones there, then as many as the bounds require.
            long now = nanoClock.getAsLong();
            while (!results.isEmpty()) {
                Route eldest = results.values().iterator().next();
                if (results.size() <= resultCapacity && routedBackends <= backendCapacity
                        && !eldest.isExpired(now)) {
                    break;
                }
                remove(eldest);
            }
        }
    }

    public TStatus cancel(TUniqueId resultId) {
        Route route;
        synchronized (this) {
            route = results.get(resultId);
            if (route != null && route.isExpired(nanoClock.getAsLong())) {
                remove(route);
                route = null;
            }
        }
        if (route == null) {
            return new TStatus(TStatusCode.NOT_FOUND);
        }
        Status reason = new Status(TStatusCode.CANCELLED, "Arrow Flight stream closed before EOF");
        TStatus status = new TStatus(TStatusCode.OK);
        List<Future<PCancelPlanFragmentResult>> futures = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        for (TNetworkAddress backend : route.backends) {
            try {
                // The existing RPC is understood by older result BEs and bypasses the Flight read pool.
                futures.add(BackendServiceProxy.getInstance()
                        .cancelPipelineXPlanFragmentAsync(backend, route.queryId, reason));
            } catch (Exception e) {
                LOG.warn("Failed to send Flight cancellation for {} to {}", route.queryId, backend, e);
                status = new TStatus(TStatusCode.INTERNAL_ERROR);
            }
        }
        for (Future<PCancelPlanFragmentResult> future : futures) {
            try {
                PCancelPlanFragmentResult response = future.get(Math.max(0L, deadline - System.nanoTime()),
                        TimeUnit.NANOSECONDS);
                if (!response.hasStatus()) {
                    status = new TStatus(TStatusCode.INTERNAL_ERROR);
                } else if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                    status = new TStatus(new Status(response.getStatus()).getErrorCode());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new TStatus(TStatusCode.CANCELLED);
            } catch (Exception e) {
                LOG.warn("Failed to complete Flight cancellation for {}", route.queryId, e);
                status = new TStatus(TStatusCode.INTERNAL_ERROR);
            }
        }
        if (status.getStatusCode() == TStatusCode.OK) {
            // An unsuccessful attempt must remain routable for the caller's bounded retry.
            synchronized (this) {
                remove(route);
            }
        }
        return status;
    }

    synchronized int routedResults() {
        return results.size();
    }

    synchronized long routedBackends() {
        return routedBackends;
    }

    // Removes the route's result ids that still map to it. Guarded by this.
    private void remove(Route route) {
        for (TUniqueId id : route.resultIds) {
            if (results.remove(id, route)) {
                unrouteOne(route);
            }
        }
    }

    // One result id no longer maps to the route: its addresses leave with its last one. Guarded by this.
    private void unrouteOne(Route route) {
        if (--route.routedIds == 0) {
            routedBackends -= route.backends.size();
        }
    }

    private static final class Route {
        private final TUniqueId queryId;
        private final List<TUniqueId> resultIds;
        private final List<TNetworkAddress> backends;
        private final long deadlineNanos;
        // The result ids that still map to this route. Guarded by the registry.
        private int routedIds;

        private Route(TUniqueId queryId, List<TUniqueId> resultIds,
                List<TNetworkAddress> backends, long deadlineNanos) {
            this.queryId = queryId;
            this.resultIds = resultIds;
            this.backends = backends;
            this.deadlineNanos = deadlineNanos;
        }

        private boolean isExpired(long nowNanos) {
            return nowNanos - deadlineNanos >= 0;
        }
    }
}
