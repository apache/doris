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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.Status;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class FlightSqlQueryCancellation {
    private static final Logger LOG = LogManager.getLogger(FlightSqlQueryCancellation.class);
    public static final FlightSqlQueryCancellation INSTANCE = new FlightSqlQueryCancellation(Ticker.systemTicker());
    private final Cache<TUniqueId, Route> results;

    FlightSqlQueryCancellation(Ticker ticker) {
        results = Caffeine.newBuilder().ticker(ticker).scheduler(Scheduler.systemScheduler())
                .expireAfter(new Expiry<TUniqueId, Route>() {
                    @Override
                    public long expireAfterCreate(TUniqueId key, Route route, long now) {
                        return route.ttlNanos;
                    }

                    @Override
                    public long expireAfterUpdate(TUniqueId key, Route route, long now, long duration) {
                        return route.ttlNanos;
                    }

                    @Override
                    public long expireAfterRead(TUniqueId key, Route route, long now, long duration) {
                        return duration;
                    }
                }).build();
    }

    public void register(TUniqueId queryId, List<TUniqueId> resultIds,
            List<TNetworkAddress> backends, int timeoutSeconds) {
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("Flight query has no cancellation backends");
        }
        // Keep only cancellation addresses, not the coordinator, scan state, or query queue slot.
        // Ordinary Flight coordinators are unregistered when GetFlightInfo returns.
        Route route = new Route(queryId.deepCopy(),
                resultIds.stream().map(TUniqueId::deepCopy).distinct().collect(Collectors.toList()),
                backends.stream().map(TNetworkAddress::deepCopy).distinct().collect(Collectors.toList()),
                TimeUnit.SECONDS.toNanos(Math.max(0L, timeoutSeconds) + 5));
        for (TUniqueId resultId : route.resultIds) {
            results.put(resultId, route);
        }
    }

    public void unregister(List<TUniqueId> resultIds) {
        results.invalidateAll(resultIds);
    }

    public TStatus cancel(TUniqueId resultId) {
        Route route = results.getIfPresent(resultId);
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
            for (TUniqueId id : route.resultIds) {
                results.asMap().remove(id, route);
            }
        }
        return status;
    }

    private static final class Route {
        private final TUniqueId queryId;
        private final List<TUniqueId> resultIds;
        private final List<TNetworkAddress> backends;
        private final long ttlNanos;

        private Route(TUniqueId queryId, List<TUniqueId> resultIds,
                List<TNetworkAddress> backends, long ttlNanos) {
            this.queryId = queryId;
            this.resultIds = resultIds;
            this.backends = backends;
            this.ttlNanos = ttlNanos;
        }
    }
}
