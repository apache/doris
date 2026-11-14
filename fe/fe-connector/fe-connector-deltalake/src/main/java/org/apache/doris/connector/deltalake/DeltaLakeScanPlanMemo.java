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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Planning-only values: no snapshots, Kernel rows, iterators, sessions or resources to close. */
final class DeltaLakeScanPlanMemo {
    static final String NAMESPACE = "deltalake.scan-plans";
    private final int maxEntries;
    private final int maxRanges;
    private final long maxRangeBytes;
    private final Map<Key, CompletableFuture<Plan>> plans = new HashMap<>();
    private int retainedRanges;
    private long retainedRangeBytes;

    DeltaLakeScanPlanMemo() {
        this(128, 16_384, 16L << 20);
    }

    DeltaLakeScanPlanMemo(int maxEntries, int maxRanges, long maxRangeBytes) {
        this.maxEntries = maxEntries;
        this.maxRanges = maxRanges;
        this.maxRangeBytes = maxRangeBytes;
    }

    Outcome get(Key key, ConnectorScanContext context, Supplier<Plan> loader) {
        checkCancelled(context);
        CompletableFuture<Plan> future;
        boolean owner = false;
        synchronized (this) {
            future = plans.get(key);
            if (future == null && plans.size() < maxEntries) {
                future = new CompletableFuture<>();
                plans.put(key, future);
                owner = true;
            }
        }
        if (future == null) {
            Plan plan = loader.get();
            checkCancelled(context);
            return new Outcome(plan, false);
        }
        if (!owner) {
            return new Outcome(await(future, context), true);
        }
        // Never load under the scope's computeIfAbsent or this memo's monitor: loading also
        // resolves statement file counts and COUNT summaries, and unrelated scans may run in parallel.
        try {
            Plan plan = loader.get();
            checkCancelled(context);
            long bytes = rangeBytes(plan);
            synchronized (this) {
                if (plan.ranges.size() <= maxRanges - retainedRanges
                        && bytes <= maxRangeBytes - retainedRangeBytes) {
                    retainedRanges += plan.ranges.size();
                    retainedRangeBytes += bytes;
                } else {
                    plans.remove(key);
                }
            }
            future.complete(plan);
            return new Outcome(plan, false);
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                plans.remove(key);
            }
            future.completeExceptionally(failure);
            throw failure;
        }
    }

    private long rangeBytes(Plan plan) {
        if (plan.ranges.size() > maxRanges) {
            return Long.MAX_VALUE;
        }
        long bytes = 64L + 8L * plan.ranges.size();
        for (ConnectorScanRange range : plan.ranges) {
            bytes += ((DeltaLakeScanRange) range).estimatedRetainedBytes();
            if (bytes > maxRangeBytes) {
                return bytes;
            }
        }
        return bytes;
    }

    private static Plan await(CompletableFuture<Plan> future, ConnectorScanContext context) {
        while (true) {
            checkCancelled(context);
            try {
                Plan plan = future.get(50, TimeUnit.MILLISECONDS);
                checkCancelled(context);
                return plan;
            } catch (TimeoutException ignored) {
                // A cancelled waiter must leave without cancelling the shared producer.
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Delta scan planning interrupted");
            } catch (ExecutionException failure) {
                if (failure.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) failure.getCause();
                }
                throw (Error) failure.getCause();
            }
        }
    }

    static void checkCancelled(ConnectorScanContext context) {
        context.checkCancelled();
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Delta scan planning interrupted");
        }
    }

    // Projection, LIMIT, aggregate/explain flags and backend capabilities do not affect Delta's
    // file enumeration. Schema/column mapping and DV descriptors are fixed by table ID + version.
    record Key(String user, String database, String table, String path, String tableId, long version,
            Optional<ConnectorExpression> filter, Set<String> partitions, boolean prunedToEmpty,
            boolean countPushdown, long splitSize, String timeZone) {
        static Key of(ConnectorSession session, ConnectorScanRequest request, long splitSize) {
            DeltaLakeTableHandle table = (DeltaLakeTableHandle) request.getTableHandle();
            return new Key(session.getUser(), table.getDatabase(), table.getTable(), table.getPath(),
                    table.getTableId(), table.getVersion(), request.getFilter(),
                    Set.copyOf(request.getRequiredPartitions()), request.isPartitionsPrunedToEmpty(),
                    request.isCountPushdown(), splitSize, session.getTimeZone());
        }
    }

    record Plan(List<ConnectorScanRange> ranges, DeltaLakeScanMetrics.Result metrics) {
        Plan {
            // The loader owns the list and never mutates it after publication. Avoid copying an
            // oversized synchronous plan just to find that it cannot be retained.
            ranges = Collections.unmodifiableList(ranges);
        }
    }

    record Outcome(Plan plan, boolean reused) {
    }
}
