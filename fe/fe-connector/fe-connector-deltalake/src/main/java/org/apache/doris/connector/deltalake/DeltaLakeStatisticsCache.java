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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.deltalake.DeltaLakeSnapshotCache.SnapshotKey;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Nonblocking, user/table-identity/version-scoped statistics. Cache ownership also owns the pending work:
 * REFRESH, eviction and catalog close cancel the old task, which can never publish into a replacement entry.
 */
final class DeltaLakeStatisticsCache {
    static final String STATISTICS = "statistics";
    private static final Logger LOG = LogManager.getLogger(DeltaLakeStatisticsCache.class);
    // Shared by this plugin's catalogs; never let a cold collection of large tables exhaust FE workers.
    private static final ThreadPoolExecutor EXECUTOR = executor();
    private final MetaCache<SnapshotKey, Load> cache;
    private final Executor executor;

    DeltaLakeStatisticsCache(CatalogMetaCache owner, Map<String, String> properties) {
        this(owner, properties, EXECUTOR);
    }

    DeltaLakeStatisticsCache(CatalogMetaCache owner, Map<String, String> properties, Executor executor) {
        this.executor = executor;
        cache = owner.create(MetaCacheDefinition.<SnapshotKey, Load>builder("deltalake-statistics",
                        DeltaLakeSnapshotCache.spec(properties, STATISTICS), SnapshotKey::scope)
                // The callable retains only immutable table coordinates and catalog-owned services.
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512 + 2 * key.estimatedBytes()))
                .removalListener((key, value, reason) -> cancel(value))
                .discardListener((key, value) -> cancel(value))
                .build());
    }

    Optional<ConnectorTableStatistics> get(ConnectorSession session, DeltaLakeTableHandle handle,
            DeltaLakeRuntime runtime) {
        Optional<ConnectorTableStatistics> fallback =
                DeltaKernelCompat.tableStatistics(handle.snapshot(runtime.engine));
        if (fallback.isPresent() && fallback.get().getRowCount() == 0) {
            return fallback;
        }
        if (session == null || !cache.isEnabled()) {
            // Without an identity there is no authorization-safe cross-query cache key.
            return fallback;
        }
        DeltaLakeTableHandle coordinates = handle.withoutSnapshot();
        return get(SnapshotKey.of(session.getUser(), handle), fallback,
                () -> runtime.execute(() -> collect(coordinates.snapshot(runtime.engine), runtime.engine)));
    }

    Optional<ConnectorTableStatistics> get(SnapshotKey key, Optional<ConnectorTableStatistics> fallback,
            Callable<ConnectorTableStatistics> loader) {
        if (!cache.isEnabled()) {
            return fallback;
        }
        Load load = cache.get(key, ignored -> new Load(key, loader));
        if (!load.task.isCancelled() && load.started.compareAndSet(false, true)) {
            try {
                // Submission is outside the cache's publication guards. A full queue returns the fallback;
                // CallerRunsPolicy would turn a supposedly asynchronous million-file scan into planning IO.
                executor.execute(load.task);
            } catch (RejectedExecutionException e) {
                cache.compareAndSet(key, load, null);
                LOG.debug("Delta statistics queue is full; using size estimate");
                return fallback;
            }
        }
        if (!load.task.isDone()) {
            return fallback;
        }
        try {
            return Optional.of(load.task.get());
        } catch (CancellationException e) {
            return fallback;
        } catch (ExecutionException e) {
            // Failed loads are retryable, and cannot remove a newer entry installed after REFRESH.
            cache.compareAndSet(key, load, null);
            return fallback;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisConnectorException("Interrupted retrieving Delta statistics", e);
        }
    }

    static ConnectorTableStatistics collect(Snapshot snapshot, Engine engine) throws IOException {
        DeltaLakeFeatureCheck.check(snapshot);
        DeltaLakeRowCount rows = new DeltaLakeRowCount();
        long[] bytes = {0};
        DeltaLakeScanFiles.forEach(snapshot, engine, true, file -> {
            rows.add(file);
            bytes[0] = Math.addExact(bytes[0], DeltaKernelCompat.fileStatus(file).getSize());
        });
        return new ConnectorTableStatistics(rows.value().orElse(-1), bytes[0]);
    }

    private void cancel(Load load) {
        load.task.cancel(true);
        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).remove(load.task);
        }
    }

    private static ThreadPoolExecutor executor() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(64), action -> {
                    Thread thread = new Thread(action, "deltalake-statistics");
                    thread.setDaemon(true);
                    thread.setContextClassLoader(DeltaLakeStatisticsCache.class.getClassLoader());
                    return thread;
                });
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private static final class Load {
        private final AtomicBoolean started = new AtomicBoolean();
        private final FutureTask<ConnectorTableStatistics> task;

        private Load(SnapshotKey key, Callable<ConnectorTableStatistics> loader) {
            task = new FutureTask<>(() -> {
                try {
                    return loader.call();
                } catch (Exception | Error e) {
                    LOG.warn("Failed to load Delta table statistics for {}", key, e);
                    throw e;
                }
            });
        }
    }
}
