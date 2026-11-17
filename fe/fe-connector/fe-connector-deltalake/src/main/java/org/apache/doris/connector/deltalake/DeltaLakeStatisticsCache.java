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
import java.util.OptionalLong;
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
import java.util.function.LongConsumer;

/**
 * Nonblocking, user/table-identity/version-scoped statistics and observed file counts.
 * Cache ownership also owns the pending work:
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
        return get(SnapshotKey.of(session.getUser(), handle), fallback, loader(handle, runtime));
    }

    Optional<ConnectorTableStatistics> get(SnapshotKey key, Optional<ConnectorTableStatistics> fallback,
            Callable<Statistics> loader) {
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
        return completed(key, load).map(Statistics::tableStatistics).or(() -> fallback);
    }

    OptionalLong cachedFileCount(ConnectorSession session, DeltaLakeTableHandle handle) {
        // A planning hint must not create work, wait for IO, or share an authorization-sensitive result
        // without a user identity. Reuse the exact same versioned entry as table statistics.
        return session == null ? OptionalLong.empty() : cachedFileCount(SnapshotKey.of(session.getUser(), handle));
    }

    OptionalLong cachedFileCount(SnapshotKey key) {
        Load load = cache.getIfPresent(key);
        if (load == null) {
            return OptionalLong.empty();
        }
        long observed = load.observedFileCount;
        return observed >= 0 ? OptionalLong.of(observed)
                : completed(key, load).map(value -> OptionalLong.of(value.fileCount())).orElseGet(OptionalLong::empty);
    }

    LongConsumer fileCountObserver(ConnectorSession session, DeltaLakeTableHandle handle, DeltaLakeRuntime runtime) {
        return session == null ? count -> { }
                : fileCountObserver(SnapshotKey.of(session.getUser(), handle), loader(handle, runtime));
    }

    LongConsumer fileCountObserver(SnapshotKey key, Callable<Statistics> loader) {
        if (!cache.isEnabled()) {
            return count -> { };
        }
        // Capture the entry before scanning. A late report can update only this entry, never replace
        // a newer entry after REFRESH/eviction. Creating it does not submit its statistics task.
        Load load = cache.get(key, ignored -> new Load(key, loader));
        return count -> load.observedFileCount = count;
    }

    private static Callable<Statistics> loader(DeltaLakeTableHandle handle, DeltaLakeRuntime runtime) {
        DeltaLakeTableHandle coordinates = handle.withoutSnapshot();
        return () -> runtime.execute(() -> collect(coordinates.snapshot(runtime.engine), runtime.engine));
    }

    private Optional<Statistics> completed(SnapshotKey key, Load load) {
        if (!load.task.isDone()) {
            return Optional.empty();
        }
        try {
            return Optional.of(load.task.get());
        } catch (CancellationException e) {
            return Optional.empty();
        } catch (ExecutionException e) {
            // Failed loads are retryable, and cannot remove a newer entry installed after REFRESH.
            cache.compareAndSet(key, load, null);
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisConnectorException("Interrupted retrieving Delta statistics", e);
        }
    }

    static Statistics collect(Snapshot snapshot, Engine engine) throws IOException {
        DeltaLakeFeatureCheck.check(snapshot);
        DeltaLakeRowCount rows = new DeltaLakeRowCount();
        long[] bytes = {0};
        long[] files = {0};
        DeltaLakeScanFiles.forEach(snapshot, engine, true, file -> {
            rows.add(file);
            bytes[0] = Math.addExact(bytes[0], DeltaKernelCompat.fileStatus(file).getSize());
            files[0]++;
        });
        return new Statistics(new ConnectorTableStatistics(rows.value().orElse(-1), bytes[0]), files[0]);
    }

    record Statistics(ConnectorTableStatistics tableStatistics, long fileCount) {
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
        private final FutureTask<Statistics> task;
        private volatile long observedFileCount = -1;

        private Load(SnapshotKey key, Callable<Statistics> loader) {
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
