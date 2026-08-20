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

package org.apache.doris.connector.paimon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Background poller that heals the Paimon {@link CachingCatalog} after an <b>out-of-band</b> schema change —
 * an {@code ALTER} a foreign engine (Spark/Flink/…) commits directly against the Paimon table.
 *
 * <p><b>Why this is needed.</b> The connector's {@code invalidateTable}/{@code invalidateDb}/
 * {@code invalidateAll} hooks (see {@link PaimonConnector}) already evict the CachingCatalog's frozen
 * {@code Table} for a <i>Doris-issued</i> {@code ALTER} (the master's DDL hook and every follower's editlog
 * replay both route through {@code RefreshManager.refreshTableInternal -> connector.invalidateTable}), and
 * {@code REFRESH TABLE} reaches the same path. But an external engine's {@code ALTER} produces <b>no Doris
 * event at all</b>: every FE's CachingCatalog keeps serving the pre-{@code ALTER} {@code Table} — whose
 * {@code rowType()} the SCAN path serializes to the BE, where the JNI merged-read fails with "jni reader
 * fields' size {N} is not matched with paimon fields' size {M}" — until the (default 24h) access-TTL. The
 * user is not told to {@code REFRESH}, so the table simply looks broken.
 *
 * <p><b>Why polling.</b> Paimon exposes no schema-change event/listener API, so the only way to notice an
 * out-of-band {@code ALTER} is to look. This poller checks periodically and, on a divergence, calls the same
 * per-table eviction the {@code REFRESH TABLE} hook uses — so the next query reloads the fresh schema. It is
 * <b>opt-in</b> (a {@code <= 0} interval disables it, preserving the exact prior behavior) and off by default
 * on a fresh connector unless the operator sets a positive interval.
 *
 * <p><b>Cheap by design.</b> A poll does NOT re-load or re-deserialize any {@code Table} (that would defeat
 * the cache). For each table it compares two schema ids:
 * <ul>
 *   <li>the <b>held</b> id — {@code ((FileStoreTable) cachedTable).schema().id()}, a purely in-memory read of
 *       the schema the cached {@code Table} was frozen at (no I/O); and</li>
 *   <li>the <b>latest</b> id — {@code ((DataTable) cachedTable).schemaManager().latest().get().id()}, a LIVE
 *       read of just the schema directory's latest metadata file (the lightest live probe the SDK offers —
 *       there is no "return only the schema id" API — it reads one small JSON, never scans data).</li>
 * </ul>
 * They diverge exactly when an out-of-band {@code ALTER} bumped the schema id without the cached handle
 * knowing (a paimon {@code ALTER} bumps the schema id WITHOUT creating a snapshot). On a divergence the poller
 * evicts; otherwise it touches nothing.
 *
 * <p><b>Only active tables are polled.</b> The poll target is exactly {@code CachingCatalog.tableCache()}'s
 * current key set — the tables the CachingCatalog has actually loaded (i.e. been queried) — so a catalog with
 * hundreds of never-queried tables costs nothing here. An entry that Caffeine has since evicted simply is not
 * in the map, so it is not probed (and needs no eviction — it is already gone).
 *
 * <p><b>Lifecycle.</b> One daemon single-thread {@link ScheduledExecutorService} per connector, scheduled at a
 * fixed delay (each run starts {@code intervalSecond} after the previous run FINISHES, so a slow poll never
 * stacks up). {@link #close()} shuts it down, so the thread dies with the connector (a {@code REFRESH CATALOG}
 * / catalog drop rebuilds/closes the connector). The catalog is supplied lazily via a {@link Supplier} so the
 * poller never forces the (possibly failing) catalog build — a never-built catalog has nothing cached, so a
 * poll is a no-op.
 */
final class PaimonExternalChangePoller implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalChangePoller.class);

    /** Distinguishes each connector's poller thread across catalogs in a log/stack dump. */
    private static final AtomicInteger POLLER_SEQ = new AtomicInteger();

    private final String catalogName;
    private final long intervalSecond;
    /** Supplies the live catalog WITHOUT forcing its creation ({@code null} until first metadata access). */
    private final Supplier<Catalog> catalogSupplier;
    /** The per-table eviction to run on a detected out-of-band change (the {@code REFRESH TABLE} path). */
    private final BiConsumer<String, String> evictTable;
    private final ScheduledExecutorService scheduler;

    /**
     * @param catalogName     the Doris catalog name, for log/thread naming only
     * @param intervalSecond  poll period in seconds; {@code <= 0} means the poller never starts (disabled)
     * @param catalogSupplier supplies the live paimon {@code Catalog} lazily (may return {@code null} before
     *                        the catalog is first built), so the poller never triggers catalog creation
     * @param evictTable      the per-table eviction callback, invoked as {@code (dbName, tableName)} — wired to
     *                        {@link PaimonConnector#invalidateTable}, the same eviction {@code REFRESH TABLE} uses
     */
    PaimonExternalChangePoller(String catalogName, long intervalSecond, Supplier<Catalog> catalogSupplier,
            BiConsumer<String, String> evictTable) {
        this.catalogName = catalogName;
        this.intervalSecond = intervalSecond;
        this.catalogSupplier = catalogSupplier;
        this.evictTable = evictTable;
        this.scheduler = intervalSecond > 0 ? Executors.newSingleThreadScheduledExecutor(daemonThreadFactory())
                : null;
    }

    /** True when a positive interval was configured (i.e. this poller has a live scheduler). */
    boolean isEnabled() {
        return scheduler != null;
    }

    /**
     * Starts the background poll loop if enabled (a {@code <= 0} interval leaves this a no-op). Scheduled at a
     * fixed delay so a poll that overruns the interval never overlaps the next one. Idempotent enough for the
     * connector's single call site; not re-entrant (one connector starts it once).
     */
    void start() {
        if (scheduler == null) {
            return;
        }
        scheduler.scheduleWithFixedDelay(this::pollQuietly, intervalSecond, intervalSecond, TimeUnit.SECONDS);
        LOG.info("Started Paimon external-change poller for catalog {} (interval={}s)", catalogName, intervalSecond);
    }

    /** Wraps {@link #pollOnce} so a single poll failure never kills the scheduled task (which would silently
     * stop all future polls). Any error is logged and swallowed; the next tick tries again. */
    private void pollQuietly() {
        try {
            pollOnce();
        } catch (Throwable t) {
            // Never let an exception escape the scheduled task: scheduleWithFixedDelay cancels all future runs
            // once a task throws. A transient probe failure (e.g. a table dropped mid-poll) must not disable
            // the poller for the connector's lifetime.
            LOG.warn("Paimon external-change poll for catalog {} failed; will retry next tick", catalogName, t);
        }
    }

    /**
     * Probes every currently-cached table once and evicts each whose latest schema id has drifted from the
     * cached handle's held schema id (an out-of-band {@code ALTER}). Package-visible and synchronous so a test
     * can drive one detection deterministically without waiting on the scheduler.
     *
     * <p>Reads the catalog via the supplier and only acts on a {@link CachingCatalog}: a never-built catalog
     * ({@code null}) or a non-caching flavor has no frozen handles to heal, so both are no-ops. Snapshots the
     * table cache's entries up front (Caffeine's {@code asMap()} is a live view) and iterates the snapshot, so
     * an eviction (which mutates the same map) does not disturb the iteration.
     */
    void pollOnce() {
        Catalog catalog = catalogSupplier.get();
        if (!(catalog instanceof CachingCatalog)) {
            return;
        }
        // Copy the entry set first: evictTable mutates the same tableCache map, and Caffeine's asMap() is a
        // live view, so iterating it directly while evicting would risk a ConcurrentModificationException.
        Map<Identifier, Table> snapshot =
                new HashMap<>(((CachingCatalog) catalog).tableCache().asMap());
        for (Map.Entry<Identifier, Table> cached : snapshot.entrySet()) {
            Identifier id = cached.getKey();
            if (hasExternalSchemaChange(id, cached.getValue())) {
                LOG.info("Paimon external-change poller detected an out-of-band schema change on {}.{} in "
                                + "catalog {}; evicting the cached table so the next query reloads it",
                        id.getDatabaseName(), id.getTableName(), catalogName);
                evictTable.accept(id.getDatabaseName(), id.getTableName());
            }
        }
    }

    /**
     * Returns {@code true} iff the cached {@code table}'s frozen (held) schema id differs from the table's
     * latest schema id read live from its schema directory — i.e. an external engine committed an {@code ALTER}
     * this cached handle has not seen. Both ids are cheap: the held id is in-memory
     * ({@link FileStoreTable#schema()}), the latest id reads one small schema metadata file
     * ({@link DataTable#schemaManager()}{@code .latest()}).
     *
     * <p>Returns {@code false} (never a false eviction) when the ids cannot be compared: a non-{@code DataTable}
     * / non-{@code FileStoreTable} backend has no schema history, and {@code latest()} being empty means the
     * schema directory is momentarily unreadable (do not evict on a probe gap — the next tick retries). A probe
     * exception is caught and treated as "no change" so one bad table never aborts the whole poll.
     */
    private boolean hasExternalSchemaChange(Identifier id, Table table) {
        // The held id needs the FileStoreTable's frozen TableSchema; the latest id needs the DataTable's
        // schemaManager. A CachingCatalog data table is both, but guard rather than blind-cast so a system /
        // FormatTable entry (no schema history) is skipped, not thrown on.
        if (!(table instanceof FileStoreTable) || !(table instanceof DataTable)) {
            return false;
        }
        try {
            long heldSchemaId = ((FileStoreTable) table).schema().id();
            Optional<TableSchema> latest = ((DataTable) table).schemaManager().latest();
            // An empty latest() (schema dir momentarily unreadable) is a probe gap, not a change: do not evict.
            return latest.isPresent() && latest.get().id() != heldSchemaId;
        } catch (RuntimeException e) {
            LOG.debug("Skipping Paimon external-change probe for {}.{} in catalog {}: {}",
                    id.getDatabaseName(), id.getTableName(), catalogName, e.getMessage());
            return false;
        }
    }

    /** Daemon-thread factory so a lingering poller never keeps the FE JVM alive; named for diagnosability. */
    private ThreadFactory daemonThreadFactory() {
        return runnable -> {
            Thread thread = new Thread(runnable,
                    "paimon-external-change-poller-" + catalogName + "-" + POLLER_SEQ.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Stops the poll loop and releases the scheduler thread. Called from {@link PaimonConnector#close()} so the
     * poller's lifetime is bound to the connector's (a {@code REFRESH CATALOG} / catalog drop rebuilds/closes
     * the connector, and thus this poller). Idempotent and no-throw. Does NOT wait for an in-flight poll: an
     * eviction is idempotent, so a poll that races the shutdown is harmless.
     */
    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
