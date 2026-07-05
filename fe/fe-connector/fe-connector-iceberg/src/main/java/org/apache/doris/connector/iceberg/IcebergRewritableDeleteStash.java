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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Per-catalog stash that carries the merge-on-read "rewritable delete" supply across the scan→write seam of a
 * single row-level DML (DELETE / UPDATE / MERGE) on a format-version&ge;3 iceberg table.
 *
 * <p><b>Why a singleton stash.</b> When a DML rewrites the deletes of a data file, the BE writes a NEW deletion
 * vector that must OR-merge that file's still-live old deletes (old DVs + old position deletes) — otherwise the
 * previously-deleted rows resurrect. The BE does not read iceberg metadata; the FE must hand it, per touched
 * data file, the list of old non-equality delete files via {@code rewritable_delete_file_sets}
 * ({@code TIcebergDeleteSink}/{@code TIcebergMergeSink}). The legacy fe-core path collected this from the
 * {@code IcebergScanNode}'s own fields at sink-finalize time (plan-scoped, GC'd with the plan). In the plugin
 * SPI the scan-plan provider and the write-plan provider are mutually-unaware, used-once objects; the only
 * cross-scan→write survivor is the long-lived per-catalog {@link IcebergConnector}. So the scan provider stashes
 * the supply here keyed by the statement's stable {@code queryId} ({@code ConnectorSession.getQueryId()} =
 * {@code DebugUtil.printId(ctx.queryId())}, identical across the scan and write sessions of one statement), and
 * the write provider retrieves it by the same key.
 *
 * <p><b>Key = RAW data-file path.</b> The map is keyed on the un-normalized {@code dataFile.path()} string
 * ({@link IcebergScanRange#getOriginalPath()}), because the BE matches a rewritable set to the file it is writing
 * a DV for by exact string equality against that raw path (mirrors legacy
 * {@code IcebergScanNode.deleteFilesByReferencedDataFile} keyed on {@code getOriginalPath()}). Keying on the
 * scheme-normalized open path would silently miss every lookup → resurrection.
 *
 * <p><b>Eviction.</b> The primary removal is the write provider's eager {@link #retrieveAndRemove} per statement
 * (covers every DML write — DELETE/MERGE consume the supply, INSERT/OVERWRITE simply discard it). A statement
 * that scans a v3-delete table but never writes (a plain {@code SELECT}, or a DML that errors before the write
 * is planned) leaves a leaked entry that {@link #retrieveAndRemove} never reaches; a lazy TTL sweep (run when a
 * new query is first seen) ages those out. Unlike a value cache, the sweep removes ONLY entries untouched for
 * longer than the TTL — never a live entry (a statement's scan→write gap is milliseconds, far below the TTL), so
 * a live supply is never dropped (which would itself resurrect rows). With a unique per-statement queryId a
 * leaked entry is unreachable (no later statement reuses the key), so a leak is bounded memory, not a stale read.
 */
final class IcebergRewritableDeleteStash {

    // Leak backstop only: a statement's scan→write gap is milliseconds (both happen in one planning pass, before
    // BE execution), so this TTL is orders of magnitude larger than any live entry's lifetime — it ages out only
    // leaked plain-SELECT / aborted-DML entries.
    private static final long DEFAULT_TTL_SECONDS = 300L;

    /** One in-flight statement's supply: rawDataFilePath -> its non-equality delete descs, plus a touch stamp. */
    private static final class Entry {
        // Concurrent because, defensively, a single statement could plan more than one iceberg scan (MERGE scans
        // both the target and the source table) — data-file paths are globally unique, so accumulating distinct
        // keys (and idempotently overwriting a split data file's identical list) is always safe.
        final Map<String, List<TIcebergDeleteFileDesc>> sets = new ConcurrentHashMap<>();
        volatile long lastTouchNanos;

        Entry(long nowNanos) {
            this.lastTouchNanos = nowNanos;
        }
    }

    private final Map<String, Entry> stash = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final LongSupplier nanoClock;

    IcebergRewritableDeleteStash() {
        this(DEFAULT_TTL_SECONDS, System::nanoTime);
    }

    /** Visible for testing: injectable TTL + clock so sweep is deterministic without sleeping. */
    IcebergRewritableDeleteStash(long ttlSeconds, LongSupplier nanoClock) {
        this.ttlNanos = TimeUnit.SECONDS.toNanos(Math.max(1L, ttlSeconds));
        this.nanoClock = nanoClock;
    }

    /**
     * Records, for {@code queryId}, the non-equality delete descs of one touched data file (keyed on its RAW
     * path). No-op when the queryId is blank (a null/absent {@code ConnectContext} coerces the queryId to "",
     * which would collide across concurrent statements — such a statement gets no supply, which is safe because a
     * real row-level DML always carries a non-null query id) or when there is nothing to supply. Splits of one
     * data file carry an identical delete list, so the per-path {@code put} is idempotent; two tables in one
     * statement contribute distinct paths.
     */
    void accumulate(String queryId, String rawDataFilePath, List<TIcebergDeleteFileDesc> deleteDescs) {
        if (queryId == null || queryId.isEmpty() || rawDataFilePath == null
                || deleteDescs == null || deleteDescs.isEmpty()) {
            return;
        }
        long now = nanoClock.getAsLong();
        Entry entry = stash.get(queryId);
        if (entry == null) {
            // First range of a not-yet-seen query: opportunistically age out leaked entries. Done OUTSIDE the
            // computeIfAbsent mapping function (ConcurrentHashMap forbids mutating other mappings from within it).
            sweepExpired(now);
            entry = stash.computeIfAbsent(queryId, k -> new Entry(now));
        }
        entry.lastTouchNanos = now;
        entry.sets.put(rawDataFilePath, deleteDescs);
    }

    /**
     * Returns and removes the supply map for {@code queryId} (the rewritable delete sets keyed by raw data-file
     * path), or {@code null} when none was stashed / the queryId is blank. The write provider calls this once per
     * statement; the remove is the primary eviction.
     */
    Map<String, List<TIcebergDeleteFileDesc>> retrieveAndRemove(String queryId) {
        if (queryId == null || queryId.isEmpty()) {
            return null;
        }
        Entry entry = stash.remove(queryId);
        return entry == null ? null : entry.sets;
    }

    /** Drops entries untouched for longer than the TTL — leaked plain-SELECT / aborted-DML supplies only. */
    private void sweepExpired(long nowNanos) {
        stash.entrySet().removeIf(e -> nowNanos - e.getValue().lastTouchNanos >= ttlNanos);
    }

    /** Test-only: current number of in-flight stashed statements. */
    int size() {
        return stash.size();
    }
}
