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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;

import com.google.common.annotations.VisibleForTesting;

/**
 * The refresh obligation seam of the synchronous Lance index mutation path (design v6 section
 * 6.4): what one mutation statement owes the FE's local caches once its outcome is classified.
 * The executor owns the obligation table (which outcome owes an invalidation and how a failed
 * one is reported); an implementation of this interface owns only the local invalidation act.
 *
 * <p>What can actually be stale after a committed index mutation - investigated facts, not
 * assumptions:
 *
 * <ul>
 * <li>The authoritative index metadata reads (SHOW INDEX, {@code lance_index_entries()}, the
 *     admission snapshot) are <b>uncached end to end</b>:
 *     {@code LanceCatalogClient#inspectTableIndexes} resolves table access uncached and opens a
 *     fresh, independent Dataset per read at storage-latest, so no FE cache can serve a stale
 *     index listing and none is invalidated here.
 * <li>The Doris-side, table-scoped caches that hold data derived from one dataset version -
 *     the external schema cache, the external row-count cache, and the Lance table-access
 *     cache - are fenced by one purely local call,
 *     {@link ExternalMetaCacheMgr#invalidateTableCache(ExternalTable)}: the same fence
 *     REFRESH TABLE applies for a typed table. It performs no network read; this is the
 *     invalidation the production refresher performs, with a bounded retry.
 * <li>The native Lance Session's metadata/index caches (the catalog client's shared Session)
 *     are keyed by dataset version and cannot be invalidated per table from the FE; a
 *     committed mutation produces new dataset versions, which are new keys, so no stale index
 *     metadata is reachable through them. The catalog-wide Session rotation of REFRESH
 *     CATALOG is a different, heavier operation and is deliberately not part of this
 *     obligation.
 * </ul>
 */
public interface LanceIndexMutationRefresher {

    /** A refresher that performs nothing; for tests and for requests carrying no table. */
    LanceIndexMutationRefresher NOOP = () -> null;

    /**
     * Performs this statement's local table-metadata invalidation.
     *
     * @return null when the local invalidation completed; otherwise a bounded human-readable
     *         reason (never a raw provider payload) describing the failure after all bounded
     *         retries
     */
    String invalidateTableMetadata();

    /**
     * The production refresher: {@link ExternalMetaCacheMgr#invalidateTableCache(ExternalTable)}
     * with a bounded retry (the invalidation is local and non-blocking, so the retry needs no
     * backoff - it only rides out a transient manager state, such as a catalog just being
     * closed).
     */
    static LanceIndexMutationRefresher forTable(ExternalTable table) {
        return new LocalTableInvalidation(table);
    }

    /**
     * Bounded-retry local invalidation of one external table's Doris-side caches. Never throws:
     * the caller decides how a failure is reported (downgraded outcome, typed refresh error, or
     * attached diagnostic), so this class only reports.
     */
    @VisibleForTesting
    final class LocalTableInvalidation implements LanceIndexMutationRefresher {
        @VisibleForTesting
        static final int MAX_ATTEMPTS = 3;

        private final ExternalTable table;

        private LocalTableInvalidation(ExternalTable table) {
            this.table = table;
        }

        @Override
        public String invalidateTableMetadata() {
            Exception last = null;
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    Env env = Env.getCurrentEnv();
                    ExternalMetaCacheMgr manager = env == null ? null : env.getExtMetaCacheMgr();
                    if (manager == null) {
                        throw new IllegalStateException("the external metadata cache manager"
                                + " is not available");
                    }
                    // Purely local: row-count cache, the Lance table-access cache retirement,
                    // and the engine schema caches for this one table. No network read.
                    manager.invalidateTableCache(table);
                    return null;
                } catch (Exception e) {
                    last = e;
                }
            }
            return "local table metadata invalidation failed after " + MAX_ATTEMPTS + " attempts: "
                    + (last == null ? "unknown cause" : last.getClass().getSimpleName());
        }
    }
}
