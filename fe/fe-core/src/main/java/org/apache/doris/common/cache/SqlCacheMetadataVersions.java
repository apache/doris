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

package org.apache.doris.common.cache;

import org.apache.doris.nereids.SqlCacheContext.FullTableName;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Process-local metadata revisions shared by SQL cache dependencies, independent of data versions. */
public final class SqlCacheMetadataVersions {
    private final AtomicLong changes = new AtomicLong();
    private final AtomicLong all = new AtomicLong();
    // A dependency snapshot retains its revision cells. Unused scopes can be collected without tombstones.
    private final Cache<Long, AtomicLong> tableIds = Caffeine.newBuilder().weakValues().build();
    private final Cache<FullTableName, AtomicLong> names = Caffeine.newBuilder().weakValues().build();

    public Snapshot snapshot() {
        return new Snapshot();
    }

    public void invalidateTable(long tableId, FullTableName name) {
        long revision = changes.incrementAndGet();
        if (tableId != -1L) {
            advance(tableIds.getIfPresent(tableId), revision);
        }
        if (name != null) {
            advance(names.getIfPresent(name), revision);
        }
    }

    public void invalidateNamespace(String catalog, String database) {
        long revision = changes.incrementAndGet();
        advance(names.getIfPresent(new FullTableName(catalog, database, null)), revision);
    }

    public void invalidateAll() {
        all.accumulateAndGet(changes.incrementAndGet(), Math::max);
    }

    private static void advance(AtomicLong version, long revision) {
        // Name-only/cold logs need not create revision cells when no query depends on that scope.
        if (version != null) {
            version.accumulateAndGet(revision, Math::max);
        }
    }

    /** One query's captured revisions; no query/context is registered with the revision owner. */
    public final class Snapshot {
        private final long start = changes.get();
        private final long allVersion = all.get();
        private final Map<AtomicLong, Long> dependencies = new LinkedHashMap<>();
        private boolean capturedBeforeChange = true;

        public void addTable(long tableId, FullTableName name) {
            // A refresh may retire an object before binding records its first dependency. Do not attach
            // a new revision to metadata the statement could have resolved before that refresh.
            capturedBeforeChange &= start == changes.get();
            capture(tableIds.get(tableId, ignored -> new AtomicLong()));
            capture(names.get(name, ignored -> new AtomicLong()));
            capture(names.get(new FullTableName(name.catalog, name.db, null), ignored -> new AtomicLong()));
            capture(names.get(new FullTableName(name.catalog, null, null), ignored -> new AtomicLong()));
            capturedBeforeChange &= start == changes.get();
        }

        private void capture(AtomicLong version) {
            dependencies.putIfAbsent(version, version.get());
        }

        public boolean isCurrent() {
            if (!capturedBeforeChange || allVersion != all.get()) {
                return false;
            }
            for (Map.Entry<AtomicLong, Long> dependency : dependencies.entrySet()) {
                if (dependency.getKey().get() != dependency.getValue()) {
                    return false;
                }
            }
            return true;
        }
    }
}
