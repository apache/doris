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

package org.apache.doris.connector.fluss;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Table infos of one catalog, kept for a short time across statements.
 *
 * <p>Every statement otherwise asks a fluss server for its table, and the server reads it from ZooKeeper:
 * under concurrent point lookups those requests queue on the server in front of the lookups themselves. A
 * dropped, recreated or altered table is seen once its entry expires.
 */
final class FlussTableInfoCache {

    static final String TTL_PROPERTY = "fluss.metadata.cache.ttl.sec";
    static final long DEFAULT_TTL_SEC = 10;

    private record Entry(TableInfo info, long loadedAtNanos) {
    }

    private static final int MAX_ENTRIES = 4096;

    private final LongSupplier nanoTime;
    private final long ttlNanos;
    private volatile ConcurrentHashMap<TablePath, Entry> entries = new ConcurrentHashMap<>();

    FlussTableInfoCache(long ttlSec) {
        this(ttlSec, System::nanoTime);
    }

    FlussTableInfoCache(long ttlSec, LongSupplier nanoTime) {
        this.ttlNanos = TimeUnit.SECONDS.toNanos(ttlSec);
        this.nanoTime = nanoTime;
    }

    TableInfo get(TablePath tablePath, Supplier<TableInfo> load) {
        if (ttlNanos <= 0) {
            return load.get();
        }
        ConcurrentHashMap<TablePath, Entry> current = entries;
        long now = nanoTime.getAsLong();
        Entry entry = current.get(tablePath);
        if (entry != null && now - entry.loadedAtNanos() < ttlNanos) {
            return entry.info();
        }
        // Coalesce concurrent misses. REFRESH swaps the map, so an in-flight load cannot repopulate it.
        Entry loaded = current.compute(tablePath, (key, previous) -> {
            long time = nanoTime.getAsLong();
            return previous != null && time - previous.loadedAtNanos() < ttlNanos
                    ? previous : new Entry(load.get(), nanoTime.getAsLong());
        });
        current.values().removeIf(value -> nanoTime.getAsLong() - value.loadedAtNanos() >= ttlNanos);
        while (current.size() > MAX_ENTRIES) {
            current.keySet().stream().findFirst().ifPresent(current::remove);
        }
        return loaded.info();
    }

    synchronized void invalidate(TablePath tablePath) {
        ConcurrentHashMap<TablePath, Entry> fresh = new ConcurrentHashMap<>(entries);
        fresh.remove(tablePath);
        entries = fresh;
    }

    synchronized void invalidateDb(String databaseName) {
        ConcurrentHashMap<TablePath, Entry> fresh = new ConcurrentHashMap<>(entries);
        fresh.keySet().removeIf(path -> path.getDatabaseName().equals(databaseName));
        entries = fresh;
    }

    synchronized void invalidateAll() {
        entries = new ConcurrentHashMap<>();
    }
}
