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

package org.apache.doris.fluss;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Table infos kept for a short time from one scan range to the next, for every range this BE reads.
 *
 * <p>{@code Connection#getTable} refreshes the table's metadata and asks for its info, and a fluss server
 * answers both from ZooKeeper. A point lookup is one range, so under concurrent lookups those two requests
 * per query queued on the tablet server in front of the lookups themselves. A range now builds its table
 * from an info another range loaded within the TTL, and leaves the bucket metadata to the fluss reader,
 * which fetches what its connection is missing and refreshes what a server rejects.
 *
 * <p>A dropped, recreated or altered table is seen once its entry expires; a range that needs a column its
 * entry lacks reloads it at once ({@link #reload}). Keyed by the whole client configuration, like
 * {@link FlussConnectionPool}, so catalogs on different clusters never share an entry; the key can hold
 * credentials and is never logged.
 */
final class FlussTableInfoCache {

    /** The catalog's {@code fluss.metadata.cache.ttl.sec}, which FE keeps its table infos for as well. */
    static final String TTL_PROPERTY = "fluss.client.metadata.cache.ttl.sec";
    static final long DEFAULT_TTL_SEC = 10;

    static final FlussTableInfoCache INSTANCE = new FlussTableInfoCache(System::nanoTime);

    private final LongSupplier nanoTime;
    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();

    FlussTableInfoCache(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    /** The info loaded for this table within {@code ttlNanos}, or a new one from {@code load}. */
    TableInfo get(Map<String, String> config, TablePath tablePath, long ttlNanos, Supplier<TableInfo> load) {
        if (ttlNanos <= 0) {
            return load.get();
        }
        Entry entry = entries.get(new Key(config, tablePath));
        if (entry != null && nanoTime.getAsLong() - entry.expiresAtNanos < 0) {
            return entry.info;
        }
        return reload(config, tablePath, ttlNanos, load);
    }

    /** A new info from {@code load}, kept for {@code ttlNanos} in place of whatever was kept before. */
    TableInfo reload(Map<String, String> config, TablePath tablePath, long ttlNanos, Supplier<TableInfo> load) {
        long now = nanoTime.getAsLong();
        TableInfo info = load.get();
        // Swept on a load, which comes at most once a TTL for each table read, so a table read once is not
        // kept for the life of the process.
        entries.values().removeIf(expired -> now - expired.expiresAtNanos >= 0);
        if (ttlNanos > 0) {
            entries.put(new Key(config, tablePath), new Entry(info, nanoTime.getAsLong() + ttlNanos));
        }
        while (entries.size() > 4096) {
            entries.keySet().stream().findFirst().ifPresent(entries::remove);
        }
        return info;
    }

    /** How many entries are kept, expired ones included until the next load sweeps them. */
    int size() {
        return entries.size();
    }

    private static final class Key {
        private final Map<String, String> config;
        private final TablePath tablePath;

        private Key(Map<String, String> config, TablePath tablePath) {
            this.config = Collections.unmodifiableMap(new HashMap<>(config));
            this.tablePath = tablePath;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key other = (Key) o;
            return config.equals(other.config) && tablePath.equals(other.tablePath);
        }

        @Override
        public int hashCode() {
            return 31 * config.hashCode() + tablePath.hashCode();
        }
    }

    private static final class Entry {
        private final TableInfo info;
        private final long expiresAtNanos;

        private Entry(TableInfo info, long expiresAtNanos) {
            this.info = info;
            this.expiresAtNanos = expiresAtNanos;
        }
    }
}
