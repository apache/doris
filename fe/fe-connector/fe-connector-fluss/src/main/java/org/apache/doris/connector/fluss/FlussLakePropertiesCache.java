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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The lake half's scan-node properties of a union read, kept across statements for a short time.
 *
 * <p>Building them has the paimon sibling read the lake table's schema and snapshot files and serialize the
 * table: tens of milliseconds of object-store reads per statement. The engine asks for scan-node properties
 * before it attaches the filter, so every statement on a partitioned primary-key table paid that, even one
 * then planned as a point lookup that reads no lake at all. Entries are keyed by the lake snapshot the read is
 * pinned to, so a tiering commit is seen at once; a schema change without one is seen once its entry expires.
 */
final class FlussLakePropertiesCache {

    private record Entry(Map<String, String> properties, long expiresAtNanos) {
    }

    private final long ttlNanos;
    private volatile ConcurrentHashMap<List<Object>, Entry> entries = new ConcurrentHashMap<>();

    FlussLakePropertiesCache(long ttlSec) {
        this.ttlNanos = TimeUnit.SECONDS.toNanos(ttlSec);
    }

    Map<String, String> get(List<Object> key, Supplier<Map<String, String>> load) {
        if (ttlNanos <= 0) {
            return load.get();
        }
        ConcurrentHashMap<List<Object>, Entry> current = entries;
        long now = System.nanoTime();
        Entry entry = current.get(key);
        if (entry != null && now - entry.expiresAtNanos() < 0) {
            return entry.properties();
        }
        Map<String, String> properties = Collections.unmodifiableMap(new LinkedHashMap<>(load.get()));
        // Swept on a load, which comes at most once a TTL for each key, so a table read once is not kept.
        current.values().removeIf(expired -> now - expired.expiresAtNanos() >= 0);
        current.put(key, new Entry(properties, System.nanoTime() + ttlNanos));
        while (current.size() > 4096) {
            current.keySet().stream().findFirst().ifPresent(current::remove);
        }
        return properties;
    }

    synchronized void invalidateAll() {
        // Drop the generation as well: an older lake-property load must not undo a REFRESH.
        entries = new ConcurrentHashMap<>();
    }
}
