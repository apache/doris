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

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The partition lists the engine prunes over, reused only within one metadata scope.
 *
 * <p>The engine lists a partitioned table's partitions on every statement, before it knows the statement is a
 * point lookup whose key already names its partition. A fluss server reads every list from ZooKeeper, the
 * table and each of its partitions, and under concurrent lookups those lists queue on the server: the list,
 * not the lookup, sets the rate. Each per-statement admin ops owns this cache, so the first pruning list of
 * a new statement is always live. Sharing entries across statements could prune away a newly created
 * partition before scan planning can discover it. Scan planning still lists live partitions separately,
 * so a dropped partition is never planned. The TTL bounds reuse only inside the current metadata scope.
 */
final class FlussPartitionListCache {

    static final String TTL_PROPERTY = "fluss.partition.cache.ttl.sec";
    static final long DEFAULT_TTL_SEC = 5;

    private record Entry(List<PartitionInfo> partitions, long loadedAtNanos) {
    }

    private static final int MAX_ENTRIES = 4096;

    private final LongSupplier nanoTime;
    private final long ttlNanos;
    private volatile ConcurrentHashMap<TablePath, Entry> entries = new ConcurrentHashMap<>();

    FlussPartitionListCache(long ttlSec) {
        this(ttlSec, System::nanoTime);
    }

    FlussPartitionListCache(long ttlSec, LongSupplier nanoTime) {
        this.ttlNanos = TimeUnit.SECONDS.toNanos(ttlSec);
        this.nanoTime = nanoTime;
    }

    List<PartitionInfo> get(TablePath tablePath, Supplier<List<PartitionInfo>> load) {
        if (ttlNanos <= 0) {
            return load.get();
        }
        ConcurrentHashMap<TablePath, Entry> current = entries;
        long now = nanoTime.getAsLong();
        Entry entry = current.get(tablePath);
        if (entry != null && now - entry.loadedAtNanos() < ttlNanos) {
            return entry.partitions();
        }
        // Coalesce concurrent misses. Invalidation swaps the map so an old load cannot repopulate it.
        Entry loaded = current.compute(tablePath, (key, previous) -> {
            long time = nanoTime.getAsLong();
            return previous != null && time - previous.loadedAtNanos() < ttlNanos
                    ? previous : new Entry(Collections.unmodifiableList(new ArrayList<>(load.get())),
                            nanoTime.getAsLong());
        });
        current.values().removeIf(value -> nanoTime.getAsLong() - value.loadedAtNanos() >= ttlNanos);
        while (current.size() > MAX_ENTRIES) {
            current.keySet().stream().findFirst().ifPresent(current::remove);
        }
        return loaded.partitions();
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
