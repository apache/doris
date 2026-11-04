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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * What the cache promises a scan range: the info another range loaded within the TTL, without asking the
 * cluster again; a fresh one once the TTL has passed, or when the range asks for it; and never one loaded
 * for another cluster. The clock is the test's, so none of this waits for a TTL to pass.
 */
public class FlussTableInfoCacheTest {

    private static final long TTL = TimeUnit.SECONDS.toNanos(10);
    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final Map<String, String> CLUSTER_A = Collections.singletonMap("bootstrap.servers", "a:9123");
    private static final Map<String, String> CLUSTER_B = Collections.singletonMap("bootstrap.servers", "b:9123");

    private final AtomicLong now = new AtomicLong();
    private final AtomicInteger loads = new AtomicInteger();
    private final FlussTableInfoCache cache = new FlussTableInfoCache(now::get);

    @Test
    public void rangeReusesTheInfoAnotherRangeLoaded() {
        TableInfo first = cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));
        now.addAndGet(TTL - 1);
        TableInfo second = cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 2));

        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, loads.get(), "loads");
    }

    @Test
    public void infoIsLoadedAgainOnceItsTtlHasPassed() {
        cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));
        now.addAndGet(TTL);

        Assertions.assertEquals(2, cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 2)).getTableId());
        Assertions.assertEquals(2, loads.get(), "loads");
    }

    /** A table recreated under the same name on another cluster is another table. */
    @Test
    public void catalogsWithOtherSettingsDoNotShareAnInfo() {
        cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));

        Assertions.assertEquals(2, cache.get(CLUSTER_B, TABLE, TTL, load(TABLE, 2)).getTableId());
        Assertions.assertEquals(2, loads.get(), "loads");
    }

    /** What a range does when the kept info lacks a column it was planned with. */
    @Test
    public void reloadReplacesTheKeptInfo() {
        cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));
        cache.reload(CLUSTER_A, TABLE, TTL, load(TABLE, 2));

        Assertions.assertEquals(2, cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 3)).getTableId());
        Assertions.assertEquals(2, loads.get(), "loads");
    }

    @Test
    public void failedLoadKeepsNothing() {
        Supplier<TableInfo> failing = () -> {
            loads.incrementAndGet();
            throw new IllegalStateException("cluster unavailable");
        };
        Assertions.assertThrows(IllegalStateException.class, () -> cache.get(CLUSTER_A, TABLE, TTL, failing));

        Assertions.assertEquals(1, cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1)).getTableId());
        Assertions.assertEquals(2, loads.get(), "loads");
    }

    /** The cache lives as long as the BE: a table read once must not stay in it. */
    @Test
    public void expiredInfosAreDroppedByTheNextLoad() {
        TablePath other = TablePath.of("db", "other");
        cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));
        now.addAndGet(TTL);
        cache.get(CLUSTER_A, other, TTL, load(other, 2));

        Assertions.assertEquals(1, cache.size(), "entries kept");
    }

    @Test
    public void zeroTtlBypassesAnAlreadyWarmEntry() {
        cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 1));
        Assertions.assertEquals(2, cache.get(CLUSTER_A, TABLE, 0, load(TABLE, 2)).getTableId());
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void changingTheCallerConfigurationDoesNotRewriteAnExistingKey() {
        Map<String, String> mutable = new HashMap<>(CLUSTER_A);
        cache.get(mutable, TABLE, TTL, load(TABLE, 1));
        mutable.put("bootstrap.servers", "b:9123");
        Assertions.assertEquals(2, cache.get(mutable, TABLE, TTL, load(TABLE, 2)).getTableId());
        Assertions.assertEquals(1, cache.get(CLUSTER_A, TABLE, TTL, load(TABLE, 3)).getTableId());
        Assertions.assertEquals(2, loads.get());
    }

    private Supplier<TableInfo> load(TablePath tablePath, long tableId) {
        return () -> {
            loads.incrementAndGet();
            return TableInfo.of(tablePath, tableId, 1, TableDescriptor.builder()
                    .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                    .distributedBy(1)
                    .build(), null, 0L, 0L);
        };
    }
}
