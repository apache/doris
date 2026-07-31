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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link IcebergCommentCache} (PERF-05). Mirrors {@link IcebergTableCacheTest} but stores the
 * {@code comment} string keyed by {@link TableIdentifier}. Covers within-TTL stability, the {@code ttl <= 0}
 * disable (load-bearing: a vended no-cache catalog builds this object but must not cache), invalidation, and the
 * not-cached-on-failure guarantee that keeps the view-handle {@code NoSuchTableException} degradation to "".
 */
public class IcebergCommentCacheTest {

    private static TableIdentifier id(String db, String tbl) {
        return TableIdentifier.of(db, tbl);
    }

    @Test
    public void cachesWithinTtlAndServesTheSameComment() {
        AtomicInteger loads = new AtomicInteger();
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);

        String first = c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "sales fact";
        });
        // Second read of the SAME table within TTL returns the cached comment, NOT a fresh remote loadTable -> this
        // is what collapses the per-table information_schema loads on repeat queries. MUTATION: loading live every
        // call -> returns "other" / loads==2 -> red.
        String second = c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "other";
        });
        Assertions.assertEquals("sales fact", first);
        Assertions.assertEquals("sales fact", second, "within TTL the cached comment must be served");
        Assertions.assertEquals(1, loads.get(), "the live loadTable must run exactly once within TTL");
        Assertions.assertEquals(1, c.loadCountForTest(), "the metric-gate load count must be 1");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void differentTableIsADifferentKey() {
        AtomicInteger loads = new AtomicInteger();
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);
        c.getOrLoad(id("db", "t1"), () -> {
            loads.incrementAndGet();
            return "c1";
        });
        String t2 = c.getOrLoad(id("db", "t2"), () -> {
            loads.incrementAndGet();
            return "c2";
        });
        Assertions.assertEquals("c2", t2);
        Assertions.assertEquals(2, loads.get());
        Assertions.assertEquals(2, c.size());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergCommentCache c = new IcebergCommentCache(0, 1000);
        c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "a";
        });
        String second = c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "b";
        });
        // ttl-second=0 (a vended no-cache catalog) loads live every time -> operator "no meta cache" intent honored
        // even though the connector built this object. MUTATION: caching despite ttl<=0 -> second=="a" / loads==1.
        Assertions.assertEquals("b", second, "ttl-second=0 must always load live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergCommentCache c = new IcebergCommentCache(-1, 1000);
        c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "a";
        });
        String second = c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "b";
        });
        Assertions.assertEquals("b", second, "ttl-second=-1 must always load live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);
        c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "old";
        });
        // REFRESH TABLE db.t drops the entry so an external ALTER ... SET TBLPROPERTIES('comment'=...) is picked up.
        // MUTATION: invalidate not clearing the key -> "old" survives -> red.
        c.invalidate(id("db", "t"));
        String after = c.getOrLoad(id("db", "t"), () -> {
            loads.incrementAndGet();
            return "new";
        });
        Assertions.assertEquals("new", after);
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);
        c.getOrLoad(id("db1", "t1"), () -> "a");
        c.getOrLoad(id("db1", "t2"), () -> "b");
        c.getOrLoad(id("db2", "t1"), () -> "c");
        Assertions.assertEquals(3, c.size());
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's entry must survive REFRESH DATABASE db1");
    }

    @Test
    public void invalidateAllClearsEverything() {
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);
        c.getOrLoad(id("db", "t1"), () -> "a");
        c.getOrLoad(id("db", "t2"), () -> "b");
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void loaderFailureIsNotCachedSoNextQueryRetries() {
        // A view handle (loadTable throws NoSuchTableException) must NOT be cached, so getTableComment keeps
        // degrading to "" via the caller's catch on every call (and a real table appearing later loads fresh). The
        // MetaCacheEntry manual-miss-load path re-throws the loader's exception verbatim and does not store it.
        // MUTATION: caching on failure -> the second call would not re-run the loader / size==1 -> red.
        IcebergCommentCache c = new IcebergCommentCache(100, 1000);
        AtomicInteger loads = new AtomicInteger();
        Assertions.assertThrows(NoSuchTableException.class, () -> c.getOrLoad(id("db", "v"), () -> {
            loads.incrementAndGet();
            throw new NoSuchTableException("not a table: db.v");
        }));
        Assertions.assertEquals(0, c.size(), "a thrown comment load must not be cached");
        String ok = c.getOrLoad(id("db", "v"), () -> {
            loads.incrementAndGet();
            return "now a table";
        });
        Assertions.assertEquals("now a table", ok);
        Assertions.assertEquals(2, loads.get(), "the loader must re-run after a failure (no sticky failure)");
        Assertions.assertEquals(1, c.size());
    }
}
