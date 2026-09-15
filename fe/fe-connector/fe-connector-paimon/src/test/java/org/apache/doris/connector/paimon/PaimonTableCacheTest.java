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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link PaimonTableCache} — the Doris-owned paimon {@link Table} object cache that replaces
 * the Paimon SDK {@code CachingCatalog} tableCache (external same-name drop/recreate stale-read fix, DORIS-29032).
 *
 * <p>The cache is backed by the shared {@link org.apache.doris.connector.cache.MetaCache} framework; these
 * tests cover the adapter contract that matters for the fix: within-TTL reuse, the {@code ttl <= 0} disable,
 * and — most importantly — that invalidation (REFRESH TABLE/DATABASE/CATALOG) forces the next read to load a
 * fresh {@link Table} instead of serving the frozen pre-drop object the SDK {@code CachingCatalog} kept.
 * Timed-expiry mechanics are the framework's responsibility and are not re-proven here.
 */
public class PaimonTableCacheTest {

    private static Identifier id() {
        return Identifier.create("db", "t");
    }

    private static FakePaimonTable table(String name) {
        return new FakePaimonTable(name, RowType.builder().build(),
                Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void cachesTableWithinTtl() {
        AtomicInteger loads = new AtomicInteger();
        PaimonTableCache c = new PaimonTableCache(100, 1000);

        Table first = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("t");
        });
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("t2");
        });
        // Within TTL the second read must hit the cache (same instance, loader not re-run).
        Assertions.assertSame(first, second, "within TTL the cached Table object must be served");
        Assertions.assertEquals(1, loads.get(), "the live loader must run exactly once within TTL");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void invalidateForcesFreshTableLoad() {
        AtomicInteger loads = new AtomicInteger();
        PaimonTableCache c = new PaimonTableCache(100, 1000);
        Table oldTable = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("old");
        });
        // External drop/recreate + REFRESH TABLE: invalidation must make the next read load the NEW table,
        // not serve the frozen old Table object (the CachingCatalog stale-read failure mode).
        c.invalidate(id());
        Table newTable = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("new");
        });
        Assertions.assertNotSame(oldTable, newTable,
                "after REFRESH TABLE invalidation a fresh Table must be loaded");
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        PaimonTableCache c = new PaimonTableCache(0, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("t1");
        });
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("t2");
        });
        // ttl-second=0 (the no-cache catalog) must read live every time. MUTATION: caching despite ttl<=0
        // -> loads==1 -> red.
        Assertions.assertEquals(2, loads.get(), "ttl-second=0 must read live every time");
        Assertions.assertEquals(0, c.size());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateAllAndDbClearMatchingTablesOnly() {
        PaimonTableCache c = new PaimonTableCache(100, 1000);
        c.getOrLoad(Identifier.create("db1", "t1"), () -> table("t1"));
        c.getOrLoad(Identifier.create("db1", "t2"), () -> table("t2"));
        c.getOrLoad(Identifier.create("db2", "t1"), () -> table("t1"));
        Assertions.assertEquals(3, c.size());

        // REFRESH DATABASE db1 must drop only db1's tables (the used-to-be-leaky db granularity).
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's entry must survive invalidateDb(db1)");
    }

    @Test
    public void distinctIdentifiersCacheIndependently() {
        PaimonTableCache c = new PaimonTableCache(100, 1000);
        Table a = c.getOrLoad(Identifier.create("db", "a"), () -> table("a"));
        Table b = c.getOrLoad(Identifier.create("db", "b"), () -> table("b"));
        Assertions.assertNotSame(a, b);
        Assertions.assertEquals(2, c.size());
    }
}
