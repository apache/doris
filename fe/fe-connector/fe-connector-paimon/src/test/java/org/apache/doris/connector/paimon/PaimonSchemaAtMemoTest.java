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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link PaimonSchemaAtMemo} (FIX-B-MC2): the bounded, immutable second-level memo of the
 * time-travel schema-at-snapshot read. Verifies key dedup (the cross-query hit), that every component of
 * the handle identity participates in the key (the {@code sysName} Rule-9 guard), and that the bound
 * degrades to a re-read rather than ever serving a stale value (the no-regression "worst case = current").
 */
public class PaimonSchemaAtMemoTest {

    private static PaimonTableHandle handle(String db, String table) {
        return new PaimonTableHandle(db, table, Collections.emptyList(), Collections.emptyList());
    }

    private static PaimonCatalogOps.PaimonSchemaSnapshot snap() {
        return new PaimonCatalogOps.PaimonSchemaSnapshot(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void sameKeyLoadsOnce() {
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(100);
        PaimonTableHandle h = handle("db", "t");
        AtomicInteger loads = new AtomicInteger();

        memo.getOrLoad(h, 5L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        memo.getOrLoad(h, 5L, () -> {
            loads.incrementAndGet();
            return snap();
        });

        // WHY: a repeat (handle, schemaId) must be a memo hit — the whole point of FIX-B-MC2 (restore the
        // legacy cross-query schemaAt hit). MUTATION: never caching -> 2 loads -> red.
        Assertions.assertEquals(1, loads.get(), "the same (handle, schemaId) must load exactly once");
        Assertions.assertEquals(1, memo.size());
    }

    @Test
    public void sysTableNameDistinguishesKey() {
        // Two handles equal in (db, table, branch, schemaId) but differing ONLY in sysTableName.
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(100);
        PaimonTableHandle base = handle("db", "t");
        PaimonTableHandle sys = PaimonTableHandle.forSystemTable("db", "t", "snapshots", false);
        AtomicInteger loads = new AtomicInteger();

        memo.getOrLoad(base, 5L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        memo.getOrLoad(sys, 5L, () -> {
            loads.incrementAndGet();
            return snap();
        });

        // WHY: sysName is part of table identity (a sys table is a distinct table with its own rowType);
        // the key must not collide a base table with its system table. MUTATION: drop sysTableName from
        // MemoKey -> one load -> red.
        Assertions.assertEquals(2, loads.get(), "base and its system table must be distinct memo keys");
    }

    @Test
    public void overflowEvictsAndReReadsNeverStale() {
        // Bound = 2: inserting a 3rd distinct key flushes the map; a previously-cached key then re-loads
        // (a re-read = the pre-fix behavior), proving eviction degrades to a re-read, never a stale value.
        PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(2);
        AtomicInteger loads = new AtomicInteger();

        memo.getOrLoad(handle("db", "t1"), 1L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        memo.getOrLoad(handle("db", "t2"), 1L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        // size() == 2 == bound -> this insert clears, then puts t3.
        memo.getOrLoad(handle("db", "t3"), 1L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        Assertions.assertEquals(3, loads.get());

        // t1 was flushed by the overflow -> re-loads now (never serves a stale value).
        memo.getOrLoad(handle("db", "t1"), 1L, () -> {
            loads.incrementAndGet();
            return snap();
        });
        Assertions.assertEquals(4, loads.get(), "an evicted key must re-read (never serve a stale value)");
        Assertions.assertTrue(memo.size() <= 2, "the memo must stay bounded");
    }
}
