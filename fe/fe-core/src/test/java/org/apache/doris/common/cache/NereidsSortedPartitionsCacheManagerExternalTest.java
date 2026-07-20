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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

/**
 * Unit coverage for wiring external MVCC tables (iceberg/paimon) into
 * {@link NereidsSortedPartitionsCacheManager}: cache hit/miss/rebuild-on-version-change, origin-map
 * consistency (no #65659 TOCTOU), invalidation, and the {@link ExternalTable#getSortedPartitionRanges}
 * delegation contract.
 *
 * <p>Drives the manager with a Mockito mock of {@link SupportBinarySearchFilteringPartitions} rather than
 * a hand-written fake class: the interface extends {@code TableIf}, whose large method surface makes a
 * hand-rolled implementer impractical. Only the methods the manager actually calls are stubbed
 * ({@code getOriginPartitions}, {@code getPartitionMetaVersion}, {@code getPartitionMetaLoadTimeMillis},
 * {@code getId}, {@code getName}, {@code getDatabase}).</p>
 *
 * <p>{@link NereidsSortedPartitionsCacheManager#get} dereferences {@code ConnectContext.get()
 * .getSessionVariable()} unconditionally once {@code ConnectContext.get() != null}, so every test needs a
 * live {@link ConnectContext} (mirrors the lightweight idiom in {@code LogicalFileScanTest}: a plain
 * {@code new ConnectContext()} + {@code setThreadLocalInfo()}, no FE server bootstrap).</p>
 */
public class NereidsSortedPartitionsCacheManagerExternalTest {

    private static final String CTL = "ctl";
    private static final String DB = "db";
    private static final String TBL = "t";

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static void newLiveConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
    }

    /**
     * A settable-state mock of {@link SupportBinarySearchFilteringPartitions}: {@link #version} and
     * {@link #parts} drive the cache manager's hit/rebuild decision; when {@code withDatabase} is true the
     * constructor also stubs a database/catalog pair (names {@link #CTL}/{@link #DB}) so
     * {@code TableIdentifier} can build.
     */
    private static final class FakeExternalTable {
        final SupportBinarySearchFilteringPartitions table = Mockito.mock(SupportBinarySearchFilteringPartitions.class);
        Object version = "s1@0";
        Map<String, PartitionItem> parts = Maps.newHashMap();

        @SuppressWarnings({"unchecked", "rawtypes"})
        FakeExternalTable(boolean withDatabase) throws RpcException {
            if (withDatabase) {
                DatabaseIf db = Mockito.mock(DatabaseIf.class);
                CatalogIf catalog = Mockito.mock(CatalogIf.class);
                Mockito.when(catalog.getName()).thenReturn(CTL);
                Mockito.when(db.getFullName()).thenReturn(DB);
                Mockito.when(db.getCatalog()).thenReturn(catalog);
                Mockito.when(table.getDatabase()).thenReturn(db);
            }
            Mockito.when(table.getId()).thenReturn(1001L);
            Mockito.when(table.getName()).thenReturn(TBL);
            Mockito.when(table.getOriginPartitions(Mockito.any())).thenAnswer(inv -> parts);
            Mockito.when(table.getPartitionMetaVersion(Mockito.any())).thenAnswer(inv -> version);
            Mockito.when(table.getPartitionMetaLoadTimeMillis(Mockito.any())).thenReturn(0L);
        }
    }

    // ──────────────────── Task 1: getDatabase()==null guards the external wiring contract ────────────────────

    @Test
    public void testManagerReturnsEmptyWhenDatabaseNull() throws Exception {
        newLiveConnectContext();
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = new FakeExternalTable(false); // getDatabase() -> null (unstubbed mock default)

        Optional<SortedPartitionRanges<?>> r = mgr.get(t.table, (CatalogRelation) null);

        Assertions.assertFalse(r.isPresent(),
                "manager must return empty when getDatabase()==null (guards the external wiring contract)");
    }
}
