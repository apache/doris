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

package org.apache.doris.nereids;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Unit tests for {@link StatementContext}'s version-aware MVCC snapshot map.
 *
 * <p>A statement that references the SAME table at different selectors (main vs {@code @branch}/{@code @tag}/
 * FOR-TIME) must pin one snapshot per selector. The pre-fix map keyed only on (catalog, db, table), so a
 * statement mixing main and {@code @branch} of one table (e.g. {@code (select max(value) from t@branch(b1))
 * ... from t}) collapsed to a single entry and the {@code @branch} reference reused main's snapshot — reading
 * the wrong data. These tests pin that keying and the version-blind fallback the metadata readers rely on.
 */
public class StatementContextMvccSnapshotTest {

    private static StatementContext newStatementContext() {
        return new StatementContext(new ConnectContext(), null);
    }

    @SuppressWarnings("unchecked")
    private static MvccTable mockMvccTable(String name) {
        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        return table;
    }

    private static TableScanParams branch(String name) {
        return new TableScanParams("branch", ImmutableMap.of(), ImmutableList.of(name));
    }

    @Test
    public void mainAndBranchOfSameTablePinSeparateSnapshots() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot mainSnap = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot branchSnap = Mockito.mock(MvccSnapshot.class);
        TableScanParams b1 = branch("b1");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(mainSnap);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(b1))).thenReturn(branchSnap);

        // The complex_queries scenario: main reference, then @branch(b1) reference of the SAME table.
        ctx.loadSnapshots(table, Optional.empty(), Optional.empty());
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(b1));

        // Version-aware: each reference resolves to ITS OWN snapshot (no first-write-wins collapse).
        Assertions.assertSame(mainSnap,
                ctx.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null),
                "main reference must read main's snapshot");
        Assertions.assertSame(branchSnap,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(b1)).orElse(null),
                "@branch reference must read the branch snapshot, not main's");
        // Content-based key: a DIFFERENT but equal @branch(b1) selector (as built independently at scan time
        // from the threaded TableScanParams) still resolves to the branch snapshot.
        Assertions.assertSame(branchSnap,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(branch("b1"))).orElse(null),
                "version key must be content-based, not identity-based");
        // Version-blind reader: with both pinned it returns the default (main) deterministically.
        Assertions.assertSame(mainSnap, ctx.getSnapshot(table).orElse(null),
                "version-blind reader returns the default (main) snapshot when one is pinned");
    }

    @Test
    public void standaloneBranchResolvesForVersionBlindReader() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot branchSnap = Mockito.mock(MvccSnapshot.class);
        TableScanParams b1 = branch("b1");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(b1))).thenReturn(branchSnap);

        // The qt_agg_max scenario: only an @branch reference, so no default ("") entry is ever pinned.
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(b1));

        // The version-blind metadata/schema readers must still see the lone pinned snapshot (else a
        // standalone @branch read would resolve schema/partitions against the wrong snapshot).
        Assertions.assertSame(branchSnap, ctx.getSnapshot(table).orElse(null),
                "a lone pinned snapshot is returned to version-blind readers");
        Assertions.assertSame(branchSnap,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(b1)).orElse(null));
    }

    @Test
    public void twoBranchesWithoutMainAreAmbiguousForVersionBlindReader() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot snap1 = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot snap2 = Mockito.mock(MvccSnapshot.class);
        TableScanParams b1 = branch("b1");
        TableScanParams b2 = branch("b2");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(b1))).thenReturn(snap1);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(b2))).thenReturn(snap2);

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(b1));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(b2));

        // Version-aware still resolves each branch precisely.
        Assertions.assertSame(snap1, ctx.getSnapshot(table, Optional.empty(), Optional.of(b1)).orElse(null));
        Assertions.assertSame(snap2, ctx.getSnapshot(table, Optional.empty(), Optional.of(b2)).orElse(null));
        // Version-blind reader: two pinned versions and no default -> ambiguous -> empty so the caller falls
        // back to latest (rather than returning an arbitrary branch, the pre-fix bug).
        Assertions.assertFalse(ctx.getSnapshot(table).isPresent(),
                "version-blind read is ambiguous with multiple versions pinned and no default");
    }

    @Test
    public void forVersionAndForTimeSelectorsKeyDistinctly() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot versionSnap = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot timeSnap = Mockito.mock(MvccSnapshot.class);
        TableSnapshot version5 = TableSnapshot.versionOf("5");
        TableSnapshot time0101 = TableSnapshot.timeOf("2024-01-01");
        Mockito.when(table.loadSnapshot(Optional.of(version5), Optional.empty())).thenReturn(versionSnap);
        Mockito.when(table.loadSnapshot(Optional.of(time0101), Optional.empty())).thenReturn(timeSnap);

        ctx.loadSnapshots(table, Optional.of(version5), Optional.empty());
        ctx.loadSnapshots(table, Optional.of(time0101), Optional.empty());

        // FOR VERSION AS OF and FOR TIME AS OF of the same table must not collapse either.
        Assertions.assertSame(versionSnap,
                ctx.getSnapshot(table, Optional.of(version5), Optional.empty()).orElse(null));
        Assertions.assertSame(timeSnap,
                ctx.getSnapshot(table, Optional.of(time0101), Optional.empty()).orElse(null));
        Assertions.assertFalse(ctx.getSnapshot(table).isPresent(),
                "two distinct time-travel selectors and no default -> version-blind read is ambiguous");
    }

    @Test
    public void nonMvccTableNeverPinsOrResolves() {
        StatementContext ctx = newStatementContext();
        TableIf plain = Mockito.mock(TableIf.class);
        // A non-MvccTable is a no-op for loadSnapshots and always empty for both getSnapshot variants.
        ctx.loadSnapshots(plain, Optional.empty(), Optional.empty());
        Assertions.assertFalse(ctx.getSnapshot(plain).isPresent());
        Assertions.assertFalse(ctx.getSnapshot(plain, Optional.empty(), Optional.empty()).isPresent());
    }
}
