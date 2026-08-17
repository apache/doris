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
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;
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
        stubTableIdentity(table, name);
        return table;
    }

    @SuppressWarnings("unchecked")
    private static void stubTableIdentity(MvccTable table, String name) {
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
    }

    private static TableScanParams branch(String name) {
        return new TableScanParams("branch", ImmutableMap.of(), ImmutableList.of(name));
    }

    private static TableScanParams options(String value) {
        return new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", value), ImmutableList.of());
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
    public void aliasesWithDifferentRelationOptionsPinSeparateSnapshots() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot first = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot second = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        TableScanParams disabled = options("false");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled))).thenReturn(first);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(disabled))).thenReturn(second);

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(disabled));

        Assertions.assertSame(first,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        Assertions.assertSame(second,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(options("false"))).orElse(null));
        Assertions.assertFalse(ctx.getSnapshot(table).isPresent(),
                "relation-scoped options on two aliases must not share an arbitrary snapshot");
    }

    @Test
    public void differentPlanningOptionsShareOneLatestSnapshotFence() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot latestFence = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot firstProjection = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot secondProjection = Mockito.mock(MvccSnapshot.class);
        TableScanParams first = options("true");
        TableScanParams second = options("false");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadLatestSnapshotFence()).thenReturn(latestFence);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(first), Optional.of(latestFence)))
                .thenReturn(firstProjection);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(second), Optional.of(latestFence)))
                .thenReturn(secondProjection);

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(first));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(second));

        // Projection identity includes planning options, while the version identity is one
        // statement fence so a commit between aliases cannot produce an S/S+1 self-join.
        Assertions.assertSame(firstProjection,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(first)).orElse(null));
        Assertions.assertSame(secondProjection,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(second)).orElse(null));
        Mockito.verify(table, Mockito.times(1)).loadLatestSnapshotFence();
    }

    @Test
    public void planningOptionsDoNotMaterializeRawLatestProjectionForTheirFence() {
        StatementContext ctx = newStatementContext();
        MvccSnapshot latestFence = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot projection = Mockito.mock(MvccSnapshot.class);
        MvccTable table = Mockito.mock(MvccTable.class, invocation -> {
            if ("loadLatestSnapshotFence".equals(invocation.getMethod().getName())) {
                return latestFence;
            }
            return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
        });
        stubTableIdentity(table, "t");
        TableScanParams params = options("true");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenThrow(
                new AssertionError("a version fence must not materialize raw latest partitions"));
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(params), Optional.of(latestFence)))
                .thenReturn(projection);

        Assertions.assertDoesNotThrow(
                () -> ctx.loadSnapshots(table, Optional.empty(), Optional.of(params)));
        Assertions.assertSame(projection,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(params)).orElse(null));
    }

    @Test
    public void injectedDefaultSnapshotBecomesTheOptionsFence() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot injected = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot projection = Mockito.mock(MvccSnapshot.class);
        TableScanParams params = options("true");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(params), Optional.of(injected)))
                .thenReturn(projection);

        ctx.setSnapshot(new MvccTableInfo(table), injected);
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(params));

        Assertions.assertSame(projection,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(params)).orElse(null));
        Mockito.verify(table, Mockito.never()).loadLatestSnapshotFence();
    }

    @Test
    public void optionAndPlainAliasesAreOrderIndependent() {
        MvccTable table = mockMvccTable("t");
        MvccSnapshot lightweightFence = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot plain = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot option = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadLatestSnapshotFence()).thenReturn(lightweightFence);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(plain);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty(), Optional.of(lightweightFence)))
                .thenReturn(plain);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled), Optional.of(lightweightFence)))
                .thenReturn(option);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled), Optional.of(plain)))
                .thenReturn(option);

        for (boolean optionFirst : new boolean[] {true, false}) {
            StatementContext ctx = newStatementContext();
            if (optionFirst) {
                ctx.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
                ctx.loadSnapshots(table, Optional.empty(), Optional.empty());
            } else {
                ctx.loadSnapshots(table, Optional.empty(), Optional.empty());
                ctx.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
            }
            Assertions.assertSame(plain,
                    ctx.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));
            Assertions.assertSame(option,
                    ctx.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        }
    }

    @Test
    public void identicalAliasesReuseResolvedStartupOptions() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        TableScanParams first = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1000"), ImmutableList.of());
        TableScanParams second = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1000"), ImmutableList.of());
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(first))).thenAnswer(invocation -> {
            first.getOrResolveMapParams(ignored -> ImmutableMap.of("scan.snapshot-id", "7"));
            return snapshot;
        });

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(first));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(second));

        Assertions.assertEquals("7", second.getResolvedMapParams()
                .orElseThrow(() -> new AssertionError("second alias was not seeded"))
                .get("scan.snapshot-id"));
        Mockito.verify(table, Mockito.times(1)).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void delimiterCharactersCannotCollideSelectorKeys() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot oneValue = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot twoValues = Mockito.mock(MvccSnapshot.class);
        TableScanParams embeddedDelimiter = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "v1, source.split.target-size=1 MB"), ImmutableList.of());
        TableScanParams separateEntry = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "v1", "source.split.target-size", "1 MB"),
                ImmutableList.of());
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(embeddedDelimiter))).thenReturn(oneValue);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(separateEntry))).thenReturn(twoValues);

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(embeddedDelimiter));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(separateEntry));

        // Map.toString() renders these two valid selectors identically. The statement key must encode
        // entry boundaries structurally or one alias silently reuses the other's pinned snapshot.
        Assertions.assertSame(oneValue,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(embeddedDelimiter)).orElse(null));
        Assertions.assertSame(twoValues,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(separateEntry)).orElse(null));
    }

    @Test
    public void selectorMapIterationOrderDoesNotChangeTheKey() {
        StatementContext ctx = newStatementContext();
        MvccTable table = mockMvccTable("t");
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Map<String, String> forward = new LinkedHashMap<>();
        forward.put("scan.tag-name", "v1");
        forward.put("source.split.target-size", "1 MB");
        Map<String, String> reverse = new LinkedHashMap<>();
        reverse.put("source.split.target-size", "1 MB");
        reverse.put("scan.tag-name", "v1");
        TableScanParams first = new TableScanParams(TableScanParams.OPTIONS, forward, ImmutableList.of());
        TableScanParams second = new TableScanParams(TableScanParams.OPTIONS, reverse, ImmutableList.of());
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(first))).thenReturn(snapshot);

        ctx.loadSnapshots(table, Optional.empty(), Optional.of(first));
        ctx.loadSnapshots(table, Optional.empty(), Optional.of(second));

        Assertions.assertSame(snapshot,
                ctx.getSnapshot(table, Optional.empty(), Optional.of(second)).orElse(null));
        Mockito.verify(table, Mockito.times(1)).loadSnapshot(Mockito.any(), Mockito.any());
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
