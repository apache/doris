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

public class StatementContextMvccSnapshotTest {
    @SuppressWarnings("unchecked")
    private static MvccTable mockTable() {
        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        return table;
    }

    private static TableScanParams options(String value) {
        return new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", value), ImmutableList.of());
    }

    @Test
    public void aliasesWithDifferentRelationOptionsPinSeparateSnapshots() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot first = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot second = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        TableScanParams disabled = options("false");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled))).thenReturn(first);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(disabled))).thenReturn(second);

        context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
        context.loadSnapshots(table, Optional.empty(), Optional.of(disabled));

        Assertions.assertSame(first,
                context.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        Assertions.assertSame(second,
                context.getSnapshot(table, Optional.empty(), Optional.of(options("false"))).orElse(null));
        Assertions.assertFalse(context.getSnapshot(table).isPresent());
    }

    @Test
    public void differentPlanningOptionsShareOneLatestSnapshotFence() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
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

        context.loadSnapshots(table, Optional.empty(), Optional.of(first));
        context.loadSnapshots(table, Optional.empty(), Optional.of(second));

        // Projection identity includes planning options, while the version identity is one
        // statement fence so a commit between aliases cannot produce an S/S+1 self-join.
        Assertions.assertSame(firstProjection,
                context.getSnapshot(table, Optional.empty(), Optional.of(first)).orElse(null));
        Assertions.assertSame(secondProjection,
                context.getSnapshot(table, Optional.empty(), Optional.of(second)).orElse(null));
        Mockito.verify(table, Mockito.times(1)).loadLatestSnapshotFence();
    }

    @Test
    public void planningOptionsDoNotMaterializeRawLatestProjectionForTheirFence() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccSnapshot latestFence = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot projection = Mockito.mock(MvccSnapshot.class);
        MvccTable table = Mockito.mock(MvccTable.class, invocation -> {
            if ("loadLatestSnapshotFence".equals(invocation.getMethod().getName())) {
                return latestFence;
            }
            return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
        });
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        TableScanParams params = options("true");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenThrow(
                new AssertionError("a version fence must not materialize raw latest partitions"));
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(params), Optional.of(latestFence)))
                .thenReturn(projection);

        Assertions.assertDoesNotThrow(
                () -> context.loadSnapshots(table, Optional.empty(), Optional.of(params)));
        Assertions.assertSame(projection,
                context.getSnapshot(table, Optional.empty(), Optional.of(params)).orElse(null));
    }

    @Test
    public void injectedDefaultSnapshotBecomesTheOptionsFence() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot injected = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot projection = Mockito.mock(MvccSnapshot.class);
        TableScanParams params = options("true");
        Mockito.when(table.requiresLatestSnapshotFence(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(params), Optional.of(injected)))
                .thenReturn(projection);

        context.setSnapshot(new MvccTableInfo(table), injected);
        context.loadSnapshots(table, Optional.empty(), Optional.of(params));

        Assertions.assertSame(projection,
                context.getSnapshot(table, Optional.empty(), Optional.of(params)).orElse(null));
        Mockito.verify(table, Mockito.never()).loadLatestSnapshotFence();
    }

    @Test
    public void optionAndPlainAliasesAreOrderIndependent() {
        MvccTable table = mockTable();
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
            StatementContext context = new StatementContext(new ConnectContext(), null);
            if (optionFirst) {
                context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
                context.loadSnapshots(table, Optional.empty(), Optional.empty());
            } else {
                context.loadSnapshots(table, Optional.empty(), Optional.empty());
                context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
            }
            Assertions.assertSame(plain,
                    context.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));
            Assertions.assertSame(option,
                    context.getSnapshot(table, Optional.empty(), Optional.of(options("true"))).orElse(null));
        }
    }

    @Test
    public void tableMetadataRetainsPinnedProjectionWhenOptionsDiffer() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot first = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot second = Mockito.mock(MvccSnapshot.class);
        TableScanParams enabled = options("true");
        TableScanParams disabled = options("false");
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(enabled))).thenReturn(first);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(disabled))).thenReturn(second);

        context.loadSnapshots(table, Optional.empty(), Optional.of(enabled));
        context.loadSnapshots(table, Optional.empty(), Optional.of(disabled));

        Assertions.assertSame(first, context.getSnapshotForTableMetadata(table).orElse(null));
    }

    @Test
    public void identicalAliasesReuseResolvedStartupOptions() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        TableScanParams first = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1000"), ImmutableList.of());
        TableScanParams second = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1000"), ImmutableList.of());
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(first))).thenAnswer(invocation -> {
            first.getOrResolveMapParams(ignored -> ImmutableMap.of("scan.snapshot-id", "7"));
            return snapshot;
        });

        context.loadSnapshots(table, Optional.empty(), Optional.of(first));
        context.loadSnapshots(table, Optional.empty(), Optional.of(second));

        Assertions.assertEquals("7", second.getResolvedMapParams()
                .orElseThrow(() -> new AssertionError("second alias was not seeded"))
                .get("scan.snapshot-id"));
        Mockito.verify(table, Mockito.times(1)).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void delimiterCharactersCannotCollideSelectorKeys() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
        MvccSnapshot oneValue = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot twoValues = Mockito.mock(MvccSnapshot.class);
        TableScanParams embeddedDelimiter = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "v1, source.split.target-size=1 MB"), ImmutableList.of());
        TableScanParams separateEntry = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "v1", "source.split.target-size", "1 MB"),
                ImmutableList.of());
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(embeddedDelimiter))).thenReturn(oneValue);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(separateEntry))).thenReturn(twoValues);

        context.loadSnapshots(table, Optional.empty(), Optional.of(embeddedDelimiter));
        context.loadSnapshots(table, Optional.empty(), Optional.of(separateEntry));

        // Map.toString() renders these two valid selectors identically. The statement key must encode
        // entry boundaries structurally or one alias silently reuses the other's pinned snapshot.
        Assertions.assertSame(oneValue,
                context.getSnapshot(table, Optional.empty(), Optional.of(embeddedDelimiter)).orElse(null));
        Assertions.assertSame(twoValues,
                context.getSnapshot(table, Optional.empty(), Optional.of(separateEntry)).orElse(null));
    }

    @Test
    public void selectorMapIterationOrderDoesNotChangeTheKey() {
        StatementContext context = new StatementContext(new ConnectContext(), null);
        MvccTable table = mockTable();
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

        context.loadSnapshots(table, Optional.empty(), Optional.of(first));
        context.loadSnapshots(table, Optional.empty(), Optional.of(second));

        Assertions.assertSame(snapshot,
                context.getSnapshot(table, Optional.empty(), Optional.of(second)).orElse(null));
        Mockito.verify(table, Mockito.times(1)).loadSnapshot(Mockito.any(), Mockito.any());
    }
}
