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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.mtmv.MTMVRefreshContext.PreparedPartitionSnapshots;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class MTMVRefreshContextBatchTest {

    @Test
    public void partitionAlignmentUsesTheCapturedTablePin() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MvccSnapshot pin = Mockito.mock(MvccSnapshot.class);
        configureTableIdentity(table);
        Mockito.when(mtmv.generateMvPartitionDescs()).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(mtmv.getMvProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getPartitionColumns()).thenReturn(Collections.emptyList());
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(table));
        Mockito.when(partitionInfo.getPctInfos())
                .thenReturn(Collections.singletonList(Mockito.mock(BaseColInfo.class)));
        Mockito.when(table.getAndCopyPartitionItems(Optional.of(pin))).thenReturn(Collections.emptyMap());

        Pair<List<String>, List<PartitionKeyDesc>> result = MTMVPartitionUtil.alignMvPartition(mtmv,
                Collections.singletonMap(new MvccTableInfo(table), pin));

        Assertions.assertTrue(result.first.isEmpty());
        Assertions.assertTrue(result.second.isEmpty());
        Mockito.verify(table).getAndCopyPartitionItems(Optional.of(pin));
        Mockito.verify(table, Mockito.never()).getAndCopyPartitionItems(Optional.empty());
    }

    @Test
    public void aggregatesOneHundredSixtyThousandMappedPartitionsIntoOneLogicalLoad()
            throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        for (int i = 0; i < 160_000; i++) {
            mappings.put("mv" + i, Collections.singletonMap(table, Collections.singleton("p" + i)));
        }
        configureContext(mtmv, table, refreshSnapshot, mappings);
        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.anyString(), Mockito.any()))
                .thenAnswer(invocation -> Collections.singleton(
                        "p" + invocation.<String>getArgument(0).substring(2)));
        Mockito.when(table.getPartitionSnapshots(Mockito.anySet(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> snapshots(invocation.getArgument(0)));

        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap());
        PreparedPartitionSnapshots prepared = context.prepareComparablePartitionSnapshots(mappings.keySet());

        Mockito.verify(table).getPartitionSnapshots(
                Mockito.argThat(names -> names.size() == 160_000), Mockito.same(context), Mockito.any());
        Assertions.assertNotNull(prepared.get(table, "p159999"));
        Mockito.verify(table, Mockito.times(1))
                .getPartitionSnapshots(Mockito.anySet(), Mockito.same(context), Mockito.any());
    }

    @Test
    public void comparablePreloadSkipsLocallyMismatchedPartitionSets() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = Collections.singletonMap(
                "mv", Collections.singletonMap(table, Collections.singleton("current")));
        configureContext(mtmv, table, refreshSnapshot, mappings);
        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.eq("mv"), Mockito.any()))
                .thenReturn(Collections.singleton("persisted"));

        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap());
        PreparedPartitionSnapshots prepared =
                context.prepareComparablePartitionSnapshots(Collections.singleton("mv"));

        Mockito.verify(table, Mockito.never())
                .getPartitionSnapshots(Mockito.anySet(), Mockito.same(context), Mockito.any());
        Assertions.assertThrows(AnalysisException.class, () -> prepared.get(table, "current"));
    }

    @Test
    public void persistencePreloadLoadsMappingsRegardlessOfPersistedState() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        mappings.put("mv1", Collections.singletonMap(table, Collections.singleton("p1")));
        mappings.put("mv2", Collections.singletonMap(table, Collections.singleton("p2")));
        configureContext(mtmv, table, refreshSnapshot, mappings);
        Mockito.when(table.getPartitionSnapshots(Mockito.anySet(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> snapshots(invocation.getArgument(0)));

        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap());
        context.preparePartitionSnapshots(mappings.keySet());

        Mockito.verify(table).getPartitionSnapshots(
                Mockito.argThat(names -> names.equals(new LinkedHashSet<>(Arrays.asList("p1", "p2")))),
                Mockito.same(context), Mockito.any());
        Mockito.verifyNoInteractions(refreshSnapshot);
    }

    @Test
    public void cachesOverlappingLoadsAndRequestsOnlyMissingSnapshots() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        mappings.put("mv1", Collections.singletonMap(table, Collections.singleton("p1")));
        mappings.put("mv2", Collections.singletonMap(table, Collections.singleton("p2")));
        configureContext(mtmv, table, refreshSnapshot, mappings);
        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap());
        Mockito.when(table.getPartitionSnapshots(Mockito.anySet(), Mockito.same(context), Mockito.any()))
                .thenAnswer(invocation -> snapshots(invocation.getArgument(0)));

        PreparedPartitionSnapshots first = context.preparePartitionSnapshots(Collections.singleton("mv1"));
        PreparedPartitionSnapshots second = context.preparePartitionSnapshots(mappings.keySet());
        Assertions.assertNotNull(first.get(table, "p1"));
        Assertions.assertNotNull(second.get(table, "p1"));
        Assertions.assertNotNull(second.get(table, "p2"));

        Mockito.verify(table).getPartitionSnapshots(
                Mockito.eq(Collections.singleton("p1")), Mockito.same(context), Mockito.any());
        Mockito.verify(table).getPartitionSnapshots(
                Mockito.eq(Collections.singleton("p2")), Mockito.same(context), Mockito.any());
    }

    @Test
    public void rejectsIncompleteOrUnexpectedBulkResults() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = Collections.singletonMap(
                "mv", Collections.singletonMap(table, Collections.singleton("missing")));
        configureContext(mtmv, table, refreshSnapshot, mappings);
        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap());
        Mockito.when(table.getPartitionSnapshots(Mockito.anySet(), Mockito.same(context), Mockito.any()))
                .thenReturn(Collections.singletonMap("unexpected", new MTMVTimestampSnapshot(1L)));

        Assertions.assertThrows(AnalysisException.class,
                () -> context.preparePartitionSnapshots(Collections.singleton("mv")));
    }

    @Test
    public void taskPinIsReusedForTheUnionAcrossExecutionGroups() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        mappings.put("mv1", Collections.singletonMap(table, Collections.singleton("p1")));
        mappings.put("mv2", Collections.singletonMap(table, Collections.singleton("p2")));
        configureContext(mtmv, table, refreshSnapshot, mappings);
        MvccSnapshot pin = Mockito.mock(MvccSnapshot.class);
        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, Collections.emptyMap(),
                Collections.singletonMap(new MvccTableInfo(table), pin));
        Mockito.when(table.getPartitionSnapshots(Mockito.anySet(), Mockito.same(context), Mockito.any()))
                .thenAnswer(invocation -> snapshots(invocation.getArgument(0)));

        PreparedPartitionSnapshots prepared = context.preparePartitionSnapshots(mappings.keySet());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<MTMVRelatedTableIf, Optional<MvccSnapshot>>> resolverCaptor =
                ArgumentCaptor.forClass(Function.class);
        Mockito.verify(mtmv).calculatePartitionMappings(Mockito.anyMap(), resolverCaptor.capture());
        Assertions.assertEquals(Optional.of(pin), resolverCaptor.getValue().apply(table));
        Mockito.verify(table).getPartitionSnapshots(Mockito.eq(new LinkedHashSet<>(Arrays.asList("p1", "p2"))),
                Mockito.same(context),
                Mockito.eq(Optional.of(pin)));
        Assertions.assertNotNull(prepared.get(table, "p1"));
        Assertions.assertNotNull(prepared.get(table, "p2"));
    }

    @Test
    public void defaultBulkAdapterPreservesNonBulkImplementations() throws AnalysisException {
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class, Mockito.CALLS_REAL_METHODS);
        MTMVRefreshContext context = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(table.getPartitionSnapshot(Mockito.anyString(), Mockito.same(context), Mockito.any()))
                .thenAnswer(invocation -> new MTMVTimestampSnapshot(invocation.<String>getArgument(0).hashCode()));

        Map<String, MTMVSnapshotIf> snapshots = table.getPartitionSnapshots(
                new LinkedHashSet<>(Arrays.asList("p1", "p2")), context, Optional.empty());

        Assertions.assertEquals(Arrays.asList("p1", "p2"), new ArrayList<>(snapshots.keySet()));
        Mockito.verify(table, Mockito.times(2))
                .getPartitionSnapshot(Mockito.anyString(), Mockito.same(context), Mockito.any());
    }

    private static void configureContext(MTMV mtmv, MTMVRelatedTableIf table,
            MTMVRefreshSnapshot refreshSnapshot,
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings) throws AnalysisException {
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap(), Mockito.any())).thenReturn(mappings);
        Mockito.when(mtmv.getRelation()).thenReturn(null);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(mtmv.getRefreshSnapshot()).thenReturn(refreshSnapshot);
        Mockito.when(table.needAutoRefresh()).thenReturn(true);
        configureTableIdentity(table);
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(table));
    }

    private static void configureTableIdentity(MTMVRelatedTableIf table) {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("database");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
    }

    private static Map<String, MTMVSnapshotIf> snapshots(Set<String> names) {
        Map<String, MTMVSnapshotIf> snapshots = new LinkedHashMap<>();
        for (String name : names) {
            snapshots.put(name, new MTMVTimestampSnapshot(name.hashCode()));
        }
        return snapshots;
    }
}
