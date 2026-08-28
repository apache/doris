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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
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
import java.util.stream.Collectors;

public class MTMVRefreshContextBatchTest {

    @Test
    public void aggregatesMissingPartitionsAndReusesCachedSnapshots() throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(Mockito.mock(MTMV.class));
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        List<List<String>> requests = new ArrayList<>();
        Mockito.when(table.getPartitionSnapshots(Mockito.anyList(), Mockito.same(context), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> names = invocation.getArgument(0);
                    requests.add(new ArrayList<>(names));
                    Map<String, MTMVSnapshotIf> result = new LinkedHashMap<>();
                    for (String name : names) {
                        result.put(name, new MTMVTimestampSnapshot(name.hashCode()));
                    }
                    return result;
                });

        Map<String, MTMVSnapshotIf> first = context.getPartitionSnapshots(table,
                new LinkedHashSet<>(Arrays.asList("p1", "p2")), Optional.empty());
        Map<String, MTMVSnapshotIf> second = context.getPartitionSnapshots(table,
                new LinkedHashSet<>(Arrays.asList("p2", "p3")), Optional.empty());

        Assertions.assertEquals(Arrays.asList(Arrays.asList("p1", "p2"), Collections.singletonList("p3")),
                requests);
        Assertions.assertSame(first.get("p2"), second.get("p2"));
        Assertions.assertEquals(Arrays.asList("p2", "p3"), new ArrayList<>(second.keySet()));
    }

    @Test
    public void rejectsIncompleteBulkResult() throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(Mockito.mock(MTMV.class));
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        Mockito.when(table.getPartitionSnapshots(Mockito.anyList(), Mockito.same(context), Mockito.any()))
                .thenReturn(Collections.emptyMap());

        Assertions.assertThrows(AnalysisException.class,
                () -> context.getPartitionSnapshots(table,
                        new LinkedHashSet<>(Collections.singletonList("missing")), Optional.empty()));
    }

    @Test
    public void resolvesBaseTableOnlyOnceForAllPartitionLoops() throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(Mockito.mock(MTMV.class));
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(baseTableInfo)).thenReturn(table);

            Assertions.assertSame(table, context.getBaseTable(baseTableInfo));
            Assertions.assertSame(table, context.getBaseTable(baseTableInfo));

            mtmvUtil.verify(() -> MTMVUtil.getTable(baseTableInfo), Mockito.times(1));
        }
    }

    @Test
    public void preloadsPartitionMappingsBeforeTheCallerLoop() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class, Mockito.RETURNS_DEEP_STUBS);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        for (int i = 0; i < 160_000; i++) {
            mappings.put("mv" + i, Collections.singletonMap(table, Collections.singleton("p" + i)));
        }
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap(), Mockito.anyMap())).thenReturn(mappings);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(mtmv.getRefreshSnapshot()).thenReturn(refreshSnapshot);
        Mockito.when(mtmv.getRelation()).thenReturn(new MTMVRelation(
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), Collections.emptySet()));
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(table));
        Mockito.when(table.getAndCopyPartitionItems(Mockito.any())).thenReturn(Collections.emptyMap());
        Mockito.when(table.supportsPartitionSnapshotBatchLoading()).thenReturn(true);
        Mockito.when(table.needAutoRefresh()).thenReturn(true);
        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.anyString(), Mockito.any()))
                .thenAnswer(invocation -> Collections.singleton(
                        "p" + invocation.<String>getArgument(0).substring(2)));
        MTMVSnapshotIf snapshot = new MTMVTimestampSnapshot(1L);
        Mockito.when(table.getPartitionSnapshots(Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> invocation.<List<String>>getArgument(0).stream().collect(Collectors.toMap(
                        name -> name, name -> snapshot, (left, right) -> left, LinkedHashMap::new)));

        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv);
        context.preloadPartitionSnapshots();

        MTMVRefreshContext compact = context.compactCloudPreload();
        Assertions.assertTrue(compact.getPartitionMappings().isEmpty());
        Assertions.assertNull(compact.getBaseVersions());
        Assertions.assertEquals(160_000,
                compact.rebuildFromCachedVersions(Collections.emptyMap()).getPartitionMappings().size());

        Mockito.verify(table, Mockito.times(1)).getPartitionSnapshots(
                Mockito.argThat(names -> names.size() == 160_000), Mockito.same(context), Mockito.any());
        Mockito.verify(table, Mockito.never())
                .getPartitionSnapshot(Mockito.anyString(), Mockito.same(context), Mockito.any());

        Mockito.clearInvocations(table);
        Mockito.doReturn(Collections.emptySet()).when(refreshSnapshot)
                .getPctSnapshots(Mockito.anyString(), Mockito.any());
        Mockito.when(mtmv.getPartitionNames()).thenReturn(mappings.keySet());
        MTMVRefreshContext mismatched = MTMVRefreshContext.buildContext(mtmv);
        Assertions.assertFalse(MTMVPartitionUtil.isMTMVSync(
                mismatched, Collections.emptySet(), Collections.emptySet()));
        Mockito.verify(table, Mockito.never())
                .getPartitionSnapshots(Mockito.anyList(), Mockito.same(mismatched), Mockito.any());
    }

    @Test
    public void persistencePreloadBatchesFirstIncompleteAndChangedBaselines() throws AnalysisException {
        assertPersistencePreloadBatches(name -> Collections.emptySet());
        assertPersistencePreloadBatches(name -> "mv1".equals(name)
                ? Collections.emptySet() : Collections.singleton("p" + name.substring(2)));
        assertPersistencePreloadBatches(name -> Collections.singleton("old_" + name));
    }

    @Test
    public void preloadSnapshotsAlsoLoadsNonPctTableFreshnessBeforeComparison() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        MTMVRelatedTableIf baseTable = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRelation relation = new MTMVRelation(
                Collections.singleton(baseTableInfo), Collections.singleton(baseTableInfo),
                Collections.singleton(baseTableInfo), Collections.emptySet(), Collections.emptySet());
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap())).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getRelation()).thenReturn(relation);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.emptySet());
        Mockito.when(baseTable.needAutoRefresh()).thenReturn(true);
        MTMVBaseVersions versions = new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap());
        MTMVSnapshotIf snapshot = new MTMVTimestampSnapshot(1L);

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtil = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(baseTableInfo)).thenReturn(baseTable);
            partitionUtil.when(() -> MTMVPartitionUtil.getBaseVersions(
                    mtmv, Collections.emptyMap(), relation.getBaseTablesOneLevelAndFromView())).thenReturn(versions);
            MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv);
            partitionUtil.when(() -> MTMVPartitionUtil.getTableSnapshotFromContext(baseTable, context))
                    .thenReturn(snapshot);

            context.preloadSnapshots();

            partitionUtil.verify(
                    () -> MTMVPartitionUtil.getTableSnapshotFromContext(baseTable, context), Mockito.times(1));
        }
    }

    @Test
    public void preloadTableSnapshotsFiltersExcludedTablesBeforeResolvingThem() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        BaseTableInfo excluded = new BaseTableInfo(new TableNameInfo("ctl", "db", "excluded"));
        BaseTableInfo included = new BaseTableInfo(new TableNameInfo("ctl", "db", "included"));
        MTMVRelatedTableIf includedTable = Mockito.mock(MTMVRelatedTableIf.class);
        Set<BaseTableInfo> currentRelationTables = new LinkedHashSet<>(Arrays.asList(excluded, included));
        Set<TableNameInfo> excludedTables = Collections.singleton(
                new TableNameInfo("ctl", "db", "excluded"));
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap())).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.emptySet());
        Mockito.when(includedTable.needAutoRefresh()).thenReturn(true);
        MTMVBaseVersions versions = new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap());
        MTMVSnapshotIf snapshot = new MTMVTimestampSnapshot(1L);

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtil = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(included)).thenReturn(includedTable);
            partitionUtil.when(() -> MTMVPartitionUtil.getBaseVersions(
                    mtmv, Collections.emptyMap(), currentRelationTables)).thenReturn(versions);
            MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, currentRelationTables);
            partitionUtil.when(() -> MTMVPartitionUtil.isTableExcluded(excludedTables, excluded))
                    .thenReturn(true);
            partitionUtil.when(() -> MTMVPartitionUtil.isTableExcluded(excludedTables, included))
                    .thenReturn(false);
            partitionUtil.when(() -> MTMVPartitionUtil.getTableSnapshotFromContext(includedTable, context))
                    .thenReturn(snapshot);

            context.preloadTableSnapshots(currentRelationTables, excludedTables);

            mtmvUtil.verify(() -> MTMVUtil.getTable(excluded), Mockito.never());
            mtmvUtil.verify(() -> MTMVUtil.getTable(included), Mockito.times(1));
            partitionUtil.verify(
                    () -> MTMVPartitionUtil.getTableSnapshotFromContext(includedTable, context), Mockito.times(1));
            Assertions.assertEquals(currentRelationTables, context.getBaseTables(),
                    "the context must retain the relation resolved for this operation");
        }
    }

    @Test
    public void buildContextCapturesLocalVersionsFromCurrentRelation() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        BaseTableInfo persisted = new BaseTableInfo(new TableNameInfo("internal", "db", "old"));
        BaseTableInfo current = new BaseTableInfo(new TableNameInfo("internal", "db", "new"));
        OlapTable currentTable = Mockito.mock(OlapTable.class);
        MTMVRelation persistedRelation = new MTMVRelation(
                Collections.singleton(persisted), Collections.singleton(persisted),
                Collections.singleton(persisted), Collections.emptySet(), Collections.emptySet());
        Set<BaseTableInfo> currentTables = Collections.singleton(current);
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap())).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getRelation()).thenReturn(persistedRelation);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.emptySet());
        Mockito.when(currentTable.getId()).thenReturn(42L);

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<OlapTable> olapTable = Mockito.mockStatic(OlapTable.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(current)).thenReturn(currentTable);
            olapTable.when(() -> OlapTable.getVisibleVersionInBatch(Collections.singletonList(currentTable)))
                    .thenReturn(Collections.singletonList(7L));

            MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, currentTables);

            Assertions.assertEquals(7L, context.getBaseVersions().getTableVersions().get(42L));
            mtmvUtil.verify(() -> MTMVUtil.getTable(current), Mockito.times(1));
            mtmvUtil.verify(() -> MTMVUtil.getTable(persisted), Mockito.never());
        }
    }

    @Test
    public void lockedLocalVersionRefreshDetachesSnapshotsPreloadedByMixedContext() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        BaseTableInfo local = new BaseTableInfo(new TableNameInfo("internal", "db", "local"));
        BaseTableInfo external = new BaseTableInfo(new TableNameInfo("hms", "db", "external"));
        Set<BaseTableInfo> baseTables = new LinkedHashSet<>(Arrays.asList(local, external));
        MTMVBaseVersions before = new MTMVBaseVersions(Collections.singletonMap(1L, 1L), Collections.emptyMap());
        MTMVBaseVersions after = new MTMVBaseVersions(Collections.singletonMap(1L, 2L), Collections.emptyMap());
        OlapTable pctTable = Mockito.mock(OlapTable.class);
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap(), Mockito.anyMap()))
                .thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(pctTable));
        Mockito.when(pctTable.getAndCopyPartitionItems(Mockito.any())).thenReturn(Collections.emptyMap());

        TableIf oldLocalTable = Mockito.mock(TableIf.class);
        TableIf newLocalTable = Mockito.mock(TableIf.class);
        TableIf externalTable = Mockito.mock(TableIf.class);
        try (MockedStatic<MTMVPartitionUtil> partitionUtil = Mockito.mockStatic(MTMVPartitionUtil.class);
                MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            partitionUtil.when(() -> MTMVPartitionUtil.getBaseVersions(
                    mtmv, Collections.emptyMap(), baseTables))
                    .thenReturn(before);
            partitionUtil.when(() -> MTMVPartitionUtil.getCachedBaseVersions(
                    mtmv, Collections.emptyMap(), baseTables))
                    .thenReturn(after);
            mtmvUtil.when(() -> MTMVUtil.getTable(local)).thenReturn(oldLocalTable, newLocalTable);
            mtmvUtil.when(() -> MTMVUtil.getTable(external)).thenReturn(externalTable);
            MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv, baseTables);
            MTMVSnapshotIf localSnapshot = new MTMVTimestampSnapshot(1L);
            MTMVSnapshotIf externalSnapshot = new MTMVTimestampSnapshot(2L);
            context.getBaseTableSnapshotCache().put(local, localSnapshot);
            context.getBaseTableSnapshotCache().put(external, externalSnapshot);
            Assertions.assertSame(oldLocalTable, context.getBaseTable(local));
            Assertions.assertSame(externalTable, context.getBaseTable(external));

            context.refreshLocalStateFromCachedVersions();

            Assertions.assertEquals(2L, context.getBaseVersions().getTableVersions().get(1L));
            Assertions.assertFalse(context.getBaseTableSnapshotCache().containsKey(local));
            Assertions.assertSame(externalSnapshot, context.getBaseTableSnapshotCache().get(external));
            Assertions.assertSame(newLocalTable, context.getBaseTable(local));
            Assertions.assertSame(externalTable, context.getBaseTable(external));
            Mockito.verify(mtmv, Mockito.times(2))
                    .calculatePartitionMappings(Mockito.anyMap(), Mockito.anyMap());
            partitionUtil.verify(() -> MTMVPartitionUtil.getBaseVersions(
                    mtmv, Collections.emptyMap(), baseTables), Mockito.times(1));
            partitionUtil.verify(() -> MTMVPartitionUtil.getCachedBaseVersions(
                    mtmv, Collections.emptyMap(), baseTables), Mockito.times(1));
        }
    }

    private static void assertPersistencePreloadBatches(Function<String, Set<String>> persistedPartitions)
            throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = new LinkedHashMap<>();
        mappings.put("mv0", Collections.singletonMap(table, Collections.singleton("p0")));
        mappings.put("mv1", Collections.singletonMap(table, Collections.singleton("p1")));
        mappings.put("mv2", Collections.singletonMap(table, Collections.singleton("p2")));
        Mockito.when(mtmv.calculatePartitionMappings(Mockito.anyMap(), Mockito.anyMap())).thenReturn(mappings);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(mtmv.getRefreshSnapshot()).thenReturn(refreshSnapshot);
        Mockito.when(mtmv.getRelation()).thenReturn(new MTMVRelation(
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), Collections.emptySet()));
        Mockito.when(partitionInfo.getPartitionType())
                .thenReturn(MTMVPartitionInfo.MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(partitionInfo.getPctTables()).thenReturn(Collections.singleton(table));
        Mockito.when(table.getAndCopyPartitionItems(Mockito.any())).thenReturn(Collections.emptyMap());
        Mockito.when(table.supportsPartitionSnapshotBatchLoading()).thenReturn(true);
        Mockito.when(table.needAutoRefresh()).thenReturn(true);
        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.anyString(), Mockito.any()))
                .thenAnswer(invocation -> persistedPartitions.apply(invocation.getArgument(0)));
        MTMVSnapshotIf snapshot = new MTMVTimestampSnapshot(1L);
        Mockito.when(table.getPartitionSnapshots(Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> invocation.<List<String>>getArgument(0).stream().collect(Collectors.toMap(
                        name -> name, name -> snapshot, (left, right) -> left, LinkedHashMap::new)));

        MTMVRefreshContext context = MTMVRefreshContext.buildContext(mtmv);
        context.preloadSnapshotsForPersistence(mappings.keySet(), Collections.emptySet());

        Mockito.verify(table, Mockito.times(1)).getPartitionSnapshots(
                Mockito.argThat(names -> new LinkedHashSet<>(names).equals(
                        new LinkedHashSet<>(Arrays.asList("p0", "p1", "p2")))),
                Mockito.same(context), Mockito.any());
        Mockito.verifyNoInteractions(refreshSnapshot);
    }
}
