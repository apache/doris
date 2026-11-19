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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PartitionCompensatorBatchTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 1020, 4999, 5000, 5001, 10001})
    public void testBoundedBatchesAreSharedAcrossRelatedTables(int partitionCount) throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= partitionCount; id++) {
            fixture.addPartition(id);
        }
        // Duplicate candidates must not cause repeated version requests.
        fixture.validPartitions.add(fixture.validPartitions.get(0));
        fixture.queryUsedPartitions.replaceAll((table, names) -> ImmutableSet.of("p1"));

        List<List<Long>> requests = new ArrayList<>();
        Mockito.when(fixture.mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    List<Long> ids = ImmutableList.copyOf(invocation.<Collection<Long>>getArgument(0));
                    Assertions.assertEquals(Optional.empty(), invocation.getArgument(1));
                    Assertions.assertTrue(ids.size() <= 5000);
                    requests.add(ids);
                    return ids;
                });

        Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> result = fixture.calculate();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.key().isEmpty());
        Assertions.assertTrue(result.value().isEmpty());
        Assertions.assertEquals((partitionCount + 4999) / 5000, requests.size());
        List<Long> requestedIds = new ArrayList<>();
        requests.forEach(requestedIds::addAll);
        Assertions.assertEquals(partitionCount, requestedIds.size());
        Set<Long> uniqueRequestedIds = new HashSet<>(requestedIds);
        Assertions.assertEquals(partitionCount, uniqueRequestedIds.size());
        for (long id = 1; id <= partitionCount; id++) {
            Assertions.assertTrue(uniqueRequestedIds.contains(id));
        }
    }

    @Test
    public void testMixedEmptyPartitionsPreserveCompensation() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addPartition(1L);
        fixture.addPartition(2L);
        fixture.queryUsedPartitions.put(fixture.firstTable.getFullQualifiers(), ImmutableSet.of("p2", "t1_new"));
        fixture.queryUsedPartitions.put(fixture.secondTable.getFullQualifiers(), ImmutableSet.of("p2", "t2_new"));
        Mockito.when(fixture.mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(ImmutableList.of(2L));

        Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> result = fixture.calculate();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.key().isEmpty());
        Assertions.assertEquals(2, result.value().size());
        result.value().values().forEach(names ->
                Assertions.assertEquals(ImmutableSet.of("t1_new", "t2_new"), names));
        Mockito.verify(fixture.mtmv).selectNonEmptyPartitionIds(ImmutableList.of(1L, 2L), Optional.empty());

        // The only non-empty MV partition does not cover p1, so it cannot answer this query.
        fixture.queryUsedPartitions.replaceAll((table, names) -> ImmutableSet.of("p1"));
        Assertions.assertNull(fixture.calculate());
    }

    @Test
    public void testAllEmptyPartitionsRejectMv() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addPartition(1L);
        fixture.addPartition(2L);
        Mockito.when(fixture.mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(ImmutableList.of());

        Assertions.assertNull(fixture.calculate());
        Mockito.verify(fixture.mtmv).selectNonEmptyPartitionIds(ImmutableList.of(1L, 2L), Optional.empty());
    }

    @Test
    public void testOnlyMappedValidPartitionsAreChecked() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addPartition(1L);
        fixture.addPartition(2L);
        fixture.addPartition(3L);
        fixture.validPartitions.remove(2);
        fixture.partitionMappings.values().forEach(mapping -> mapping.remove("mv_p2"));
        Mockito.when(fixture.mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenAnswer(invocation -> ImmutableList.copyOf(invocation.<Collection<Long>>getArgument(0)));

        Assertions.assertNotNull(fixture.calculate());
        Mockito.verify(fixture.mtmv).selectNonEmptyPartitionIds(ImmutableList.of(1L), Optional.empty());
    }

    @Test
    public void testNoMappedValidPartitionsSkipVersionRequests() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addPartition(1L);
        fixture.partitionMappings.values().forEach(Map::clear);

        Assertions.assertNull(fixture.calculate());
        Mockito.verify(fixture.mtmv, Mockito.never())
                .selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testNoValidPartitionsSkipVersionRequests() throws Exception {
        Fixture fixture = new Fixture();

        Assertions.assertNull(fixture.calculate());
        Mockito.verify(fixture.mtmv, Mockito.never())
                .selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testFailedBatchDoesNotReturnPartialCompensation() throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= 5001; id++) {
            fixture.addPartition(id);
        }
        RuntimeException failure = new RuntimeException("get version from meta service failed");
        Mockito.when(fixture.mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenAnswer(invocation -> ImmutableList.copyOf(invocation.<Collection<Long>>getArgument(0)))
                .thenThrow(failure);

        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, fixture::calculate));
        Mockito.verify(fixture.mtmv, Mockito.times(2))
                .selectNonEmptyPartitionIds(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    private static class Fixture {
        private final MTMV mtmv = Mockito.mock(MTMV.class);
        private final List<Partition> validPartitions = new ArrayList<>();
        private final Map<MTMVRelatedTableIf, Map<String, Set<String>>> partitionMappings = new HashMap<>();
        private final Map<List<String>, Set<String>> queryUsedPartitions = new HashMap<>();
        private final AsyncMaterializationContext materializationContext
                = Mockito.mock(AsyncMaterializationContext.class);
        private final CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        private final Plan rewrittenPlan = Mockito.mock(Plan.class);
        private final MTMVRelatedTableIf firstTable;
        private final MTMVRelatedTableIf secondTable;

        Fixture() throws AnalysisException {
            CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.getName()).thenReturn("internal");
            DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
            Mockito.when(database.getId()).thenReturn(2L);
            Mockito.when(database.getFullName()).thenReturn("db");
            Mockito.when(database.getCatalog()).thenReturn(catalog);
            firstTable = mockTable("t1", 10L, database);
            secondTable = mockTable("t2", 20L, database);
            partitionMappings.put(firstTable, new HashMap<>());
            partitionMappings.put(secondTable, new HashMap<>());
            queryUsedPartitions.put(firstTable.getFullQualifiers(), ImmutableSet.of("p1"));
            queryUsedPartitions.put(secondTable.getFullQualifiers(), ImmutableSet.of("p1"));

            Mockito.when(mtmv.getId()).thenReturn(100L);
            Mockito.when(mtmv.getName()).thenReturn("mv");
            Mockito.when(mtmv.getDatabase()).thenReturn(database);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(partitionInfo.getType()).thenReturn(PartitionType.RANGE);
            Mockito.when(mtmv.getPartitionInfo()).thenReturn(partitionInfo);
            MTMVPartitionInfo mvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
            Mockito.when(mvPartitionInfo.getPctTables()).thenReturn(ImmutableSet.of(firstTable, secondTable));
            List<BaseColInfo> pctInfos = ImmutableList.of(
                    new BaseColInfo("dt", new BaseTableInfo(firstTable)),
                    new BaseColInfo("dt", new BaseTableInfo(secondTable)));
            Mockito.when(mvPartitionInfo.getPctInfos()).thenReturn(pctInfos);
            Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mvPartitionInfo);

            Mockito.when(materializationContext.getMtmv()).thenReturn(mtmv);
            Mockito.when(materializationContext.calculatePartitionMappings(ArgumentMatchers.any()))
                    .thenReturn(partitionMappings);
            StatementContext statementContext = Mockito.mock(StatementContext.class);
            Map<BaseTableInfo, Collection<Partition>> canRewritePartitions
                    = ImmutableMap.of(new BaseTableInfo(mtmv), validPartitions);
            Mockito.when(statementContext.getMvCanRewritePartitionsMap())
                    .thenReturn(canRewritePartitions);
            Mockito.when(cascadesContext.getStatementContext()).thenReturn(statementContext);
            Mockito.when(rewrittenPlan.collectToList(ArgumentMatchers.any())).thenReturn(ImmutableList.of());
        }

        private void addPartition(long id) {
            String name = "mv_p" + id;
            validPartitions.add(new Partition(id, name, null, null));
            partitionMappings.values().forEach(mapping -> mapping.put(name, ImmutableSet.of("p" + id)));
        }

        private Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calculate()
                throws AnalysisException {
            return PartitionCompensator.calcInvalidPartitions(
                    queryUsedPartitions, rewrittenPlan, materializationContext, cascadesContext);
        }

        private static MTMVRelatedTableIf mockTable(String name, long id, DatabaseIf<?> database) {
            MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
            Mockito.when(table.getId()).thenReturn(id);
            Mockito.when(table.getName()).thenReturn(name);
            Mockito.when(table.getDatabase()).thenReturn(database);
            Mockito.when(table.getFullQualifiers()).thenReturn(ImmutableList.of("internal", "db", name));
            return table;
        }
    }
}
