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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AsyncMaterializationContextTest {

    @Test
    public void testCalculatePartitionMappingsMergesUncoveredFilters() throws Exception {
        List<String> qualifiers = ImmutableList.of("ctl", "db", "t1");
        MTMVRelatedTableIf pctTable = mockPctTable(qualifiers);
        MTMV mtmv = mockMtmv(pctTable);
        AsyncMaterializationContext context = createContext(mtmv);

        Map<List<String>, Set<String>> queryUsedPartitionsA = Maps.newHashMap();
        queryUsedPartitionsA.put(qualifiers, ImmutableSet.of("p1"));
        Map<List<String>, Set<String>> queryUsedPartitionsB = Maps.newHashMap();
        queryUsedPartitionsB.put(qualifiers, ImmutableSet.of("p2"));

        Mockito.when(mtmv.getEffectiveQueryUsedBaseTablePartitionMap(queryUsedPartitionsA))
                .thenReturn(queryUsedPartitionsA);
        Mockito.when(mtmv.getEffectiveQueryUsedBaseTablePartitionMap(queryUsedPartitionsB))
                .thenReturn(queryUsedPartitionsB);
        Mockito.when(mtmv.calculatePartitionMappings(queryUsedPartitionsA))
                .thenReturn(buildPartitionMultiMap("mv_p1", pctTable, "p1"));
        Mockito.when(mtmv.calculatePartitionMappings(queryUsedPartitionsB))
                .thenReturn(buildPartitionMultiMap("mv_p2", pctTable, "p2"));

        Map<MTMVRelatedTableIf, Map<String, Set<String>>> resultA
                = context.calculatePartitionMappings(queryUsedPartitionsA);
        Map<MTMVRelatedTableIf, Map<String, Set<String>>> resultB
                = context.calculatePartitionMappings(queryUsedPartitionsB);

        Assertions.assertEquals(ImmutableSet.of("p1"), resultA.get(pctTable).get("mv_p1"));
        Assertions.assertEquals(ImmutableSet.of("p1"), resultB.get(pctTable).get("mv_p1"));
        Assertions.assertEquals(ImmutableSet.of("p2"), resultB.get(pctTable).get("mv_p2"));
        Mockito.verify(mtmv).calculatePartitionMappings(queryUsedPartitionsA);
        Mockito.verify(mtmv).calculatePartitionMappings(queryUsedPartitionsB);
    }

    @Test
    public void testCalculatePartitionMappingsReusesCoverageAfterFullMapping() throws Exception {
        List<String> qualifiers = ImmutableList.of("ctl", "db", "t1");
        MTMVRelatedTableIf pctTable = mockPctTable(qualifiers);
        MTMV mtmv = mockMtmv(pctTable);
        AsyncMaterializationContext context = createContext(mtmv);

        Map<List<String>, Set<String>> noFilter = Maps.newHashMap();
        Map<List<String>, Set<String>> queryUsedPartitions = Maps.newHashMap();
        queryUsedPartitions.put(qualifiers, ImmutableSet.of("p1"));

        Mockito.when(mtmv.getEffectiveQueryUsedBaseTablePartitionMap(noFilter)).thenReturn(noFilter);
        Mockito.when(mtmv.getEffectiveQueryUsedBaseTablePartitionMap(queryUsedPartitions))
                .thenReturn(queryUsedPartitions);
        Mockito.when(mtmv.calculatePartitionMappings(noFilter))
                .thenReturn(buildPartitionMultiMap("mv_p1", pctTable, "p1"));

        Map<MTMVRelatedTableIf, Map<String, Set<String>>> resultA = context.calculatePartitionMappings(noFilter);
        Map<MTMVRelatedTableIf, Map<String, Set<String>>> resultB
                = context.calculatePartitionMappings(queryUsedPartitions);

        Assertions.assertSame(resultA, resultB);
        Assertions.assertEquals(ImmutableSet.of("p1"), resultB.get(pctTable).get("mv_p1"));
        Mockito.verify(mtmv).calculatePartitionMappings(noFilter);
        Mockito.verify(mtmv, Mockito.never()).calculatePartitionMappings(queryUsedPartitions);
    }

    private static AsyncMaterializationContext createContext(MTMV mtmv) {
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(statementContext);
        StructInfo structInfo = Mockito.mock(StructInfo.class);
        Mockito.doReturn(Collections.<Expression>emptyList()).when(structInfo).getPlanOutputShuttledExpressions();
        Plan plan = Mockito.mock(Plan.class);
        return new AsyncMaterializationContext(mtmv, plan, plan, ImmutableList.of(), ImmutableList.of(),
                cascadesContext, structInfo);
    }

    private static MTMV mockMtmv(MTMVRelatedTableIf pctTable) throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo mtmvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mtmvPartitionInfo);
        Mockito.when(mtmvPartitionInfo.getPctTables()).thenReturn(ImmutableSet.of(pctTable));
        return mtmv;
    }

    private static MTMVRelatedTableIf mockPctTable(List<String> qualifiers) {
        MTMVRelatedTableIf pctTable = Mockito.mock(MTMVRelatedTableIf.class);
        Mockito.when(pctTable.getFullQualifiers()).thenReturn(qualifiers);
        return pctTable;
    }

    private static Map<String, Map<MTMVRelatedTableIf, Set<String>>> buildPartitionMultiMap(
            String mvPartitionName, MTMVRelatedTableIf pctTable, String basePartitionName) {
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMultiMap = Maps.newHashMap();
        Map<MTMVRelatedTableIf, Set<String>> relatedTableToPartitions = Maps.newHashMap();
        relatedTableToPartitions.put(pctTable, ImmutableSet.of(basePartitionName));
        partitionMultiMap.put(mvPartitionName, relatedTableToPartitions);
        return partitionMultiMap;
    }
}
