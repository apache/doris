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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.AbstractSet;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class QueryPartitionCollectorTest {
    private static final List<String> QUALIFIERS = List.of("catalog", "db", "table");

    private final StatementContext statement = new StatementContext();
    private final CascadesContext context = Mockito.mock(CascadesContext.class);
    private final ExternalTable table = Mockito.mock(ExternalTable.class);
    private final QueryPartitionCollector collector = new QueryPartitionCollector();

    private LogicalFileScan scan(int id, String... names) {
        Mockito.when(context.getStatementContext()).thenReturn(statement);
        Mockito.when(table.getDatabase()).thenReturn(Mockito.mock(DatabaseIf.class));
        Mockito.when(table.getFullQualifiers()).thenReturn(QUALIFIERS);
        Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
        ImmutableMap.Builder<String, PartitionItem> items = ImmutableMap.builder();
        for (String name : names) {
            items.put(name, Mockito.mock(PartitionItem.class));
        }
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getRelationId()).thenReturn(new RelationId(id));
        Mockito.when(scan.getSelectedPartitions()).thenReturn(new SelectedPartitions(10, items.build(), true));
        return scan;
    }

    @Test
    void nativePlanningRecordsUnknownAsAllPartitionsWithoutLoadingTheView() {
        LogicalFileScan scan = scan(1);
        Mockito.when(scan.getSelectedPartitions()).thenReturn(SelectedPartitions.NATIVE);
        Mockito.clearInvocations(table);
        collector.visitLogicalCatalogRelation(scan, context);
        Assertions.assertTrue(PartitionCompensator.getQueryUsedPartitions(statement, new BitSet())
                .containsKey(QUALIFIERS));
        Assertions.assertSame(PartitionCompensator.ALL_PARTITIONS,
                statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next());
        Mockito.verify(table, Mockito.never()).supportInternalPartitionPruned();
    }

    @Test
    void keepsTheSelectedSnapshotKeyView() {
        LogicalFileScan first = scan(1, "p1", "p2");
        collector.visitLogicalCatalogRelation(first, context);
        Set<String> recorded = statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next().value();
        Assertions.assertSame(first.getSelectedPartitions().selectedPartitions.keySet(), recorded);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> recorded.remove("p1"));
        collector.visitLogicalCatalogRelation(scan(2, "p3"), context);
        Assertions.assertEquals(Set.of("p1", "p2"), recorded);
    }

    @Test
    void selfJoinAndRepeatedRecordsPreserveUnionAndRelationFiltering() {
        LogicalFileScan first = scan(1, "p1", "p2");
        collector.visitLogicalCatalogRelation(first, context);
        collector.visitLogicalCatalogRelation(scan(2, "p2", "p3"), context);
        collector.visitLogicalCatalogRelation(first, context);
        Map<List<String>, Set<String>> union = PartitionCompensator.getQueryUsedPartitions(statement, new BitSet());
        Assertions.assertEquals(Set.of("p1", "p2", "p3"), union.get(QUALIFIERS));
        BitSet relation = new BitSet();
        relation.set(1);
        Assertions.assertEquals(Set.of("p1", "p2"),
                PartitionCompensator.getQueryUsedPartitions(statement, relation).get(QUALIFIERS));
        union.get(QUALIFIERS).clear();
        Assertions.assertEquals(Set.of("p1", "p2"), first.getSelectedPartitions().selectedPartitions.keySet());
    }

    @Test
    void recordingDoesNotVisitOrHashPartitionNames() {
        Set<String> names = new AbstractSet<>() {
            @Override
            public Iterator<String> iterator() {
                throw new AssertionError("recording must not iterate partition names");
            }

            @Override
            public int size() {
                return 120000;
            }

            @Override
            public int hashCode() {
                throw new AssertionError("recording must not hash partition names");
            }
        };
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, Pair.of(new RelationId(1), names));
        Assertions.assertSame(names,
                statement.getTableUsedPartitionNameMap().get(QUALIFIERS).iterator().next().value());
    }

    @Test
    void emptySelectionAndAllPartitionsKeepTheirDistinctMeaning() {
        collector.visitLogicalCatalogRelation(scan(1), context);
        Assertions.assertEquals(Collections.emptySet(),
                PartitionCompensator.getQueryUsedPartitions(statement, new BitSet()).get(QUALIFIERS));
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, PartitionCompensator.ALL_PARTITIONS);
        statement.getTableUsedPartitionNameMap().put(QUALIFIERS, PartitionCompensator.ALL_PARTITIONS);
        Assertions.assertNull(PartitionCompensator.getQueryUsedPartitions(statement, new BitSet()).get(QUALIFIERS));
    }
}
