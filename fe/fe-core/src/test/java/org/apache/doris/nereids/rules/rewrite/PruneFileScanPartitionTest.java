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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HivePartitionFilterBuilder;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PruneFileScanPartitionTest {
    @Test
    public void testIntegerJdoFallbackPreservesCanonicalAndLeadingZeroNames() throws Exception {
        assertIntegerPartitionPruning(false);
    }

    @Test
    public void testPreloadedPartitionViewIsLocallyPrunedWithoutAnotherHmsCall() throws Exception {
        assertIntegerPartitionPruning(true);
    }

    private void assertIntegerPartitionPruning(boolean preloaded) throws Exception {
        Column partitionColumn = new Column("p", Type.INT, true);
        PartitionKey canonical = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("1")), Collections.singletonList(Type.INT), true);
        PartitionKey leadingZero = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("01")), Collections.singletonList(Type.INT), true);
        PartitionKey other = PartitionKey.createListPartitionKeyWithTypes(
                Collections.singletonList(new PartitionValue("2")), Collections.singletonList(Type.INT), true);
        Map<String, PartitionItem> partitions = ImmutableMap.of(
                "p=1", new ListPartitionItem(Collections.singletonList(canonical)),
                "p=01", new ListPartitionItem(Collections.singletonList(leadingZero)),
                "p=2", new ListPartitionItem(Collections.singletonList(other)));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(Collections.singletonList(partitionColumn));
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(Collections.singletonList(partitionColumn));
        Mockito.when(table.getName()).thenReturn("hive_tbl");
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(preloaded
                ? new SelectedPartitions(partitions.size(), partitions, false)
                : SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(table.getNameToPartitionItems(Mockito.any())).thenReturn(partitions);
        Mockito.when(table.getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            String filter = HivePartitionFilterBuilder.build(invocation.getArgument(1),
                    Collections.singletonList(partitionColumn), ImmutableMap.of("p", "int"));
            return filter == null ? Optional.empty() : Optional.of(Collections.emptyMap());
        });
        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                Collections.singleton(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(1))), scan);
        ConnectContext connect = Mockito.mock(ConnectContext.class);
        Mockito.when(connect.getSessionVariable()).thenReturn(new SessionVariable());
        StatementContext statement = new StatementContext(connect, null);
        Mockito.when(connect.getStatementContext()).thenReturn(statement);
        SelectedPartitions result = Deencapsulation.invoke(new PruneFileScanPartition(),
                "pruneExternalPartitions", table, filter, scan,
                CascadesContext.initContext(statement, filter, PhysicalProperties.ANY));
        Assertions.assertEquals(new java.util.HashSet<>(Arrays.asList("p=1", "p=01")),
                result.selectedPartitions.keySet());
        Assertions.assertEquals(Collections.singletonList("01"), leadingZero.getOriginHiveKeys());
        Assertions.assertTrue(result.hasPartitionPredicate);
        Mockito.verify(table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
        if (preloaded) {
            Mockito.verify(table, Mockito.never()).getNameToPartitionItemsByFilter(Mockito.any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).getNameToPartitionItems(Mockito.any());
        }
    }

    @Test
    public void testPartitionColumnsUseRelationSnapshot() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        List<Column> partitionColumns = Collections.singletonList(new Column("old_partition", Type.INT, true));
        Mockito.when(scan.getRelationSnapshot()).thenReturn(Optional.of(relationSnapshot));
        Mockito.when(table.getPartitionColumns(Optional.of(relationSnapshot))).thenReturn(partitionColumns);

        Assertions.assertSame(partitionColumns,
                PruneFileScanPartition.getPartitionColumnsForScan(table, scan));
        Mockito.verify(table).getPartitionColumns(Optional.of(relationSnapshot));
    }
}
