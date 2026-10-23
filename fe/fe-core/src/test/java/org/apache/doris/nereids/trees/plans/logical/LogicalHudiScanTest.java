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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hudi.source.IncrementalRelation;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalHudiScanToPhysicalHudiScan;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHudiScan;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class LogicalHudiScanTest {
    @Test
    void testAliasRewritePreservesHudiScanAndIncrementalState() {
        LogicalHudiScan scan = newScan();
        CascadesContext context = Mockito.mock(CascadesContext.class);
        Plan rewritten = new LogicalSubQueryAliasToLogicalProject().build()
                .transform(new LogicalSubQueryAlias<>("t1", scan), context).get(0);
        LogicalHudiScan aliased = Assertions.assertInstanceOf(LogicalHudiScan.class, rewritten.child(0));
        assertScanState(scan, aliased);
        Assertions.assertEquals("t1", aliased.getTableAlias());

        Plan physical = new LogicalHudiScanToPhysicalHudiScan().build().transform(aliased, context).get(0);
        PhysicalHudiScan hudiScan = Assertions.assertInstanceOf(PhysicalHudiScan.class, physical);
        Assertions.assertSame(scan.getIncrementalRelation(), hudiScan.getIncrementalRelation());
        Assertions.assertSame(scan.getRelationSnapshot(), hudiScan.getRelationSnapshot());
        Assertions.assertSame(scan.getSelectedPartitions(), hudiScan.getSelectedPartitions());
    }

    @Test
    void testCopiesPreserveHudiAliasAndScanState() {
        LogicalHudiScan scan = newScan();
        LogicalHudiScan aliased = Assertions.assertInstanceOf(LogicalHudiScan.class, scan.withTableAlias("t1"));
        List<Plan> copies = Arrays.asList(
                aliased.withGroupExpression(Optional.empty()),
                aliased.withGroupExprLogicalPropChildren(Optional.empty(),
                        Optional.of(aliased.getLogicalProperties()), Collections.emptyList()),
                aliased.withSelectedPartitions(aliased.getSelectedPartitions()),
                aliased.withRelationId(new RelationId(2)),
                aliased.withOperativeSlots(aliased.getOperativeSlots()),
                aliased.withCachedOutput(aliased.getOutput()),
                aliased.withScanParams((HMSExternalTable) aliased.getTable(), Optional.empty()));
        for (Plan copy : copies) {
            LogicalHudiScan hudiScan = Assertions.assertInstanceOf(LogicalHudiScan.class, copy);
            Assertions.assertEquals("t1", hudiScan.getTableAlias());
            Assertions.assertSame(scan.getRelationSnapshot(), hudiScan.getRelationSnapshot());
            Assertions.assertSame(scan.getSelectedPartitions(), hudiScan.getSelectedPartitions());
            Assertions.assertEquals(scan.getOutput(), hudiScan.getOutput());
        }
        assertScanState(scan, aliased);
        Assertions.assertEquals("", scan.getTableAlias());
    }

    private LogicalHudiScan newScan() {
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.getName()).thenReturn("hudi_table");
        Slot slot = new SlotReference("id", IntegerType.INSTANCE);
        List<Slot> output = Collections.singletonList(slot);
        return new LogicalHudiScan(new RelationId(1), table, Collections.singletonList("db"),
                new LogicalFileScan.SelectedPartitions(1, Collections.emptyMap(), true, true),
                Optional.of(Mockito.mock(TableSample.class)), Optional.of(Mockito.mock(TableSnapshot.class)),
                Optional.of(Mockito.mock(TableScanParams.class)), Optional.of(Mockito.mock(IncrementalRelation.class)),
                output, Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.of(output),
                Optional.of(Collections.singletonList(new Column("id", Type.INT))),
                Optional.of(Mockito.mock(MvccSnapshot.class)));
    }

    private void assertScanState(LogicalHudiScan expected, LogicalHudiScan actual) {
        Assertions.assertSame(expected.getTable(), actual.getTable());
        Assertions.assertEquals(expected.getRelationId(), actual.getRelationId());
        Assertions.assertEquals(expected.getQualifier(), actual.getQualifier());
        Assertions.assertSame(expected.getSelectedPartitions(), actual.getSelectedPartitions());
        Assertions.assertSame(expected.getTableSample(), actual.getTableSample());
        Assertions.assertSame(expected.getTableSnapshot(), actual.getTableSnapshot());
        Assertions.assertSame(expected.getScanParams(), actual.getScanParams());
        Assertions.assertSame(expected.getIncrementalRelation(), actual.getIncrementalRelation());
        Assertions.assertSame(expected.getRelationSnapshot(), actual.getRelationSnapshot());
        Assertions.assertSame(expected.relationSchema, actual.relationSchema);
        Assertions.assertEquals(expected.getOperativeSlots(), actual.getOperativeSlots());
        Assertions.assertEquals(expected.getOutput(), actual.getOutput());
    }
}
