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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

public class SlotLineageAnalyzerTest {

    @Test
    public void testDirectSourcePreservesResolvedAccessPaths() {
        SlotReference requestedSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference relationSlot = requestedSlot.withColumn(column("a", 1)).withAccessPaths(
                ImmutableList.of(ColumnAccessPath.data(ImmutableList.of("nested"))), ImmutableList.of());
        PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

        MaterializationAnalysisResult<OutputLineage> result =
                new SlotLineageAnalyzer().analyze(scan, requestedSlot);

        Assertions.assertTrue(result.isApplicable());
        Assertions.assertEquals(OutputLineage.Kind.DIRECT, result.getValue().getKind());
        Assertions.assertSame(relationSlot, result.getValue().getSource().get().getBaseSlot());
        Assertions.assertEquals(relationSlot.getAllAccessPaths(),
                result.getValue().getSource().get().getBaseSlot().getAllAccessPaths());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAliasIsForwardedToPhysicalSource() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE).withColumn(column("a", 1));
        Alias alias = new Alias(baseSlot, "renamed");
        SlotReference aliasSlot = (SlotReference) alias.toSlot();
        PhysicalOlapScan scan = mockBaseOlapScan(baseSlot);
        PhysicalProject<PhysicalOlapScan> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(aliasSlot));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.child()).thenReturn(scan);

        MaterializationAnalysisResult<OutputLineage> result =
                new SlotLineageAnalyzer().analyze(project, aliasSlot);

        Assertions.assertTrue(result.isApplicable());
        Assertions.assertEquals(OutputLineage.Kind.FORWARDED, result.getValue().getKind());
        Assertions.assertSame(baseSlot, result.getValue().getSource().get().getBaseSlot());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDerivedExpressionRecordsEagerInputs() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE).withColumn(column("a", 1));
        Alias derived = new Alias(new Add(baseSlot, new IntegerLiteral(1)), "derived");
        SlotReference derivedSlot = (SlotReference) derived.toSlot();
        PhysicalProject<PhysicalOlapScan> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(derivedSlot));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(derived));

        MaterializationAnalysisResult<OutputLineage> result =
                new SlotLineageAnalyzer().analyze(project, derivedSlot);

        Assertions.assertTrue(result.isApplicable());
        Assertions.assertEquals(OutputLineage.Kind.DERIVED, result.getValue().getKind());
        Assertions.assertEquals(ImmutableList.of(baseSlot), result.getValue().getInputs());
        Assertions.assertFalse(result.getValue().getSource().isPresent());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBatchAnalysisResolvesInternalProjectInputFromProducer() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE).withColumn(column("a", 1));
        Alias alias = new Alias(baseSlot, "renamed");
        SlotReference aliasSlot = (SlotReference) alias.toSlot();
        PhysicalOlapScan scan = mockBaseOlapScan(baseSlot);
        PhysicalProject<PhysicalOlapScan> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(aliasSlot));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(alias));
        Mockito.when(project.child()).thenReturn(scan);
        Mockito.when(project.children()).thenReturn(ImmutableList.of(scan));

        Map<Slot, MaterializationAnalysisResult<OutputLineage>> results =
                new SlotLineageAnalyzer().analyze(project, ImmutableList.of(aliasSlot, baseSlot));

        Assertions.assertEquals(OutputLineage.Kind.FORWARDED, results.get(aliasSlot).getValue().getKind());
        Assertions.assertEquals(OutputLineage.Kind.DIRECT, results.get(baseSlot).getValue().getKind());
        Assertions.assertSame(baseSlot, results.get(baseSlot).getValue().getSource().get().getBaseSlot());
    }

    @Test
    public void testLightSchemaChangeDisabledIsNotApplicable() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE).withColumn(column("a", 1));
        PhysicalOlapScan scan = mockBaseOlapScan(baseSlot);
        Mockito.when(scan.getTable().getEnableLightSchemaChange()).thenReturn(false);

        MaterializationAnalysisResult<OutputLineage> result =
                new SlotLineageAnalyzer().analyze(scan, baseSlot);

        Assertions.assertFalse(result.isApplicable());
        Assertions.assertEquals(MaterializationAnalysisResult.NotApplicableReason.UNSUPPORTED_SOURCE,
                result.getReason().get());
    }

    private PhysicalOlapScan mockBaseOlapScan(SlotReference outputSlot) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(outputSlot));
        Mockito.when(scan.getRelationId()).thenReturn(new RelationId(1));
        return scan;
    }

    private Column column(String name, int uniqueId) {
        Column column = new Column(name, Type.INT);
        column.setUniqueId(uniqueId);
        return column;
    }
}
