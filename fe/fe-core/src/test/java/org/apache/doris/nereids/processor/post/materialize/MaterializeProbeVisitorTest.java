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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class MaterializeProbeVisitorTest {

    @Test
    public void testOlapScanRejectsRequiredMaterializedSlots() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockBaseOlapScan(baseSlot);

        Set<Slot> requiredMaterializedSlots = new HashSet<>();
        requiredMaterializedSlots.add(baseSlot);
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(
                baseSlot, requiredMaterializedSlots);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalOlapScan(scan, context);

        Assertions.assertFalse(source.isPresent());
    }

    @Test
    public void testOlapScanUsesRelationSlotWithAccessPaths() {
        SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference relationSlot = contextSlot.withAccessPaths(
                ImmutableList.of(ColumnAccessPath.data(ImmutableList.of("nested"))), ImmutableList.of());
        contextSlot = (SlotReference) contextSlot.withNullable(false);
        PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(contextSlot);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalOlapScan(scan, context);

        Assertions.assertTrue(source.isPresent());
        Assertions.assertSame(relationSlot, source.get().baseSlot);
        Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFilterUsingIndexUsesRelationSlotWithAccessPaths() {
        ConnectContext oldContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().topNLazyMaterializationUsingIndex = true;
        context.setThreadLocalInfo();
        try {
            SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
            SlotReference relationSlot = contextSlot.withAccessPaths(
                    ImmutableList.of(ColumnAccessPath.data(ImmutableList.of("nested"))), ImmutableList.of());
            contextSlot = (SlotReference) contextSlot.withNullable(false);
            PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

            PhysicalFilter<PhysicalOlapScan> filter = Mockito.mock(PhysicalFilter.class);
            Mockito.when(filter.child()).thenReturn(scan);
            Mockito.when(filter.getInputSlots()).thenReturn(ImmutableSet.of(contextSlot));

            MaterializeProbeVisitor.ProbeContext probeContext = new MaterializeProbeVisitor.ProbeContext(contextSlot);
            Optional<MaterializeSource> source =
                    new MaterializeProbeVisitor().visitPhysicalFilter(filter, probeContext);

            Assertions.assertTrue(source.isPresent());
            Assertions.assertSame(relationSlot, source.get().baseSlot);
            Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
        } finally {
            if (oldContext == null) {
                ConnectContext.remove();
            } else {
                oldContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testComplexProjectInputSlotsAreRequiredMaterialized() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE);
        Alias complexAlias = new Alias(new Add(baseSlot, new IntegerLiteral(1)), "x");
        SlotReference aliasSlot = (SlotReference) complexAlias.toSlot();
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(baseSlot, aliasSlot));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(baseSlot, complexAlias));
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(project.child()).thenReturn(child);
        Mockito.when(child.accept(Mockito.any(MaterializeProbeVisitor.class),
                Mockito.any(MaterializeProbeVisitor.ProbeContext.class))).thenReturn(Optional.empty());

        Set<Slot> requiredMaterializedSlots = new HashSet<>();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(
                aliasSlot, requiredMaterializedSlots);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalProject(project, context);

        Assertions.assertFalse(source.isPresent());
        Assertions.assertEquals(ImmutableList.of(baseSlot), ImmutableList.copyOf(requiredMaterializedSlots));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPushedDownProjectSlotInputsAreRequiredMaterialized() {
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference pushedDownSlot = new SlotReference("pushed", IntegerType.INSTANCE);
        Alias pushedDownAlias = new Alias(pushedDownSlot, "x");
        SlotReference aliasSlot = (SlotReference) pushedDownAlias.toSlot();
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getOutput()).thenReturn(ImmutableList.of(baseSlot, aliasSlot));
        Mockito.when(project.getProjects()).thenReturn(ImmutableList.of(baseSlot, pushedDownAlias));
        Mockito.when(project.getInputSlots()).thenReturn(ImmutableSet.of(baseSlot, pushedDownSlot));
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(project.child()).thenReturn(child);
        Mockito.when(child.accept(Mockito.any(MaterializeProbeVisitor.class),
                Mockito.any(MaterializeProbeVisitor.ProbeContext.class))).thenReturn(Optional.empty());

        Set<Slot> requiredMaterializedSlots = new HashSet<>();
        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(
                aliasSlot, requiredMaterializedSlots);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalProject(project, context);

        Assertions.assertFalse(source.isPresent());
        Assertions.assertEquals(ImmutableSet.of(baseSlot, pushedDownSlot), requiredMaterializedSlots);
    }

    private PhysicalOlapScan mockBaseOlapScan(SlotReference outputSlot) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        // lazy materialization requires light_schema_change to be enabled
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(outputSlot));
        return scan;
    }

    @Test
    public void testPluginDrivenTableSupportedWhenConnectorDeclaresLazyTopN() {
        // Post-flip iceberg becomes a PluginDrivenExternalTable subclass (not in the legacy exact-class
        // SUPPORT_RELATION_TYPES set); it is admitted for Top-N lazy materialization only via the connector
        // capability.
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsTopNLazyMaterialize()).thenReturn(true);
        PhysicalCatalogRelation relation = Mockito.mock(PhysicalCatalogRelation.class);
        Mockito.when(relation.getTable()).thenReturn(table);

        Assertions.assertTrue(new MaterializeProbeVisitor().checkRelationTableSupportedType(relation));
    }

    @Test
    public void testPluginDrivenTableUnsupportedWhenConnectorLacksLazyTopN() {
        // A plugin-driven table whose connector does NOT declare the capability (e.g. jdbc/es, which also
        // become PluginDrivenExternalTable) stays excluded — guards against a blanket isAssignableFrom that
        // would wrongly enable lazy materialization for row/passthrough connectors.
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsTopNLazyMaterialize()).thenReturn(false);
        PhysicalCatalogRelation relation = Mockito.mock(PhysicalCatalogRelation.class);
        Mockito.when(relation.getTable()).thenReturn(table);

        Assertions.assertFalse(new MaterializeProbeVisitor().checkRelationTableSupportedType(relation));
    }
}
