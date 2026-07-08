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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class IvmDeltaRewriteHelperTest extends IvmDeltaTestBase {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    // ==================== isIncrementalDeltaScan ====================

    @Test
    void testIsIncrementalDeltaScan_regularScan_returnsFalse() {
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Assertions.assertFalse(helper.isIncrementalDeltaScan(scan));
    }

    @Test
    void testIsIncrementalDeltaScan_streamScanNotIncremental_returnsFalse() {
        LogicalOlapTableStreamScan scan = Mockito.mock(LogicalOlapTableStreamScan.class);
        Mockito.when(scan.isIncrementalScan()).thenReturn(false);
        Assertions.assertFalse(helper.isIncrementalDeltaScan(scan));
    }

    @Test
    void testIsIncrementalDeltaScan_streamScanIncremental_returnsTrue() {
        LogicalOlapTableStreamScan scan = Mockito.mock(LogicalOlapTableStreamScan.class);
        Mockito.when(scan.isIncrementalScan()).thenReturn(true);
        Assertions.assertTrue(helper.isIncrementalDeltaScan(scan));
    }

    // ==================== findSlotByName ====================

    @Test
    void testFindSlotByName_found() {
        List<Slot> slots = new ArrayList<>();
        Slot slot = Mockito.mock(Slot.class);
        Mockito.when(slot.getName()).thenReturn("foundSlot");
        slots.add(slot);

        Slot result = helper.findSlotByName(slots, "foundSlot");
        Assertions.assertSame(slot, result);
    }

    @Test
    void testFindSlotByName_notFound_throws() {
        List<Slot> slots = new ArrayList<>();
        Slot slot = Mockito.mock(Slot.class);
        Mockito.when(slot.getName()).thenReturn("otherSlot");
        slots.add(slot);

        Assertions.assertThrows(AnalysisException.class,
                () -> helper.findSlotByName(slots, "missingSlot"));
    }

    @Test
    void testFindSlotByName_findsColumnBinlogOperationCol() {
        List<Slot> slots = new ArrayList<>();
        Slot opSlot = Mockito.mock(Slot.class);
        Mockito.when(opSlot.getName()).thenReturn(Column.BINLOG_OPERATION_COL);
        slots.add(opSlot);

        Slot result = helper.findSlotByName(slots, Column.BINLOG_OPERATION_COL);
        Assertions.assertSame(opSlot, result);
    }

    @Test
    void testRemapScanOutputForPreSnapshotPreservesExprId() throws Exception {
        LogicalOlapScan scan = buildScanForTable(1, "t_pre");
        OlapTableStream stream = (OlapTableStream) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("test_db")
                .getTableOrAnalysisException(IvmUtil.streamName(0L, "t_pre"));

        LogicalPlan preSnapshot = (LogicalPlan) scan.withPreSnapshot(Optional.of(stream));
        LogicalPlan remapped = helper.remapScanOutput(scan, preSnapshot);

        Assertions.assertInstanceOf(LogicalProject.class, remapped);
        Assertions.assertInstanceOf(LogicalOlapTableStreamScan.class, remapped.child(0));
        LogicalOlapTableStreamScan snapshotChild = (LogicalOlapTableStreamScan) remapped.child(0);
        Assertions.assertTrue(snapshotChild.isSnapshot());
        Assertions.assertFalse(snapshotChild.isIncrementalScan());
        Assertions.assertFalse(snapshotChild.isReset());
        for (int i = 0; i < scan.getOutput().size(); i++) {
            Assertions.assertEquals(scan.getOutput().get(i).getExprId(), remapped.getOutput().get(i).getExprId());
            Assertions.assertEquals(scan.getOutput().get(i).getName(), remapped.getOutput().get(i).getName());
        }
    }

    @Test
    void testRemapScanOutputForPostSnapshotPreservesExprId() {
        LogicalOlapScan scan = buildScanForTable(2, "t_post");

        LogicalPlan postSnapshot = (LogicalPlan) scan.withPostSnapshot();
        LogicalPlan remapped = helper.remapScanOutput(scan, postSnapshot);

        Assertions.assertInstanceOf(LogicalProject.class, remapped);
        Assertions.assertInstanceOf(LogicalOlapScan.class, remapped.child(0));
        Assertions.assertFalse(remapped.child(0) instanceof LogicalOlapTableStreamScan);
        for (int i = 0; i < scan.getOutput().size(); i++) {
            Assertions.assertEquals(scan.getOutput().get(i).getExprId(), remapped.getOutput().get(i).getExprId());
            Assertions.assertEquals(scan.getOutput().get(i).getName(), remapped.getOutput().get(i).getName());
        }
    }

    @Test
    void testRemapScanOutputMissingVisibleColumnThrows() {
        LogicalOlapScan scan = buildScanForTable(3, "t_missing");
        LogicalPlan newPlan = Mockito.mock(LogicalPlan.class);
        Mockito.when(newPlan.getOutput()).thenReturn(ImmutableList.of());

        Assertions.assertThrows(AnalysisException.class, () -> helper.remapScanOutput(scan, newPlan));
    }

    @Test
    void testRemapProjectChildToPreSnapshotPreservesExprId() {
        LogicalOlapTableStreamScan deltaScan = buildDeltaScanForTable(4, "t_proj_pre");
        NamedExpression aliasId = new Alias(deltaScan.getOutput().get(0), "alias_id");
        NamedExpression passthroughName = (NamedExpression) deltaScan.getOutput().get(1);
        LogicalProject<LogicalOlapTableStreamScan> oldProject = new LogicalProject<>(
                ImmutableList.of(aliasId, passthroughName), deltaScan);

        LogicalPlan newChild = (LogicalPlan) deltaScan.withPreSnapshot(Optional.empty());
        LogicalProject<?> remapped = helper.remapProjectChildToNewPlan(oldProject, newChild);

        Assertions.assertInstanceOf(LogicalOlapTableStreamScan.class, remapped.child());
        Assertions.assertEquals(oldProject.getOutput().get(0).getExprId(), remapped.getOutput().get(0).getExprId());
        Assertions.assertEquals(oldProject.getOutput().get(0).getName(), remapped.getOutput().get(0).getName());
        Assertions.assertEquals(oldProject.getOutput().get(1).getExprId(), remapped.getOutput().get(1).getExprId());
        Assertions.assertEquals(oldProject.getOutput().get(1).getName(), remapped.getOutput().get(1).getName());
    }

    @Test
    void testRemapProjectChildToPostSnapshotPreservesExprId() {
        LogicalOlapTableStreamScan deltaScan = buildDeltaScanForTable(5, "t_proj_post");
        NamedExpression aliasId = new Alias(deltaScan.getOutput().get(0), "alias_id");
        NamedExpression passthroughName = (NamedExpression) deltaScan.getOutput().get(1);
        LogicalProject<LogicalOlapTableStreamScan> oldProject = new LogicalProject<>(
                ImmutableList.of(aliasId, passthroughName), deltaScan);

        LogicalPlan newChild = (LogicalPlan) deltaScan.withPostSnapshot();
        LogicalProject<?> remapped = helper.remapProjectChildToNewPlan(oldProject, newChild);

        Assertions.assertInstanceOf(LogicalOlapScan.class, remapped.child());
        Assertions.assertFalse(remapped.child() instanceof LogicalOlapTableStreamScan);
        Assertions.assertEquals(oldProject.getOutput().get(0).getExprId(), remapped.getOutput().get(0).getExprId());
        Assertions.assertEquals(oldProject.getOutput().get(1).getExprId(), remapped.getOutput().get(1).getExprId());
    }

    @Test
    void testRemapProjectChildToPostSnapshotFillsHiddenSlotWithNull() {
        LogicalOlapTableStreamScan deltaScan = buildDeltaScanForTable(6, "t_proj_missing");
        NamedExpression missingStreamColumn = new SlotReference("__DORIS_FAKE_HIDDEN__", IntegerType.INSTANCE, true);
        LogicalProject<LogicalOlapTableStreamScan> oldProject = new LogicalProject<>(
                ImmutableList.of(missingStreamColumn), deltaScan);

        LogicalPlan newChild = (LogicalPlan) deltaScan.withPostSnapshot();
        LogicalProject<?> remapped = helper.remapProjectChildToNewPlan(oldProject, newChild);
        Assertions.assertInstanceOf(Alias.class, remapped.getProjects().get(0));
        Assertions.assertInstanceOf(NullLiteral.class,
                ((Alias) remapped.getProjects().get(0)).child());
    }

    @Test
    void testRemapProjectChildToPostSnapshotKeepsAliasNullLiteral() {
        LogicalOlapTableStreamScan deltaScan = buildDeltaScanForTable(7, "t_proj_alias_missing");
        NamedExpression hiddenAlias = new Alias(new NullLiteral(IntegerType.INSTANCE), "__DORIS_FAKE_HIDDEN__");
        LogicalProject<LogicalOlapTableStreamScan> oldProject = new LogicalProject<>(
                ImmutableList.of(hiddenAlias), deltaScan);

        LogicalPlan newChild = (LogicalPlan) deltaScan.withPostSnapshot();
        LogicalProject<?> remapped = helper.remapProjectChildToNewPlan(oldProject, newChild);
        Assertions.assertInstanceOf(Alias.class, remapped.getProjects().get(0));
        Assertions.assertInstanceOf(NullLiteral.class, ((Alias) remapped.getProjects().get(0)).child());
    }

}
