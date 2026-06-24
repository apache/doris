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

package org.apache.doris.datasource;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileQueryScanNodeTest {
    private static final long MB = 1024L * 1024L;
    private static final Method UPDATE_REQUIRED_SLOTS_METHOD;

    static {
        try {
            UPDATE_REQUIRED_SLOTS_METHOD = FileQueryScanNode.class.getDeclaredMethod("updateRequiredSlots");
            UPDATE_REQUIRED_SLOTS_METHOD.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private TableIf table;

    private static class TestFileQueryScanNode extends FileQueryScanNode {
        private TableIf targetTable;

        TestFileQueryScanNode(SessionVariable sv) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "test", ScanContext.EMPTY, false, sv);
        }

        TupleDescriptor getTupleDescriptor() {
            return desc;
        }

        void setTargetTable(TableIf targetTable) {
            this.targetTable = targetTable;
        }

        @Override
        protected TFileFormatType getFileFormatType() throws UserException {
            return TFileFormatType.FORMAT_ORC;
        }

        @Override
        protected List<String> getPathPartitionKeys() throws UserException {
            return Collections.emptyList();
        }

        @Override
        protected TableIf getTargetTable() throws UserException {
            return targetTable;
        }

        @Override
        protected Map<String, String> getLocationProperties() throws UserException {
            return Collections.emptyMap();
        }
    }

    @Before
    public void setUp() {
        table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("test_table");
    }

    @Test
    public void testApplyMaxFileSplitNumLimitRaisesTargetSize() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitKeepsTargetSizeWhenSmall() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 500L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(0);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testUpdateRequiredSlotsPreservesInlineDefaultValueExpr() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        node.setTargetTable(table);

        TupleDescriptor desc = node.getTupleDescriptor();
        desc.setTable(table);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), desc.getId());
        slot.setColumn(new Column("c1", Type.INT));
        desc.addSlot(slot);
        Mockito.when(table.getFullSchema()).thenReturn(Arrays.asList(slot.getColumn()));

        TExpr defaultExpr = new TExpr();
        defaultExpr.setNodes(Collections.emptyList());
        TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
        slotInfo.setSlotId(slot.getId().asInt());
        slotInfo.setCategory(TColumnCategory.REGULAR);
        slotInfo.setIsFileSlot(true);
        slotInfo.setDefaultValueExpr(defaultExpr);

        TFileScanRangeParams params = new TFileScanRangeParams();
        params.setRequiredSlots(Arrays.asList(slotInfo));
        node.params = params;

        UPDATE_REQUIRED_SLOTS_METHOD.invoke(node);

        TFileScanSlotInfo updatedSlotInfo = node.params.getRequiredSlots().get(0);
        Assert.assertSame(slotInfo, updatedSlotInfo);
        Assert.assertTrue(updatedSlotInfo.isSetDefaultValueExpr());
        Assert.assertSame(defaultExpr, updatedSlotInfo.getDefaultValueExpr());
    }
}
