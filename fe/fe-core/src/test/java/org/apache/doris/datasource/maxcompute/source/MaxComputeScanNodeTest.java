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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaxComputeScanNodeTest {
    @Mock
    private MaxComputeExternalTable table;
    @Mock
    private com.aliyun.odps.Table odpsTable;
    @Mock
    private TupleDescriptor desc;

    private MaxComputeScanNode node;

    @Before
    public void setUp() {
        Mockito.when(table.getOdpsTable()).thenReturn(odpsTable);
        Mockito.when(desc.getTable()).thenReturn(table);
        Mockito.when(desc.getId()).thenReturn(new TupleId(0));
        node = new MaxComputeScanNode(new PlanNodeId(0), desc,
                SelectedPartitions.NOT_PRUNED, false, new SessionVariable(), ScanContext.EMPTY);
    }

    @Test
    public void testGetSplitsRejectsOdpsExternalTable() {
        assertGetSplitsRejectsUnsupportedOdpsTable(true, false, "mc_external_table");
    }

    @Test
    public void testGetSplitsRejectsOdpsLogicalView() {
        assertGetSplitsRejectsUnsupportedOdpsTable(false, true, "mc_logical_view");
    }

    private void assertGetSplitsRejectsUnsupportedOdpsTable(boolean isExternalTable, boolean isVirtualView,
            String tableName) {
        Mockito.when(odpsTable.isExternalTable()).thenReturn(isExternalTable);
        Mockito.when(odpsTable.isVirtualView()).thenReturn(isVirtualView);
        Mockito.when(table.getDbName()).thenReturn("default");
        Mockito.when(table.getName()).thenReturn(tableName);

        UserException exception = Assert.assertThrows(UserException.class, () -> node.getSplits(1));
        Assert.assertTrue(exception.getMessage().contains(
                "Reading MaxCompute external table or logical view is not supported: default." + tableName));
        Mockito.verify(odpsTable, Mockito.never()).getFileNum();
    }
}
