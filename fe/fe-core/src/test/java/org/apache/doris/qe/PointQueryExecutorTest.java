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

package org.apache.doris.qe;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.OlapScanNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PointQueryExecutorTest {
    @Test
    public void testCandidateBackendsShuffleDependsOnQuerySelectionOrder() {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(false);
        Assertions.assertTrue(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(true);
        Assertions.assertFalse(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));
    }

    @Test
    public void testEmptyDecisionReturnsBeforeTabletPruning() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseSchemaKeyColumns()).thenReturn(java.util.Collections.emptyList());
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        ShortCircuitQueryContext queryContext = new ShortCircuitQueryContext(scanNode, new StatementContext());
        PointQueryExecutor executor = new PointQueryExecutor(queryContext,
                ShortCircuitQueryContext.PointQueryExecutionContext.empty(), 1024);

        Mockito.clearInvocations(scanNode);
        Assertions.assertNotNull(executor.getNext());
        // lazyEvaluateRangeLocations is the first operation that can resolve a tablet and lead to a BE RPC.
        Mockito.verifyNoInteractions(scanNode);
    }
}
