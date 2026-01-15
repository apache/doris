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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for LogicalOlapScan
 */
public class LogicalOlapScanTest {

    private ConnectContext connectContext;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setSessionVariable(new SessionVariable());
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);
    }

    @AfterEach
    public void tearDown() {
        if (mockedConnectContext != null) {
            mockedConnectContext.close();
        }
    }

    private SlotReference createMockSlot(String name, boolean isVisible) {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Mockito.when(slot.isVisible()).thenReturn(isVisible);
        Mockito.when(slot.getName()).thenReturn(name);
        return slot;
    }

    /**
     * Test constructReplaceMap returns empty map when origin outputs size != target outputs size
     */
    @Test
    public void testConstructReplaceMapSizeMismatch() throws Exception {
        // Create mock MTMV
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVCache cache = Mockito.mock(MTMVCache.class);
        Plan originalPlan = Mockito.mock(Plan.class);

        // Set up origin outputs with 3 visible slots
        SlotReference originSlot1 = createMockSlot("col1", true);
        SlotReference originSlot2 = createMockSlot("col2", true);
        SlotReference originSlot3 = createMockSlot("col3", true);
        List<Slot> originOutputs = ImmutableList.of(originSlot1, originSlot2, originSlot3);

        Mockito.when(mtmv.getOrGenerateCache(Mockito.any())).thenReturn(cache);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(cache.getOriginalFinalPlan()).thenReturn(originalPlan);
        Mockito.when(originalPlan.getOutput()).thenReturn(originOutputs);

        // Create LogicalOlapScan with only 2 visible output slots (size mismatch)
        SlotReference targetSlot1 = createMockSlot("col1", true);
        SlotReference targetSlot2 = createMockSlot("col2", true);
        List<Slot> targetOutputs = ImmutableList.of(targetSlot1, targetSlot2);

        // Mock OlapTable for LogicalOlapScan constructor
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getId()).thenReturn(1L);
        Mockito.when(olapTable.getName()).thenReturn("test_table");
        Mockito.when(olapTable.getFullQualifiers()).thenReturn(ImmutableList.of("db", "test_table"));

        // Create LogicalOlapScan with mocked cachedOutput
        LogicalOlapScan scan = Mockito.spy(new LogicalOlapScan(
                new RelationId(1),
                olapTable,
                ImmutableList.of("db"),
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty(),
                Collections.emptyList()
        ));

        // Mock getOutput() to return target outputs (size = 2)
        Mockito.doReturn(targetOutputs).when(scan).getOutput();

        // Call constructReplaceMap - should return empty map due to size mismatch
        Map<Slot, Slot> replaceMap = scan.constructReplaceMap(mtmv);

        // Verify that the map is empty because originOutputs.size() (3) != targetOutputs.size() (2)
        Assertions.assertTrue(replaceMap.isEmpty(),
                "replaceMap should be empty when origin and target output sizes don't match");
    }
}

