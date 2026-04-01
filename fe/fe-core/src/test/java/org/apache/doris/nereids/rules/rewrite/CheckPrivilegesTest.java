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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.BDPAuthContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class CheckPrivilegesTest {
    private CheckPrivileges checkPrivileges;
    private BDPAuthContext authContext;

    @BeforeEach
    void setUp() {
        checkPrivileges = new CheckPrivileges();
        authContext = new BDPAuthContext(
            "test_user", "test_source", "test_name", "test_token");
        authContext.setThreadLocalInfo();
    }

    @Test
    void testNormalColumns() {
        List<Slot> outputs = Arrays.asList(
                createSlotRef("col1", true, false),
                createSlotRef("col2", true, false)
        );
        Plan mockPlan = createMockPlan(outputs);
        RoaringBitmap requiredSlotIds = new RoaringBitmap();
        requiredSlotIds.add(0);
        requiredSlotIds.add(1);
        Set<String> result = checkPrivileges.computeUsedColumns(mockPlan, requiredSlotIds);

        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsAll(Arrays.asList("col1", "col2")));
    }

    @Test
    void testNoPermissionColumn() {
        List<Slot> outputs = Arrays.asList(
                createSlotRef("valid_col", true, false),
                createSlotRef("no_perm_col", false, false) // 无权限列
        );
        Plan mockPlan = createMockPlan(outputs);
        RoaringBitmap requiredSlotIds = new RoaringBitmap();
        requiredSlotIds.add(0);
        requiredSlotIds.add(1);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> checkPrivileges.computeUsedColumns(mockPlan, requiredSlotIds));
        Assertions.assertTrue(exception.getMessage().contains("no permission on column [no_perm_col]"));
    }

    @Test
    void testNonExistColumn() {
        SlotReference existCol = createSlotRef("exist_col", true, false);
        List<Slot> outputs = Arrays.asList(existCol);
        Plan mockPlan = createMockPlan(outputs);
        RoaringBitmap requiredSlotIds = new RoaringBitmap();
        requiredSlotIds.add(0);

        Set<String> result = checkPrivileges.computeUsedColumns(mockPlan, requiredSlotIds);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains("exist_col"));
    }

    private SlotReference createSlotRef(String name, boolean hasPerm, boolean isHidden) {
        SlotReference slot = Mockito.spy(new SlotReference(name, IntegerType.INSTANCE));
        Column mock = Mockito.mock(Column.class);
        Mockito.when(slot.hasPermission()).thenReturn(hasPerm);
        Mockito.when(mock.hasPermission()).thenReturn(hasPerm);
        Mockito.when(mock.isVisible()).thenReturn(true);
        if (isHidden) {
            Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(mock));
        } else {
            Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(mock));
        }
        return slot;
    }

    private Plan createMockPlan(List<Slot> outputs) {
        Plan plan = Mockito.mock(Plan.class);
        Mockito.when(plan.getOutput()).thenReturn(outputs);
        return plan;
    }
}
