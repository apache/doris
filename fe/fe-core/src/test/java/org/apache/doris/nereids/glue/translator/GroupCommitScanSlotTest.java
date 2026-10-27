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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.planner.GroupCommitScanNode;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class GroupCommitScanSlotTest {
    @Test
    public void unusedMiddleSlotKeepsLaterColumnPosition() {
        TupleDescriptor tuple = tupleOfThreeColumns();
        List<SlotDescriptor> original = ImmutableList.copyOf(tuple.getSlots());
        prune(GroupCommitScanNode.class, tuple, Sets.newHashSet(original.get(0).getId(), original.get(2).getId()));
        Assertions.assertEquals(original, tuple.getSlots());
    }

    @Test
    public void constantProjectionKeepsFullQueueLayout() {
        TupleDescriptor tuple = tupleOfThreeColumns();
        List<SlotDescriptor> original = ImmutableList.copyOf(tuple.getSlots());
        prune(GroupCommitScanNode.class, tuple, Sets.newHashSet());
        Assertions.assertEquals(original, tuple.getSlots());
    }

    @Test
    public void ordinaryScanStillPrunesUnusedSlots() {
        TupleDescriptor tuple = tupleOfThreeColumns();
        List<SlotDescriptor> expected = ImmutableList.of(tuple.getSlots().get(0), tuple.getSlots().get(2));
        prune(ScanNode.class, tuple, Sets.newHashSet(expected.get(0).getId(), expected.get(1).getId()));
        Assertions.assertEquals(expected, tuple.getSlots());
    }

    private TupleDescriptor tupleOfThreeColumns() {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        for (String name : ImmutableList.of("c1", "c2", "c3")) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple);
            slot.setColumn(new Column(name, PrimitiveType.INT));
        }
        return tuple;
    }

    private void prune(Class<? extends ScanNode> nodeClass, TupleDescriptor tuple, Set<SlotId> required) {
        ScanNode scan = Mockito.mock(nodeClass, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(tuple).when(scan).getTupleDesc();
        Deencapsulation.invoke(new PhysicalPlanTranslator(), "updateScanSlotsMaterialization",
                scan, required, Sets.newHashSet(), new PlanTranslatorContext());
    }
}
