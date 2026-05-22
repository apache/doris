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

package org.apache.doris.planner;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TPlanNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlanNodeTest {
    @Test
    public void testGetPointQueryProjectListFlattensIntermediateProjection() {
        TestPlanNode planNode = new TestPlanNode();

        TupleDescriptor scanTuple = new TupleDescriptor(new TupleId(0));
        SlotDescriptor scanSlotA = createColumnSlot(scanTuple, 0, "a");
        SlotDescriptor scanSlotB = createColumnSlot(scanTuple, 1, "b");

        TupleDescriptor intermediateTuple = new TupleDescriptor(new TupleId(1));
        SlotDescriptor projectedSlotA = createColumnSlot(intermediateTuple, 2, "a");
        SlotDescriptor projectedExprSlot = createExprSlot(intermediateTuple, 3, "expr_col");

        planNode.addIntermediateOutputTupleDescList(intermediateTuple);
        planNode.addIntermediateProjectList(Lists.newArrayList(
                new SlotRef(scanSlotA),
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new SlotRef(scanSlotB),
                        new IntLiteral(1), Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT, true)));
        planNode.setProjectList(Lists.newArrayList(
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new SlotRef(projectedSlotA),
                        new SlotRef(projectedExprSlot), Type.BIGINT,
                        NullableMode.DEPEND_ON_ARGUMENT, true)));

        List<Expr> pointQueryProjectList = planNode.getPointQueryProjectList();
        Assertions.assertEquals(1, pointQueryProjectList.size());

        Set<SlotId> slotIds = new HashSet<>();
        Expr.extractSlots(pointQueryProjectList.get(0), slotIds);
        Assertions.assertEquals(Sets.newHashSet(scanSlotA.getId(), scanSlotB.getId()), slotIds);
        Assertions.assertFalse(slotIds.contains(projectedSlotA.getId()));
        Assertions.assertFalse(slotIds.contains(projectedExprSlot.getId()));
    }

    private static SlotDescriptor createColumnSlot(TupleDescriptor tupleDescriptor, int slotId,
            String columnName) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(slotId), tupleDescriptor.getId());
        slotDescriptor.setColumn(new Column(columnName, Type.BIGINT));
        slotDescriptor.setLabel(columnName);
        tupleDescriptor.addSlot(slotDescriptor);
        return slotDescriptor;
    }

    private static SlotDescriptor createExprSlot(TupleDescriptor tupleDescriptor, int slotId,
            String label) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(slotId), tupleDescriptor.getId());
        slotDescriptor.setType(Type.BIGINT);
        slotDescriptor.setLabel(label);
        tupleDescriptor.addSlot(slotDescriptor);
        return slotDescriptor;
    }

    private static class TestPlanNode extends PlanNode {
        TestPlanNode() {
            super(new PlanNodeId(0), "TEST");
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }
    }
}
