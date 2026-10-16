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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * The consumers of a CTE map to the outputs of its producer by exprId, so RewriteCteChildren has to know which
 * output exprId became which one when a rule of the producer body renames an output.
 */
public class RewriteCteChildrenProducerRemapTest {
    private static final ExprId K_0 = new ExprId(0);
    private static final ExprId V_1 = new ExprId(1);
    private static final ExprId CNT_3 = new ExprId(3);

    @Test
    void testUnchangedRenamedAndPrunedOutputs() {
        Slot unchanged = slot(CNT_3, "cnt", null, null);
        Map<ExprId, ExprId> remap = RewriteCteChildren.computeProducerExprIdRemap(
                ImmutableList.of(slot(K_0, "k", null, null), slot(V_1, "v", null, null), unchanged),
                ImmutableList.of(slot(new ExprId(10), "k", null, null), unchanged));
        // k is renamed, v is pruned, cnt keeps its exprId
        Assertions.assertEquals(ImmutableMap.of(K_0, new ExprId(10)), remap);
    }

    @Test
    void testSameNameOutputsAreMatchedByLineage() {
        OlapTable leftTable = PlanConstructor.newOlapTable(0, "t1", 0);
        OlapTable rightTable = PlanConstructor.newOlapTable(1, "t2", 0);
        Slot oldLeft = slot(K_0, "k", leftTable, new Column("k", PrimitiveType.INT));
        Slot oldRight = slot(V_1, "k", rightTable, new Column("k", PrimitiveType.INT));
        // both are renamed, and the order of the new outputs is not the order of the old ones
        Slot newRight = slot(new ExprId(11), "k", rightTable, new Column("k", PrimitiveType.INT));
        Slot newLeft = slot(new ExprId(10), "k", leftTable, new Column("k", PrimitiveType.INT));

        Map<ExprId, ExprId> remap = RewriteCteChildren.computeProducerExprIdRemap(
                ImmutableList.of(oldLeft, oldRight), ImmutableList.of(newRight, newLeft));
        Assertions.assertEquals(ImmutableMap.of(K_0, new ExprId(10), V_1, new ExprId(11)), remap);
    }

    @Test
    void testSameNameOutputsWithoutLineageAreNotGuessed() {
        Map<ExprId, ExprId> remap = RewriteCteChildren.computeProducerExprIdRemap(
                ImmutableList.of(slot(K_0, "k", null, null), slot(V_1, "k", null, null)),
                ImmutableList.of(slot(new ExprId(10), "k", null, null), slot(new ExprId(11), "k", null, null)));
        Assertions.assertTrue(remap.isEmpty(), remap.toString());
    }

    @Test
    void testRenamedOutputWhoseSameNamedTwinKeepsItsExprId() {
        Slot keptTwin = slot(V_1, "k", null, null);
        Map<ExprId, ExprId> remap = RewriteCteChildren.computeProducerExprIdRemap(
                ImmutableList.of(slot(K_0, "k", null, null), keptTwin),
                ImmutableList.of(keptTwin, slot(new ExprId(10), "k", null, null)));
        Assertions.assertEquals(ImmutableMap.of(K_0, new ExprId(10)), remap);
    }

    private static Slot slot(ExprId exprId, String name, OlapTable table, Column column) {
        return new SlotReference(exprId, name, IntegerType.INSTANCE, true, ImmutableList.of("test", "t"),
                table, column, table, column);
    }
}
