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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link Repeat} interface default methods:
 * toShapes, indexesOfOutput, getGroupingSetsIndexesInOutput, computeRepeatSlotIdList.
 */
public class RepeatTest {

    private LogicalOlapScan scan;
    private Slot id;
    private Slot gender;
    private Slot name;
    private Slot age;

    @BeforeEach
    public void setUp() {
        scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        id = scan.getOutput().get(0);
        gender = scan.getOutput().get(1);
        name = scan.getOutput().get(2);
        age = scan.getOutput().get(3);
    }

    @Test
    public void testToShapes() {
        // grouping sets: (id, name), (id), ()
        // flatten = [id, name], shapes: [false,false], [false,true], [true,true]
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(id, name),
                ImmutableList.of(id),
                ImmutableList.of()
        );
        Alias alias = new Alias(new Sum(name), "sum(name)");
        Repeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                ImmutableList.of(id, name, alias),
                RepeatType.GROUPING_SETS,
                scan
        );

        Repeat.GroupingSetShapes shapes = repeat.toShapes();

        Assertions.assertEquals(2, shapes.flattenGroupingSetExpression.size());
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(id));
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(name));
        Assertions.assertEquals(3, shapes.shapes.size());

        // (id, name) -> [false, false]
        Assertions.assertFalse(shapes.shapes.get(0).shouldBeErasedToNull(0));
        Assertions.assertFalse(shapes.shapes.get(0).shouldBeErasedToNull(1));
        Assertions.assertEquals(0L, shapes.shapes.get(0).computeLongValue());

        // (id) -> [false, true] (id in set, name not)
        Assertions.assertFalse(shapes.shapes.get(1).shouldBeErasedToNull(0));
        Assertions.assertTrue(shapes.shapes.get(1).shouldBeErasedToNull(1));
        Assertions.assertEquals(1L, shapes.shapes.get(1).computeLongValue());

        // () -> [true, true]
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(0));
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(1));
        Assertions.assertEquals(3L, shapes.shapes.get(2).computeLongValue());
    }

    @Test
    public void testToShapesWithGroupingFunction() {
        // grouping(id) adds id to flatten if not present; single set (name) has flatten [name, id]
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(name), ImmutableList.of(name, id), ImmutableList.of());
        Alias groupingAlias = new Alias(new GroupingId(gender, age), "grouping_id(id)");
        Repeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                ImmutableList.of(name, groupingAlias),
                RepeatType.GROUPING_SETS,
                scan
        );

        Repeat.GroupingSetShapes shapes = repeat.toShapes();

        // flatten = [name] from getGroupBy + [id] from grouping function arg
        Assertions.assertEquals(4, shapes.flattenGroupingSetExpression.size());
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(name));
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(id));
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(gender));
        Assertions.assertTrue(shapes.flattenGroupingSetExpression.contains(age));

        Assertions.assertEquals(3, shapes.shapes.size());
        // (name) -> name not erased, id,gender,age erased
        Assertions.assertFalse(shapes.shapes.get(0).shouldBeErasedToNull(0));
        Assertions.assertTrue(shapes.shapes.get(0).shouldBeErasedToNull(1));
        Assertions.assertTrue(shapes.shapes.get(0).shouldBeErasedToNull(2));
        Assertions.assertTrue(shapes.shapes.get(0).shouldBeErasedToNull(3));
        // (name, id) -> name,id not erased, gender and age erased
        Assertions.assertFalse(shapes.shapes.get(1).shouldBeErasedToNull(0));
        Assertions.assertFalse(shapes.shapes.get(1).shouldBeErasedToNull(1));
        Assertions.assertTrue(shapes.shapes.get(1).shouldBeErasedToNull(2));
        Assertions.assertTrue(shapes.shapes.get(1).shouldBeErasedToNull(3));
        //() -> all erased
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(0));
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(1));
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(2));
        Assertions.assertTrue(shapes.shapes.get(2).shouldBeErasedToNull(3));
    }

    @Test
    public void testIndexesOfOutput() {
        List<Slot> outputSlots = ImmutableList.of(id, gender, name, age);
        Map<Expression, Integer> indexes = Repeat.indexesOfOutput(outputSlots);
        Assertions.assertEquals(4, indexes.size());
        Assertions.assertEquals(0, indexes.get(id));
        Assertions.assertEquals(1, indexes.get(gender));
        Assertions.assertEquals(2, indexes.get(name));
        Assertions.assertEquals(3, indexes.get(age));
    }

    @Test
    public void testGetGroupingSetsIndexesInOutput() {
        // groupingSets=((name, id), (id), (gender)), output=[id, name, gender]
        // expect:((1,0),(0),(2))
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(name, id),
                ImmutableList.of(id),
                ImmutableList.of(gender)
        );
        Alias groupingId = new Alias(new GroupingId(id, name));
        Repeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                ImmutableList.of(id, name, gender, groupingId),
                RepeatType.GROUPING_SETS,
                scan
        );
        List<Slot> outputSlots = ImmutableList.of(id, name, gender, groupingId.toSlot());

        List<Set<Integer>> result = repeat.getGroupingSetsIndexesInOutput(outputSlots);

        Assertions.assertEquals(3, result.size());
        // (name, id) -> indexes {1, 0}
        Assertions.assertEquals(Sets.newLinkedHashSet(ImmutableList.of(1, 0)), result.get(0));
        // (id) -> index {0}
        Assertions.assertEquals(Sets.newLinkedHashSet(ImmutableList.of(0)), result.get(1));
        // (gender) -> index {2}
        Assertions.assertEquals(Sets.newLinkedHashSet(ImmutableList.of(2)), result.get(2));
    }

    @Test
    public void testComputeRepeatSlotIdList() {
        // groupingSets=((name, id), (id)), output=[id, name], slotIdList=[3, 4] (id->3, name->4)
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(name, id),
                ImmutableList.of(id)
        );
        Repeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                ImmutableList.of(id, name),
                RepeatType.GROUPING_SETS,
                scan
        );
        List<Slot> outputSlots = ImmutableList.of(id, name);
        List<Integer> slotIdList = ImmutableList.of(3, 4);

        List<Set<Integer>> result = repeat.computeRepeatSlotIdList(slotIdList, outputSlots);

        Assertions.assertEquals(2, result.size());
        // (name, id) -> indexes {1,0} -> slot ids {4, 3}
        Assertions.assertEquals(Sets.newLinkedHashSet(ImmutableList.of(4, 3)), result.get(0));
        // (id) -> index {0} -> slot id {3}
        Assertions.assertEquals(Sets.newLinkedHashSet(ImmutableList.of(3)), result.get(1));
    }
}
