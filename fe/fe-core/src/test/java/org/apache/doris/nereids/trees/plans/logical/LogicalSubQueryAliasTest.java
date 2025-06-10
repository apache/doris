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

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for LogicalSubQueryAlias, particularly the computeOutput() method
 * and qualifier replacement logic.
 */
public class LogicalSubQueryAliasTest {

    /**
     * Test the fix for nested view qualifier replacement.
     * This tests the specific case where a slot has a short qualifier (from table alias)
     * and LogicalSubQueryAlias has a longer qualifier (view name).
     */
    @Test
    public void testQualifierReplacementForNestedViews() {
        // Create a slot that simulates coming from a table alias 'b' in an inner view
        SlotReference slotFromTableAlias = new SlotReference(
                new ExprId(1),
                "column_name",
                StringType.INSTANCE,
                true,
                ImmutableList.of("b")  // Short qualifier from table alias
        );

        // Create a mock child plan that returns this slot
        LogicalEmptyRelation mockChild = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.plans.RelationId.createGenerator().getNextId(),
                ImmutableList.of(slotFromTableAlias)
        );

        // Create LogicalSubQueryAlias with a longer qualifier (simulating a view name)
        List<String> viewQualifier = ImmutableList
                .of("hive", "app", "v_view_table_name");
        LogicalSubQueryAlias<LogicalPlan> subQueryAlias = new LogicalSubQueryAlias<>(viewQualifier, mockChild);

        // Get the computed output
        List<Slot> output = subQueryAlias.computeOutput();

        // Verify the results
        Assertions.assertEquals(1, output.size(), "Should have one output slot");

        Slot resultSlot = output.get(0);
        Assertions.assertEquals("column_name", resultSlot.getName(), "Slot name should be preserved");

        List<String> resultQualifier = resultSlot.getQualifier();
        Assertions.assertEquals(3, resultQualifier.size(), "Qualifier should have 3 parts");
        Assertions.assertEquals("hive", resultQualifier.get(0), "First qualifier part should be 'hive'");
        Assertions.assertEquals("app", resultQualifier.get(1), "Second qualifier part should be 'app'");
        Assertions.assertEquals("v_view_table_name",
                resultQualifier.get(2), "Third qualifier part should be the view name");
    }

    /**
     * Test qualifier replacement when original qualifier is longer than new qualifier.
     * This should replace the last parts of the original qualifier.
     */
    @Test
    public void testQualifierReplacementLongerOriginal() {
        // Create a slot with a longer qualifier
        SlotReference slotWithLongQualifier = new SlotReference(
                new ExprId(1),
                "column_name",
                StringType.INSTANCE,
                true,
                ImmutableList.of("catalog", "database", "table", "extra")  // 4 parts
        );

        LogicalEmptyRelation mockChild = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.plans.RelationId.createGenerator().getNextId(),
                ImmutableList.of(slotWithLongQualifier)
        );

        // Create LogicalSubQueryAlias with shorter qualifier
        List<String> aliasQualifier = ImmutableList.of("new_db", "new_table");
        LogicalSubQueryAlias<LogicalPlan> subQueryAlias = new LogicalSubQueryAlias<>(aliasQualifier, mockChild);

        // Get the computed output
        List<Slot> output = subQueryAlias.computeOutput();

        // Verify the results
        Assertions.assertEquals(1, output.size(), "Should have one output slot");

        Slot resultSlot = output.get(0);
        List<String> resultQualifier = resultSlot.getQualifier();
        Assertions.assertEquals(4, resultQualifier.size(), "Qualifier should still have 4 parts");
        Assertions.assertEquals("catalog", resultQualifier.get(0), "First part should be unchanged");
        Assertions.assertEquals("database", resultQualifier.get(1), "Second part should be unchanged");
        Assertions.assertEquals("new_db", resultQualifier.get(2), "Third part should be replaced");
        Assertions.assertEquals("new_table", resultQualifier.get(3), "Fourth part should be replaced");
    }

    /**
     * Test qualifier replacement when original qualifier is empty.
     * This should add the new qualifier directly.
     */
    @Test
    public void testQualifierReplacementEmptyOriginal() {
        // Create a slot with empty qualifier
        SlotReference slotWithEmptyQualifier = new SlotReference(
                new ExprId(1),
                "column_name",
                StringType.INSTANCE,
                true,
                ImmutableList.of()  // Empty qualifier
        );

        LogicalEmptyRelation mockChild = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.plans.RelationId.createGenerator().getNextId(),
                ImmutableList.of(slotWithEmptyQualifier)
        );

        // Create LogicalSubQueryAlias with qualifier
        List<String> aliasQualifier = ImmutableList.of("db", "table");
        LogicalSubQueryAlias<LogicalPlan> subQueryAlias = new LogicalSubQueryAlias<>(aliasQualifier, mockChild);

        // Get the computed output
        List<Slot> output = subQueryAlias.computeOutput();

        // Verify the results
        Assertions.assertEquals(1, output.size(), "Should have one output slot");

        Slot resultSlot = output.get(0);
        List<String> resultQualifier = resultSlot.getQualifier();
        Assertions.assertEquals(2, resultQualifier.size(), "Qualifier should have 2 parts");
        Assertions.assertEquals("db", resultQualifier.get(0), "First part should be 'db'");
        Assertions.assertEquals("table", resultQualifier.get(1), "Second part should be 'table'");
    }
}
