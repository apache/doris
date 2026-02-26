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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Tests for {@link Alias#toSlot()} to ensure column metadata (originalTable,
 * originalColumn, oneLevelTable, oneLevelColumn, subPath) is preserved through
 * expression wrappers like Cast and ElementAt (variant subcolumn access).
 *
 * This is critical for MATCH expressions: when a MATCH references an alias
 * output slot, ExpressionTranslator.visitMatch() needs originalColumn to
 * look up the inverted index. Without proper metadata propagation, queries
 * with MATCH inside OR predicates crash.
 */
public class AliasToSlotTest {

    private static final OlapTable MOCK_ORIGINAL_TABLE = Mockito.mock(OlapTable.class);
    private static final Column MOCK_ORIGINAL_COLUMN = Mockito.mock(Column.class);
    private static final OlapTable MOCK_ONE_LEVEL_TABLE = Mockito.mock(OlapTable.class);
    private static final Column MOCK_ONE_LEVEL_COLUMN = Mockito.mock(Column.class);

    /**
     * Alias(SlotReference) — child is a direct SlotReference.
     * originalColumn, originalTable, subPath should be preserved directly.
     */
    @Test
    public void testToSlotWithDirectSlotReference() {
        SlotReference slot = createSlotWithColumnInfo("col1", VarcharType.SYSTEM_DEFAULT,
                ImmutableList.of());

        Alias alias = new Alias(new ExprId(100), slot, "alias_col");
        SlotReference result = (SlotReference) alias.toSlot();

        Assertions.assertEquals(MOCK_ORIGINAL_TABLE, result.getOriginalTable().orElse(null));
        Assertions.assertEquals(MOCK_ORIGINAL_COLUMN, result.getOriginalColumn().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_TABLE, result.getOneLevelTable().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_COLUMN, result.getOneLevelColumn().orElse(null));
        Assertions.assertTrue(result.getSubPath().isEmpty());
    }

    /**
     * Alias(Cast(SlotReference)) — alias wraps a type-conversion of a column.
     * This is the pattern for: CAST(name AS VARCHAR(200)) AS name_casted
     * originalColumn and originalTable should be preserved through Cast.
     */
    @Test
    public void testToSlotWithCastSlotReference() {
        SlotReference slot = createSlotWithColumnInfo("name", VarcharType.createVarcharType(100),
                ImmutableList.of());
        Cast cast = new Cast(slot, VarcharType.createVarcharType(200));

        Alias alias = new Alias(new ExprId(101), cast, "name_casted");
        SlotReference result = (SlotReference) alias.toSlot();

        Assertions.assertEquals(MOCK_ORIGINAL_TABLE, result.getOriginalTable().orElse(null));
        Assertions.assertEquals(MOCK_ORIGINAL_COLUMN, result.getOriginalColumn().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_TABLE, result.getOneLevelTable().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_COLUMN, result.getOneLevelColumn().orElse(null));
    }

    /**
     * Alias(Cast(ElementAt(SlotReference, Literal))) — variant subcolumn access.
     * This is the pattern for: CAST(OVERFLOWPROPERTIES['firstname'] AS VARCHAR) AS firstname
     * The expression tree is Cast → ElementAt(SlotRef, VarcharLiteral).
     * originalColumn, originalTable, and subPath should all be preserved.
     */
    @Test
    public void testToSlotWithCastElementAtVariantSubcolumn() {
        List<String> subPath = ImmutableList.of("firstname");
        SlotReference variantSlot = createSlotWithColumnInfo("OVERFLOWPROPERTIES",
                VariantType.INSTANCE, subPath);

        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("firstname"));
        Cast cast = new Cast(elementAt, VarcharType.SYSTEM_DEFAULT);

        Alias alias = new Alias(new ExprId(102), cast, "firstname");
        SlotReference result = (SlotReference) alias.toSlot();

        Assertions.assertEquals(MOCK_ORIGINAL_TABLE, result.getOriginalTable().orElse(null));
        Assertions.assertEquals(MOCK_ORIGINAL_COLUMN, result.getOriginalColumn().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_TABLE, result.getOneLevelTable().orElse(null));
        Assertions.assertEquals(MOCK_ONE_LEVEL_COLUMN, result.getOneLevelColumn().orElse(null));
        Assertions.assertEquals(subPath, result.getSubPath(),
                "subPath should be preserved for variant subcolumn access");
    }

    /**
     * Alias with multi-slot expression — e.g. Add(SlotRef1, SlotRef2).
     * When there are multiple input slots, column origin is ambiguous,
     * so originalColumn/originalTable should remain null.
     */
    @Test
    public void testToSlotWithMultiSlotExpression() {
        SlotReference slot1 = createSlotWithColumnInfo("col1", IntegerType.INSTANCE,
                ImmutableList.of());
        SlotReference slot2 = new SlotReference(new ExprId(11), "col2",
                IntegerType.INSTANCE, false, ImmutableList.of());

        Expression addExpr = new org.apache.doris.nereids.trees.expressions.Add(slot1, slot2);

        Alias alias = new Alias(new ExprId(103), addExpr, "sum_col");
        SlotReference result = (SlotReference) alias.toSlot();

        Assertions.assertFalse(result.getOriginalColumn().isPresent(),
                "originalColumn should be null for multi-slot expressions (ambiguous origin)");
        Assertions.assertFalse(result.getOriginalTable().isPresent(),
                "originalTable should be null for multi-slot expressions");
        Assertions.assertFalse(result.getOneLevelTable().isPresent(),
                "oneLevelTable should be null for multi-slot expressions");
        Assertions.assertFalse(result.getOneLevelColumn().isPresent(),
                "oneLevelColumn should be null for multi-slot expressions");
    }

    /**
     * Alias with a literal expression (no slots at all).
     * originalColumn should be null.
     */
    @Test
    public void testToSlotWithLiteralExpression() {
        Alias alias = new Alias(new ExprId(104), new VarcharLiteral("constant"), "const_col");
        SlotReference result = (SlotReference) alias.toSlot();

        Assertions.assertFalse(result.getOriginalColumn().isPresent(),
                "originalColumn should be null for literal expressions");
        Assertions.assertFalse(result.getOriginalTable().isPresent(),
                "originalTable should be null for literal expressions");
        Assertions.assertFalse(result.getOneLevelTable().isPresent(),
                "oneLevelTable should be null for literal expressions");
        Assertions.assertFalse(result.getOneLevelColumn().isPresent(),
                "oneLevelColumn should be null for literal expressions");
    }

    private static SlotReference createSlotWithColumnInfo(String name, org.apache.doris.nereids.types.DataType type,
            List<String> subPath) {
        return new SlotReference(new ExprId(10), name, type, true, ImmutableList.of(),
                MOCK_ORIGINAL_TABLE, MOCK_ORIGINAL_COLUMN,
                MOCK_ONE_LEVEL_TABLE, MOCK_ONE_LEVEL_COLUMN, subPath);
    }
}
