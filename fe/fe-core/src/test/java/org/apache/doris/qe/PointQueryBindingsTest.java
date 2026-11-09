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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class PointQueryBindingsTest {
    @Test
    void keyOrderAndChangingParametersDoNotCacheValues() throws Exception {
        Column key = new Column("k", PrimitiveType.BIGINT);
        Column second = new Column("k2", PrimitiveType.BIGINT);
        BinaryPredicate first = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "k"), new IntLiteral(1));
        BinaryPredicate reversed = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new IntLiteral(10), new SlotRef(null, "k2"));
        List<Expr> expressions = Arrays.asList(reversed, first);
        PointQueryBindings bindings = PointQueryBindings.create(Arrays.asList(key, second), expressions);
        Assertions.assertNotNull(bindings);
        PlaceholderId p0 = new PlaceholderId(0);
        PlaceholderId p1 = new PlaceholderId(1);
        StatementContext context = Mockito.mock(StatementContext.class);
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        SlotReference firstSlot = Mockito.mock(SlotReference.class);
        SlotReference secondSlot = Mockito.mock(SlotReference.class);
        Mockito.when(firstSlot.getOriginalColumn()).thenReturn(Optional.of(key));
        Mockito.when(secondSlot.getOriginalColumn()).thenReturn(Optional.of(second));
        Map<PlaceholderId, SlotReference> slots = new HashMap<>();
        slots.put(p0, secondSlot);
        slots.put(p1, firstSlot);
        Map<PlaceholderId, Expression> values = new HashMap<>();
        Mockito.when(context.getIdToComparisonSlot()).thenReturn(slots);
        Mockito.when(context.getIdToPlaceholderRealExpr()).thenReturn(values);
        Mockito.when(command.getPlaceholders()).thenReturn(Arrays.asList(new Placeholder(p0, 8), new Placeholder(p1, 8)));
        for (long k : new long[] {2, 999, 1}) {
            values.put(p0, new BigIntLiteral(k * 10));
            values.put(p1, new BigIntLiteral(k));
            Assertions.assertTrue(bindings.bindParameters(context, command));
            Assertions.assertEquals(Arrays.asList(Long.toString(k), Long.toString(k * 10)),
                    Arrays.asList(bindings.getKeyLiteral(0).getStringValue(),
                            bindings.getKeyLiteral(1).getStringValue()));
            Assertions.assertTrue(bindings.matches(expressions));
        }
        // A wire-type transition and NULL take the existing fallback without partially changing the AST.
        Mockito.when(command.getPlaceholders()).thenReturn(Arrays.asList(new Placeholder(p0, 3), new Placeholder(p1, 3)));
        Assertions.assertFalse(bindings.bindParameters(context, command));
        Assertions.assertTrue(bindings.bindParameters(context, command));
        values.put(p1, new NullLiteral());
        Assertions.assertFalse(bindings.bindParameters(context, command));
        Assertions.assertEquals("1", first.getChild(1).getStringValue());
        values.put(p1, new BigIntLiteral(42));
        Assertions.assertTrue(bindings.bindParameters(context, command));
        Assertions.assertEquals("42", first.getChild(1).getStringValue());
        first.setChild(0, new SlotRef(null, "replacement"));
        Assertions.assertFalse(bindings.matches(expressions));
    }

    @Test
    void preparedStringParameterIsRetypedBeforeUuidRouting() throws Exception {
        Column key = new Column("k", Type.UUID);
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "k"), new IntLiteral(0));
        PointQueryBindings bindings = PointQueryBindings.create(Collections.singletonList(key),
                Collections.singletonList(predicate));
        PlaceholderId id = new PlaceholderId(0);
        StatementContext context = Mockito.mock(StatementContext.class);
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        SlotReference slot = Mockito.mock(SlotReference.class);
        Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(key));
        Mockito.when(context.getIdToComparisonSlot()).thenReturn(Collections.singletonMap(id, slot));
        String uuid = "00112233-4455-6677-8899-aabbccddeeff";
        Mockito.when(context.getIdToPlaceholderRealExpr()).thenReturn(
                Collections.singletonMap(id, new StringLiteral(uuid)));
        Mockito.when(command.getPlaceholders()).thenReturn(Collections.singletonList(new Placeholder(id, 253)));

        Assertions.assertTrue(bindings.bindParameters(context, command));
        Assertions.assertEquals(Type.UUID, predicate.getChild(1).getType());
        Assertions.assertEquals(uuid, bindings.getKeyLiteral(0).getStringValue());
    }

    @Test
    void unsupportedAndAmbiguousLayoutsUseExistingPath() throws Exception {
        Column key = new Column("k", PrimitiveType.BIGINT);
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "k"), new IntLiteral(1));
        Assertions.assertNull(PointQueryBindings.create(Collections.emptyList(), Collections.singletonList(eq)));
        Assertions.assertNull(PointQueryBindings.create(Collections.singletonList(key), Arrays.asList(eq, eq)));
        Assertions.assertNull(PointQueryBindings.create(Collections.singletonList(key), Collections.emptyList()));
        BinaryPredicate notEqual = new BinaryPredicate(BinaryPredicate.Operator.NE,
                new SlotRef(null, "k"), new IntLiteral(1));
        Assertions.assertNull(PointQueryBindings.create(Collections.singletonList(key),
                Collections.singletonList(notEqual)));
    }
}
