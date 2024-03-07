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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class UpdateMvByPartitionCommandTest {
    @Test
    void testMaxMin() throws AnalysisException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {
        Method m = UpdateMvByPartitionCommand.class.getDeclaredMethod("convertPartitionItemToPredicate", PartitionItem.class,
                Slot.class);
        m.setAccessible(true);
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(PartitionValue.MAX_VALUE), ImmutableList.of(column));
        PartitionKey lower = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)), ImmutableList.of(column));
        Range<PartitionKey> range = Range.closedOpen(lower, upper);
        RangePartitionItem rangePartitionItem = new RangePartitionItem(range);
        Expression expr = (Expression) m.invoke(null, rangePartitionItem, new SlotReference("s", IntegerType.INSTANCE));
        Assertions.assertTrue(expr instanceof GreaterThanEqual);
    }

    @Test
    void testNull() throws AnalysisException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {
        Method m = UpdateMvByPartitionCommand.class.getDeclaredMethod("convertPartitionItemToPredicate", PartitionItem.class,
                Slot.class);
        m.setAccessible(true);
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = (Expression) m.invoke(null, listPartitionItem, new SlotReference("s", IntegerType.INSTANCE));
        Assertions.assertTrue(expr instanceof IsNull);

        PartitionKey v1 = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        PartitionKey v2 = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("1", false)), ImmutableList.of(column.getType()), false);
        listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        expr = (Expression) m.invoke(null, listPartitionItem, new SlotReference("s", IntegerType.INSTANCE));
        Assertions.assertEquals("(s IS NULL OR s IN (1))", expr.toSql());
    }
}
