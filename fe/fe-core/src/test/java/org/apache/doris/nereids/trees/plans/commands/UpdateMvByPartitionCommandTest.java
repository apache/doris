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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionExprDateTruncDateAddSub;
import org.apache.doris.mtmv.MTMVPartitionExprFactory;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

class UpdateMvByPartitionCommandTest {
    @Test
    void testFirstPartWithoutLowerBound() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range1 = Range.lessThan(upper);
        RangePartitionItem item1 = new RangePartitionItem(range1);

        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(item1), "s");
        Assertions.assertEquals("OR[(s < 1),s IS NULL]", predicates.iterator().next().toSql());

    }

    @Test
    void testMaxMin() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(PartitionValue.MAX_VALUE),
                ImmutableList.of(column));
        PartitionKey lower = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range = Range.closedOpen(lower, upper);
        RangePartitionItem rangePartitionItem = new RangePartitionItem(range);
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(rangePartitionItem),
                "s");
        Expression expr = predicates.iterator().next();
        System.out.println(expr.toSql());
        Assertions.assertEquals("(s >= 1)", expr.toSql());
    }

    @Test
    void testNull() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s")
                .iterator().next();
        Assertions.assertTrue(expr instanceof IsNull);

        PartitionKey v1 = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        PartitionKey v2 = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("1", false)),
                ImmutableList.of(column.getType()), false);
        listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s").iterator()
                .next();
        Assertions.assertEquals("OR[s IS NULL,s IN (1)]", expr.toSql());
    }

    // =========================================================================
    // New tests — constructPredicates(Expression) overload (hour-offset support)
    // =========================================================================

    @Test
    void testConstructPredicatesWithHoursAddExpressionRange() throws AnalysisException {
        // constructPredicates(Set<PartitionItem>, Expression): target is hours_add(k2, 3)
        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00")), ImmutableList.of(col));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-26 00:00:00")), ImmutableList.of(col));
        RangePartitionItem item = new RangePartitionItem(Range.closedOpen(lower, upper));

        UnboundSlot slot = new UnboundSlot("k2");
        Expression adjustedExpr = new HoursAdd(slot, new IntegerLiteral(3));
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(
                Sets.newHashSet(item), adjustedExpr);
        Assertions.assertEquals(1, predicates.size());
        String sql = predicates.iterator().next().toSql();
        Assertions.assertTrue(sql.contains("hours_add"), "Expected hours_add in predicate: " + sql);
        Assertions.assertTrue(sql.contains("2025-07-25"), "Expected lower bound in predicate: " + sql);
        Assertions.assertTrue(sql.contains("2025-07-26"), "Expected upper bound in predicate: " + sql);
    }

    @Test
    void testConstructPredicatesWithHoursSubExpressionRange() throws AnalysisException {
        // constructPredicates(Expression): target is hours_sub(k2, 3) for negative offsets
        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00")), ImmutableList.of(col));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-26 00:00:00")), ImmutableList.of(col));
        RangePartitionItem item = new RangePartitionItem(Range.closedOpen(lower, upper));

        UnboundSlot slot = new UnboundSlot("k2");
        Expression adjustedExpr = new HoursSub(slot, new IntegerLiteral(3));
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(
                Sets.newHashSet(item), adjustedExpr);
        Assertions.assertEquals(1, predicates.size());
        String sql = predicates.iterator().next().toSql();
        Assertions.assertTrue(sql.contains("hours_sub"), "Expected hours_sub in predicate: " + sql);
    }

    @Test
    void testConstructPredicatesWithExpressionEmptySet() throws AnalysisException {
        // constructPredicates(Expression) with empty set → BooleanLiteral.TRUE
        UnboundSlot slot = new UnboundSlot("k2");
        Expression adjustedExpr = new HoursAdd(slot, new IntegerLiteral(3));
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(
                Sets.newHashSet(), adjustedExpr);
        Assertions.assertEquals(1, predicates.size());
        Assertions.assertEquals("TRUE", predicates.iterator().next().toSql());
    }

    @Test
    void testConstructPredicatesWithHoursAddExpressionList() throws AnalysisException {
        // constructPredicates(Expression) for LIST partition
        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00", false)),
                ImmutableList.of(col.getType()), false);
        ListPartitionItem listItem = new ListPartitionItem(ImmutableList.of(v));

        UnboundSlot slot = new UnboundSlot("k2");
        Expression adjustedExpr = new HoursAdd(slot, new IntegerLiteral(3));
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(
                Sets.newHashSet(listItem), adjustedExpr);
        Assertions.assertEquals(1, predicates.size());
        String sql = predicates.iterator().next().toSql();
        Assertions.assertTrue(sql.contains("hours_add"), "Expected hours_add in list predicate: " + sql);
    }

    // =========================================================================
    // Mock-based tests for constructTableWithPredicates (private method)
    // =========================================================================

    @Test
    void testConstructTableWithPredicatesWithHourOffset() throws Exception {
        // Test the private constructTableWithPredicates method with hour offset
        MTMV mv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        TableIf table = Mockito.mock(TableIf.class);
        MTMVPartitionExprDateTruncDateAddSub exprService = Mockito.mock(MTMVPartitionExprDateTruncDateAddSub.class);

        // Setup partition item
        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00")), ImmutableList.of(col));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-26 00:00:00")), ImmutableList.of(col));
        RangePartitionItem partitionItem = new RangePartitionItem(Range.closedOpen(lower, upper));

        // Mock MTMV behavior
        Mockito.when(mv.getPartitionItemOrAnalysisException("p20250725")).thenReturn(partitionItem);
        Mockito.when(mv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getExpr()).thenReturn(Mockito.mock(org.apache.doris.analysis.Expr.class));
        Mockito.when(exprService.getOffsetHours()).thenReturn(3L);

        Map<TableIf, String> tableWithPartKey = ImmutableMap.of(table, "k2");
        Set<String> partitionNames = Sets.newHashSet("p20250725");

        // Use reflection to call private method
        Method method = UpdateMvByPartitionCommand.class.getDeclaredMethod(
                "constructTableWithPredicates", MTMV.class, Set.class, Map.class);
        method.setAccessible(true);

        try (MockedStatic<MTMVPartitionExprFactory> factoryMock = Mockito.mockStatic(MTMVPartitionExprFactory.class)) {
            factoryMock.when(() -> MTMVPartitionExprFactory.getExprService(Mockito.any()))
                    .thenReturn(exprService);

            @SuppressWarnings("unchecked")
            Map<TableIf, Set<Expression>> result = (Map<TableIf, Set<Expression>>)
                    method.invoke(null, mv, partitionNames, tableWithPartKey);

            Assertions.assertEquals(1, result.size());
            Set<Expression> predicates = result.get(table);
            Assertions.assertNotNull(predicates);
            Assertions.assertEquals(1, predicates.size());
            String sql = predicates.iterator().next().toSql();
            // Should contain hours_add due to positive offset
            Assertions.assertTrue(sql.contains("hours_add"), "Expected hours_add: " + sql);
        }
    }

    @Test
    void testConstructTableWithPredicatesWithNegativeHourOffset() throws Exception {
        // Test with negative hour offset (date_sub case)
        MTMV mv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        TableIf table = Mockito.mock(TableIf.class);
        MTMVPartitionExprDateTruncDateAddSub exprService = Mockito.mock(MTMVPartitionExprDateTruncDateAddSub.class);

        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00")), ImmutableList.of(col));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-26 00:00:00")), ImmutableList.of(col));
        RangePartitionItem partitionItem = new RangePartitionItem(Range.closedOpen(lower, upper));

        Mockito.when(mv.getPartitionItemOrAnalysisException("p20250725")).thenReturn(partitionItem);
        Mockito.when(mv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getExpr()).thenReturn(Mockito.mock(org.apache.doris.analysis.Expr.class));
        Mockito.when(exprService.getOffsetHours()).thenReturn(-5L);

        Map<TableIf, String> tableWithPartKey = ImmutableMap.of(table, "k2");
        Set<String> partitionNames = Sets.newHashSet("p20250725");

        Method method = UpdateMvByPartitionCommand.class.getDeclaredMethod(
                "constructTableWithPredicates", MTMV.class, Set.class, Map.class);
        method.setAccessible(true);

        try (MockedStatic<MTMVPartitionExprFactory> factoryMock = Mockito.mockStatic(MTMVPartitionExprFactory.class)) {
            factoryMock.when(() -> MTMVPartitionExprFactory.getExprService(Mockito.any()))
                    .thenReturn(exprService);

            @SuppressWarnings("unchecked")
            Map<TableIf, Set<Expression>> result = (Map<TableIf, Set<Expression>>)
                    method.invoke(null, mv, partitionNames, tableWithPartKey);

            Assertions.assertEquals(1, result.size());
            Set<Expression> predicates = result.get(table);
            String sql = predicates.iterator().next().toSql();
            // Should contain hours_sub due to negative offset
            Assertions.assertTrue(sql.contains("hours_sub"), "Expected hours_sub: " + sql);
        }
    }

    @Test
    void testConstructTableWithPredicatesWithZeroOffset() throws Exception {
        // Test with zero offset (no date_add/sub)
        MTMV mv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        TableIf table = Mockito.mock(TableIf.class);

        Column col = new Column("k2", PrimitiveType.DATETIME);
        PartitionKey lower = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-25 00:00:00")), ImmutableList.of(col));
        PartitionKey upper = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("2025-07-26 00:00:00")), ImmutableList.of(col));
        RangePartitionItem partitionItem = new RangePartitionItem(Range.closedOpen(lower, upper));

        Mockito.when(mv.getPartitionItemOrAnalysisException("p20250725")).thenReturn(partitionItem);
        Mockito.when(mv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getExpr()).thenReturn(null); // No expr → zero offset

        Map<TableIf, String> tableWithPartKey = ImmutableMap.of(table, "k2");
        Set<String> partitionNames = Sets.newHashSet("p20250725");

        Method method = UpdateMvByPartitionCommand.class.getDeclaredMethod(
                "constructTableWithPredicates", MTMV.class, Set.class, Map.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<TableIf, Set<Expression>> result = (Map<TableIf, Set<Expression>>)
                method.invoke(null, mv, partitionNames, tableWithPartKey);

        Assertions.assertEquals(1, result.size());
        Set<Expression> predicates = result.get(table);
        String sql = predicates.iterator().next().toSql();
        // Should NOT contain hours_add or hours_sub
        Assertions.assertFalse(sql.contains("hours_add"), "Should not have hours_add: " + sql);
        Assertions.assertFalse(sql.contains("hours_sub"), "Should not have hours_sub: " + sql);
    }
}
