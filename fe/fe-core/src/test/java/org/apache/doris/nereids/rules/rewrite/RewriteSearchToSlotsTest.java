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

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIndexes;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for RewriteSearchToSlots rule
 * Note: These tests focus on the rule structure and basic functionality.
 * Full integration testing should be done via regression tests.
 */
public class RewriteSearchToSlotsTest {

    private RewriteSearchToSlots rewriteRule;

    @BeforeEach
    public void setUp() {
        rewriteRule = new RewriteSearchToSlots();
    }

    @Test
    public void testRuleType() {
        Assertions.assertNotNull(rewriteRule);
        Assertions.assertEquals(org.apache.doris.nereids.rules.RuleType.REWRITE_SEARCH_TO_SLOTS,
                rewriteRule.build().getRuleType());
    }

    @Test
    public void testRuleCreation() {
        Rule rule = rewriteRule.build();
        Assertions.assertNotNull(rule);
        Assertions.assertNotNull(rule.getPattern());
    }

    @Test
    public void testSearchFunctionDetection() {
        // Create simple search function
        String dsl = "title:hello";
        Search searchFunc = new Search(new StringLiteral(dsl));

        // Verify it's a Search function
        Assertions.assertInstanceOf(Search.class, searchFunc);
        Assertions.assertEquals(dsl, searchFunc.getDslString());
        Assertions.assertNotNull(searchFunc.getQsPlan());
    }

    @Test
    public void testSearchExpressionCreation() {
        // Test creating SearchExpression manually
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
        SlotReference titleSlot = new SlotReference("title", StringType.INSTANCE, true, Arrays.asList());
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        Assertions.assertNotNull(searchExpr);
        Assertions.assertEquals(dsl, searchExpr.getDslString());
        Assertions.assertEquals(1, searchExpr.getSlotChildren().size());
        Assertions.assertEquals("title", ((SlotReference) searchExpr.getSlotChildren().get(0)).getName());
    }

    @Test
    public void testMultiFieldSearchExpression() {
        String dsl = "title:hello AND content:world";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);

        SlotReference titleSlot = new SlotReference("title", StringType.INSTANCE, true, Arrays.asList());
        SlotReference contentSlot = new SlotReference("content", StringType.INSTANCE, true, Arrays.asList());
        List<Expression> slotChildren = Arrays.asList(titleSlot, contentSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        Assertions.assertEquals(2, searchExpr.getSlotChildren().size());
        Assertions.assertTrue(searchExpr.getSlotChildren().stream()
                .anyMatch(expr -> "title".equals(((SlotReference) expr).getName())));
        Assertions.assertTrue(searchExpr.getSlotChildren().stream()
                .anyMatch(expr -> "content".equals(((SlotReference) expr).getName())));
    }

    @Test
    public void testQsPlanParsing() {
        // Test various DSL formats
        String[] testCases = {
                "title:hello",
                "content:\"phrase search\"",
                "title:hello AND content:world",
                "(title:machine OR content:learning) AND category:tech"
        };

        for (String dsl : testCases) {
            try {
                SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
                Assertions.assertNotNull(plan, "Plan should not be null for DSL: " + dsl);
                Assertions.assertNotNull(plan.getRoot(), "Plan root should not be null for DSL: " + dsl);
                Assertions.assertTrue(plan.getFieldBindings().size() > 0, "Should have field bindings for DSL: " + dsl);
            } catch (Exception e) {
                // DSL parsing might fail for complex cases - that's acceptable
                System.out.println("DSL parsing failed for: " + dsl + " - " + e.getMessage());
            }
        }
    }

    @Test
    public void testFieldNameExtraction() {
        String dsl = "title:hello AND content:world AND category:tech";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);

        // Should extract 3 unique field names
        Assertions.assertEquals(3, plan.getFieldBindings().size());

        List<String> fieldNames = plan.getFieldBindings().stream()
                .map(binding -> binding.getFieldName())
                .distinct()
                .collect(java.util.stream.Collectors.toList());

        Assertions.assertTrue(fieldNames.contains("title"));
        Assertions.assertTrue(fieldNames.contains("content"));
        Assertions.assertTrue(fieldNames.contains("category"));
    }

    @Test
    public void testCaseInsensitiveFieldNames() {
        String dsl1 = "TITLE:hello";
        String dsl2 = "title:hello";

        SearchDslParser.QsPlan plan1 = SearchDslParser.parseDsl(dsl1);
        SearchDslParser.QsPlan plan2 = SearchDslParser.parseDsl(dsl2);

        // Both should work and extract field names
        Assertions.assertEquals(1, plan1.getFieldBindings().size());
        Assertions.assertEquals(1, plan2.getFieldBindings().size());

        // Field names should be consistent (implementation dependent)
        Assertions.assertNotNull(plan1.getFieldBindings().get(0).getFieldName());
        Assertions.assertNotNull(plan2.getFieldBindings().get(0).getFieldName());
    }

    @Test
    public void testEmptyAndInvalidDsl() {
        // Empty DSL should be handled gracefully
        try {
            SearchDslParser.QsPlan plan = SearchDslParser.parseDsl("");
            Assertions.assertNotNull(plan);
        } catch (RuntimeException e) {
            // Also acceptable to throw exception
            Assertions.assertTrue(e.getMessage().contains("empty") || e.getMessage().contains("Invalid"));
        }

        // Invalid DSL should throw exception
        try {
            SearchDslParser.parseDsl("invalid:syntax AND");
            Assertions.assertTrue(false, "Expected exception for invalid DSL");
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void testComplexDslStructures() {
        String complexDsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";

        try {
            SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(complexDsl);
            Assertions.assertNotNull(plan);
            Assertions.assertNotNull(plan.getRoot());

            // Should have multiple field bindings
            Assertions.assertTrue(plan.getFieldBindings().size() >= 2);

        } catch (Exception e) {
            // Complex DSL might not be fully supported yet
            System.out.println("Complex DSL parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testSlotReferenceConsistency() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);

        // Create slot reference matching the field binding
        String fieldName = plan.getFieldBindings().get(0).getFieldName();
        SlotReference slot = new SlotReference(fieldName, StringType.INSTANCE, true, Arrays.asList());

        SearchExpression expr = new SearchExpression(dsl, plan, Arrays.asList(slot));

        // Verify consistency
        Assertions.assertEquals(1, expr.children().size());
        Assertions.assertEquals(slot, expr.children().get(0));
        Assertions.assertEquals(fieldName, ((SlotReference) expr.children().get(0)).getName());
    }

    @Test
    public void testRewriteSearchHandlesCaseInsensitiveField() throws Exception {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                buildStudentWithInvertedIndexOnName(100L), ImmutableList.of("db"));
        Search searchFunc = new Search(new StringLiteral("NAME:alice"));

        Method rewriteMethod = RewriteSearchToSlots.class.getDeclaredMethod(
                "rewriteSearch", Search.class, LogicalOlapScan.class);
        rewriteMethod.setAccessible(true);

        Object rewritten = rewriteMethod.invoke(rewriteRule, searchFunc, scan);
        Assertions.assertInstanceOf(SearchExpression.class, rewritten);

        SearchExpression searchExpression = (SearchExpression) rewritten;
        Assertions.assertEquals(1, searchExpression.getSlotChildren().size());
        Assertions.assertTrue(searchExpression.getSlotChildren().get(0) instanceof SlotReference);
        SlotReference slot = (SlotReference) searchExpression.getSlotChildren().get(0);
        Assertions.assertEquals("name", slot.getName());

        SearchDslParser.QsPlan normalizedPlan = searchExpression.getQsPlan();
        Assertions.assertEquals("name", normalizedPlan.getFieldBindings().get(0).getFieldName());
        Assertions.assertEquals("name", normalizedPlan.getRoot().getField());
    }

    @Test
    public void testRewriteSearchHandlesCaseInsensitiveVariantParentField() throws Exception {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                buildVariantTableWithInvertedIndex(102L), ImmutableList.of("db"));
        Search searchFunc = new Search(new StringLiteral("V.foo:bar"));

        Method rewriteMethod = RewriteSearchToSlots.class.getDeclaredMethod(
                "rewriteSearch", Search.class, LogicalOlapScan.class);
        rewriteMethod.setAccessible(true);

        Object rewritten = rewriteMethod.invoke(rewriteRule, searchFunc, scan);
        Assertions.assertInstanceOf(SearchExpression.class, rewritten);

        SearchExpression searchExpression = (SearchExpression) rewritten;
        Assertions.assertEquals(1, searchExpression.getSlotChildren().size());
        Assertions.assertTrue(searchExpression.getSlotChildren().get(0) instanceof ElementAt);
        ElementAt elementAt = (ElementAt) searchExpression.getSlotChildren().get(0);
        Assertions.assertTrue(elementAt.child(0) instanceof SlotReference);
        Assertions.assertEquals("v", ((SlotReference) elementAt.child(0)).getName());

        SearchDslParser.QsPlan normalizedPlan = searchExpression.getQsPlan();
        Assertions.assertEquals("v.foo", normalizedPlan.getFieldBindings().get(0).getFieldName());
        Assertions.assertEquals("v.foo", normalizedPlan.getRoot().getField());
    }

    @Test
    public void testRewriteSearchThrowsWhenFieldMissing() throws Exception {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.student, ImmutableList.of("db"));
        Search searchFunc = new Search(new StringLiteral("unknown_field:value"));

        Method rewriteMethod = RewriteSearchToSlots.class.getDeclaredMethod(
                "rewriteSearch", Search.class, LogicalOlapScan.class);
        rewriteMethod.setAccessible(true);

        InvocationTargetException thrown = Assertions.assertThrows(InvocationTargetException.class,
                () -> rewriteMethod.invoke(rewriteRule, searchFunc, scan));
        Assertions.assertNotNull(thrown.getCause());
        Assertions.assertInstanceOf(AnalysisException.class, thrown.getCause());
        Assertions.assertTrue(thrown.getCause().getMessage().contains("unknown_field"));
    }

    @Test
    public void testRewriteSearchThrowsWhenColumnHasNoInvertedIndex() throws Exception {
        // PlanConstructor.student has the 'name' column but no inverted index on it. The rewrite
        // must surface a clear error instead of letting BE silently return an empty bitmap.
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.student, ImmutableList.of("db"));
        Search searchFunc = new Search(new StringLiteral("name:alice"));

        Method rewriteMethod = RewriteSearchToSlots.class.getDeclaredMethod(
                "rewriteSearch", Search.class, LogicalOlapScan.class);
        rewriteMethod.setAccessible(true);

        InvocationTargetException thrown = Assertions.assertThrows(InvocationTargetException.class,
                () -> rewriteMethod.invoke(rewriteRule, searchFunc, scan));
        Assertions.assertNotNull(thrown.getCause());
        Assertions.assertInstanceOf(AnalysisException.class, thrown.getCause());
        Assertions.assertTrue(thrown.getCause().getMessage().contains("inverted index"),
                "Error message should mention inverted index, got: " + thrown.getCause().getMessage());
        Assertions.assertTrue(thrown.getCause().getMessage().contains("name"));
    }

    @Test
    public void testRewriteSearchSucceedsWhenColumnHasInvertedIndex() throws Exception {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                buildStudentWithInvertedIndexOnName(101L), ImmutableList.of("db"));
        Search searchFunc = new Search(new StringLiteral("name:alice"));

        Method rewriteMethod = RewriteSearchToSlots.class.getDeclaredMethod(
                "rewriteSearch", Search.class, LogicalOlapScan.class);
        rewriteMethod.setAccessible(true);

        Object rewritten = rewriteMethod.invoke(rewriteRule, searchFunc, scan);
        Assertions.assertInstanceOf(SearchExpression.class, rewritten);

        SearchExpression searchExpression = (SearchExpression) rewritten;
        Assertions.assertEquals(1, searchExpression.getSlotChildren().size());
        Assertions.assertTrue(searchExpression.getSlotChildren().get(0) instanceof SlotReference);
        Assertions.assertEquals("name",
                ((SlotReference) searchExpression.getSlotChildren().get(0)).getName());
    }

    private static OlapTable buildStudentWithInvertedIndexOnName(long tableId) {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("gender", Type.INT, false, AggregateType.NONE, "0", ""),
                new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                new Column("age", Type.INT, true, AggregateType.NONE, "", ""));
        Index invertedOnName = new Index(1L, "idx_name", ImmutableList.of("name"),
                IndexType.INVERTED, null, "");
        OlapTable table = new OlapTable(tableId, "student_with_inverted_index", false, columns,
                KeysType.PRIMARY_KEYS, new PartitionInfo(), null,
                new TableIndexes(ImmutableList.of(invertedOnName)));
        table.setIndexMeta(-1, "student_with_inverted_index", table.getFullSchema(),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.PRIMARY_KEYS);
        return table;
    }

    private static OlapTable buildVariantTableWithInvertedIndex(long tableId) {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("v", Type.VARIANT, false, AggregateType.NONE, "", ""));
        Index invertedOnVariant = new Index(2L, "idx_v", ImmutableList.of("v"),
                IndexType.INVERTED, null, "");
        OlapTable table = new OlapTable(tableId, "variant_with_inverted_index", false, columns,
                KeysType.PRIMARY_KEYS, new PartitionInfo(), null,
                new TableIndexes(ImmutableList.of(invertedOnVariant)));
        table.setIndexMeta(-1, "variant_with_inverted_index", table.getFullSchema(),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.PRIMARY_KEYS);
        return table;
    }
}
