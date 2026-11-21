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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Which pushed predicates one scalar index can drive segment by segment, and the expression budget. */
class LanceScalarIndexPlannerTest {
    private static final Field LABELS = new Field("labels", FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE)));
    private static final List<LanceFragmentInfo> TWO_FRAGMENTS = Arrays.asList(
            new LanceFragmentInfo(1, 10, 10), new LanceFragmentInfo(2, 10, 10));

    @Test
    void booleanDriverSelectionPreservesAllBranches() {
        ConnectorExpression key = equal("key", 1);
        ConnectorExpression otherKey = equal("key", 2);
        ConnectorExpression residual = equal("other", 3);
        for (ConnectorExpression filter : Arrays.asList(or(key, otherKey),
                or(and(key, residual), and(otherKey, residual)), and(not(and(key, residual)), key))) {
            LanceScalarIndexPlanner.Plan plan = plan(filter);
            Assertions.assertNotNull(plan, filter.toString());
            Assertions.assertEquals("key_idx", plan.indexName);
            Assertions.assertEquals(1, plan.splits.splitCount());
            Assertions.assertTrue(plan.splits.isCoveredByIndexSegment(1));
            Assertions.assertTrue(plan.splits.isCoveredByIndexSegment(2));
        }
        // Pruning either an OR branch or an AND below NOT could drop matching rows.
        for (ConnectorExpression filter : Arrays.asList(not(key), not(or(key, otherKey)), or(key, residual),
                not(and(key, residual)), not(or(key, residual)), or(and(key, residual), residual))) {
            Assertions.assertNull(plan(filter), filter.toString());
        }
    }

    @Test
    void convertedArrayPredicatesSelectLabelList() {
        Schema schema = new Schema(Collections.singletonList(LABELS));
        List<LanceFragmentInfo> fragments = Collections.singletonList(new LanceFragmentInfo(1, 10, 10));
        LanceIndexMetadata indexes = new LanceIndexMetadata(Collections.singletonMap("labels", 9),
                Collections.singletonList(segment("labels_idx", 9, IndexType.LABEL_LIST, 1L)));
        ConnectorExpression red = contains("red");
        ConnectorExpression blue = contains("blue");
        for (ConnectorExpression filter : Arrays.asList(red, and(red, blue), or(red, blue))) {
            LancePredicateConverter converter = new LancePredicateConverter(schema);
            LancePredicateConverter.ConversionResult converted = converter.convert(Collections.singletonList(filter));
            Assertions.assertTrue(converted.allPushed());
            LanceScalarIndexPlanner.Plan plan = LanceScalarIndexPlanner.plan(metadata(schema, fragments), indexes,
                    converter, converted.getPushedConjuncts(), visible(fragments));
            Assertions.assertNotNull(plan);
            Assertions.assertEquals("labels_idx", plan.indexName);
            Assertions.assertEquals(1, plan.splits.splitCount());
        }
    }

    @Test
    void positiveDriverWithComplementSearchesSegmentOnce() {
        ConnectorExpression key = equal("key", 1);
        ConnectorExpression otherNotEqual = new ConnectorComparison(ConnectorComparison.Operator.NE,
                bigint("other"), ConnectorLiteral.ofLong(0));
        Assertions.assertEquals(1, plan(Arrays.asList(key, otherNotEqual), false).splits.splitCount());
        Assertions.assertEquals(1, plan(and(key, otherNotEqual)).splits.splitCount());
        Assertions.assertEquals(1, plan(and(key, not(equal("key", 2)))).splits.splitCount());
    }

    @Test
    void unindexableOrBranchDoesNotGroupFragments() {
        ConnectorExpression suffix = like("%y%");
        ConnectorExpression key = stringEqual("x");
        Assertions.assertNull(plan(Collections.singletonList(or(key, suffix)), true));
        Assertions.assertNotNull(plan(Collections.singletonList(and(key, suffix)), true));
        Assertions.assertEquals(1, plan(Collections.singletonList(or(key, like("y%"))), true).splits.splitCount());
    }

    @Test
    void expressionDepthBudget() {
        // Nested two-operand ANDs, the shape whose depth grows with every conjunct.
        ConnectorExpression filter = equal("key", 1);
        for (int depth = 1; depth <= 32; depth++) {
            filter = and(filter, equal("key", depth));
        }
        Assertions.assertNotNull(plan(filter));
        Assertions.assertNull(plan(and(filter, equal("key", 33))));
    }

    @Test
    void overlapExpressionBudget() {
        Schema schema = new Schema(Collections.singletonList(LABELS));
        LanceIndexMetadata indexes = new LanceIndexMetadata(Collections.singletonMap("labels", 9),
                Collections.singletonList(segment("labels_idx", 9, IndexType.LABEL_LIST, 1L, 2L)));
        for (int count : Arrays.asList(64, 65)) {
            ConnectorExpression overlap = overlap(count);
            LancePredicateConverter converter = new LancePredicateConverter(schema);
            LancePredicateConverter.ConversionResult converted = converter.convert(Collections.singletonList(overlap));
            Assertions.assertTrue(converted.allPushed());
            LanceScalarIndexPlanner.Plan selected = LanceScalarIndexPlanner.plan(metadata(schema, TWO_FRAGMENTS),
                    indexes, converter, converted.getPushedConjuncts(), visible(TWO_FRAGMENTS));
            if (count == 64) {
                Assertions.assertNotNull(selected);
                Assertions.assertEquals(1, selected.splits.splitCount());
                // Count the enclosing AND and its other leaf, not just overlap's 127 nodes.
                Assertions.assertNull(LanceScalarIndexPlanner.plan(metadata(schema, TWO_FRAGMENTS), indexes,
                        converter, Arrays.asList(overlap, contains("extra")), visible(TWO_FRAGMENTS)));
            } else {
                Assertions.assertNull(selected);
            }
        }
    }

    @Test
    void wideConjunctionUsesBalancedDepth() {
        List<ConnectorExpression> filters = new ArrayList<>();
        filters.add(equal("key", 1));
        for (int i = 0; i < 33; i++) {
            filters.add(equal("filter_" + i, i));
        }
        Assertions.assertNotNull(plan(filters, false));
    }

    @Test
    void shortInExpansionAtOverlapBudgetBoundary() {
        Schema schema = new Schema(Arrays.asList(LABELS, Field.nullable("category", new ArrowType.Int(64, true))));
        Map<String, Integer> fields = new HashMap<>();
        fields.put("labels", 9);
        fields.put("category", 10);
        LanceIndexMetadata indexes = new LanceIndexMetadata(fields, Arrays.asList(
                segment("labels_idx", 9, IndexType.LABEL_LIST, 1L, 2L),
                segment("category_idx", 10, IndexType.BTREE, 1L, 2L)));
        ConnectorExpression in = new ConnectorIn(bigint("category"),
                Arrays.asList(ConnectorLiteral.ofLong(0), ConnectorLiteral.ofLong(1)), false);
        for (int count : Arrays.asList(62, 63)) {
            LanceScalarIndexPlanner.Plan selected = LanceScalarIndexPlanner.plan(metadata(schema, TWO_FRAGMENTS),
                    indexes, new LancePredicateConverter(schema), Arrays.asList(overlap(count), in),
                    visible(TWO_FRAGMENTS));
            if (count == 62) {
                Assertions.assertNotNull(selected);
            } else {
                Assertions.assertNull(selected);
            }
        }
    }

    @Test
    void literalAndRefinedPrefixes() {
        ConnectorExpression key = stringEqual("x");
        for (String prefix : Arrays.asList("test_ns$", "literal%value", "literal\\value")) {
            ConnectorExpression startsWith = new ConnectorFunctionCall("starts_with", ConnectorType.of("BOOLEAN"),
                    Arrays.asList(string("key"), ConnectorLiteral.ofString(prefix)));
            Assertions.assertNotNull(plan(Collections.singletonList(startsWith), true));
            Assertions.assertNotNull(plan(Collections.singletonList(or(key, startsWith)), true));
        }
        for (String pattern : Arrays.asList("foo%bar%", "foo_bar%")) {
            ConnectorExpression like = like(pattern);
            Assertions.assertNotNull(plan(Collections.singletonList(like), true));
            // Native cannot union an index branch that still carries a refine predicate.
            Assertions.assertNull(plan(Collections.singletonList(or(key, like)), true));
            Assertions.assertNull(plan(Collections.singletonList(or(and(key, like), key)), true));
        }
    }

    private static LanceScalarIndexPlanner.Plan plan(ConnectorExpression filter) {
        return plan(Collections.singletonList(filter), false);
    }

    /** One BTREE index, key_idx, with a single segment covering both fragments. */
    private static LanceScalarIndexPlanner.Plan plan(List<ConnectorExpression> filters, boolean stringKey) {
        Schema schema = new Schema(Arrays.asList(
                Field.nullable("key", stringKey ? ArrowType.Utf8.INSTANCE : new ArrowType.Int(64, true)),
                Field.nullable("other", new ArrowType.Int(64, true))));
        Map<String, Integer> fields = new HashMap<>();
        fields.put("key", 9);
        fields.put("other", 10);
        LanceIndexMetadata indexes = new LanceIndexMetadata(fields,
                Collections.singletonList(segment("key_idx", 9, IndexType.BTREE, 1L, 2L)));
        return LanceScalarIndexPlanner.plan(metadata(schema, TWO_FRAGMENTS), indexes,
                new LancePredicateConverter(schema), filters, visible(TWO_FRAGMENTS));
    }

    private static LanceTableMetadata metadata(Schema schema, List<LanceFragmentInfo> fragments) {
        return new LanceTableMetadata(new LanceTableAccess("s3://bucket/labels.lance", Collections.emptyMap()), 42,
                schema, fragments);
    }

    private static Map<Long, LanceFragmentInfo> visible(List<LanceFragmentInfo> fragments) {
        Map<Long, LanceFragmentInfo> visible = new LinkedHashMap<>();
        fragments.forEach(fragment -> visible.put(fragment.getId(), fragment));
        return visible;
    }

    private static LanceIndexSegmentInfo segment(String name, int fieldId, IndexType type, Long... fragmentIds) {
        return new LanceIndexSegmentInfo(UUID.randomUUID(), name, Collections.singletonList(fieldId),
                Arrays.asList(fragmentIds), type);
    }

    private static ConnectorColumnRef bigint(String column) {
        return new ConnectorColumnRef(column, ConnectorType.of("BIGINT"));
    }

    private static ConnectorColumnRef string(String column) {
        return new ConnectorColumnRef(column, ConnectorType.of("STRING"));
    }

    private static ConnectorExpression equal(String column, int value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, bigint(column), ConnectorLiteral.ofLong(value));
    }

    private static ConnectorExpression stringEqual(String value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, string("key"), ConnectorLiteral.ofString(value));
    }

    private static ConnectorExpression like(String pattern) {
        return new ConnectorLike(ConnectorLike.Operator.LIKE, string("key"), ConnectorLiteral.ofString(pattern));
    }

    private static ConnectorExpression contains(String label) {
        return new ConnectorFunctionCall("array_contains", ConnectorType.of("BOOLEAN"),
                Arrays.asList(new ConnectorColumnRef("labels", ConnectorType.arrayOf(ConnectorType.of("STRING"))),
                        ConnectorLiteral.ofString(label)));
    }

    /** arrays_overlap(labels, ['label_0', ..., 'label_{count - 1}']), the literal as the engine hands it over. */
    private static ConnectorExpression overlap(int count) {
        List<Object> labels = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            labels.add("label_" + i);
        }
        ConnectorType stringArray = ConnectorType.arrayOf(ConnectorType.of("STRING"));
        return new ConnectorFunctionCall("arrays_overlap", ConnectorType.of("BOOLEAN"), Arrays.asList(
                new ConnectorColumnRef("labels", stringArray),
                new ConnectorLiteral(stringArray, Collections.unmodifiableList(labels))));
    }

    private static ConnectorExpression and(ConnectorExpression left, ConnectorExpression right) {
        return new ConnectorAnd(Arrays.asList(left, right));
    }

    private static ConnectorExpression or(ConnectorExpression left, ConnectorExpression right) {
        return new ConnectorOr(Arrays.asList(left, right));
    }

    private static ConnectorExpression not(ConnectorExpression operand) {
        return new ConnectorNot(operand);
    }
}
