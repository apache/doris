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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LanceScalarIndexPlannerTest {
    @Test
    public void testBooleanDriverSelectionPreservesAllBranches() {
        Expr key = equal("key", 1);
        Expr otherKey = equal("key", 2);
        Expr residual = equal("other", 3);
        for (Expr filter : Arrays.asList(or(key, otherKey),
                or(and(key, residual), and(otherKey, residual)), and(not(and(key, residual)), key))) {
            LanceScalarIndexPlanner.Plan plan = plan(filter);
            Assertions.assertNotNull(plan, filter.toSql());
            Assertions.assertEquals("key_idx", plan.indexName);
            Assertions.assertEquals(1, plan.splits.splitCount());
            Assertions.assertTrue(plan.splits.isCoveredByIndexSegment(1));
            Assertions.assertTrue(plan.splits.isCoveredByIndexSegment(2));
        }
        // Pruning either an OR branch or an AND below NOT could drop matching rows.
        for (Expr filter : Arrays.asList(not(key), not(or(key, otherKey)), or(key, residual), not(and(key, residual)),
                not(or(key, residual)), or(and(key, residual), residual))) {
            Assertions.assertNull(plan(filter), filter.toSql());
        }
    }

    @Test
    public void testConvertedArrayPredicatesSelectLabelList() {
        Schema schema = new Schema(Collections.singletonList(new Field("labels",
                FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE)))));
        LanceFragmentInfo fragment = new LanceFragmentInfo(1, 10, 10);
        LanceTableMetadata metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/labels.lance", Collections.emptyMap()), 42, schema,
                Collections.singletonList(fragment), Collections.singletonMap("labels", 9),
                Collections.singletonList(new LanceIndexSegmentInfo(UUID.randomUUID(), "labels_idx",
                        Collections.singletonList(9), Collections.singletonList(1L), IndexType.LABEL_LIST, null)));
        FunctionCallExpr red = contains("red");
        FunctionCallExpr blue = contains("blue");
        for (Expr filter : Arrays.asList(red, and(red, blue), or(red, blue))) {
            LancePredicateConverter.ConversionResult converted =
                    new LancePredicateConverter(schema).convert(Collections.singletonList(filter));
            Assertions.assertTrue(converted.getResidualConjuncts().isEmpty());
            LanceScalarIndexPlanner.Plan plan = LanceScalarIndexPlanner.plan(metadata,
                    converted.getPushedConjuncts(), Collections.singletonMap(1L, fragment));
            Assertions.assertNotNull(plan);
            Assertions.assertEquals("labels_idx", plan.indexName);
            Assertions.assertEquals(1, plan.splits.splitCount());
        }
    }

    @Test
    public void testPositiveDriverWithComplementSearchesSegmentOnce() {
        Expr key = equal("key", 1);
        Expr otherNotEqual = new BinaryPredicate(BinaryPredicate.Operator.NE,
                new SlotRef(null, "other"), new IntLiteral(0));
        Assertions.assertEquals(1, plan(Arrays.asList(key, otherNotEqual), false).splits.splitCount());
        Assertions.assertEquals(1, plan(and(key, otherNotEqual)).splits.splitCount());
        Assertions.assertEquals(1, plan(and(key, not(equal("key", 2)))).splits.splitCount());
    }

    @Test
    public void testUnindexableOrBranchDoesNotGroupFragments() {
        Expr suffix = new LikePredicate(LikePredicate.Operator.LIKE,
                new SlotRef(null, "key"), new StringLiteral("%y%"));
        Expr key = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "key"), new StringLiteral("x"));
        Assertions.assertNull(plan(Collections.singletonList(or(key, suffix)), true));
        Assertions.assertNotNull(plan(Collections.singletonList(and(key, suffix)), true));
        Expr prefix = new LikePredicate(LikePredicate.Operator.LIKE,
                new SlotRef(null, "key"), new StringLiteral("y%"));
        Assertions.assertEquals(1, plan(Collections.singletonList(or(key, prefix)), true).splits.splitCount());
    }

    @Test
    public void testExpressionDepthBudget() {
        Expr filter = equal("key", 1);
        for (int depth = 1; depth <= 32; depth++) {
            filter = and(filter, equal("key", depth));
        }
        Assertions.assertNotNull(plan(filter));
        Assertions.assertNull(plan(and(filter, equal("key", 33))));
    }

    @Test
    public void testOverlapExpressionBudget() throws Exception {
        Schema schema = new Schema(Collections.singletonList(new Field("labels",
                FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE)))));
        LanceFragmentInfo first = new LanceFragmentInfo(1, 10, 10);
        LanceFragmentInfo second = new LanceFragmentInfo(2, 10, 10);
        Map<Long, LanceFragmentInfo> fragments = new HashMap<>();
        fragments.put(1L, first);
        fragments.put(2L, second);
        LanceTableMetadata metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/labels.lance", Collections.emptyMap()), 42, schema,
                Arrays.asList(first, second), Collections.singletonMap("labels", 9),
                Collections.singletonList(new LanceIndexSegmentInfo(UUID.randomUUID(), "labels_idx",
                        Collections.singletonList(9), Arrays.asList(1L, 2L), IndexType.LABEL_LIST, null)));
        for (int count : Arrays.asList(64, 65)) {
            StringLiteral[] labels = new StringLiteral[count];
            for (int i = 0; i < count; i++) {
                labels[i] = new StringLiteral("label_" + i);
            }
            FunctionCallExpr overlap = new FunctionCallExpr("arrays_overlap", Arrays.asList(
                    new SlotRef(null, "labels"), new ArrayLiteral(ArrayType.create(Type.STRING, true), labels)));
            overlap.setFn(new ScalarFunction(new FunctionName("arrays_overlap"),
                    Arrays.asList(ArrayType.create(Type.STRING, true), ArrayType.create(Type.STRING, true)),
                    Type.BOOLEAN, false, true));
            LancePredicateConverter.ConversionResult converted = new LancePredicateConverter(schema)
                    .convert(Collections.singletonList(overlap));
            Assertions.assertTrue(converted.getResidualConjuncts().isEmpty());
            LanceScalarIndexPlanner.Plan selected = LanceScalarIndexPlanner.plan(metadata,
                    converted.getPushedConjuncts(), fragments);
            if (count == 64) {
                Assertions.assertNotNull(selected);
                Assertions.assertEquals(1, selected.splits.splitCount());
                // Count the enclosing AND and its other leaf, not just overlap's 127 nodes.
                Assertions.assertNull(LanceScalarIndexPlanner.plan(metadata,
                        Arrays.asList(overlap, contains("extra")), fragments));
            } else {
                Assertions.assertNull(selected);
            }
        }
    }

    private static FunctionCallExpr contains(String label) {
        FunctionCallExpr function = new FunctionCallExpr("array_contains",
                Arrays.asList(new SlotRef(null, "labels"), new StringLiteral(label)));
        function.setFn(new ScalarFunction(new FunctionName("array_contains"),
                Arrays.asList(ArrayType.create(Type.STRING, true), Type.STRING), Type.BOOLEAN, false, true));
        return function;
    }

    private static LanceScalarIndexPlanner.Plan plan(Expr filter) {
        return plan(Collections.singletonList(filter), false);
    }

    private static LanceScalarIndexPlanner.Plan plan(List<Expr> filters, boolean stringKey) {
        LanceFragmentInfo first = new LanceFragmentInfo(1, 10, 10);
        LanceFragmentInfo second = new LanceFragmentInfo(2, 10, 10);
        Map<Long, LanceFragmentInfo> fragments = new HashMap<>();
        fragments.put(1L, first);
        fragments.put(2L, second);
        Map<String, Integer> fields = new HashMap<>();
        fields.put("key", 9);
        fields.put("other", 10);
        LanceTableMetadata metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/labels.lance", Collections.emptyMap()), 42,
                new Schema(Arrays.asList(Field.nullable("key", stringKey ? ArrowType.Utf8.INSTANCE : new ArrowType.Int(64, true)),
                        Field.nullable("other", new ArrowType.Int(64, true)))),
                Arrays.asList(first, second), fields, Collections.singletonList(new LanceIndexSegmentInfo(
                        UUID.randomUUID(), "key_idx", Collections.singletonList(9),
                        Arrays.asList(1L, 2L), IndexType.BTREE, null)));
        return LanceScalarIndexPlanner.plan(metadata, filters, fragments);
    }

    private static Expr equal(String column, int value) {
        return new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(null, column), new IntLiteral(value));
    }

    private static Expr and(Expr left, Expr right) {
        return new CompoundPredicate(CompoundPredicate.Operator.AND, left, right);
    }

    private static Expr or(Expr left, Expr right) {
        return new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);
    }

    private static Expr not(Expr child) {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null);
    }
}
