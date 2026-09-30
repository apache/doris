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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.IntLiteral;
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
import java.util.Map;
import java.util.UUID;

public class LanceScalarIndexPlannerTest {
    @Test
    public void testBooleanDriverSelectionPreservesAllBranches() {
        Expr key = equal("key", 1);
        Expr otherKey = equal("key", 2);
        Expr residual = equal("other", 3);
        for (Expr filter : Arrays.asList(or(key, otherKey), not(key), not(or(key, otherKey)),
                or(and(key, residual), and(otherKey, residual)), and(not(and(key, residual)), key))) {
            LanceScalarIndexPlanner.Plan plan = plan(filter);
            Assertions.assertNotNull(plan, filter.toSql());
            Assertions.assertEquals("key_idx", plan.indexName);
            Assertions.assertEquals(1, plan.splits.splitCount());
            Assertions.assertTrue(plan.splits.isCoveredByIndexSegment(1));
            Assertions.assertFalse(plan.splits.isCoveredByIndexSegment(2));
        }
        // Pruning either an OR branch or an AND below NOT could drop matching rows.
        for (Expr filter : Arrays.asList(or(key, residual), not(and(key, residual)),
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
        for (Expr filter : Arrays.asList(red, and(red, blue), or(red, blue), not(red))) {
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

    private static FunctionCallExpr contains(String label) {
        FunctionCallExpr function = new FunctionCallExpr("array_contains",
                Arrays.asList(new SlotRef(null, "labels"), new StringLiteral(label)));
        function.setFn(new ScalarFunction(new FunctionName("array_contains"),
                Arrays.asList(ArrayType.create(Type.STRING, true), Type.STRING), Type.BOOLEAN, false, true));
        return function;
    }

    private static LanceScalarIndexPlanner.Plan plan(Expr filter) {
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
                new Schema(Arrays.asList(Field.nullable("key", new ArrowType.Int(64, true)),
                        Field.nullable("other", new ArrowType.Int(64, true)))),
                Arrays.asList(first, second), fields, Collections.singletonList(new LanceIndexSegmentInfo(
                        UUID.randomUUID(), "key_idx", Collections.singletonList(9),
                        Collections.singletonList(1L), IndexType.BTREE, null)));
        return LanceScalarIndexPlanner.plan(metadata, Collections.singletonList(filter), fragments);
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
