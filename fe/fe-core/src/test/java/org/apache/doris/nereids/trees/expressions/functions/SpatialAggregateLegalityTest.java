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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.CheckLegalityAfterRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.rewrite.DistinctWindowExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectSet;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupArrayIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupArrayUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Histogram;
import org.apache.doris.nereids.trees.expressions.functions.agg.LinearHistogram;
import org.apache.doris.nereids.trees.expressions.functions.agg.MapAggV2;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.SumMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNArray;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialAggregateLegalityTest {
    @Test
    public void linearHistogramAndMapAggRejectUnsupportedSpatialArguments() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                Expression shape = new SlotReference("shape", type, nullable);
                for (AggregateFunction function : List.of(new LinearHistogram(shape, new DoubleLiteral(1.0)),
                        new LinearHistogram(shape, new DoubleLiteral(1.0), new DoubleLiteral(0.0)),
                        new MapAggV2(shape, integer))) {
                    String expected = function instanceof MapAggV2 ? "map_agg does not support spatial keys"
                            : "linear_histogram does not support spatial arguments";
                    for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(expected, error.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void mapAggregatesRejectSpatialKeysBeforeCoercionAndAfterRewrite() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                Expression map = new SlotReference("values", MapType.of(type, IntegerType.INSTANCE), nullable);
                for (AggregateFunction function : List.of(new CountMap(map), new SumMap(map), new AvgMap(map),
                        new MinMap(map), new MaxMap(map))) {
                    for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " does not support spatial MAP keys",
                                error.getMessage());
                        Assertions.assertThrows(AnalysisException.class, entry::checkLegalityAfterRewrite);
                    }
                }
            }
        }
    }

    @Test
    public void ordinaryMapKeysKeepSupportedSpatialValues() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                Expression shape = new SlotReference("shape", type, nullable);
                Expression map = new SlotReference("values", MapType.of(IntegerType.INSTANCE, type), nullable);
                MapAggV2 mapAgg = new MapAggV2(integer, shape);
                for (BoundFunction entry : List.of(mapAgg, StateCombinator.create(mapAgg),
                        new CombineCombinator(mapAgg.getArguments(), mapAgg))) {
                    Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                }
                Assertions.assertEquals(type, ((MapType) mapAgg.getDataType()).getValueType());
                for (AggregateFunction function : List.of(new CountMap(map), new MinMap(map), new MaxMap(map))) {
                    for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertDoesNotThrow(entry::checkLegalityAfterRewrite);
                    }
                    DataType expected = function instanceof CountMap ? BigIntType.INSTANCE : type;
                    Assertions.assertEquals(expected, ((MapType) function.getDataType()).getValueType());
                }
                Assertions.assertThrows(AnalysisException.class, new SumMap(map)::getDataType);
                Assertions.assertThrows(AnalysisException.class, new AvgMap(map)::getDataType);
            }
        }
    }

    @Test
    public void minMaxByRejectScalarSpatialValuesAndKeys() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                Expression shape = new SlotReference("shape", type, nullable);
                for (AggregateFunction function : List.of(new MinBy(shape, integer), new MinBy(integer, shape),
                        new MaxBy(shape, integer), new MaxBy(integer, shape))) {
                    for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                                error.getMessage());
                        Assertions.assertThrows(AnalysisException.class, entry::checkLegalityAfterRewrite);
                    }
                }
                // These arguments already use the owning generic-column implementation.
                Expression shapes = new SlotReference("shapes", ArrayType.of(type), nullable);
                for (BoundFunction function : List.of(new MinBy(shapes, integer), new MinBy(integer, shapes),
                        new MaxBy(shapes, integer), new MaxBy(integer, shapes))) {
                    Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                    Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
                }
            }
        }
    }

    @Test
    public void topNAndGroupArrayRejectUnsupportedSpatialArguments() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                for (DataType type : List.of(spatial, ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)))) {
                    Expression value = new SlotReference("value", type, nullable);
                    for (BoundFunction function : List.of(new TopNArray(value, new IntegerLiteral(2)),
                            new TopNArray(value, new IntegerLiteral(2), new IntegerLiteral(4)))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals("topn_array does not support spatial arguments", error.getMessage());
                    }
                }
                for (DataType type : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)))) {
                    Expression values = new SlotReference("values", type, nullable);
                    for (BoundFunction function : List.of(new GroupArrayUnion(values),
                            new GroupArrayIntersect(values))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                                error.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void multiDistinctSpatialArgumentsAreRejectedAcrossPlanningStages() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                Expression shape = new SlotReference("shape", type, nullable);
                MultiDistinctCount function = new MultiDistinctCount(shape);
                Assertions.assertThrows(AnalysisException.class, function::checkLegalityBeforeTypeCoercion);
                Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
                Count count = new Count(true, shape);
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        count::convertToMultiDistinct);
                Assertions.assertEquals("multi_distinct_count does not support spatial arguments", error.getMessage());
                Assertions.assertDoesNotThrow(new Count(shape)::checkLegalityBeforeTypeCoercion);
                Assertions.assertDoesNotThrow(new Count(shape)::checkLegalityAfterRewrite);
            }
        }
    }

    @Test
    public void analyticDistinctRewriteChecksTheGeneratedAggregate() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"), IntegerType.INSTANCE)) {
            for (boolean nullable : List.of(false, true)) {
                SlotReference value = new SlotReference("value", type, nullable);
                LogicalEmptyRelation input = new LogicalEmptyRelation(
                        StatementScopeIdGenerator.newRelationId(), List.of(value));
                WindowExpression expression = new WindowExpression(new Count(true, value), List.of(), List.of());
                PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(),
                        new LogicalWindow<>(List.of(new Alias(expression)), input))
                        .applyTopDown(new DistinctWindowExpression());
                WindowExpression rewritten = (WindowExpression) ((LogicalWindow<?>) checker.getPlan())
                        .getWindowExpressions().get(0).child(0);
                Assertions.assertInstanceOf(MultiDistinctCount.class, rewritten.getFunction());
                if (type.isSpatialType()) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> checker.applyBottomUp(new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE)));
                    Assertions.assertEquals("multi_distinct_count does not support spatial arguments",
                            error.getMessage());
                } else {
                    Assertions.assertDoesNotThrow(
                            () -> checker.applyBottomUp(new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE)));
                }
            }
        }
    }

    @Test
    public void ordinaryParallelAggregateFactoriesRemainAdmitted() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        Expression integers = new SlotReference("ids", ArrayType.of(IntegerType.INSTANCE));
        Expression map = new SlotReference("values", MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE));
        for (BoundFunction function : List.of(new MinBy(integer, integer), new MaxBy(integer, integer),
                new TopNArray(integer, new IntegerLiteral(2)),
                new TopNArray(integer, new IntegerLiteral(2), new IntegerLiteral(4)),
                new GroupArrayUnion(integers), new GroupArrayIntersect(integers), new MultiDistinctCount(integer),
                new LinearHistogram(integer, new DoubleLiteral(1.0)),
                new LinearHistogram(integer, new DoubleLiteral(1.0), new DoubleLiteral(0.0)),
                new MapAggV2(integer, integer), new CountMap(map), new SumMap(map), new AvgMap(map),
                new MinMap(map), new MaxMap(map))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
            Assertions.assertDoesNotThrow(function::getDataType);
        }
        Assertions.assertDoesNotThrow(new Count(true, integer)::convertToMultiDistinct);
    }

    @Test
    public void nestedSpatialArrayAggIsRejectedBeforeStateSerialization() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nested : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                for (boolean nullable : List.of(false, true)) {
                    Expression value = new SlotReference("shapes", nested, nullable);
                    ArrayAgg function = new ArrayAgg(value);
                    for (BoundFunction entry : List.of(function, new ArrayAgg(true, value),
                            StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion, entry.getName());
                        Assertions.assertEquals("array_agg does not support nested spatial arguments",
                                error.getMessage());
                    }
                }
            }
        }
        // Scalar WKB and ordinary nested values retain their existing admission.
        for (DataType type : List.of(ArrayType.of(IntegerType.INSTANCE),
                new GeometryType("OGC:CRS84"), new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                ArrayAgg function = new ArrayAgg(new SlotReference("value", type, nullable));
                for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                        new CombineCombinator(function.getArguments(), function))) {
                    Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                }
            }
        }
    }

    @Test
    public void nestedSpatialCollectListIsRejectedBeforeStateSerialization() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nested : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                Expression value = new SlotReference("shapes", nested);
                for (BoundFunction function : List.of(new CollectList(value),
                        new CollectList(value, new IntegerLiteral(2)), new CollectList(true, value))) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            function::checkLegalityBeforeTypeCoercion);
                    Assertions.assertEquals("collect_list does not support nested spatial arguments",
                            error.getMessage());
                }
            }
        }
        // Ordinary complex values and nonnested spatial cells keep their existing admission.
        for (DataType type : List.of(ArrayType.of(IntegerType.INSTANCE),
                new GeometryType("OGC:CRS84"), new GeographyType("OGC:CRS84", "spherical"))) {
            CollectList function = new CollectList(new SlotReference("value", type));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        }
    }

    @Test
    public void unsupportedSpatialAggregateKernelsAreRejectedBeforeCoercion() {
        for (DataType type : List.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            Expression shape = new SlotReference("shape", type);
            for (BoundFunction function : List.of(new Ndv(shape), new Histogram(shape),
                    new Histogram(shape, new IntegerLiteral(2)), new CollectSet(shape),
                    new CollectSet(shape, new IntegerLiteral(2)))) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        function::checkLegalityBeforeTypeCoercion, function.getName());
                Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                        error.getMessage());
            }
        }
    }

    @Test
    public void singleValueSpatialAndOrdinaryAggregatesRemainAdmitted() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"), IntegerType.INSTANCE)) {
            Expression value = new SlotReference("value", type);
            for (BoundFunction function : List.of(new AnyValue(value), new Min(value), new Max(value))) {
                Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                Assertions.assertEquals(type, function.getDataType());
            }
        }
        Expression integer = new SlotReference("value", IntegerType.INSTANCE);
        for (BoundFunction function : List.of(new Ndv(integer), new Histogram(integer), new CollectSet(integer))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        }
    }
}
