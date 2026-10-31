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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.CheckLegalityAfterRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.rewrite.CheckDataTypes;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.CountAtMax;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountAtMin;
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
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.types.AggStateType;
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
    public void spatialWindowPartitionKeysRejectWithoutBlockingSpatialValues() {
        for (String spatial : List.of("GEOMETRY", "GEOMETRY('EPSG:3857')", "GEOGRAPHY",
                "GEOGRAPHY('OGC:CRS84', 'vincenty')")) {
            String source = " from (select number id, CAST(NULL AS " + spatial
                    + ") shape from numbers('number'='3')) shapes";
            for (String key : List.of("shape", "array(shape)", "map(id, shape)",
                    "named_struct('shape', shape)")) {
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                                .analyze("select count(*) over (partition by " + key + ")" + source).rewrite());
                Assertions.assertTrue(error.getMessage().contains("PARTITION BY does not support spatial keys"),
                        error.getMessage());
            }
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select first_value(shape) over (partition by id order by id), "
                            + "last_value(shape) over (partition by id order by id), "
                            + "count(shape) over (partition by id)" + source).rewrite());
        }
    }

    @Test
    public void countAtExtremesRejectSpatialArgumentsAndDeclaredStates() {
        for (DataType spatial : List.of(new GeometryType("OGC:CRS84"), new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType type : List.of(spatial, ArrayType.of(spatial))) {
                for (boolean nullable : List.of(false, true)) {
                    Expression value = new SlotReference("shape", type, nullable);
                    for (AggregateFunction function : List.of(new CountAtMin(value), new CountAtMax(value))) {
                        Assertions.assertThrows(AnalysisException.class, function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
                        Assertions.assertThrows(AnalysisException.class,
                                StateCombinator.create(function)::checkLegalityBeforeTypeCoercion);
                    }
                }
            }
            for (String name : List.of("count_at_min", "count_at_max")) {
                String type = spatial.toSql();
                for (String query : List.of(
                        "select " + name + "(shape) from (select CAST(NULL AS " + type
                                + ") shape from numbers('number'='3')) shapes",
                        "select CAST(NULL AS AGG_STATE<" + name + "(" + type + ")>)")) {
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(query).rewrite());
                    Assertions.assertTrue(error.getMessage().contains("does not support spatial arguments"),
                            error.getMessage());
                }
            }
        }
        Expression integer = new SlotReference("id", IntegerType.INSTANCE, true);
        for (AggregateFunction function : List.of(new CountAtMin(integer), new CountAtMax(integer))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void spatialSetComparisonsRejectKeysButUnionAllKeepsValues() {
        for (String spatial : List.of("GEOMETRY", "GEOMETRY('EPSG:3857')",
                "GEOGRAPHY", "GEOGRAPHY('OGC:CRS84', 'vincenty')")) {
            for (String type : List.of(spatial, "ARRAY<" + spatial + ">")) {
                String input = "select CAST(NULL AS " + type + ") shape";
                for (String operation : List.of("UNION", "UNION DISTINCT", "INTERSECT", "EXCEPT")) {
                    String sql = input + " " + operation + " " + input;
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
                    Assertions.assertTrue(error.getMessage().contains(
                            "GROUP BY and DISTINCT do not support spatial keys"), error.getMessage());
                }
                Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze(input + " UNION ALL " + input).rewrite());
            }
        }
        for (String operation : List.of("UNION", "UNION DISTINCT", "INTERSECT", "EXCEPT", "UNION ALL")) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select 1 " + operation + " select 2").rewrite());
        }
    }

    @Test
    public void distinctSpatialListsRejectComparisonButPlainListsRemainAdmitted() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                for (Expression value : List.of(new SlotReference("shape", spatial, nullable),
                        new NullLiteral(spatial))) {
                    for (AggregateFunction function : List.of(new ArrayAgg(true, value), new CollectList(true, value),
                            new CollectList(true, value, new IntegerLiteral(2)))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " DISTINCT does not support spatial arguments",
                                error.getMessage());
                        Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
                        Assertions.assertThrows(AnalysisException.class,
                                StateCombinator.create(function)::checkLegalityBeforeTypeCoercion);
                    }
                    for (BoundFunction function : List.of(new ArrayAgg(value), new CollectList(value),
                            new CollectList(value, new IntegerLiteral(2)))) {
                        Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
                    }
                }
            }
        }
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (BoundFunction function : List.of(new ArrayAgg(true, integer), new CollectList(true, integer),
                new CollectList(true, integer, new IntegerLiteral(2)))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void spatialGroupingAndDistinctAreRejectedBeforeSimplification() {
        for (String spatial : List.of("GEOMETRY", "GEOMETRY('EPSG:3857')",
                "GEOGRAPHY", "GEOGRAPHY('OGC:CRS84', 'vincenty')")) {
            String source = " from (select 1 id, CAST(NULL AS " + spatial + ") shape) shapes";
            for (String query : List.of("select shape, count(*)" + source + " group by shape",
                    "select id, shape, count(*)" + source + " group by id, shape",
                    "select shape, count(*)" + source + " group by grouping sets ((shape), ())",
                    "select distinct shape" + source,
                    "select distinct id, shape" + source,
                    "select array(shape), count(*)" + source + " group by array(shape)")) {
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(query).rewrite(), query);
                Assertions.assertTrue(error.getMessage().contains(
                        "GROUP BY and DISTINCT do not support spatial keys"), error.getMessage());
            }
            // Group only the ordinary key; spatial expressions remain values.
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select id, any_value(shape), count(shape), map_agg(id, shape)"
                            + source + " group by id").rewrite());
            for (String signature : List.of("min(" + spatial + ")", "max(" + spatial + ")")) {
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                                .analyze("select CAST(NULL AS AGG_STATE<" + signature + ">)").rewrite());
                Assertions.assertTrue(error.getMessage().contains("does not support spatial arguments"),
                        error.getMessage());
            }
        }
    }

    @Test
    public void minMaxAndCountDistinctRejectSpatialComparisons() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType spatial : List.of(new GeometryType("OGC:CRS84"), new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"), new GeographyType("OGC:CRS84", "vincenty"))) {
            for (DataType type : List.of(spatial, ArrayType.of(spatial))) {
                for (boolean nullable : List.of(false, true)) {
                    for (Expression value : List.of(new SlotReference("shape", type, nullable),
                            new NullLiteral(type))) {
                        for (AggregateFunction function : List.of(new Min(value), new Max(value))) {
                            for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                                    new CombineCombinator(function.getArguments(), function))) {
                                Assertions.assertThrows(AnalysisException.class,
                                        entry::checkLegalityBeforeTypeCoercion);
                                Assertions.assertThrows(AnalysisException.class, entry::checkLegalityAfterRewrite);
                            }
                        }
                        for (Count count : List.of(new Count(true, value), new Count(true, integer, value))) {
                            Assertions.assertThrows(AnalysisException.class, count::checkLegalityBeforeTypeCoercion);
                            Assertions.assertThrows(AnalysisException.class, count::checkLegalityAfterRewrite);
                        }
                        Assertions.assertDoesNotThrow(new Count(value)::checkLegalityBeforeTypeCoercion);
                        Assertions.assertDoesNotThrow(new AnyValue(value)::checkLegalityBeforeTypeCoercion);
                    }
                }
            }
        }
        for (BoundFunction function : List.of(new Min(integer), new Max(integer), new Count(true, integer))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void declaredSpatialMapStatesRejectKeysWithoutAProducer() {
        MemoTestUtils.createConnectContext();
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                for (String function : List.of("count_map", "sum_map", "avg_map", "min_map", "max_map")) {
                    AggStateType state = new AggStateType(function,
                            List.of(MapType.of(spatial, IntegerType.INSTANCE)), List.of(nullable), false);
                    for (DataType type : List.of(state, ArrayType.of(state),
                            MapType.of(IntegerType.INSTANCE, state),
                            new StructType(List.of(new StructField("state", state, true, ""))))) {
                        LogicalOneRowRelation plan = new LogicalOneRowRelation(
                                StatementScopeIdGenerator.newRelationId(), List.of(new Alias(new NullLiteral(type))));
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                () -> new CheckDataTypes().rewriteRoot(plan, null));
                        Assertions.assertEquals(function + " does not support spatial MAP keys", error.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void typedNullSpatialStatesAreCheckedThroughSqlRewrite() {
        for (String spatial : List.of("GEOMETRY('EPSG:3857')", "GEOGRAPHY")) {
            for (String function : List.of("count_map", "sum_map", "avg_map", "min_map", "max_map")) {
                String type = "AGG_STATE<" + function + "(MAP<" + spatial + ", INT>)>";
                String state = "CAST(NULL AS " + type + ")";
                for (String sql : List.of("select " + state,
                        "select s from (select " + state + " s) states",
                        "select " + function + "_merge(s) from (select " + state + " s) states",
                        "select TRY_CAST(NULL AS " + type + ")",
                        "select CONVERT(NULL, " + type + ")")) {
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
                    Assertions.assertTrue(error.getMessage().contains(
                            function + " does not support spatial MAP keys"), error.getMessage());
                }
            }
            for (String signature : List.of("map_agg(" + spatial + ", INT)",
                    "map_agg_v2(" + spatial + ", INT)", "linear_histogram(" + spatial + ", DOUBLE)",
                    "linear_histogram(" + spatial + ", DOUBLE, DOUBLE)")) {
                String sql = "select CAST(NULL AS AGG_STATE<" + signature + ">)";
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
                Assertions.assertTrue(error.getMessage().contains("does not support spatial"), error.getMessage());
            }
        }
    }

    @Test
    public void declaredSupportedSpatialStatesRemainAdmitted() {
        for (String spatial : List.of("GEOMETRY('EPSG:3857')", "GEOGRAPHY")) {
            for (String signature : List.of("map_agg(INT, " + spatial + ")",
                    "map_agg_v2(INT, " + spatial + ")", "count_map(MAP<INT, " + spatial + ">)",
                    "any_value(" + spatial + ")",
                    "count(" + spatial + ")", "array_agg(" + spatial + ")",
                    "collect_list(" + spatial + ")", "collect_list(" + spatial + ", INT)")) {
                String sql = "select CAST(NULL AS AGG_STATE<" + signature + ">)";
                Assertions.assertDoesNotThrow(
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
            }
        }
        for (String signature : List.of("linear_histogram(INT, DOUBLE)", "map_agg(INT, INT)",
                "count_map(MAP<INT, INT>)", "sum_map(MAP<INT, INT>)", "avg_map(MAP<INT, INT>)",
                "min_map(MAP<INT, INT>)", "max_map(MAP<INT, INT>)")) {
            String sql = "select CAST(NULL AS AGG_STATE<" + signature + ">)";
            Assertions.assertDoesNotThrow(
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
        }
    }

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
    public void distinctMapAggregatesRejectSpatialValuesRecursively() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType valueType : List.of(spatial, ArrayType.of(spatial),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                MapType mapType = MapType.of(IntegerType.INSTANCE, valueType);
                for (Expression value : List.of(new SlotReference("values", mapType, false),
                        new SlotReference("values", mapType, true), new NullLiteral(mapType))) {
                    for (AggregateFunction function : List.of(new CountMap(true, value), new MinMap(true, value),
                            new MaxMap(true, value), new SumMap(true, value), new AvgMap(true, value))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " DISTINCT does not support spatial arguments",
                                error.getMessage());
                        Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
                    }
                }
            }
        }
        Expression ordinary = new SlotReference("values", MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE));
        for (AggregateFunction function : List.of(new CountMap(true, ordinary), new MinMap(true, ordinary),
                new MaxMap(true, ordinary), new SumMap(true, ordinary), new AvgMap(true, ordinary))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void distinctSpatialMapsRejectDuringAnalysisBeforeAggregateLowering() {
        for (String spatial : List.of("GEOMETRY", "GEOGRAPHY")) {
            String source = " from (select number id, CAST(NULL AS " + spatial
                    + ") shape from numbers('number'='3')) shapes";
            for (String function : List.of("count_map", "min_map", "max_map")) {
                for (String value : List.of("shape", "array(shape)", "map(id, shape)",
                        "named_struct('shape', shape)")) {
                    String query = "select " + function + "(distinct map(1, " + value + "))" + source;
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(query).rewrite());
                    Assertions.assertTrue(error.getMessage().contains(
                            function + " DISTINCT does not support spatial arguments"), error.getMessage());
                }
            }
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                    .analyze("select count_map(map(1, shape))" + source).rewrite());
        }
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select count_map(distinct map(1, number)) from numbers('number'='3')").rewrite());
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
                for (AggregateFunction function : List.of(new CountMap(map))) {
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
    public void mapExtremaRejectSpatialValuesInAllEntryPoints() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType valueType : List.of(spatial, ArrayType.of(spatial),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                MapType mapType = MapType.of(IntegerType.INSTANCE, valueType);
                for (Expression value : List.of(new SlotReference("values", mapType, false),
                        new SlotReference("values", mapType, true), new NullLiteral(mapType))) {
                    for (AggregateFunction function : List.of(new MinMap(value), new MaxMap(value))) {
                        for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                                new CombineCombinator(function.getArguments(), function))) {
                            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                    entry::checkLegalityBeforeTypeCoercion);
                            Assertions.assertEquals(function.getName() + " does not support spatial MAP values",
                                    error.getMessage());
                            Assertions.assertThrows(AnalysisException.class, entry::checkLegalityAfterRewrite);
                        }
                    }
                }
            }
            for (String function : List.of("min_map", "max_map")) {
                String type = "AGG_STATE<" + function + "(MAP<INT, " + spatial.toSql() + ">)>";
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                                .analyze("select CAST(NULL AS " + type + ")").rewrite());
                Assertions.assertTrue(error.getMessage().contains(
                        function + " does not support spatial MAP values"), error.getMessage());
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
                for (BoundFunction function : List.of(new MinBy(shapes, integer), new MaxBy(shapes, integer))) {
                    Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                    Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
                }
            }
        }
    }

    @Test
    public void minMaxByRejectNestedSpatialOrderingKeysAcrossEntryPoints() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType type : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                for (Expression shapes : List.of(new SlotReference("shapes", type, false),
                        new SlotReference("shapes", type, true), new NullLiteral(type))) {
                    for (AggregateFunction function : List.of(new MinBy(integer, shapes), new MaxBy(integer, shapes))) {
                        for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                                new CombineCombinator(function.getArguments(), function))) {
                            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                    entry::checkLegalityBeforeTypeCoercion);
                            Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                                    error.getMessage());
                            Assertions.assertThrows(AnalysisException.class, entry::checkLegalityAfterRewrite);
                        }
                    }
                    for (AggregateFunction function : List.of(new MinBy(shapes, integer), new MaxBy(shapes, integer))) {
                        for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                                new CombineCombinator(function.getArguments(), function))) {
                            Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                            Assertions.assertDoesNotThrow(entry::checkLegalityAfterRewrite);
                        }
                    }
                }
                for (String function : List.of("min_by", "max_by")) {
                    String state = "AGG_STATE<" + function + "(INT, " + type.toSql() + ")>";
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                                    .analyze("select CAST(NULL AS " + state + ")").rewrite());
                    Assertions.assertTrue(error.getMessage().contains(function + " does not support spatial arguments"),
                            error.getMessage());
                }
            }
        }
        Expression keys = new SlotReference("keys", ArrayType.of(IntegerType.INSTANCE));
        for (BoundFunction function : List.of(new MinBy(integer, keys), new MaxBy(integer, keys))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
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
            for (DataType argumentType : List.of(type, ArrayType.of(type), ArrayType.of(ArrayType.of(type)))) {
                for (Expression shape : List.of(new SlotReference("shape", argumentType, false),
                        new SlotReference("shape", argumentType, true), new NullLiteral(argumentType))) {
                    MultiDistinctCount function = new MultiDistinctCount(shape);
                    Assertions.assertThrows(AnalysisException.class, function::checkLegalityBeforeTypeCoercion);
                    Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
                    Count count = new Count(true, shape);
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            count::convertToMultiDistinct);
                    Assertions.assertEquals("multi_distinct_count does not support spatial arguments",
                            error.getMessage());
                    Assertions.assertDoesNotThrow(new Count(shape)::checkLegalityBeforeTypeCoercion);
                    Assertions.assertDoesNotThrow(new Count(shape)::checkLegalityAfterRewrite);
                }
            }
        }
        for (DataType type : List.of(IntegerType.INSTANCE, ArrayType.of(IntegerType.INSTANCE),
                ArrayType.of(ArrayType.of(IntegerType.INSTANCE)))) {
            MultiDistinctCount function = new MultiDistinctCount(new SlotReference("value", type, true));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
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
    public void collectSetRejectsNestedSpatialInputsAndStates() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType type : List.of(ArrayType.of(spatial), MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                Expression value = new SlotReference("value", type);
                for (CollectSet aggregate : List.of(new CollectSet(value),
                        new CollectSet(value, new IntegerLiteral(2)))) {
                    for (BoundFunction entry : List.of(aggregate, StateCombinator.create(aggregate),
                            new CombineCombinator(aggregate.getArguments(), aggregate))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals("collect_set does not support spatial arguments", error.getMessage());
                    }
                }
            }
            String source = " from (select cast(null as " + spatial.toSql() + ") shape) shapes";
            for (String function : List.of("collect_set", "collect_set_state")) {
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> PlanChecker.from(MemoTestUtils.createConnectContext())
                                .analyze("select " + function + "(array(shape))" + source).rewrite());
                Assertions.assertTrue(error.getMessage().contains("collect_set does not support spatial arguments"),
                        error.getMessage());
            }
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
    public void mapAggRejectsNestedSpatialKeysButKeepsValues() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nested : List.of(ArrayType.of(spatial), MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                for (Expression value : List.of(new SlotReference("shape", nested, false),
                        new SlotReference("shape", nested, true), new NullLiteral(nested))) {
                    MapAggV2 rejected = new MapAggV2(value, integer);
                    for (BoundFunction entry : List.of(rejected, StateCombinator.create(rejected),
                            new CombineCombinator(rejected.getArguments(), rejected))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals("map_agg does not support spatial keys", error.getMessage());
                    }
                    MapAggV2 accepted = new MapAggV2(integer, value);
                    for (BoundFunction entry : List.of(accepted, StateCombinator.create(accepted),
                            new CombineCombinator(accepted.getArguments(), accepted))) {
                        Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                    }
                    Assertions.assertEquals(nested, ((MapType) accepted.getDataType()).getValueType());
                }
            }
        }
    }

    @Test
    public void nestedSpatialMapKeysAndNdvRejectThroughSqlRewrite() {
        for (String spatial : List.of("GEOMETRY('EPSG:3857')", "GEOGRAPHY")) {
            for (String nested : List.of("ARRAY<" + spatial + ">", "MAP<INT, " + spatial + ">",
                    "STRUCT<shape:" + spatial + ">")) {
                String source = " from (select CAST(NULL AS " + nested
                        + ") shape from numbers('number'='3')) shapes";
                for (String sql : List.of("select map_agg_v2(shape, 1)" + source,
                        "select map_agg_v2_state(shape, 1)" + source,
                        "select CAST(NULL AS AGG_STATE<map_agg_v2(" + nested + ", INT)>)")) {
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
                    Assertions.assertTrue(error.getMessage().contains("map_agg does not support spatial keys"),
                            error.getMessage());
                }
                for (String sql : List.of("select ndv(shape)" + source, "select ndv_state(shape)" + source,
                        "select CAST(NULL AS AGG_STATE<ndv(" + nested + ")>)")) {
                    Exception error = Assertions.assertThrows(Exception.class,
                            () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite(), sql);
                    Assertions.assertTrue(error.getMessage().contains(Type.OnlyMetricTypeErrorMsg),
                            error.getMessage());
                }
                Assertions.assertDoesNotThrow(() -> PlanChecker.from(MemoTestUtils.createConnectContext())
                        .analyze("select map_agg_v2(1, shape)" + source).rewrite());
            }
        }
    }

    @Test
    public void singleValueSpatialAndOrdinaryAggregatesRemainAdmitted() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"), IntegerType.INSTANCE)) {
            Expression value = new SlotReference("value", type);
            AnyValue function = new AnyValue(value);
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertEquals(type, function.getDataType());
        }
        Expression integer = new SlotReference("value", IntegerType.INSTANCE);
        for (BoundFunction function : List.of(new Min(integer), new Max(integer),
                new Ndv(integer), new Histogram(integer), new CollectSet(integer))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        }
    }
}
