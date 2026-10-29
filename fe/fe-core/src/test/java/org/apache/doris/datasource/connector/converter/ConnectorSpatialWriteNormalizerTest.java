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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.ProjectMergeable;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

public class ConnectorSpatialWriteNormalizerTest {
    @Test
    public void testRepeatedNullOutputsPreserveOrdinarySinkCastAfterMerge() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        Type geography = ScalarType.createGeographyType("OGC:CRS84", "spherical");
        LogicalPlan seed = row(new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE));
        LogicalPlan source = new LogicalProject<>(ImmutableList.of(seed.getOutput().get(0),
                seed.getOutput().get(0), seed.getOutput().get(0)), seed);
        LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(ImmutableList.of(
                new Column("n", Type.BIGINT), new Column("geom", geometry),
                new Column("geog", geography)), source, new CTEContext());
        Assertions.assertSame(source.getOutput().get(0), normalized.getOutput().get(0));
        Assertions.assertNotEquals(normalized.getOutput().get(0).getExprId(),
                normalized.getOutput().get(1).getExprId());
        Assertions.assertNotEquals(normalized.getOutput().get(0).getExprId(),
                normalized.getOutput().get(2).getExprId());
        Assertions.assertNotEquals(normalized.getOutput().get(1).getExprId(),
                normalized.getOutput().get(2).getExprId());
        LogicalProject<?> sink = new LogicalProject<>(ImmutableList.of(new Alias(new Cast(
                normalized.getOutput().get(0), BigIntType.INSTANCE), "n"),
                normalized.getOutput().get(1), normalized.getOutput().get(2)), normalized);
        Plan merged = MergeProjectable.mergeProjectable(sink);
        Expression ordinary = ((ProjectMergeable) merged).getProjects().get(0).child(0);
        Assertions.assertInstanceOf(Cast.class, ordinary);
        Assertions.assertEquals(BigIntType.INSTANCE, ordinary.getDataType());
        Assertions.assertEquals(IntegerType.INSTANCE, ordinary.child(0).getDataType());
        Assertions.assertEquals(DataType.fromCatalogType(geometry), normalized.getOutput().get(1).getDataType());
        Assertions.assertEquals(DataType.fromCatalogType(geography), normalized.getOutput().get(2).getDataType());
    }

    @Test
    public void testSharedCteExpressionsAreNotExpanded() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        LogicalPlan source = row(new SlotReference("input", DataType.fromCatalogType(geometry)));
        CTEContext context = new CTEContext();
        for (int i = 1; i <= 30; i++) {
            LogicalSubQueryAlias<Plan> producer = new LogicalSubQueryAlias<>("source" + i,
                    new LogicalProject<>(ImmutableList.of(new Alias(new If(
                            new SlotReference("condition", BooleanType.INSTANCE),
                            source.getOutput().get(0), source.getOutput().get(0)), "shape")), source));
            CTEId id = new CTEId(i);
            context = new CTEContext(id, producer, context);
            context.setAnalyzedPlan(producer);
            source = new LogicalCTEConsumer(new RelationId(i + 1), id, "source" + i, producer);
        }
        LogicalPlan finalSource = source;
        CTEContext finalContext = context;
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> Assertions.assertSame(finalSource,
                ConnectorSpatialWriteNormalizer.normalize(ImmutableList.of(new Column("shape", geometry)),
                        finalSource, finalContext)));
    }

    private LogicalOneRowRelation row(Expression value) {
        return new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(new Alias(value, "shape")));
    }

    private void assertNullProjection(Type target, LogicalPlan source, CTEContext context) {
        LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(
                ImmutableList.of(new Column("shape", target)), source, context);
        Assertions.assertInstanceOf(LogicalProject.class, normalized);
        Assertions.assertSame(source, normalized.child(0));
        Assertions.assertNotEquals(source.getOutput().get(0).getExprId(), normalized.getOutput().get(0).getExprId());
        Assertions.assertEquals(DataType.fromCatalogType(target), normalized.getOutput().get(0).getDataType());
        Expression value = ((LogicalProject<?>) normalized).getProjects().get(0).child(0);
        Assertions.assertTrue(value.isNullLiteral());
    }

    @Test
    public void testSelectAndProjectedTypedNulls() {
        for (Type target : ImmutableList.of(ScalarType.createGeometryType("OGC:CRS84"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"))) {
            for (DataType sourceType : ImmutableList.of(IntegerType.INSTANCE,
                    DataType.fromCatalogType(ScalarType.createGeometryType("EPSG:3857")),
                    DataType.fromCatalogType(ScalarType.createGeographyType("OGC:CRS84", "vincenty")))) {
                LogicalPlan source = row(new Cast(NullLiteral.INSTANCE, sourceType));
                assertNullProjection(target, source, new CTEContext());
                LogicalPlan project = new LogicalProject<>(ImmutableList.of(
                        new Alias(source.getOutput().get(0), "renamed")), source);
                assertNullProjection(target, project, new CTEContext());
            }
        }
    }

    @Test
    public void testTypedNullCteProvenanceBeforeAndAfterAnchoring() {
        Type target = ScalarType.createGeometryType("OGC:CRS84");
        LogicalSubQueryAlias<Plan> producer = new LogicalSubQueryAlias<>("source",
                row(new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE)));
        CTEId id = new CTEId(1);
        CTEContext context = new CTEContext(id, producer, new CTEContext());
        context.setAnalyzedPlan(producer);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(new RelationId(2), id, "source", producer);
        assertNullProjection(target, consumer, context);
        LogicalCTEAnchor<?, ?> anchor = new LogicalCTEAnchor<>(id,
                new LogicalCTEProducer<>(id, producer), consumer);
        assertNullProjection(target, anchor, new CTEContext());
    }

    @Test
    public void testNestedNullConstructorsRetainOrdinaryMapKey() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        Expression typedNull = new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE);
        List<Type> targets = ImmutableList.of(new ArrayType(geometry), new MapType(Type.STRING, geometry),
                new StructType(new StructField("geom", geometry)));
        List<Expression> values = ImmutableList.of(new Array(typedNull),
                new CreateMap(new StringLiteral("ordinary-key"), typedNull),
                new CreateNamedStruct(new StringLiteral("geom"), typedNull));
        for (int i = 0; i < targets.size(); i++) {
            LogicalPlan source = row(values.get(i));
            LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(
                    ImmutableList.of(new Column("shape", targets.get(i))), source, new CTEContext());
            Expression result = ((LogicalProject<?>) normalized).getProjects().get(0).child(0);
            int leaf = result instanceof Array ? 0 : 1;
            Assertions.assertTrue(result.child(leaf).isNullLiteral());
            Assertions.assertEquals(DataType.fromCatalogType(geometry), result.child(leaf).getDataType());
            if (result instanceof CreateMap) {
                Assertions.assertEquals(new StringLiteral("ordinary-key"), result.child(0));
            }
        }
    }

    @Test
    public void testCastNullArrayProvenanceAtBoundWriteSink() {
        DataType integers = org.apache.doris.nereids.types.ArrayType.of(IntegerType.INSTANCE);
        for (Type spatial : ImmutableList.of(ScalarType.createGeometryType("OGC:CRS84"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"))) {
            Type target = new ArrayType(spatial);
            for (Expression array : ImmutableList.of(new Array(new NullLiteral(IntegerType.INSTANCE)),
                    new ArrayLiteral(ImmutableList.of(new NullLiteral(IntegerType.INSTANCE))))) {
                Cast value = new Cast(array, integers, true, true);
                LogicalPlan source = row(value);
                LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(
                        ImmutableList.of(new Column("shape", target)), source, new CTEContext());
                Expression result = ((LogicalProject<?>) normalized).getProjects().get(0).child(0);
                Assertions.assertInstanceOf(Cast.class, result);
                Assertions.assertEquals(DataType.fromCatalogType(target), result.getDataType());
                Assertions.assertFalse(result.isNullLiteral());
                Assertions.assertTrue(((Cast) result).isExplicitType());
                Assertions.assertTrue(((Cast) result).isStrict());
                Assertions.assertSame(source, normalized.child(0));
            }
        }
    }

    @Test
    public void testCastNullMapAndStructProvenanceAtBoundWriteSink() {
        for (Type spatial : ImmutableList.of(ScalarType.createGeometryType("OGC:CRS84"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"))) {
            Expression typedNull = new NullLiteral(IntegerType.INSTANCE);
            List<Expression> values = ImmutableList.of(new CreateMap(
                    new Cast(new StringLiteral("ordinary-key"), StringType.INSTANCE, true, true), typedNull),
                    new CreateNamedStruct(new StringLiteral("shape"), typedNull,
                            new StringLiteral("label"),
                            new Cast(new StringLiteral("ordinary"), StringType.INSTANCE, true, true)));
            List<Type> targets = ImmutableList.of(new MapType(Type.STRING, spatial),
                    new StructType(new StructField("shape", spatial), new StructField("label", Type.STRING)));
            List<Type> sources = ImmutableList.of(new MapType(Type.STRING, Type.INT),
                    new StructType(new StructField("shape", Type.INT), new StructField("label", Type.STRING)));
            for (int i = 0; i < values.size(); i++) {
                DataType sourceType = DataType.fromCatalogType(sources.get(i));
                Cast value = new Cast(new Cast(values.get(i), sourceType, true, true), sourceType, true, true);
                LogicalPlan source = row(value);
                LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(
                        ImmutableList.of(new Column("shape", targets.get(i))), source, new CTEContext());
                Expression result = ((LogicalProject<?>) normalized).getProjects().get(0).child(0);
                Assertions.assertEquals(DataType.fromCatalogType(targets.get(i)), result.getDataType());
                Assertions.assertInstanceOf(Cast.class, result);
                Assertions.assertTrue(((Cast) result).isExplicitType());
                Assertions.assertTrue(((Cast) result).isStrict());
                String ordinary = i == 0 ? "ordinary-key" : "ordinary";
                Assertions.assertTrue(result.anyMatch(child -> child instanceof StringLiteral
                        && ((StringLiteral) child).getValue().equals(ordinary)));
                Assertions.assertSame(source, normalized.child(0));
            }
        }
    }

    @Test
    public void testCastNullContainerLiteralsKeepOrdinaryCastTargets() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        DataType map = DataType.fromCatalogType(new MapType(Type.TINYINT, Type.INT));
        Expression value = new Cast(new MapLiteral(ImmutableMap.of(new StringLiteral("7"),
                new NullLiteral(IntegerType.INSTANCE))), map, true, true);
        LogicalPlan source = row(value);
        LogicalPlan normalized = ConnectorSpatialWriteNormalizer.normalize(ImmutableList.of(
                new Column("shape", new MapType(Type.STRING, geometry))), source, new CTEContext());
        Expression result = ((LogicalProject<?>) normalized).getProjects().get(0).child(0);
        Assertions.assertEquals(DataType.fromCatalogType(new MapType(Type.TINYINT, geometry)), result.getDataType());
        Assertions.assertTrue(((Cast) result).isStrict());

        Type struct = new StructType(new StructField("shape", Type.INT), new StructField("n", Type.TINYINT));
        Expression fields = new StructLiteral(ImmutableList.of(new NullLiteral(IntegerType.INSTANCE),
                new IntegerLiteral(7)), DataType.fromCatalogType(struct));
        Type target = new StructType(new StructField("shape", geometry), new StructField("n", Type.BIGINT));
        LogicalPlan structSource = row(new Cast(fields, DataType.fromCatalogType(struct), true, true));
        LogicalPlan structResult = ConnectorSpatialWriteNormalizer.normalize(ImmutableList.of(
                new Column("shape", target)), structSource, new CTEContext());
        Expression output = ((LogicalProject<?>) structResult).getProjects().get(0).child(0);
        Type patched = new StructType(new StructField("shape", geometry), new StructField("n", Type.TINYINT));
        Assertions.assertEquals(DataType.fromCatalogType(patched), output.getDataType());
    }

    @Test
    public void testContainerCastsCannotHideValuesOrIncompatibleShapes() {
        DataType map = DataType.fromCatalogType(new MapType(Type.STRING, Type.INT));
        Type target = new MapType(Type.STRING, ScalarType.createGeometryType("OGC:CRS84"));
        Expression nullMap = new CreateMap(new StringLiteral("ordinary-key"), new NullLiteral(IntegerType.INSTANCE));
        for (Expression value : ImmutableList.of(new Cast(new CreateMap(new StringLiteral("ordinary-key"),
                new IntegerLiteral(1)), map), new Cast(new Cast(nullMap, StringType.INSTANCE), map),
                new Cast(new Array(new NullLiteral(IntegerType.INSTANCE)), map))) {
            Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteNormalizer.normalize(
                    ImmutableList.of(new Column("shape", target)), row(value), new CTEContext()));
        }
    }

    @Test
    public void testCastArraysCannotHideNonNullValuesOrInvalidDimensions() {
        DataType integers = org.apache.doris.nereids.types.ArrayType.of(IntegerType.INSTANCE);
        Type target = new ArrayType(ScalarType.createGeometryType("OGC:CRS84"));
        for (Expression child : ImmutableList.of(new Array(new IntegerLiteral(1)),
                new Array(new Array(NullLiteral.INSTANCE)))) {
            LogicalPlan source = row(new Cast(child, integers));
            Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteNormalizer.normalize(
                    ImmutableList.of(new Column("shape", target)), source, new CTEContext()));
        }
    }

    @Test
    public void testAnalyzerCteWithNestedProducer() {
        Type target = ScalarType.createGeometryType("OGC:CRS84");
        LogicalPlan inner = row(new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE));
        CTEId innerId = new CTEId(1);
        LogicalCTEConsumer innerConsumer = new LogicalCTEConsumer(new RelationId(2), innerId, "inner", inner);
        LogicalCTEAnchor<?, ?> innerAnchor = new LogicalCTEAnchor<>(innerId,
                new LogicalCTEProducer<>(innerId, inner), innerConsumer);
        LogicalSubQueryAlias<Plan> outer = new LogicalSubQueryAlias<>("outer", innerAnchor);
        CTEId outerId = new CTEId(2);
        CTEContext context = new CTEContext(outerId, outer, new CTEContext());
        context.setAnalyzedPlan(outer);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(new RelationId(3), outerId, "outer", outer);
        assertNullProjection(target, consumer, context);
    }

    @Test
    public void testProjectionCannotHideNonNullIdentityMismatch() {
        Type target = ScalarType.createGeometryType("OGC:CRS84");
        for (Type wrong : ImmutableList.of(ScalarType.createGeometryType("EPSG:3857"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"), Type.INT)) {
            SlotReference input = new SlotReference("input", DataType.fromCatalogType(wrong));
            LogicalPlan source = row(input);
            LogicalPlan project = new LogicalProject<>(ImmutableList.of(new Alias(
                    new Cast(source.getOutput().get(0), DataType.fromCatalogType(target)), "shape")), source);
            Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteNormalizer.normalize(
                    ImmutableList.of(new Column("shape", target)), project, new CTEContext()));
        }
    }

    @Test
    public void testNonconstantOrdinaryKeyAndSpatialValueRemainInProducer() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        LogicalPlan source = row(new CreateMap(new SlotReference("key", StringType.INSTANCE),
                new SlotReference("value", DataType.fromCatalogType(geometry))));
        Assertions.assertSame(source, ConnectorSpatialWriteNormalizer.normalize(
                ImmutableList.of(new Column("shape", new MapType(Type.STRING, geometry))), source, new CTEContext()));
    }

    @Test
    public void testOuterJoinNullPaddingIsNotReplacedByAContainer() {
        Type geometry = ScalarType.createGeometryType("OGC:CRS84");
        LogicalPlan left = row(new StringLiteral("left"));
        LogicalPlan right = row(new Array(new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE)));
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN, left, right, new JoinReorderContext());
        LogicalPlan project = new LogicalProject<>(ImmutableList.of(join.getOutput().get(1)), join);
        Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteNormalizer.normalize(
                ImmutableList.of(new Column("shape", new ArrayType(geometry))), project, new CTEContext()));
    }
}
