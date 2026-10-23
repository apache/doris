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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.connector.spi.write.ConnectorChangelogMode;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.ConnectorChangelogRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ConnectorChangelogPlanBuilderTest {
    private static final ConnectorChangelogMode MODE =
            new ConnectorChangelogMode("connector_operation", (byte) 4, (byte) 6, (byte) 8);
    private static final List<Column> SCHEMA = ImmutableList.of(
            new Column("id", ScalarType.createType(PrimitiveType.INT)),
            new Column("value", ScalarType.createType(PrimitiveType.INT)));
    private static final ExternalTable TARGET_TABLE = Mockito.mock(ExternalTable.class);

    @Test
    void updateUsesConnectorOwnedOperationEncoding() {
        LogicalPlan child = targetRow();
        CascadesContext context = context(child);
        ConnectorChangelogRowChangeSpec.Update spec = new ConnectorChangelogRowChangeSpec.Update(
                ImmutableList.of("target"), ImmutableList.of(
                        new EqualTo(new UnboundSlot("value"), new IntegerLiteral(99))));

        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(ImmutableList.of("connector_operation", "id", "value"),
                result.getOutput().stream().map(NamedExpression::getName)
                        .collect(ImmutableList.toImmutableList()));
        Assertions.assertTrue(((LogicalProject<?>) result).getProjects().get(0).toSql().contains("6"));
        Assertions.assertTrue(((LogicalProject<?>) result).getProjects().get(2).toSql().contains("99"));
    }

    @Test
    void updateRejectsPrimaryKeyAssignment() {
        LogicalPlan child = targetRow();
        ConnectorChangelogRowChangeSpec.Update spec = new ConnectorChangelogRowChangeSpec.Update(
                ImmutableList.of("target"), ImmutableList.of(
                        new EqualTo(new UnboundSlot("ID"), new IntegerLiteral(2))));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorChangelogPlanBuilder.build(
                        SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context(child)));

        Assertions.assertTrue(exception.getMessage().contains("cannot modify primary-key column 'ID'"));
    }

    @Test
    void matchedMergeUpdateRejectsPrimaryKeyAssignment() {
        LogicalPlan child = targetRow();
        MergeMatchedClause clause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot("id"), new IntegerLiteral(2))), false);
        ConnectorChangelogRowChangeSpec.Merge spec = new ConnectorChangelogRowChangeSpec.Merge(
                ImmutableList.of("target"), ImmutableList.of(clause), ImmutableList.of());

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorChangelogPlanBuilder.build(
                        SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context(child)));

        Assertions.assertTrue(exception.getMessage().contains("cannot modify primary-key column 'id'"));
    }

    @Test
    void deleteUsingDeduplicatesByConnectorPrimaryKey() {
        LogicalPlan child = targetRow();
        CascadesContext context = context(child);
        ConnectorChangelogRowChangeSpec.Delete spec = new ConnectorChangelogRowChangeSpec.Delete(
                ImmutableList.of("target"), true);

        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context);

        Assertions.assertInstanceOf(LogicalAggregate.class, result);
        Assertions.assertEquals(ImmutableList.of("connector_operation", "id", "value"),
                result.getOutput().stream().map(NamedExpression::getName)
                        .collect(ImmutableList.toImmutableList()));
    }

    @Test
    void updateResolvesDefaultFromPinnedWriteSchema() {
        List<Column> writeSchema = schemaWithValueDefault("42");
        LogicalPlan child = targetRow();
        CascadesContext context = context(child);
        ConnectorChangelogRowChangeSpec.Update spec = new ConnectorChangelogRowChangeSpec.Update(
                ImmutableList.of("target"), ImmutableList.of(new EqualTo(
                        new UnboundSlot("value"),
                        new Default(new UnboundSlot("target", "value")))));

        LogicalProject<?> result = (LogicalProject<?>) ConnectorChangelogPlanBuilder.build(
                writeSchema, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context);

        Assertions.assertTrue(result.getProjects().get(2).toSql().contains("42"));
        Assertions.assertFalse(result.getProjects().get(2).toSql().contains("DEFAULT"));
    }

    @Test
    void matchedMergeUpdateResolvesDefaultFromPinnedWriteSchema() {
        List<Column> writeSchema = schemaWithValueDefault("42");
        LogicalPlan child = targetRow();
        CascadesContext context = context(child);
        MergeMatchedClause clause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot("value"),
                        new Default(new UnboundSlot("target", "value")))), false);
        ConnectorChangelogRowChangeSpec.Merge spec = new ConnectorChangelogRowChangeSpec.Merge(
                ImmutableList.of("target"), ImmutableList.of(clause), ImmutableList.of());

        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                writeSchema, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context);
        String projections = result.<LogicalProject<?>>collectToList(LogicalProject.class::isInstance)
                .stream().flatMap(project -> project.getProjects().stream())
                .map(NamedExpression::toSql).collect(java.util.stream.Collectors.joining(" "));

        Assertions.assertTrue(projections.contains("42"));
        Assertions.assertFalse(projections.contains("DEFAULT"));
    }

    @Test
    void spatialUpdateAndMergeRejectCrsChangeBeforeCoercion() {
        ScalarType geometry = ScalarType.createGeometryType("EPSG:4326");
        List<Column> schema = ImmutableList.of(SCHEMA.get(0), new Column("value", geometry));
        LogicalPlan child = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(
                new SlotReference("id", IntegerType.INSTANCE, false, ImmutableList.of("target")),
                new SlotReference("value", DataType.fromCatalogType(geometry), true,
                        ImmutableList.of("target")),
                new SlotReference("shape", DataType.fromCatalogType(
                        ScalarType.createGeometryType("EPSG:3857")), true, ImmutableList.of("source"))));
        EqualTo assignment = new EqualTo(new UnboundSlot("value"), new UnboundSlot("source", "shape"));
        ConnectorChangelogRowChangeSpec.Update update = new ConnectorChangelogRowChangeSpec.Update(
                ImmutableList.of("target"), ImmutableList.of(assignment));
        ConnectorChangelogRowChangeSpec.Merge merge = new ConnectorChangelogRowChangeSpec.Merge(
                ImmutableList.of("target"), ImmutableList.of(new MergeMatchedClause(
                        Optional.empty(), ImmutableList.of(assignment), false)), ImmutableList.of());
        for (ConnectorChangelogRowChangeSpec spec : ImmutableList.of(update, merge)) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> ConnectorChangelogPlanBuilder.build(schema, TARGET_TABLE,
                            ImmutableList.of("id"), MODE, spec, child, context(child)));
            Assertions.assertTrue(exception.getMessage().contains("CRS mismatch"));
        }
    }

    @Test
    void matchedMergeAcceptsNullPrimaryKeyForUpdateAndDelete() {
        for (boolean delete : new boolean[] {false, true}) {
            LogicalPlan child = targetRow(true);
            MergeMatchedClause clause = new MergeMatchedClause(Optional.empty(), delete ? ImmutableList.of()
                    : ImmutableList.of(new EqualTo(new UnboundSlot("value"), new IntegerLiteral(99))), delete);
            ConnectorChangelogRowChangeSpec.Merge spec = new ConnectorChangelogRowChangeSpec.Merge(
                    ImmutableList.of("target"), ImmutableList.of(clause), ImmutableList.of());
            LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                    SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context(child));
            Map<Expression, Expression> nullRow = new HashMap<>();
            child.getOutput().forEach(slot -> nullRow.put(slot, new NullLiteral(slot.getDataType())));

            Assertions.assertEquals(new IntegerLiteral(0), FoldConstantRuleOnFE.evaluateWithoutContext(
                    ExpressionUtils.replace(branchLabel(result), nullRow)));
        }
    }

    @Test
    void mergeDistinguishesNullTargetValuesFromNullExtendedRows() {
        LogicalPlan target = targetRow(true);
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(2), ImmutableList.of(
                new SlotReference("id", IntegerType.INSTANCE, true, ImmutableList.of("source")),
                new SlotReference("__DORIS_CHANGELOG_TARGET_PRESENT__", IntegerType.INSTANCE, true,
                        ImmutableList.of("source"))));
        LogicalPlan child = MergeUtils.buildMergeJoin(target, source,
                new EqualTo(target.getOutput().get(0), source.getOutput().get(0)), true);
        MergeMatchedClause update = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot("value"),
                        new UnboundSlot("__DORIS_CHANGELOG_TARGET_PRESENT__"))), false);
        MergeNotMatchedClause insert = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.copyOf(source.getOutput()));
        ConnectorChangelogRowChangeSpec.Merge spec = new ConnectorChangelogRowChangeSpec.Merge(
                ImmutableList.of("target"), ImmutableList.of(update), ImmutableList.of(insert));
        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                SCHEMA, TARGET_TABLE, ImmutableList.of("id"), MODE, spec, child, context(child));
        LogicalJoin<?, ?> join = result.<LogicalJoin<?, ?>>collectToList(LogicalJoin.class::isInstance).get(0);
        Slot marker = join.getOutput().get(target.getOutput().size());
        Assertions.assertTrue(marker.nullable(), "the marker must inherit outer-join null-extension");
        Map<Expression, Expression> nullRow = new HashMap<>();
        join.getOutput().forEach(slot -> nullRow.put(slot, new NullLiteral(slot.getDataType())));
        Expression label = branchLabel(result);

        Assertions.assertEquals(new IntegerLiteral(1), FoldConstantRuleOnFE.evaluateWithoutContext(
                ExpressionUtils.replace(label, nullRow)));
        nullRow.put(marker, new TinyIntLiteral((byte) 1));
        Assertions.assertEquals(new IntegerLiteral(0), FoldConstantRuleOnFE.evaluateWithoutContext(
                ExpressionUtils.replace(label, nullRow)));
        // A source column sharing the private marker's name must still supply the UPDATE value.
        nullRow.put(source.getOutput().get(1), new IntegerLiteral(555));
        Expression value = result.<LogicalProject<?>>collectToList(LogicalProject.class::isInstance).stream()
                .flatMap(project -> project.getProjects().stream())
                .filter(project -> project instanceof Alias && project.getName().equals("value"))
                .findFirst().get().child(0);
        value.getInputSlots().stream().filter(slot -> slot.getName().equals("__DORIS_CHANGELOG_BRANCH__"))
                .forEach(slot -> nullRow.put(slot, new IntegerLiteral(0)));
        Assertions.assertEquals(new IntegerLiteral(555), FoldConstantRuleOnFE.evaluateWithoutContext(
                ExpressionUtils.replace(value, nullRow)));
    }

    private Expression branchLabel(LogicalPlan plan) {
        return plan.<LogicalProject<?>>collectToList(LogicalProject.class::isInstance).stream()
                .flatMap(project -> project.getProjects().stream())
                .filter(project -> project.getName().equals("__DORIS_CHANGELOG_BRANCH__"))
                .findFirst().get().child(0);
    }

    private List<Column> schemaWithValueDefault(String defaultValue) {
        return ImmutableList.of(
                new Column("id", ScalarType.createType(PrimitiveType.INT)),
                new Column("value", ScalarType.createType(PrimitiveType.INT),
                        false, null, defaultValue, ""));
    }

    private LogicalPlan targetRow() {
        return targetRow(false);
    }

    private LogicalPlan targetRow(boolean nullableKey) {
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE, nullableKey,
                ImmutableList.of("target"));
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE, true,
                ImmutableList.of("target"));
        return new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(id, value));
    }

    private CascadesContext context(LogicalPlan child) {
        CascadesContext context = MemoTestUtils.createCascadesContext(child);
        context.getConnectContext().setDatabase("test");
        return context;
    }
}
