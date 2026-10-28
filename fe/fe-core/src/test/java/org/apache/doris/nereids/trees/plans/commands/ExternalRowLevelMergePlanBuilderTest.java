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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.connector.converter.ConnectorSpatialWriteValidator;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

/**
 * Origin of the SQL MERGE cardinality requirement (upstream #66112).
 *
 * <p>{@code MERGE INTO} and {@code UPDATE} synthesize the SAME sink
 * ({@link LogicalExternalRowLevelMergeSink}), so the statement kind must be stamped onto it here — the plan
 * builder is the last place that still knows it. The counterpart assertion (UPDATE stamps {@code false}) lives
 * in {@code ExternalRowLevelUpdatePlanBuilderTest}.
 */
public class ExternalRowLevelMergePlanBuilderTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void mergeSinkRequestsTheSqlMergeCardinalityCheck() {
        ConnectContext ctx = new ConnectContext();
        // Writer schemas are pinned per statement in production; the focused builder test needs the same scope.
        ctx.setStatementContext(new StatementContext());
        ctx.setThreadLocalInfo();
        LogicalPlan source = new LogicalOneRowRelation(new RelationId(0),
                ImmutableList.of(new UnboundAlias(new IntegerLiteral(1), "dummy")));
        ExternalRowLevelMergePlanBuilder builder = new ExternalRowLevelMergePlanBuilder(
                ImmutableList.of("test_catalog", "test_db", "test_table"),
                Optional.empty(), Optional.empty(), source, BooleanLiteral.TRUE,
                // one WHEN MATCHED THEN UPDATE SET c1 = 1: a MERGE with no clause at all is not a statement
                // the parser can produce, and the synthesis indexes the clause list.
                ImmutableList.of(new MergeMatchedClause(Optional.empty(),
                        ImmutableList.of(new EqualTo(new UnboundSlot("c1"), new IntegerLiteral(1))), false)),
                ImmutableList.of());

        // The row id is a connector-declared synthetic write column; the merge synthesis refuses to run
        // without it (RowLevelDmlRowIdUtils.getRowIdColumn), so the fake table must publish it.
        Column rowId = new Column(Column.ICEBERG_ROWID_COL, ScalarType.createStringType());
        rowId.setIsVisible(false);
        Column data = new Column("c1", ScalarType.createType(PrimitiveType.INT));
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalTable.WriteSchemaSnapshot writeSchema =
                Mockito.mock(PluginDrivenExternalTable.WriteSchemaSnapshot.class);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.doReturn(Mockito.mock(ExternalDatabase.class)).when(table).getDatabase();
        Mockito.doReturn(ImmutableList.of(data)).when(writeSchema).getBaseSchema();
        Mockito.doReturn("uuid-u0/schema-1").when(writeSchema).getWriteMetadataIdentity();
        Mockito.doReturn(writeSchema).when(table).getWriteSchemaSnapshot();
        Mockito.doReturn(Optional.of(ImmutableList.of(data)))
                .when(table).resolveWriteColumns(Optional.empty());
        Mockito.doReturn(ImmutableList.of(data, rowId)).when(table).getFullSchema();

        LogicalPlan plan = builder.buildMergePlan(ctx, table);

        // WHY: the SQL cardinality rule ("a target row matched by more than one source row is an error") can
        // only be checked by the BE sink, and only if the plan keeps the merge distribution. Both depend on
        // this single literal. MUTATION: passing false here disables BE's duplicate-match validation AND lets
        // enable_strict_consistency_dml=false relax the distribution, so a duplicate-source MERGE silently
        // commits a corrupt result instead of erroring.
        Assertions.assertTrue(plan instanceof LogicalExternalRowLevelMergeSink);
        Assertions.assertTrue(((LogicalExternalRowLevelMergeSink<?>) plan).isRequireMergeCardinalityCheck(),
                "SQL MERGE INTO must request the cardinality validation");
        // The branch projections, sink columns, and conflict fence must come from one generation.
        Assertions.assertEquals("uuid-u0/schema-1",
                ((LogicalExternalRowLevelMergeSink<?>) plan).getBoundWriteMetadataIdentity());
        // The snapshot identity must be read only after resolving the request-scoped writer columns.
        InOrder metadataOrder = Mockito.inOrder(table);
        metadataOrder.verify(table).resolveWriteColumns(Optional.empty());
        metadataOrder.verify(table).getWriteSchemaSnapshot();
    }

    @Test
    public void matchedUpdateKeepsTargetValuesWhileNotMatchedInsertUsesWriteDefaults() {
        // A WHEN MATCHED UPDATE keeps the target row's value for every unassigned column; only the WHEN NOT MATCHED
        // INSERT branch fills an omitted column with its pinned write default.
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setThreadLocalInfo();
        LogicalPlan source = new LogicalOneRowRelation(new RelationId(0),
                ImmutableList.of(new UnboundAlias(new IntegerLiteral(1), "dummy")));
        ExternalRowLevelMergePlanBuilder builder = new ExternalRowLevelMergePlanBuilder(
                ImmutableList.of("test_catalog", "test_db", "test_table"),
                Optional.empty(), Optional.empty(), source, BooleanLiteral.TRUE,
                ImmutableList.of(new MergeMatchedClause(Optional.empty(),
                        ImmutableList.of(new EqualTo(new UnboundSlot("c1"), new IntegerLiteral(1))), false)),
                ImmutableList.of(new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1"),
                        ImmutableList.of(new UnboundAlias(new IntegerLiteral(5))))));
        Column rowId = new Column(Column.ICEBERG_ROWID_COL, ScalarType.createStringType());
        rowId.setIsVisible(false);
        Column assigned = new Column("c1", ScalarType.createType(PrimitiveType.INT));
        assigned.setConnectorDefaultValueSql("4141");
        Column omitted = new Column("c2", ScalarType.createType(PrimitiveType.INT));
        omitted.setConnectorDefaultValueSql("4242");
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalTable.WriteSchemaSnapshot writeSchema =
                Mockito.mock(PluginDrivenExternalTable.WriteSchemaSnapshot.class);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.doReturn(Mockito.mock(ExternalDatabase.class)).when(table).getDatabase();
        Mockito.doReturn(writeSchema).when(table).getWriteSchemaSnapshot();
        Mockito.doReturn(Optional.of(ImmutableList.of(assigned, omitted)))
                .when(table).resolveWriteColumns(Optional.empty());
        Mockito.doReturn(ImmutableList.of(assigned, omitted, rowId)).when(table).getFullSchema();

        LogicalPlan plan = builder.buildMergePlan(ctx, table);

        Expression c2 = ((LogicalProject<?>) plan.child(0)).getProjects().stream()
                .filter(project -> project instanceof UnboundAlias
                        && ((UnboundAlias) project).getAlias().equals(Optional.of("c2")))
                .findFirst().orElseThrow(AssertionError::new).child(0);
        // The outermost selection picks the NOT MATCHED insert branch; its else-branch selects the MATCHED update.
        Expression insertBranch = c2.child(1);
        Expression updateBranch = c2.child(2).child(1);
        Assertions.assertTrue(insertBranch.anyMatch(expression -> isLiteral(expression, "4242")), c2.toString());
        Assertions.assertTrue(updateBranch.anyMatch(expression -> expression instanceof UnboundSlot
                && "c2".equalsIgnoreCase(((UnboundSlot) expression).getNameParts().get(
                        ((UnboundSlot) expression).getNameParts().size() - 1))), c2.toString());
        Assertions.assertFalse(updateBranch.anyMatch(expression -> isLiteral(expression, "4242")), c2.toString());
    }

    private static boolean isLiteral(Object expression, String value) {
        return expression instanceof Literal && value.equals(((Literal) expression).getStringValue());
    }

    @Test
    public void mergeBranchesUseThePinnedWriterType() {
        VarBinaryType uuidType = VarBinaryType.createVarBinaryType(16);
        Cast cachedTargetValue = new Cast(new StringLiteral("target"), uuidType);
        Cast unboundedDefault = new Cast(new StringLiteral("default"), VarBinaryType.MAX_VARBINARY_TYPE);

        List<NamedExpression> projections = ExternalRowLevelMergePlanBuilder.generateFinalProjections(
                ImmutableList.of("uuid_col"), ImmutableList.of(uuidType),
                ImmutableList.of(ImmutableList.of(cachedTargetValue), ImmutableList.of(unboundedDefault)));

        Assertions.assertEquals(uuidType, projections.get(0).child(0).getDataType(),
                "MERGE branch selection must be analyzable even when an expression starts with a wider type");
        Assertions.assertInstanceOf(ShortCircuitIf.class, projections.get(0).child(0),
                "Iceberg MERGE must evaluate only the selected branch regardless of the session setting");
    }

    @Test
    public void mergeBranchesNormalizeSpatialTypedNullsBeforeTargetCasts() {
        for (DataType target : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nullType : ImmutableList.of(IntegerType.INSTANCE, new GeometryType("EPSG:3857"),
                    new GeographyType("OGC:CRS84", "vincenty"))) {
                Expression value = new Cast(NullLiteral.INSTANCE, nullType);
                List<NamedExpression> projections = ExternalRowLevelMergePlanBuilder.generateFinalProjections(
                        ImmutableList.of("shape"), ImmutableList.of(target),
                        ImmutableList.of(ImmutableList.of(value),
                                ImmutableList.of(new Cast(value, nullType))));
                Expression selection = projections.get(0).child(0);
                Assertions.assertTrue(selection.child(1).isNullLiteral());
                Assertions.assertEquals(target, selection.child(1).getDataType());
                Assertions.assertTrue(selection.child(2).child(1).isNullLiteral());
                Assertions.assertEquals(target, selection.child(2).child(1).getDataType());
            }
        }
    }

    @Test
    public void mergeBranchesKeepNonNullSpatialCastRejections() {
        for (DataType target : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> ExternalRowLevelMergePlanBuilder.generateFinalProjections(
                            ImmutableList.of("shape"), ImmutableList.of(target),
                            ImmutableList.of(ImmutableList.of(new Cast(new IntegerLiteral(1),
                                    IntegerType.INSTANCE)))));
        }
    }

    @Test
    public void mergeBranchesBindNestedTypedNullConstructors() {
        ExpressionAnalyzer analyzer = mergeExpressionAnalyzer();
        NereidsParser parser = new NereidsParser();
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            StructType struct = new StructType(ImmutableList.of(
                    new StructField("shape", spatial, true, ""),
                    new StructField("label", StringType.INSTANCE, true, ""),
                    new StructField("n", BigIntType.INSTANCE, true, "")));
            List<DataType> targets = ImmutableList.of(ArrayType.of(spatial),
                    MapType.of(StringType.INSTANCE, spatial), struct, struct,
                    MapType.of(StringType.INSTANCE, ArrayType.of(struct)));
            List<String> constructors = ImmutableList.of("array(%s)", "map('ordinary-key', %s)",
                    "struct(%s, 'kept', cast(7 as tinyint))",
                    "named_struct('shape', %s, 'label', 'kept', 'n', cast(7 as tinyint))",
                    "map('ordinary-key', array(named_struct('shape', %s, 'label', 'kept', 'n', cast(7 as tinyint))))");
            for (int i = 0; i < targets.size(); i++) {
                DataType target = targets.get(i);
                for (String typedNull : ImmutableList.of("cast(null as int)",
                        "cast(null as geometry('EPSG:3857'))")) {
                    String sql = String.format(constructors.get(i), typedNull);
                    Expression branch = mergeBranch(parser.parseExpression(sql), target);
                    Expression analyzed = analyzer.analyze(branch);
                    Assertions.assertEquals(target, analyzed.getDataType(), sql);
                    Assertions.assertFalse(analyzed.isNullLiteral(), sql);
                    Assertions.assertTrue(analyzed.anyMatch(expression -> expression instanceof NullLiteral
                            && ((NullLiteral) expression).getDataType().equals(spatial)), sql);
                    if (i == 1 || i == 4) {
                        Assertions.assertTrue(
                                branch.anyMatch(expression -> isLiteral(expression, "ordinary-key")), sql);
                    }
                    if (i >= 2) {
                        Assertions.assertTrue(branch.anyMatch(expression -> isLiteral(expression, "kept")), sql);
                        Assertions.assertTrue(branch.anyMatch(expression -> expression instanceof Cast
                                && ((Cast) expression).getDataType().equals(TinyIntType.INSTANCE)), sql);
                    }
                }
            }
        }
    }

    @Test
    public void mergeBranchesBindCastWrappedNullArrays() {
        ExpressionAnalyzer analyzer = mergeExpressionAnalyzer();
        NereidsParser parser = new NereidsParser();
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (String sql : ImmutableList.of("cast(array(cast(null as int)) as array<int>)",
                    "cast(cast(array(cast(null as int)) as array<int>) as array<bigint>)")) {
                DataType target = ArrayType.of(spatial);
                Expression analyzed = Assertions.assertDoesNotThrow(
                        () -> analyzer.analyze(mergeBranch(parser.parseExpression(sql), target)), sql);
                Assertions.assertEquals(target, analyzed.getDataType(), sql);
                Assertions.assertFalse(analyzed.isNullLiteral(), sql);
            }
            DataType nested = ArrayType.of(ArrayType.of(spatial));
            Assertions.assertEquals(nested, analyzer.analyze(mergeBranch(parser.parseExpression(
                    "cast(array(array(cast(null as int))) as array<array<int>>)"), nested)).getDataType());
            Expression literal = new Cast(new ArrayLiteral(ImmutableList.of(new NullLiteral(IntegerType.INSTANCE))),
                    ArrayType.of(IntegerType.INSTANCE), true, true);
            Expression branch = mergeBranch(literal, ArrayType.of(spatial));
            Assertions.assertTrue(branch.anyMatch(expression -> expression instanceof Cast
                    && ((Cast) expression).isExplicitType() && ((Cast) expression).isStrict()));
            Assertions.assertEquals(ArrayType.of(spatial), analyzer.analyze(branch).getDataType());
            Expression wrongDepth = new Cast(new ArrayLiteral(ImmutableList.of(
                    new ArrayLiteral(ImmutableList.of(new NullLiteral(IntegerType.INSTANCE))))),
                    ArrayType.of(ArrayType.of(IntegerType.INSTANCE)));
            Assertions.assertThrows(AnalysisException.class,
                    () -> analyzer.analyze(mergeBranch(wrongDepth, ArrayType.of(spatial))));
            for (String invalid : ImmutableList.of(
                    "cast(array(array(cast(null as int))) as array<int>)",
                    "cast(cast(array(array(cast(null as int))) as array<array<int>>) as array<int>)",
                    "cast(cast(array(array(cast(null as int))) as array<int>) as array<array<int>>)")) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> analyzer.analyze(mergeBranch(parser.parseExpression(invalid), nested)), invalid);
            }
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(mergeBranch(
                    parser.parseExpression("cast(array(cast(null as int), 1) as array<int>)"),
                    ArrayType.of(spatial))));
        }
    }

    @Test
    public void mergeNestedNullsKeepOrdinaryKeysAndNonNullSpatialIdentity() {
        ExpressionAnalyzer analyzer = mergeExpressionAnalyzer();
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            Expression typedNull = new Cast(NullLiteral.INSTANCE, IntegerType.INSTANCE);
            SlotReference key = new SlotReference("ordinary_key", StringType.INSTANCE);
            SlotReference value = new SlotReference("spatial_value", spatial);
            DataType map = MapType.of(StringType.INSTANCE, spatial);
            Expression matching = analyzer.analyze(mergeBranch(new UnboundFunction("map", ImmutableList.of(
                    key, typedNull, new StringLiteral("kept"), value)), map));
            Assertions.assertEquals(map, matching.getDataType());
            Assertions.assertTrue(matching.anyMatch(expression -> expression == key));
            Assertions.assertTrue(matching.anyMatch(expression -> expression == value));
            Assertions.assertDoesNotThrow(() -> ConnectorSpatialWriteValidator.validate(
                    new Column("payload", map.toCatalogDataType()), matching));

            DataType array = ArrayType.of(spatial);
            List<DataType> incompatible = spatial instanceof GeometryType
                    ? ImmutableList.of(new GeometryType("EPSG:3857"),
                            new GeographyType("OGC:CRS84", "spherical"))
                    : ImmutableList.of(new GeographyType("EPSG:4326", "spherical"),
                            new GeographyType("OGC:CRS84", "vincenty"), new GeometryType("OGC:CRS84"));
            for (DataType wrongType : incompatible) {
                SlotReference wrong = new SlotReference("original_spatial_value", wrongType);
                // The NULL is safe to retag, but the adjacent explicit cast must not erase source identity.
                Expression mixed = analyzer.analyze(mergeBranch(new UnboundFunction("array", ImmutableList.of(
                        typedNull, new Cast(wrong, spatial))), array));
                Assertions.assertTrue(mixed.anyMatch(expression -> expression == wrong));
                Assertions.assertThrows(AnalysisException.class, () -> ConnectorSpatialWriteValidator.validate(
                        new Column("payload", array.toCatalogDataType()), mixed));
            }
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(mergeBranch(
                    new NereidsParser().parseExpression("array(cast(null as int), cast(1 as int))"), array)));
        }
    }

    @Test
    public void mergeNestedNullsRecomputeCachedConstructorSignatures() {
        ExpressionAnalyzer analyzer = mergeExpressionAnalyzer();
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            Expression bound = analyzer.analyze(new NereidsParser().parseExpression("array(cast(null as int))"));
            Assertions.assertEquals(ArrayType.of(IntegerType.INSTANCE), bound.getDataType());
            DataType target = ArrayType.of(spatial);
            Expression rewritten = MoreFieldsThread.keepFunctionSignature(
                    () -> analyzer.analyze(mergeBranch(bound, target)));
            Assertions.assertEquals(target, rewritten.getDataType());
        }
    }

    @Test
    public void mergeNestedNullsKeepRequiredFieldsAndMalformedConstructorRejections() {
        ExpressionAnalyzer analyzer = mergeExpressionAnalyzer();
        NereidsParser parser = new NereidsParser();
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            StructType required = new StructType(ImmutableList.of(new StructField("shape", spatial, false, "")));
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(mergeBranch(
                    parser.parseExpression("named_struct('shape', cast(null as int))"), required)));
            StructType twoFields = new StructType(ImmutableList.of(
                    new StructField("shape", spatial, true, ""),
                    new StructField("label", StringType.INSTANCE, true, "")));
            for (String malformed : ImmutableList.of("struct(cast(null as int))",
                    "named_struct('shape', cast(null as int), 'label')")) {
                Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(mergeBranch(
                        parser.parseExpression(malformed), twoFields)), malformed);
            }
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(mergeBranch(
                    parser.parseExpression("map('ordinary-key', cast(null as int), 'dangling')"),
                    MapType.of(StringType.INSTANCE, spatial))));
        }
    }

    private static Expression mergeBranch(Expression value, DataType target) {
        return ExternalRowLevelMergePlanBuilder.generateFinalProjections(
                ImmutableList.of("payload"), ImmutableList.of(target),
                ImmutableList.of(ImmutableList.of(value))).get(0).child(0).child(1);
    }

    private static ExpressionAnalyzer mergeExpressionAnalyzer() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setThreadLocalInfo();
        return new ExpressionAnalyzer(null, new Scope(ImmutableList.of()), null, true, true);
    }

    @Test
    public void mergeBranchesAreCastToTheVariantTargetBeforeTheyAreSelected() {
        // Each branch is cast to the Variant target before the branch selection, so selecting among a Variant and a
        // numeric branch never derives a numeric common type that would turn an object or array Variant into NULL.
        SlotReference targetPayload = new SlotReference("target_payload", ConnectorComputeVariantType.INSTANCE);
        List<NamedExpression> projections = ExternalRowLevelMergePlanBuilder.generateFinalProjections(
                ImmutableList.of("payload"), ImmutableList.of(ConnectorComputeVariantType.INSTANCE),
                ImmutableList.of(ImmutableList.of(targetPayload), ImmutableList.of(new IntegerLiteral(1))));

        Expression selection = projections.get(0).child(0);
        Assertions.assertInstanceOf(ShortCircuitIf.class, selection);
        Expression insertedBranch = selection.child(1);
        Assertions.assertInstanceOf(Cast.class, insertedBranch);
        Assertions.assertEquals(ConnectorComputeVariantType.INSTANCE, insertedBranch.getDataType());
        Assertions.assertEquals(IntegerType.INSTANCE, insertedBranch.child(0).getDataType());
        Assertions.assertEquals(ConnectorComputeVariantType.INSTANCE, selection.child(2).child(1).getDataType());
    }
}
