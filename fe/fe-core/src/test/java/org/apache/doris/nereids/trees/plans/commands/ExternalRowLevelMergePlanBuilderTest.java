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
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ShortCircuitIf;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarBinaryType;
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
