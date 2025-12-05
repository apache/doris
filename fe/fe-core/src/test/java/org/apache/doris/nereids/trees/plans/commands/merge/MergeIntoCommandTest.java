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

package org.apache.doris.nereids.trees.plans.commands.merge;

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class MergeIntoCommandTest {

    @Test
    public void testGenerateBasePlanWithAlias() throws Exception {
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, new NullLiteral(),
                ImmutableList.of(), ImmutableList.of()
        );

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateBasePlan = clazz.getDeclaredMethod("generateBasePlan");
        generateBasePlan.setAccessible(true);
        LogicalPlan result = (LogicalPlan) generateBasePlan.invoke(command);
        Assertions.assertInstanceOf(LogicalJoin.class, result);
        LogicalJoin<?, ?> logicalJoin = (LogicalJoin<?, ?>) result;
        Assertions.assertEquals(1, logicalJoin.getOtherJoinConjuncts().size());
        Expression onClause = logicalJoin.getOtherJoinConjuncts().get(0);
        Assertions.assertEquals(new NullLiteral(), onClause);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());
        Assertions.assertEquals(source, logicalJoin.left());
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, logicalJoin.right());
        LogicalSubQueryAlias<?> alias = (LogicalSubQueryAlias<?>) logicalJoin.right();
        Assertions.assertEquals("alias", alias.getAlias());
    }

    @Test
    public void testGenerateBasePlanWithoutAlias() throws Exception {
        List<String> nameParts = ImmutableList.of("ctl", "db", "tbl");
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        MergeIntoCommand command = new MergeIntoCommand(
                nameParts, Optional.empty(), Optional.empty(),
                source, new NullLiteral(),
                ImmutableList.of(), ImmutableList.of()
        );

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateBasePlan = clazz.getDeclaredMethod("generateBasePlan");
        generateBasePlan.setAccessible(true);
        LogicalPlan result = (LogicalPlan) generateBasePlan.invoke(command);
        Assertions.assertInstanceOf(LogicalJoin.class, result);
        LogicalJoin<?, ?> logicalJoin = (LogicalJoin<?, ?>) result;
        Assertions.assertEquals(1, logicalJoin.getOtherJoinConjuncts().size());
        Expression onClause = logicalJoin.getOtherJoinConjuncts().get(0);
        Assertions.assertEquals(new NullLiteral(), onClause);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, logicalJoin.getJoinType());
        Assertions.assertEquals(source, logicalJoin.left());
        Assertions.assertInstanceOf(LogicalCheckPolicy.class, logicalJoin.right());
        Assertions.assertInstanceOf(UnboundRelation.class, logicalJoin.right().child(0));
        UnboundRelation unboundRelation = (UnboundRelation) logicalJoin.right().child(0);
        Assertions.assertEquals(nameParts, unboundRelation.getNameParts());
    }

    @Test
    public void testGenerateBranchLabel() throws Exception {
        List<MergeMatchedClause> matchedClauses = ImmutableList.of(
                new MergeMatchedClause(Optional.of(new IntegerLiteral(1)), ImmutableList.of(), true),
                new MergeMatchedClause(Optional.of(new IntegerLiteral(2)), ImmutableList.of(), true),
                new MergeMatchedClause(Optional.empty(), ImmutableList.of(), true)
        );
        List<MergeNotMatchedClause> notMatchedClauses = ImmutableList.of(
                new MergeNotMatchedClause(Optional.of(new IntegerLiteral(3)), ImmutableList.of(), ImmutableList.of()),
                new MergeNotMatchedClause(Optional.of(new IntegerLiteral(4)), ImmutableList.of(), ImmutableList.of()),
                new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(), ImmutableList.of())
        );
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, matchedClauses, notMatchedClauses);
        UnboundSlot unboundSlot = new UnboundSlot("alias", "__DORIS_DELETE_SIGN__");

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateBranchLabel = clazz.getDeclaredMethod("generateBranchLabel", NamedExpression.class);
        generateBranchLabel.setAccessible(true);
        NamedExpression result = (NamedExpression) generateBranchLabel.invoke(command, unboundSlot);
        Expression matchedLabel = new If(new IntegerLiteral(1), new IntegerLiteral(0),
                new If(new IntegerLiteral(2), new IntegerLiteral(1), new IntegerLiteral(2)));
        Expression notMatchedLabel = new If(new IntegerLiteral(3), new IntegerLiteral(3),
                new If(new IntegerLiteral(4), new IntegerLiteral(4), new IntegerLiteral(5)));
        NamedExpression expected = new UnboundAlias(new If(new Not(new IsNull(unboundSlot)),
                matchedLabel, notMatchedLabel), "__DORIS_MERGE_INTO_BRANCH_LABEL__");
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateDeleteProjection() throws Exception {
        List<Column> columns = ImmutableList.of(
                new Column("c1", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateDeleteProjection = clazz.getDeclaredMethod("generateDeleteProjection", List.class);
        generateDeleteProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateDeleteProjection.invoke(command, columns);
        Assertions.assertEquals(4, result.size());
        Assertions.assertEquals(ImmutableList.of(
                new UnboundSlot(ImmutableList.of("alias", "c1")),
                new TinyIntLiteral(((byte) 1)),
                new UnboundSlot(ImmutableList.of("alias", Column.SEQUENCE_COL)),
                new UnboundSlot(ImmutableList.of("alias", "c3"))), result);
    }

    @Test
    public void testGenerateUpdateProjection() throws Exception {
        MergeMatchedClause mergeMatchedClause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot(Column.SEQUENCE_COL), new IntegerLiteral(1)),
                new EqualTo(new UnboundSlot("c4"), new IntegerLiteral(2)),
                new EqualTo(new UnboundSlot("c5"), new IntegerLiteral(3))
        ), false);

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c4", PrimitiveType.BIGINT),
                new Column("c5", PrimitiveType.BIGINT),
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                        null, new HashSet<>(), null)
        );

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateUpdateProjection = clazz.getDeclaredMethod("generateUpdateProjection",
                MergeMatchedClause.class, List.class, OlapTable.class, ConnectContext.class);
        generateUpdateProjection.setAccessible(true);

        try (MockedStatic<UpdateCommand> mockedUpdate = Mockito.mockStatic(UpdateCommand.class)) {
            mockedUpdate.when(() -> UpdateCommand.checkAssignmentColumn(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(x -> null);
            List<Expression> result = (List<Expression>) generateUpdateProjection.invoke(command,
                    mergeMatchedClause, columns, olapTable, null);
            List<Expression> expected = ImmutableList.of(
                    new Cast(new UnboundSlot(ImmutableList.of("alias", "c1")), BigIntType.INSTANCE),
                    new Cast(new UnboundSlot(ImmutableList.of("alias", Column.DELETE_SIGN)), BigIntType.INSTANCE),
                    new Cast(new IntegerLiteral(1), BigIntType.INSTANCE),
                    new Cast(new UnboundSlot(ImmutableList.of("alias", "c3")), BigIntType.INSTANCE),
                    new Cast(new IntegerLiteral(2), BigIntType.INSTANCE),
                    new Cast(new IntegerLiteral(3), BigIntType.INSTANCE),
                    new Cast(new UnboundFunction("CURRENT_TIMESTAMP", ImmutableList.of()), BigIntType.INSTANCE)
            );
            Assertions.assertEquals(expected, result);
        }
    }

    @Test
    public void testGenerateUpdateProjectionWithDuplicateColumn() throws Exception {
        MergeMatchedClause mergeMatchedClause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot(Column.SEQUENCE_COL), new IntegerLiteral(1)),
                new EqualTo(new UnboundSlot("c4"), new IntegerLiteral(2)),
                new EqualTo(new UnboundSlot("c4"), new IntegerLiteral(3))
        ), false);

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c4", PrimitiveType.BIGINT),
                new Column("c5", PrimitiveType.BIGINT),
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                        null, new HashSet<>(), null)
        );

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateUpdateProjection = clazz.getDeclaredMethod("generateUpdateProjection",
                MergeMatchedClause.class, List.class, OlapTable.class, ConnectContext.class);
        generateUpdateProjection.setAccessible(true);

        try (MockedStatic<UpdateCommand> mockedUpdate = Mockito.mockStatic(UpdateCommand.class)) {
            mockedUpdate.when(() -> UpdateCommand.checkAssignmentColumn(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(x -> null);
            try {
                generateUpdateProjection.invoke(command, mergeMatchedClause, columns, olapTable, null);
            } catch (InvocationTargetException e) {
                Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
            }
        }
    }

    @Test
    public void testGenerateUpdateProjectionWithKey() throws Exception {
        MergeMatchedClause mergeMatchedClause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot(Column.SEQUENCE_COL), new IntegerLiteral(1)),
                new EqualTo(new UnboundSlot("c1"), new IntegerLiteral(2)),
                new EqualTo(new UnboundSlot("c5"), new IntegerLiteral(3))
        ), false);

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c4", PrimitiveType.BIGINT),
                new Column("c5", PrimitiveType.BIGINT),
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                        null, new HashSet<>(), null)
        );

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateUpdateProjection = clazz.getDeclaredMethod("generateUpdateProjection",
                MergeMatchedClause.class, List.class, OlapTable.class, ConnectContext.class);
        generateUpdateProjection.setAccessible(true);

        try (MockedStatic<UpdateCommand> mockedUpdate = Mockito.mockStatic(UpdateCommand.class)) {
            mockedUpdate.when(() -> UpdateCommand.checkAssignmentColumn(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(x -> null);
            try {
                generateUpdateProjection.invoke(command, mergeMatchedClause, columns, olapTable, null);
            } catch (InvocationTargetException e) {
                Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
            }
        }
    }

    @Test
    public void testGenerateUpdateProjectionWithGeneratedColumn() throws Exception {
        MergeMatchedClause mergeMatchedClause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot(Column.SEQUENCE_COL), new IntegerLiteral(1)),
                new EqualTo(new UnboundSlot("c2"), new IntegerLiteral(2)),
                new EqualTo(new UnboundSlot("c5"), new IntegerLiteral(3))
        ), false);

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c4", PrimitiveType.BIGINT),
                new Column("c5", PrimitiveType.BIGINT),
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                        null, new HashSet<>(), null)
        );

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateUpdateProjection = clazz.getDeclaredMethod("generateUpdateProjection",
                MergeMatchedClause.class, List.class, OlapTable.class, ConnectContext.class);
        generateUpdateProjection.setAccessible(true);

        try (MockedStatic<UpdateCommand> mockedUpdate = Mockito.mockStatic(UpdateCommand.class)) {
            mockedUpdate.when(() -> UpdateCommand.checkAssignmentColumn(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(x -> null);
            try {
                generateUpdateProjection.invoke(command, mergeMatchedClause, columns, olapTable, null);
            } catch (InvocationTargetException e) {
                Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
            }
        }
    }

    @Test
    public void testGenerateUpdateProjectionWithNonExistsColumn() throws Exception {
        MergeMatchedClause mergeMatchedClause = new MergeMatchedClause(Optional.empty(), ImmutableList.of(
                new EqualTo(new UnboundSlot(Column.SEQUENCE_COL), new IntegerLiteral(1)),
                new EqualTo(new UnboundSlot("c1"), new IntegerLiteral(2)),
                new EqualTo(new UnboundSlot("c10"), new IntegerLiteral(3))
        ), false);

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c4", PrimitiveType.BIGINT),
                new Column("c5", PrimitiveType.BIGINT),
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                        null, new HashSet<>(), null)
        );

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateUpdateProjection = clazz.getDeclaredMethod("generateUpdateProjection",
                MergeMatchedClause.class, List.class, OlapTable.class, ConnectContext.class);
        generateUpdateProjection.setAccessible(true);

        try (MockedStatic<UpdateCommand> mockedUpdate = Mockito.mockStatic(UpdateCommand.class)) {
            mockedUpdate.when(() -> UpdateCommand.checkAssignmentColumn(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(x -> null);
            try {
                generateUpdateProjection.invoke(command, mergeMatchedClause, columns, olapTable, null);
            } catch (InvocationTargetException e) {
                Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
            }
        }
    }

    @Test
    public void testGenerateInsertWithoutColListProjectionWithoutSeqCol() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundAlias(new DefaultValueSlot()), new UnboundSlot("c3")));

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithoutColListProjection = clazz.getDeclaredMethod("generateInsertWithoutColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, int.class, Optional.class, Optional.class);
        generateInsertWithoutColListProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateInsertWithoutColListProjection.invoke(command, clause, columns, olapTable, false, -1, Optional.empty(), Optional.empty());
        List<Expression> expected = ImmutableList.of(
                new Cast(new UnboundSlot("c1"), BigIntType.INSTANCE),
                new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE),
                new TinyIntLiteral(((byte) 0))
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateInsertWithoutColListProjectionWithSeqColumnWithSeqIndex() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundAlias(new DefaultValueSlot()), new UnboundSlot("c3")));

        Column seqCol = new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                AggregateType.NONE, false, -1, "default", "",
                true, null, -1, "", true, new DefaultValueExprDef("CURRENT_TIMESTAMP"), 1,
                null, new HashSet<>(), null);

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithoutColListProjection = clazz.getDeclaredMethod("generateInsertWithoutColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, int.class, Optional.class, Optional.class);
        generateInsertWithoutColListProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateInsertWithoutColListProjection.invoke(command, clause, columns, olapTable, true, 2, Optional.of(seqCol), Optional.of(ScalarType.createType(PrimitiveType.BIGINT)));
        List<Expression> expected = ImmutableList.of(
                new Cast(new UnboundSlot("c1"), BigIntType.INSTANCE),
                new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE),
                new TinyIntLiteral(((byte) 0)),
                new Cast(new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE), BigIntType.INSTANCE)
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateInsertWithoutColListProjectionWithSeqColumnWithSeqIndexWithoutDefaultValue() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundAlias(new DefaultValueSlot()), new UnboundSlot("c3")));

        Column seqCol = new Column("c3", PrimitiveType.BIGINT);

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithoutColListProjection = clazz.getDeclaredMethod("generateInsertWithoutColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, int.class, Optional.class, Optional.class);
        generateInsertWithoutColListProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateInsertWithoutColListProjection.invoke(command, clause, columns, olapTable, true, 2, Optional.of(seqCol), Optional.of(ScalarType.createType(PrimitiveType.BIGINT)));
        List<Expression> expected = ImmutableList.of(
                new Cast(new UnboundSlot("c1"), BigIntType.INSTANCE),
                new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE),
                new TinyIntLiteral(((byte) 0)),
                new Cast(new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE), BigIntType.INSTANCE)
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateInsertWithoutColListProjectionWithSeqColumnWithoutSeqIndex() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1),
                new Column(Column.VERSION_COL, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundAlias(new DefaultValueSlot()), new UnboundSlot("c3")));

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithoutColListProjection = clazz.getDeclaredMethod("generateInsertWithoutColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, int.class, Optional.class, Optional.class);
        generateInsertWithoutColListProjection.setAccessible(true);
        try {
            generateInsertWithoutColListProjection.invoke(command, clause, columns, olapTable, true, -1, Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateInsertWithoutColListProjectionWithGeneratedColumnWithoutDefaultValue() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c2"), new UnboundSlot("c3")));

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithoutColListProjection = clazz.getDeclaredMethod("generateInsertWithoutColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, int.class, Optional.class, Optional.class);
        generateInsertWithoutColListProjection.setAccessible(true);
        try {
            generateInsertWithoutColListProjection.invoke(command, clause, columns, olapTable, false, -1, Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithoutSeqCol() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c2", "c3"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundAlias(new DefaultValueSlot()), new UnboundSlot("c3")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                // generated column
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, -1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column("c3", PrimitiveType.BIGINT),
                // auto inc
                new Column("c4", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, 1, null, "",
                        true, null, -1, "", false, null, 1,
                        null, new HashSet<>(), null),
                // null default
                new Column("c5", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, true, null, ""),
                // default expr
                new Column("c6", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "1", ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, false, "", Optional.empty(), Optional.empty());
        List<Expression> expected = ImmutableList.of(
                new Cast(new UnboundSlot("c1"), BigIntType.INSTANCE),
                new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE),
                new NullLiteral(BigIntType.INSTANCE),
                new NullLiteral(BigIntType.INSTANCE),
                new Cast(new TinyIntLiteral((byte) 1), BigIntType.INSTANCE),
                new TinyIntLiteral(((byte) 0))
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithSeqColWithSeqIndex() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c3"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c3")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());
        Column seqCol = new Column(Column.SEQUENCE_COL, ScalarType.createType(PrimitiveType.DATEV2), false,
                AggregateType.NONE, false, -1, null, "",
                true, null, -1, "", false, null, 1,
                null, new HashSet<>(), null);

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        List<Expression> result = (List<Expression>) generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, true, "c3", Optional.of(seqCol), Optional.of(ScalarType.createType(PrimitiveType.DATEV2)));
        List<Expression> expected = ImmutableList.of(
                new Cast(new UnboundSlot("c1"), BigIntType.INSTANCE),
                new Cast(new UnboundSlot("c3"), BigIntType.INSTANCE),
                new TinyIntLiteral(((byte) 0)),
                new Cast(new UnboundSlot("c3"), DateV2Type.INSTANCE)
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithSeqColWithoutSeqIndexWithoutDefaultValue() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c3"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c3")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        try {
            generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, true, "", Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithDuplicateColLabel() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c1"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c3")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c3", PrimitiveType.BIGINT),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        try {
            generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, false, "", Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithGeneratedCol() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c2"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c2")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c2", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, -1, "default", "",
                        true, null, -1, "", false, null, 1,
                        new GeneratedColumnInfo("cc", new IntLiteral(1)), new HashSet<>(), null),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        try {
            generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, false, "", Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateInsertWithColListProjectionWithNotNullWithoutDefaultValue() throws Exception {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getName()).thenReturn("olap_table");

        MergeNotMatchedClause clause = new MergeNotMatchedClause(Optional.empty(), ImmutableList.of("c1", "c3"),
                ImmutableList.of(new UnboundSlot("c1"), new UnboundSlot("c3")));

        List<Column> columns = ImmutableList.of(
                new Column("c1", ScalarType.createType(PrimitiveType.BIGINT), true, AggregateType.NONE, "", ""),
                new Column("c3", PrimitiveType.BIGINT),
                new Column("c5", ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, null, ""),
                new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                        AggregateType.NONE, false, "", false, -1)
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateInsertWithColListProjection = clazz.getDeclaredMethod("generateInsertWithColListProjection",
                MergeNotMatchedClause.class, List.class, OlapTable.class, boolean.class, String.class, Optional.class, Optional.class);
        generateInsertWithColListProjection.setAccessible(true);
        try {
            generateInsertWithColListProjection.invoke(command, clause, columns, olapTable, false, "", Optional.empty(), Optional.empty());
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }

    @Test
    public void testGenerateFinalProjections() throws Exception {
        List<String> colNames = ImmutableList.of("c1", "c2");
        List<List<Expression>> finalProjections = ImmutableList.of(
                ImmutableList.of(new IntegerLiteral(11), new IntegerLiteral(12)),
                ImmutableList.of(new IntegerLiteral(21), new IntegerLiteral(22)),
                ImmutableList.of(new IntegerLiteral(31), new IntegerLiteral(32)),
                ImmutableList.of(new IntegerLiteral(41), new IntegerLiteral(42))
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateFinalProjections = clazz.getDeclaredMethod("generateFinalProjections", List.class, List.class);
        generateFinalProjections.setAccessible(true);
        List<NamedExpression> result = (List<NamedExpression>) generateFinalProjections.invoke(command, colNames, finalProjections);
        List<NamedExpression> expected = ImmutableList.of(
                new UnboundAlias(new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(3)), new IntegerLiteral(41),
                        new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(2)), new IntegerLiteral(31),
                                new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(1)), new IntegerLiteral(21),
                                        new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(0)), new IntegerLiteral(11), new NullLiteral())))), "c1"),
                new UnboundAlias(new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(3)), new IntegerLiteral(42),
                        new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(2)), new IntegerLiteral(32),
                                new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(1)), new IntegerLiteral(22),
                                        new If(new EqualTo(new UnboundSlot("__DORIS_MERGE_INTO_BRANCH_LABEL__"), new IntegerLiteral(0)), new IntegerLiteral(12), new NullLiteral())))), "c2")
        );

        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGenerateFinalProjectionsWithDiffSize() throws Exception {
        List<String> colNames = ImmutableList.of("c1", "c2", "c3");
        List<List<Expression>> finalProjections = ImmutableList.of(
                ImmutableList.of(new IntegerLiteral(11), new IntegerLiteral(12)),
                ImmutableList.of(new IntegerLiteral(21), new IntegerLiteral(22)),
                ImmutableList.of(new IntegerLiteral(31), new IntegerLiteral(32)),
                ImmutableList.of(new IntegerLiteral(41), new IntegerLiteral(42))
        );

        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        Expression onClause = new NullLiteral();
        MergeIntoCommand command = new MergeIntoCommand(
                ImmutableList.of("ctl", "db", "tbl"), Optional.of("alias"), Optional.empty(),
                source, onClause, ImmutableList.of(), ImmutableList.of());

        Class<?> clazz = Class.forName("org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand");
        Method generateFinalProjections = clazz.getDeclaredMethod("generateFinalProjections", List.class, List.class);
        generateFinalProjections.setAccessible(true);
        try {
            generateFinalProjections.invoke(command, colNames, finalProjections);
        } catch (InvocationTargetException e) {
            Assertions.assertInstanceOf(AnalysisException.class, e.getCause());
        }
    }
}
