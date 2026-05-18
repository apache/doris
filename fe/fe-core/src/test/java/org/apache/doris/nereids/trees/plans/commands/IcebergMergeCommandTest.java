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
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

public class IcebergMergeCommandTest {

    @Test
    public void testExecuteWithExternalTableBatchModeDisabledRestoresValueOnSuccess() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().enableExternalTableBatchMode = true;

        Boolean result = IcebergMergeCommand.executeWithExternalTableBatchModeDisabled(ctx, () -> {
            Assertions.assertFalse(ctx.getSessionVariable().enableExternalTableBatchMode);
            return Boolean.TRUE;
        });

        Assertions.assertTrue(result);
        Assertions.assertTrue(ctx.getSessionVariable().enableExternalTableBatchMode);
    }

    @Test
    public void testExecuteWithExternalTableBatchModeDisabledRestoresValueOnException() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().enableExternalTableBatchMode = false;

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> IcebergMergeCommand.executeWithExternalTableBatchModeDisabled(ctx, () -> {
                    Assertions.assertFalse(ctx.getSessionVariable().enableExternalTableBatchMode);
                    throw new RuntimeException("expected");
                }));

        Assertions.assertEquals("expected", exception.getMessage());
        Assertions.assertFalse(ctx.getSessionVariable().enableExternalTableBatchMode);
    }

    @Test
    public void testWritesDataFilesForMergeClauses() {
        Assertions.assertFalse(IcebergMergeCommand.writesDataFiles(
                ImmutableList.of(new MergeMatchedClause(Optional.empty(), ImmutableList.of(), true)),
                ImmutableList.of()));

        Assertions.assertTrue(IcebergMergeCommand.writesDataFiles(
                ImmutableList.of(new MergeMatchedClause(Optional.empty(), ImmutableList.of(), false)),
                ImmutableList.of()));

        Assertions.assertTrue(IcebergMergeCommand.writesDataFiles(
                ImmutableList.of(new MergeMatchedClause(Optional.empty(), ImmutableList.of(), true)),
                ImmutableList.of(new MergeNotMatchedClause(Optional.empty(), ImmutableList.of(),
                        ImmutableList.of()))));
    }

    @Test
    public void testDeleteProjectionDoesNotReadVisibleTargetColumns() throws Exception {
        IcebergMergeCommand command = new IcebergMergeCommand(
                ImmutableList.of("catalog", "db", "target"),
                Optional.of("t"),
                Optional.empty(),
                new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of()),
                BooleanLiteral.TRUE,
                ImmutableList.of(new MergeMatchedClause(Optional.empty(), ImmutableList.of(), true)),
                ImmutableList.of());
        Method method = IcebergMergeCommand.class.getDeclaredMethod(
                "buildDeleteProjection", Expression.class, List.class);
        method.setAccessible(true);

        Column id = new Column("id", Type.INT, true);
        Column variant = new Column("v", Type.VARIANT, true);
        Column rowId = new Column(IcebergUtils.ICEBERG_ROW_ID_COL, Type.BIGINT, true);
        List<Expression> projection = (List<Expression>) method.invoke(
                command, new NullLiteral(DataType.fromCatalogType(Type.BIGINT)), ImmutableList.of(id, variant, rowId));

        Assertions.assertTrue(projection.get(2) instanceof NullLiteral);
        Assertions.assertTrue(projection.get(3) instanceof NullLiteral);
        Assertions.assertTrue(projection.get(4) instanceof UnboundSlot);
        Assertions.assertEquals(ImmutableList.of("t", IcebergUtils.ICEBERG_ROW_ID_COL),
                ((UnboundSlot) projection.get(4)).getNameParts());
    }
}
