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
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class IcebergMergeCommandTest {

    @Test
    public void testVariantActionsAreCoercedBeforeFinalIf() {
        org.apache.doris.catalog.Type variantType = IcebergUtils.icebergTypeToDorisType(
                org.apache.iceberg.types.Types.VariantType.get(), false, false);
        DataType variantV2Type = DataType.fromCatalogType(variantType);
        List<Column> columns = ImmutableList.of(new Column("payload", variantType));
        List<List<Expression>> actions = ImmutableList.of(
                ImmutableList.of(
                        new TinyIntLiteral((byte) 1),
                        new SlotReference("row_id", StringType.INSTANCE),
                        new SlotReference("target_payload", variantV2Type)),
                ImmutableList.of(
                        new TinyIntLiteral((byte) 2),
                        new SlotReference("row_id", StringType.INSTANCE),
                        new IntegerLiteral(1)));

        List<List<Expression>> coerced = IcebergMergeCommand.coerceVariantActionProjections(
                actions, columns);

        Assertions.assertInstanceOf(Cast.class, coerced.get(0).get(2));
        Assertions.assertInstanceOf(Cast.class, coerced.get(1).get(2));
        Assertions.assertEquals(variantV2Type, coerced.get(0).get(2).getDataType());
        Assertions.assertEquals(variantV2Type, coerced.get(1).get(2).getDataType());
        Assertions.assertEquals(variantV2Type, coerced.get(0).get(2).child(0).getDataType());
        Assertions.assertEquals(org.apache.doris.nereids.types.IntegerType.INSTANCE,
                coerced.get(1).get(2).child(0).getDataType());
    }

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
    public void mergeBranchesUseThePinnedWriterType() {
        VarBinaryType uuidType = VarBinaryType.createVarBinaryType(16);
        Cast cachedTargetValue = new Cast(new StringLiteral("target"), uuidType);
        Cast unboundedDefault = new Cast(new StringLiteral("default"), VarBinaryType.MAX_VARBINARY_TYPE);

        List<NamedExpression> projections = IcebergMergeCommand.generateFinalProjections(
                ImmutableList.of("uuid_col"), ImmutableList.of(uuidType),
                ImmutableList.of(ImmutableList.of(cachedTargetValue), ImmutableList.of(unboundedDefault)));

        Assertions.assertEquals(uuidType, projections.get(0).child(0).getDataType());
    }
}
