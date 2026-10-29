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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

public class IcebergStaticPartitionLiteralTest {
    @Test
    public void preserveNullSeparatelyFromTextAndEmptyBytes() throws Exception {
        LogicalPlan child = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of());
        Map<String, Expression> literals = ImmutableMap.of(
                "null_value", NullLiteral.INSTANCE,
                "text_value", new StringLiteral("null"),
                "empty_bytes", new VarBinaryLiteral(new byte[0]));
        UnboundIcebergTableSink<LogicalPlan> sink = new UnboundIcebergTableSink<>(
                ImmutableList.of("catalog", "db", "table"), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                DMLCommandType.INSERT, Optional.empty(), Optional.empty(), child, literals, false);
        InsertOverwriteTableCommand command = new InsertOverwriteTableCommand(
                sink, Optional.empty(), Optional.empty(), Optional.empty());
        IcebergInsertCommandContext context = new IcebergInsertCommandContext();
        Method method = InsertOverwriteTableCommand.class.getDeclaredMethod("setStaticPartitionToContext",
                UnboundIcebergTableSink.class, IcebergInsertCommandContext.class);
        method.setAccessible(true);
        method.invoke(command, sink, context);
        Assertions.assertTrue(context.getStaticPartitionValues().containsKey("null_value"));
        Assertions.assertNull(context.getStaticPartitionValues().get("null_value"));
        Assertions.assertEquals("null", context.getStaticPartitionValues().get("text_value"));
        Assertions.assertEquals("0x", context.getStaticPartitionValues().get("empty_bytes"));
    }
}
