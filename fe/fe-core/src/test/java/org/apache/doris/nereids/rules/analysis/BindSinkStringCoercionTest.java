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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BindSinkStringCoercionTest {
    @Test
    void truncateBoundedAndUnboundedStringsBeforeSubstitution() {
        for (DataType sourceType : new DataType[] {VarcharType.createVarcharType(10),
                CharType.createCharType(10), StringType.INSTANCE}) {
            for (DataType targetType : new DataType[] {VarcharType.createVarcharType(2),
                    CharType.createCharType(2)}) {
                Expression source = SlotReference.of("source", sourceType);
                Expression result = BindSink.coerceColumnExpression(source, targetType, true);
                Assertions.assertEquals(new Substring(source, Literal.of(1), Literal.of(2)), result);
                Assertions.assertSame(source, BindSink.coerceColumnExpression(source, targetType, false));
            }
        }
    }

    @Test
    void retainStringsThatFitTheirTargetColumn() {
        Expression source = SlotReference.of("source", VarcharType.createVarcharType(2));
        Assertions.assertSame(source,
                BindSink.coerceColumnExpression(source, VarcharType.createVarcharType(2), true));
        Assertions.assertSame(source,
                BindSink.coerceColumnExpression(source, VarcharType.createVarcharType(10), true));
        Expression unbounded = BindSink.coerceColumnExpression(source, StringType.INSTANCE, true);
        Assertions.assertEquals(StringType.INSTANCE, unbounded.getDataType());
        Assertions.assertSame(source, unbounded.child(0));
    }

    @Test
    void connectorMappingHonorsInsertAndLoadTruncationPolicies() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            Column column = new Column("c", VarcharType.createVarcharType(2).toCatalogDataType());
            LogicalOneRowRelation child = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                    ImmutableList.of(new Alias(new StringLiteral("abcd"), "input")));
            for (boolean autoCast : new boolean[] {false, true}) {
                for (boolean strictCast : new boolean[] {false, true}) {
                    for (boolean insert : new boolean[] {false, true}) {
                        context.getSessionVariable().enableInsertValueAutoCast = autoCast;
                        context.getSessionVariable().enableStrictCast = strictCast;
                        Alias output = (Alias) new BindSink(insert)
                                .getConnectorColumnToOutput(ImmutableList.of(column), child).get("c");
                        Assertions.assertEquals(insert && autoCast && !strictCast,
                                output.child() instanceof Substring);
                        Assertions.assertEquals("c", output.getName());
                    }
                }
            }
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
