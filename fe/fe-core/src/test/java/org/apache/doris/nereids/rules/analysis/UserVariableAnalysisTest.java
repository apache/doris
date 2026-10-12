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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.TimeStampNsLiteral;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for user variable handling in expression analysis. */
public class UserVariableAnalysisTest {

    @Test
    public void testUserVarIntegerType() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        // set user var @a = TINY_INT_MAX (tiny int)
        ctx.setUserVar("a", new IntLiteral(Byte.MAX_VALUE));
        // set user var @a = SMALL_INT_MAX (small int)
        ctx.setUserVar("b", new IntLiteral(Short.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("c", new IntLiteral(Integer.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("d", new IntLiteral(Long.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("e", new LargeIntLiteral(LargeIntLiteral.LARGE_INT_MAX));

        Assertions.assertEquals(TinyIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("a").getDataType());
        Assertions.assertEquals(SmallIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("b").getDataType());
        Assertions.assertEquals(IntegerType.INSTANCE, ConnectContext.get().getLiteralForUserVar("c").getDataType());
        Assertions.assertEquals(BigIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("d").getDataType());
        Assertions.assertEquals(LargeIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("e").getDataType());
    }

    @Test
    public void testUserVarTimestampTzType() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.setUserVar("ts", new DateLiteral(
                2024, 11, 3, 5, 5, 0, 123456, ScalarType.createTimeStampTzType(6)));

        Literal literal = ctx.getLiteralForUserVar("ts");
        Assertions.assertInstanceOf(TimestampTzLiteral.class, literal);
        Assertions.assertEquals(TimeStampTzType.of(6), literal.getDataType());
        Assertions.assertEquals("2024-11-03 05:05:00.123456+00:00", literal.getStringValue());
    }

    @Test
    public void testUserVarTimeStampNsTypeAndPrecision() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.setUserVar("ts", new TimeStampNsLiteral(
                2024, 2, 29, 12, 34, 56, 123456789));

        org.apache.doris.nereids.trees.expressions.literal.Literal literal
                = ctx.getLiteralForUserVar("ts");
        Assertions.assertEquals(TimeStampNsType.INSTANCE, literal.getDataType());
        Assertions.assertEquals("2024-02-29 12:34:56.123456789", literal.getStringValue());
    }

    @Test
    public void testBindUserVariableToRealExpressionAndRecordSqlCacheDependency() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.setUserVar("v", new IntLiteral(42));
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, "select @v");
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, new Scope(ImmutableList.of()),
                cascadesContext, false, false);

        Expression analyzed = analyzer.analyze(new UnboundVariable("v", VariableType.USER));

        Assertions.assertInstanceOf(Literal.class, analyzed);
        List<Variable> usedVariables = cascadesContext.getStatementContext().getSqlCacheContext()
                .orElseThrow().getUsedVariables();
        Assertions.assertEquals(1, usedVariables.size());
        Assertions.assertEquals("v", usedVariables.get(0).getName());
        Assertions.assertEquals(VariableType.USER, usedVariables.get(0).getType());
        Assertions.assertEquals(analyzed, usedVariables.get(0).getRealExpression());
    }
}
