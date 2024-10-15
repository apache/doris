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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;

public abstract class ExpressionRewriteTestHelper extends ExpressionRewrite {
    protected static final NereidsParser PARSER = new NereidsParser();
    protected ExpressionRuleExecutor executor;

    protected ExpressionRewriteContext context;

    public ExpressionRewriteTestHelper() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    protected final void assertRewrite(String expression, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    protected void assertRewrite(String expression, Expression expectedExpression) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        needRewriteExpression = typeCoercion(needRewriteExpression);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    protected void assertRewrite(Expression expression, Expression expectedExpression) {
        expression = typeCoercion(expression);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    protected void assertNotRewrite(Expression expression, Expression expectedExpression) {
        expression = typeCoercion(expression);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertNotEquals(expectedExpression, rewrittenExpression);
    }

    protected void assertRewriteAfterTypeCoercion(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        needRewriteExpression = typeCoercion(replaceUnboundSlot(needRewriteExpression, mem));
        Expression expectedExpression = PARSER.parseExpression(expected);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression.toSql(), rewrittenExpression.toSql());
    }

    protected Expression replaceUnboundSlot(Expression expression, Map<String, Slot> mem) {
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = replaceUnboundSlot(child, mem);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        if (expression instanceof UnboundSlot) {
            String name = ((UnboundSlot) expression).getName();
            mem.putIfAbsent(name, SlotReference.of(name, getType(name.charAt(0))));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    protected Expression typeCoercion(Expression expression) {
        return ExpressionAnalyzer.FUNCTION_ANALYZER_RULE.rewrite(expression, null);
    }

    protected DataType getType(char t) {
        switch (t) {
            case 'T':
                return TinyIntType.INSTANCE;
            case 'I':
                return IntegerType.INSTANCE;
            case 'D':
                return DoubleType.INSTANCE;
            case 'S':
                return StringType.INSTANCE;
            case 'V':
                return VarcharType.SYSTEM_DEFAULT;
            case 'B':
                return BooleanType.INSTANCE;
            case 'C':
                return DateV2Type.INSTANCE;
            case 'M':
                return DecimalV3Type.SYSTEM_DEFAULT;
            default:
                return BigIntType.INSTANCE;
        }
    }
}
