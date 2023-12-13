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
import org.apache.doris.nereids.rules.expression.rules.SimplifyRange;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class SimplifyRangeTest {

    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;
    private ExpressionRewriteContext context;

    public SimplifyRangeTest() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    @Test
    public void testSimplify() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyRange.INSTANCE));
        assertRewrite("TA", "TA");
        assertRewrite("(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)", "(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)");
        assertRewrite("(TA > 3 and TA < 1) or (TA > 7 and TA < 5)", "FALSE");
        assertRewrite("TA > 3 and TA < 1", "FALSE");
        assertRewrite("TA >= 3 and TA < 3", "TA >= 3 and TA < 3");
        assertRewrite("TA = 1 and TA > 10", "FALSE");
        assertRewrite("TA > 5 or TA < 1", "TA > 5 or TA < 1");
        assertRewrite("TA > 5 or TA > 1 or TA > 10", "TA > 1");
        assertRewrite("TA > 5 or TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA > 1 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 and TA > 1 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 1 or TA < 1", "TA > 1 or TA < 1");
        assertRewrite("TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 5 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 + 1 and TA > 10", "TA > 5 + 1 and TA > 10");
        assertRewrite("TA > 5 + 1 and TA > 10", "TA > 5 + 1 and TA > 10");
        assertRewrite("(TA > 1 and TA > 10) or TA > 20", "TA > 10");
        assertRewrite("(TA > 1 or TA > 10) and TA > 20", "TA > 20");
        assertRewrite("(TA + TB > 1 or TA + TB > 10) and TA + TB > 20", "TA + TB > 20");
        assertRewrite("TA > 10 or TA > 10", "TA > 10");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB < 20)", "TA > 10 and (TB > 10 and TB < 20) ");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB > 20)", "TA > 10 and TB > 20");
        assertRewrite("((TB > 30 and TA > 40) and TA > 20) and (TB > 10 and TB > 20)", "TB > 30 and TA > 40");
        assertRewrite("(TA > 10 and TB > 10) or (TB > 10 and TB > 20)", "TA > 10 and TB > 10 or TB > 20");
        assertRewrite("((TA > 10 or TA > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA > 5 and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))");
        assertRewrite("TA in (1,2,3) and TA > 10", "FALSE");
        assertRewrite("TA in (1,2,3) and TA >= 1", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA > 1", "((TA = 2) OR (TA = 3))");
        assertRewrite("TA in (1,2,3) or TA >= 1", "TA >= 1");
        assertRewrite("TA in (1)", "TA in (1)");
        assertRewrite("TA in (1,2,3) and TA < 10", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA < 1", "FALSE");
        assertRewrite("TA in (1,2,3) or TA < 1", "TA in (1,2,3) or TA < 1");
        assertRewrite("TA in (1,2,3) or TA in (2,3,4)", "TA in (1,2,3,4)");
        assertRewrite("TA in (1,2,3) or TA in (4,5,6)", "TA in (1,2,3,4,5,6)");
        assertRewrite("TA in (1,2,3) and TA in (4,5,6)", "FALSE");
        assertRewrite("TA in (1,2,3) and TA in (3,4,5)", "TA = 3");
        assertRewrite("TA + TB in (1,2,3) and TA + TB in (3,4,5)", "TA + TB = 3");
        assertRewrite("TA in (1,2,3) and DA > 1.5", "TA in (1,2,3) and DA > 1.5");
        assertRewrite("TA = 1 and TA = 3", "FALSE");
        assertRewrite("TA in (1) and TA in (3)", "FALSE");
        assertRewrite("TA in (1) and TA in (1)", "TA = 1");
        assertRewrite("(TA > 3 and TA < 1) and TB < 5", "FALSE");
        assertRewrite("(TA > 3 and TA < 1) or TB < 5", "TB < 5");
        assertRewrite("((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1", "((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1");
    }

    private void assertRewrite(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = replaceUnboundSlot(PARSER.parseExpression(expression), mem);
        Expression expectedExpression = replaceUnboundSlot(PARSER.parseExpression(expected), mem);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private Expression replaceUnboundSlot(Expression expression, Map<String, Slot> mem) {
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
            mem.putIfAbsent(name, new SlotReference(name, getType(name.charAt(0))));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    private DataType getType(char t) {
        switch (t) {
            case 'T':
                return TinyIntType.INSTANCE;
            case 'I':
                return IntegerType.INSTANCE;
            case 'D':
                return DoubleType.INSTANCE;
            case 'S':
                return StringType.INSTANCE;
            case 'B':
                return BooleanType.INSTANCE;
            default:
                return BigIntType.INSTANCE;
        }
    }
}
