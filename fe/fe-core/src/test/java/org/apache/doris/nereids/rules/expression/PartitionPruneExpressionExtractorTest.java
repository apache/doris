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
import org.apache.doris.nereids.rules.expression.rules.PartitionPruneExpressionExtractor;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This unit is used to check whether {@link PartitionPruneExpressionExtractor} is correct or not.
 * Slot P1 ~ P5 are partition slots.
 */
public class PartitionPruneExpressionExtractorTest {
    private static final NereidsParser PARSER = new NereidsParser();
    private final CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
        new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
    private final Map<String, Slot> slotMemo = Maps.newHashMap();
    private final Set<Slot> partitionSlots;
    private final PartitionPruneExpressionExtractor.ExpressionEvaluableDetector evaluableDetector;

    public PartitionPruneExpressionExtractorTest() {
        Map<String, Slot> partitions = createPartitionSlots();
        slotMemo.putAll(partitions);
        partitionSlots = ImmutableSet.copyOf(partitions.values());
        evaluableDetector = new PartitionPruneExpressionExtractor.ExpressionEvaluableDetector(partitionSlots);
    }

    /**
     * Expect: All expressions which contains non-partition slot are not evaluable.
     */
    @Test
    public void testExpressionEvaluableDetector() {
        // expression does not contains any non-partition slot.
        assertDeterminateEvaluable("P1 = '20240614'", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614'", true);
        assertDeterminateEvaluable("P1 = '20240614' or P2 = '20240614'", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' and true", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' and false", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' and 5 > 10", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' or 'a' = 'b'", true);
        assertDeterminateEvaluable("P1 = '20240614' and not(P2 = '20240614') or 'a' = 'b'", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' or not('a' = 'b')", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "case when(P2 = '20240614' or P2 = 'abc') then P3 = 'abc' else false end", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "case when(P2 = '20240614' and P1 = 'abc') then P3 = 'abc' else false end", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "if(P2 = '20240614' and P1 = 'abc', P3 = 'abc', false)", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "if(P2 = '20240614' and '123' = 'abc', P1 = 'abc', false)", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "to_date('20240614', '%Y%m%d') = P2", true);

        // expression contains non-partition slot.
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' and I1 = 'abc'", false);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' or I1 = 'abc'", false);
        assertDeterminateEvaluable("P1 = '20240614' and (P2 = '20240614' and I1 = 'abc')", false);
        assertDeterminateEvaluable("P1 = '20240614' and (P2 = '20240614' or I1 = 'abc')", false);
        assertDeterminateEvaluable("P1 = '20240614' and (P2 = '20240614' or I1 = 'abc')", false);
        assertDeterminateEvaluable("P1 = '20240614' and not(P2 = '20240614') or 'S1' = 'b'", true);
        assertDeterminateEvaluable("P1 = '20240614' and P2 = '20240614' or not('S2' = 'b')", true);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "case when(P2 = '20240614' or I1 = 'abc') then I2 = 'abc' else false end", false);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "case when(P2 = '20240614' and I1 = 'abc') then I2 = 'abc' else false end", false);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "if(P2 = '20240614' and I1 = 'abc', I2 = 'abc', false)", false);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "if(P2 = '20240614' and I1 = 'abc', I2 = 'abc', false)", false);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "to_date('20240614', '%Y%m%d') = S1", false);
        assertDeterminateEvaluable("P1 = '20240614' and "
                + "(select 'a' from t limit 1) = S1", false);
    }

    @Test
    public void testExpressionExtract() {
        assertExtract("P1 = '20240614'", "P1 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614'", "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' or P2 = '20240614'", "P1 = '20240614' or P2 = '20240614'");

        assertExtract("P1 = '20240614' and P2 = '20240614' and true",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' and false", "false");
        assertExtract("P1 = '20240614' and P2 = '20240614' and 5 > 10", "false");
        assertExtract("P1 = '20240614' and P2 = '20240614' and I1 = 'abc'",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' and (5 > 10 and I2 = 123)", "false");
        assertExtract("P1 = '20240614' and P2 = '20240614' or I1 = 'abc'", "true");

        assertExtract("P1 = '20240614' and P2 = '20240614' or false",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' or 5 < 10", "true");
        assertExtract("P1 = '20240614' and P2 = '20240614' or (5 < 10 or I2 = 123)", "true");
        assertExtract("P1 = '20240614' and P2 = '20240614' or (5 < 10 and I2 = 123)", "true");
        assertExtract("P1 = '20240614' and (P2 = '20240614' and I1 = 'abc')",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and (P2 = '20240614' or I1 = 'abc')", "P1 = '20240614'");
        assertExtract("P1 = '20240614' and (P2 = '20240614' or I1 = 'abc')", "P1 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' or I2 = 123", "true");
        assertExtract("P1 = '20240614' and P2 = '20240614' or not(I2 = 123)", "true");
        assertExtract("P1 = '20240614' and P2 = '20240614' or not(P3 = '20240614' and I2 = 123)", "true");

        assertExtract("P1 = '20240614' and P2 = '20240614' or (5 > 10 and I2 = 123)",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' and not(5 > 10 and I2 = 123)",
                "P1 = '20240614' and P2 = '20240614'");

        assertExtract("P1 = '20240614' and P2 = '20240614' and (P3 = '20240614' and (P4 = '20240614' or I1 = 123))",
                "P1 = '20240614' and P2 = '20240614' and P3 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' and "
                    + "(P3 = '20240614' or (P4 = '20240614' and P5 = '20240614' or I1 = 123))",
                "P1 = '20240614' and P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' and "
                + "(P3 = '20240614' or (P4 = '20240614' or I1 = 123 and P5 = '20240614'))",
                "P1 = '20240614' and P2 = '20240614' and (P3 = '20240614' or (P4 = '20240614' or P5 = '20240614'))");
        assertExtract("P1 = '20240614' and P2 = '20240614' and "
                + "(P3 = '20240614' or ((P4 = '20240614' or I1 = 123) and P5 = '20240614'))",
                "P1 = '20240614' and P2 = '20240614' and (P3 = '20240614' or P5 = '20240614')");
        assertExtract("I2 = 345 or (P1 = '20240614' and P2 = '20240614') and "
                + "(P3 = '20240614' or (P4 = '20240614' and P5 = '20240614' and I1 = 123))",
                "true");
        assertExtract("(I2 = 345 or P1 = '20240614') and P2 = '20240614' and "
                + "(P3 = '20240614' or (P4 = '20240614' and P5 = '20240614' and I1 = 123))",
                "P2 = '20240614' and (P3 = '20240614' or (P4 = '20240614' and P5 = '20240614'))");
        assertExtract("(I2 = 345 or P1 = '20240614') and P2 = '20240614' and "
                + "(P3 = '20240614' or (P4 = '20240614' and P5 = '20240614' or I1 = 123))",
                "P2 = '20240614'");
        assertExtract("P1 = '20240614' and P2 = '20240614' or "
                + "(P3 = '20240614' or (P4 = '20240614' and P5 = '20240614' or I1 = 123))",
                "true");
        assertExtract("P1 = '20240614' and case when(P2 = '20240614' or P2 = 'abc') then P3 = 'abc' else false end",
                "P1 = '20240614' and case when(P2 = '20240614' or P2 = 'abc') then P3 = 'abc' else false end");
        assertExtract("P1 = '20240614' and case when(P2 = '20240614' and P1 = 'abc') then P3 = 'abc' else false end",
                "P1 = '20240614' and case when(P2 = '20240614' and P1 = 'abc') then P3 = 'abc' else false end");
        assertExtract("P1 = '20240614' and case when(P2 = '20240614' or I1 = 'abc') then I2 = 'abc' else false end",
                "P1 = '20240614'");
        assertExtract("P1 = '20240614' and case when(P2 = '20240614' and I1 = 'abc') then I2 = 'abc' else false end",
                "P1 = '20240614'");
        assertExtract("P1 = '20240614' or if(P2 = '20240614' and P1 = 'abc', P3 = 'abc', false)",
                "P1 = '20240614' or if(P2 = '20240614' and P1 = 'abc', P3 = 'abc', false)");
        assertExtract("P1 = '20240614' or if(P2 = '20240614' and '123' = 'abc', P1 = 'abc', false)",
                "P1 = '20240614' or if(false, P1 = 'abc', false)");
        assertExtract("P1 = '20240614' or if(P2 = '20240614' and I1 = 'abc', I2 = 'abc', false)", "true");
        assertExtract("P1 = '20240614' or if(P2 = '20240614' and I1 = 'abc', I2 = 'abc', false)", "true");
        assertExtract("P1 = '20240614' and to_date('20240614', '%Y%m%d') = P2",
                "P1 = '20240614' and to_date('20240614', '%Y%m%d') = P2");
        assertExtract("P1 = '20240614' and to_date('20240614', '%Y%m%d') = S1",
                "P1 = '20240614'");
        assertExtract("P1 = '20240614' or (select 'a' from t limit 1) = S1", "true");
    }

    private void assertDeterminateEvaluable(String expressionString, boolean evaluable) {
        Expression expression = replaceUnboundSlot(PARSER.parseExpression(expressionString), slotMemo);
        Assertions.assertEquals(evaluableDetector.detect(expression), evaluable);
    }

    private void assertExtract(String expression, String expected) {
        Expression needRewriteExpression = replaceUnboundSlot(PARSER.parseExpression(expression), slotMemo);
        Expression expectedExpression = replaceUnboundSlot(PARSER.parseExpression(expected), slotMemo);
        Expression rewrittenExpression =
                PartitionPruneExpressionExtractor.extract(needRewriteExpression, partitionSlots, cascadesContext);
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

    private Expression replaceNotNullUnboundSlot(Expression expression, Map<String, Slot> mem) {
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = replaceNotNullUnboundSlot(child, mem);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        if (expression instanceof UnboundSlot) {
            String name = ((UnboundSlot) expression).getName();
            mem.putIfAbsent(name, new SlotReference(name, getType(name.charAt(0)), false));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    private Map<String, Slot> createPartitionSlots() {
        SlotReference slotReference1 = new SlotReference("P1", StringType.INSTANCE);
        SlotReference slotReference2 = new SlotReference("P2", IntegerType.INSTANCE);
        SlotReference slotReference3 = new SlotReference("P3", StringType.INSTANCE);
        SlotReference slotReference4 = new SlotReference("P4", IntegerType.INSTANCE);
        SlotReference slotReference5 = new SlotReference("P5", StringType.INSTANCE);
        return ImmutableMap.of(
            "P1", slotReference1,
            "P2", slotReference2,
            "P3", slotReference3,
            "P4", slotReference4,
            "P5", slotReference5);
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
