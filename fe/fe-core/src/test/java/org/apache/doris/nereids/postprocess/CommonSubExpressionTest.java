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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.nereids.processor.post.CommonSubExpressionCollector;
import org.apache.doris.nereids.processor.post.CommonSubExpressionOpt;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CommonSubExpressionTest extends ExpressionRewriteTestHelper {
    @Test
    public void testExtractCommonExpr() {
        List<NamedExpression> exprs = parseProjections("a+b, a+b+1, abs(a+b+1), a");
        CommonSubExpressionCollector collector = new CommonSubExpressionCollector();
        exprs.forEach(expr -> collector.visit(expr, null));
        Assertions.assertEquals(2, collector.commonExprByDepth.size());
        List<Expression> l1 = new ArrayList<>(collector.commonExprByDepth.get(1));
        List<Expression> l2 = new ArrayList<>(collector.commonExprByDepth.get(2));
        Assertions.assertEquals(1, l1.size());
        assertExpression(l1.get(0), "a+b");
        Assertions.assertEquals(1, l2.size());
        assertExpression(l2.get(0), "a+b+1");
    }

    @Test
    void testLambdaExpression() {
        ArrayItemReference ref = new ArrayItemReference("x", new SlotReference(new ExprId(1), "y",
                ArrayType.of(IntegerType.INSTANCE), true, ImmutableList.of()));
        Expression add = new Add(ref.toSlot(), Literal.of(1));
        Expression and = new And(add, add);
        ArrayMap arrayMap = new ArrayMap(new Lambda(ImmutableList.of("x"), and, ImmutableList.of(ref)));
        List<NamedExpression> exprs = Lists.newArrayList(
                new Alias(new ExprId(10000), arrayMap, "c1"),
                new Alias(new ExprId(10001), arrayMap, "c2")
        );
        CommonSubExpressionCollector collector = new CommonSubExpressionCollector();
        exprs.forEach(expr -> collector.visit(expr, false));
        Assertions.assertEquals(1, collector.commonExprByDepth.size());
        Assertions.assertEquals(1, collector.commonExprByDepth.get(4).size());
        Assertions.assertEquals(arrayMap, collector.commonExprByDepth.get(4).iterator().next());
    }

    @Test
    public void testMultiLayers() throws Exception {
        List<NamedExpression> exprs = parseProjections("a, a+b, a+b+1, abs(a+b+1), a");
        Set<Slot> inputSlots = exprs.get(0).getInputSlots();
        CommonSubExpressionOpt opt = new CommonSubExpressionOpt();
        Method computeMultLayerProjectionsMethod = CommonSubExpressionOpt.class
                .getDeclaredMethod("computeMultiLayerProjections", Set.class, List.class);
        computeMultLayerProjectionsMethod.setAccessible(true);
        List<List<NamedExpression>> multiLayers = (List<List<NamedExpression>>) computeMultLayerProjectionsMethod
                .invoke(opt, inputSlots, exprs);
        Assertions.assertEquals(3, multiLayers.size());
        List<NamedExpression> l0 = multiLayers.get(0);
        Assertions.assertEquals(2, l0.size());
        Assertions.assertTrue(l0.contains(ExprParser.INSTANCE.parseExpression("a")));
        Assertions.assertInstanceOf(Alias.class, l0.get(1));
        assertExpression(l0.get(1).child(0), "a+b");
        Assertions.assertEquals(3, multiLayers.get(1).size());
        Assertions.assertEquals(5, multiLayers.get(2).size());
        List<NamedExpression> l2 = multiLayers.get(2);
        for (int i = 0; i < 5; i++) {
            Assertions.assertEquals(exprs.get(i).getExprId().asInt(), l2.get(i).getExprId().asInt());
        }

    }

    private void assertExpression(Expression expr, String str) {
        Assertions.assertEquals(ExprParser.INSTANCE.parseExpression(str), expr);
    }

    private List<NamedExpression> parseProjections(String exprList) {
        List<NamedExpression> result = new ArrayList<>();
        String[] exprArray = exprList.split(",");
        for (String item : exprArray) {
            Expression expr = ExprParser.INSTANCE.parseExpression(item);
            if (expr instanceof NamedExpression) {
                result.add((NamedExpression) expr);
            } else {
                result.add(new Alias(expr));
            }
        }
        return result;
    }

    public static class ExprParser {
        public static ExprParser INSTANCE = new ExprParser();
        HashMap<String, SlotReference> slotMap = new HashMap<>();

        public Expression parseExpression(String str) {
            Expression expr = PARSER.parseExpression(str);
            return expr.accept(DataTypeAssignor.INSTANCE, slotMap);
        }
    }

    public static class DataTypeAssignor extends DefaultExpressionRewriter<Map<String, SlotReference>> {
        public static DataTypeAssignor INSTANCE = new DataTypeAssignor();

        @Override
        public Expression visitSlot(Slot slot, Map<String, SlotReference> slotMap) {
            SlotReference exitsSlot = slotMap.get(slot.getName());
            if (exitsSlot != null) {
                return exitsSlot;
            } else {
                SlotReference slotReference = new SlotReference(slot.getName(), IntegerType.INSTANCE);
                slotMap.put(slot.getName(), slotReference);
                return slotReference;
            }
        }
    }

}
