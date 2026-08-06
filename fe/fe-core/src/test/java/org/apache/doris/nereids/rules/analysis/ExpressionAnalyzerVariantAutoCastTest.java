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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionAnalyzerVariantAutoCastTest {

    @AfterEach
    public void cleanup() {
        ConnectContext.remove();
    }

    private CascadesContext createContext(boolean enableAutoCast) {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().enableVariantSchemaAutoCast = enableAutoCast;
        ctx.setThreadLocalInfo();
        return CascadesContext.initTempContext();
    }

    private Expression analyze(Expression expr, Scope scope, boolean enableAutoCast) {
        CascadesContext cascadesContext = createContext(enableAutoCast);
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, scope, cascadesContext, true, true);
        return analyzer.analyze(expr);
    }

    private SlotReference buildVariantSlot(VariantType variantType) {
        return new SlotReference(new org.apache.doris.nereids.trees.expressions.ExprId(1),
                "data", variantType, true, ImmutableList.of());
    }

    private VariantType buildVariantType() {
        VariantField numField = new VariantField("num_*", BigIntType.INSTANCE, "");
        VariantField strField = new VariantField("str_*", StringType.INSTANCE, "");
        return new VariantType(ImmutableList.of(numField, strField));
    }

    private void assertCastElementAt(Expression expr) {
        Assertions.assertTrue(expr instanceof Cast, "expect Cast wrapping ElementAt");
        Cast cast = (Cast) expr;
        Assertions.assertTrue(cast.child() instanceof ElementAt, "cast child should be ElementAt");
    }

    @Test
    public void testSelectAutoCastElementAt() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Expression result = analyze(elementAt, scope, true);
        assertCastElementAt(result);
    }

    @Test
    public void testSelectDotSyntaxAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        UnboundSlot unbound = new UnboundSlot("data", "num_a");
        Expression result = analyze(unbound, scope, true);
        Assertions.assertTrue(result instanceof Alias);
        Alias alias = (Alias) result;
        assertCastElementAt(alias.child());
    }

    @Test
    public void testWhereAutoCastComparison() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        GreaterThan predicate = new GreaterThan(elementAt, new BigIntLiteral(10));
        Expression result = analyze(predicate, scope, true);

        Assertions.assertTrue(result instanceof GreaterThan);
        GreaterThan gt = (GreaterThan) result;
        assertCastElementAt(gt.left());
    }

    @Test
    public void testOrderByExpressionAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Expression result = analyze(elementAt, scope, true);
        assertCastElementAt(result);
    }

    @Test
    public void testGroupByExpressionAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("str_name"));
        Expression result = analyze(elementAt, scope, true);
        assertCastElementAt(result);
    }

    @Test
    public void testAggregateFunctionAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Sum sum = new Sum(elementAt);
        Expression result = analyze(sum, scope, true);

        Assertions.assertTrue(result instanceof Sum);
        Sum analyzedSum = (Sum) result;
        assertCastElementAt(analyzedSum.child());
    }

    @Test
    public void testHavingAutoCastWithAggregate() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Sum sum = new Sum(elementAt);
        GreaterThan having = new GreaterThan(sum, new BigIntLiteral(100));
        Expression result = analyze(having, scope, true);

        Assertions.assertTrue(result instanceof GreaterThan);
        GreaterThan gt = (GreaterThan) result;
        Assertions.assertTrue(gt.left() instanceof Sum);
        Sum analyzedSum = (Sum) gt.left();
        assertCastElementAt(analyzedSum.child());
    }

    @Test
    public void testNonLiteralKeyNoAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        SlotReference keySlot = new SlotReference(new ExprId(2), "col", StringType.INSTANCE, true, ImmutableList.of());
        Scope scope = new Scope(ImmutableList.of(slot, keySlot));

        ElementAt elementAt = new ElementAt(slot, keySlot);
        Expression result = analyze(elementAt, scope, true);
        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }

    @Test
    public void testNoMatchingTemplateNoAutoCast() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("unknown"));
        Expression result = analyze(elementAt, scope, true);
        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }

    @Test
    public void testChainedPathOnlyOuterCast() {
        VariantField nestedField = new VariantField("int_nested.level1_num_1", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(nestedField));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt inner = new ElementAt(slot, new StringLiteral("int_nested"));
        ElementAt outer = new ElementAt(inner, new StringLiteral("level1_num_1"));
        Expression result = analyze(outer, scope, true);

        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        Assertions.assertTrue(cast.child() instanceof ElementAt);
        ElementAt castChild = (ElementAt) cast.child();
        Assertions.assertTrue(castChild.left() instanceof ElementAt);
        Assertions.assertFalse(castChild.left() instanceof Cast);
    }

    @Test
    public void testDotPathMergedAliasCast() {
        VariantField nestedField = new VariantField("int_nested.level1_num_1", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(nestedField));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        UnboundSlot unbound = new UnboundSlot("data", "int_nested", "level1_num_1");
        Expression result = analyze(unbound, scope, true);
        Assertions.assertTrue(result instanceof Alias);
        Alias alias = (Alias) result;
        assertCastElementAt(alias.child());
    }

    @Test
    public void testExplicitCastStillAutoCastsInner() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Cast explicit = new Cast(elementAt, IntegerType.INSTANCE);
        Expression result = analyze(explicit, scope, true);

        Assertions.assertTrue(result instanceof Cast);
        Cast outer = (Cast) result;
        Assertions.assertTrue(outer.child() instanceof Cast);
        Cast inner = (Cast) outer.child();
        Assertions.assertTrue(inner.child() instanceof ElementAt);
    }

    @Test
    public void testWhereBetweenAndIn() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Between between = new Between(elementAt, new BigIntLiteral(10), new BigIntLiteral(20));
        Expression betweenResult = analyze(between, scope, true);
        Assertions.assertTrue(betweenResult.containsType(Cast.class));
        Assertions.assertTrue(betweenResult.containsType(ElementAt.class));
        Assertions.assertTrue(betweenResult.collectFirst(
                expr -> expr instanceof Cast && ((Cast) expr).child() instanceof ElementAt).isPresent());

        ElementAt elementAtStr = new ElementAt(slot, new StringLiteral("str_name"));
        InPredicate inPredicate = new InPredicate(elementAtStr,
                ImmutableList.of(new StringLiteral("alice"), new StringLiteral("bob")));
        Expression inResult = analyze(inPredicate, scope, true);
        Assertions.assertTrue(inResult.containsType(Cast.class));
        Assertions.assertTrue(inResult.containsType(ElementAt.class));
        Assertions.assertTrue(inResult.collectFirst(
                expr -> expr instanceof Cast && ((Cast) expr).child() instanceof ElementAt).isPresent());
    }

    @Test
    public void testAggregateMinMaxAvgCountDistinct() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Min min = new Min(elementAt);
        Max max = new Max(elementAt);
        Avg avg = new Avg(elementAt);
        Count countDistinct = new Count(true, elementAt);

        Expression minResult = analyze(min, scope, true);
        Assertions.assertTrue(minResult instanceof Min);
        assertCastElementAt(((Min) minResult).child());

        Expression maxResult = analyze(max, scope, true);
        Assertions.assertTrue(maxResult instanceof Max);
        assertCastElementAt(((Max) maxResult).child());

        Expression avgResult = analyze(avg, scope, true);
        Assertions.assertTrue(avgResult instanceof Avg);
        assertCastElementAt(((Avg) avgResult).child());

        Expression countResult = analyze(countDistinct, scope, true);
        Assertions.assertTrue(countResult instanceof Count);
        assertCastElementAt(((Count) countResult).child(0));
    }

    @Test
    public void testAutoCastDisabled() {
        VariantType variantType = buildVariantType();
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("num_a"));
        Expression result = analyze(elementAt, scope, false);

        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }
}
