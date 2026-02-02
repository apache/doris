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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DereferenceExpression;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Optional;

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
        return new SlotReference(new ExprId(1), "data", variantType, true, ImmutableList.of());
    }

    @Test
    public void testVisitElementAtAutoCastEnabled() {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("number_latency"));
        Expression result = analyze(elementAt, scope, true);

        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
        Assertions.assertTrue(cast.child() instanceof ElementAt);
    }

    @Test
    public void testVisitElementAtAutoCastDisabled() {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(slot, new StringLiteral("number_latency"));
        Expression result = analyze(elementAt, scope, false);

        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }

    @Test
    public void testVisitDereferenceExpressionAutoCast() {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        DereferenceExpression deref = new DereferenceExpression(slot, new StringLiteral("number_latency"));
        Expression result = analyze(deref, scope, true);

        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
        Assertions.assertTrue(cast.child() instanceof ElementAt);
    }

    @Test
    public void testResolveVariantElementAtPathChain() {
        VariantField field = new VariantField("int_nested.level1_num_1", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        ElementAt elementAt = new ElementAt(
                new ElementAt(slot, new StringLiteral("int_nested")),
                new StringLiteral("level1_num_1")
        );
        Expression result = analyze(elementAt, scope, true);

        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
        Assertions.assertTrue(cast.child() instanceof ElementAt);
    }

    @Test
    public void testGetVariantPathKeyNonString() throws Exception {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);

        ElementAt elementAt = new ElementAt(slot, new BigIntLiteral(1));
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, new Scope(ImmutableList.of(slot)),
                createContext(true), true, true);

        Method getVariantPathKey = ExpressionAnalyzer.class.getDeclaredMethod("getVariantPathKey", Expression.class);
        getVariantPathKey.setAccessible(true);
        @SuppressWarnings("unchecked")
        Optional<String> key = (Optional<String>) getVariantPathKey.invoke(analyzer, new BigIntLiteral(1));
        Assertions.assertFalse(key.isPresent());

        Method resolvePath = ExpressionAnalyzer.class.getDeclaredMethod("resolveVariantElementAtPath", ElementAt.class);
        resolvePath.setAccessible(true);
        @SuppressWarnings("unchecked")
        Optional<?> path = (Optional<?>) resolvePath.invoke(analyzer, elementAt);
        Assertions.assertFalse(path.isPresent());
    }

    @Test
    public void testMaybeCastAliasExpression() {
        VariantField field = new VariantField("int_nested.level1_num_1", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));
        SlotReference slot = buildVariantSlot(variantType);
        Scope scope = new Scope(ImmutableList.of(slot));

        UnboundSlot unbound = new UnboundSlot("data", "int_nested", "level1_num_1");
        Expression result = analyze(unbound, scope, true);

        Assertions.assertTrue(result instanceof Alias);
        Alias alias = (Alias) result;
        Assertions.assertTrue(alias.child() instanceof Cast);
        Cast cast = (Cast) alias.child();
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
    }
}
