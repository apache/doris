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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Unit tests for VariantSchemaCast expression rewriting.
 */
public class VariantSchemaCastTest {

    // Expression rewriter extracted from VariantSchemaCast for testing
    private static final Function<Expression, Expression> EXPRESSION_REWRITER = expr -> {
        if (!(expr instanceof ElementAt)) {
            return expr;
        }
        ElementAt elementAt = (ElementAt) expr;
        Expression left = elementAt.left();
        Expression right = elementAt.right();

        if (!(left.getDataType() instanceof VariantType)) {
            return expr;
        }
        if (!(right instanceof VarcharLiteral)) {
            return expr;
        }

        VariantType variantType = (VariantType) left.getDataType();
        String fieldName = ((VarcharLiteral) right).getStringValue();

        return variantType.findMatchingField(fieldName)
                .map(field -> (Expression) new Cast(elementAt, field.getDataType()))
                .orElse(expr);
    };

    private Expression rewriteExpression(Expression expr) {
        return expr.rewriteDownShortCircuit(EXPRESSION_REWRITER);
    }

    @Test
    public void testRewriteElementAtWithMatchingPattern() {
        // Create variant type with schema template
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField));

        // Create element_at expression: variant['number_latency']
        MockVariantSlot variantSlot = new MockVariantSlot(variantType);
        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("number_latency"));

        // Rewrite
        Expression result = rewriteExpression(elementAt);

        // Should be wrapped with Cast
        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
        Assertions.assertTrue(cast.child() instanceof ElementAt);
    }

    @Test
    public void testRewriteElementAtWithNoMatchingPattern() {
        // Create variant type with schema template
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField));

        // Create element_at expression: variant['string_message'] (no match)
        MockVariantSlot variantSlot = new MockVariantSlot(variantType);
        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("string_message"));

        // Rewrite
        Expression result = rewriteExpression(elementAt);

        // Should NOT be wrapped with Cast
        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }

    @Test
    public void testRewriteElementAtWithEmptySchemaTemplate() {
        // Create variant type without schema template
        VariantType variantType = new VariantType(0);

        // Create element_at expression
        MockVariantSlot variantSlot = new MockVariantSlot(variantType);
        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("any_field"));

        // Rewrite
        Expression result = rewriteExpression(elementAt);

        // Should NOT be wrapped with Cast
        Assertions.assertTrue(result instanceof ElementAt);
        Assertions.assertFalse(result instanceof Cast);
    }

    @Test
    public void testRewriteCompoundExpression() {
        // Create variant type with schema template
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField));

        // Create compound expression: variant['number_latency'] > 100
        MockVariantSlot variantSlot = new MockVariantSlot(variantType);
        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("number_latency"));
        GreaterThan greaterThan = new GreaterThan(elementAt, new BigIntLiteral(100));

        // Rewrite
        Expression result = rewriteExpression(greaterThan);

        // Should be GreaterThan with Cast(ElementAt) on left
        Assertions.assertTrue(result instanceof GreaterThan);
        GreaterThan rewrittenGt = (GreaterThan) result;
        Assertions.assertTrue(rewrittenGt.left() instanceof Cast);
        Cast cast = (Cast) rewrittenGt.left();
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
    }

    @Test
    public void testRewriteMultiplePatterns() {
        // Create variant type with multiple patterns
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantField stringField = new VariantField("string_*", StringType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField, stringField));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // Test number pattern
        ElementAt numberElementAt = new ElementAt(variantSlot, new VarcharLiteral("number_count"));
        Expression numberResult = rewriteExpression(numberElementAt);
        Assertions.assertTrue(numberResult instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) numberResult).getDataType());

        // Test string pattern
        ElementAt stringElementAt = new ElementAt(variantSlot, new VarcharLiteral("string_msg"));
        Expression stringResult = rewriteExpression(stringElementAt);
        Assertions.assertTrue(stringResult instanceof Cast);
        Assertions.assertEquals(StringType.INSTANCE, ((Cast) stringResult).getDataType());
    }

    @Test
    public void testRewriteAndCondition() {
        // Create variant type with schema template
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // Create AND condition: variant['number_a'] > 10 AND variant['number_b'] < 100
        ElementAt elementAtA = new ElementAt(variantSlot, new VarcharLiteral("number_a"));
        ElementAt elementAtB = new ElementAt(variantSlot, new VarcharLiteral("number_b"));
        GreaterThan gt = new GreaterThan(elementAtA, new BigIntLiteral(10));
        LessThan lt = new LessThan(elementAtB, new BigIntLiteral(100));
        And andExpr = new And(gt, lt);

        // Rewrite
        Expression result = rewriteExpression(andExpr);

        // Should be And with Cast on both sides
        Assertions.assertTrue(result instanceof And);
        And rewrittenAnd = (And) result;

        // Left side: Cast(ElementAt) > 10
        Assertions.assertTrue(rewrittenAnd.child(0) instanceof GreaterThan);
        GreaterThan leftGt = (GreaterThan) rewrittenAnd.child(0);
        Assertions.assertTrue(leftGt.left() instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) leftGt.left()).getDataType());

        // Right side: Cast(ElementAt) < 100
        Assertions.assertTrue(rewrittenAnd.child(1) instanceof LessThan);
        LessThan rightLt = (LessThan) rewrittenAnd.child(1);
        Assertions.assertTrue(rightLt.left() instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) rightLt.left()).getDataType());
    }

    @Test
    public void testRewriteOrCondition() {
        // Create variant type with multiple patterns
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantField stringField = new VariantField("string_*", StringType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField, stringField));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // Create OR condition: variant['number_a'] > 10 OR variant['string_b'] = 'test'
        ElementAt elementAtA = new ElementAt(variantSlot, new VarcharLiteral("number_a"));
        ElementAt elementAtB = new ElementAt(variantSlot, new VarcharLiteral("string_b"));
        GreaterThan gt = new GreaterThan(elementAtA, new BigIntLiteral(10));
        EqualTo eq = new EqualTo(elementAtB, new VarcharLiteral("test"));
        Or orExpr = new Or(gt, eq);

        // Rewrite
        Expression result = rewriteExpression(orExpr);

        // Should be Or with Cast on both sides
        Assertions.assertTrue(result instanceof Or);
        Or rewrittenOr = (Or) result;

        // Left side: Cast(ElementAt) > 10 with BIGINT
        Assertions.assertTrue(rewrittenOr.child(0) instanceof GreaterThan);
        GreaterThan leftGt = (GreaterThan) rewrittenOr.child(0);
        Assertions.assertTrue(leftGt.left() instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) leftGt.left()).getDataType());

        // Right side: Cast(ElementAt) = 'test' with STRING
        Assertions.assertTrue(rewrittenOr.child(1) instanceof EqualTo);
        EqualTo rightEq = (EqualTo) rewrittenOr.child(1);
        Assertions.assertTrue(rightEq.left() instanceof Cast);
        Assertions.assertEquals(StringType.INSTANCE, ((Cast) rightEq.left()).getDataType());
    }

    @Test
    public void testFirstMatchWins() {
        // Create variant type with overlapping patterns - first match should win
        // 'num*' matches 'number_val', 'number_*' also matches 'number_val'
        // First pattern 'num*' should be used
        VariantField numField = new VariantField("num*", BigIntType.INSTANCE, "");
        VariantField numberField = new VariantField("number_*", DoubleType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numField, numberField));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // 'number_val' matches both patterns, but 'num*' is first
        ElementAt elementAt = new ElementAt(variantSlot, new VarcharLiteral("number_val"));
        Expression result = rewriteExpression(elementAt);

        Assertions.assertTrue(result instanceof Cast);
        Cast cast = (Cast) result;
        // Should be BIGINT (from 'num*'), not DOUBLE (from 'number_*')
        Assertions.assertEquals(BigIntType.INSTANCE, cast.getDataType());
    }

    @Test
    public void testMixedMatchingAndNonMatching() {
        // Create variant type with one pattern
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // Create condition: variant['number_a'] > variant['other_field']
        // number_a matches, other_field does not
        ElementAt matchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("number_a"));
        ElementAt nonMatchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("other_field"));
        GreaterThan gt = new GreaterThan(matchingElementAt, nonMatchingElementAt);

        // Rewrite
        Expression result = rewriteExpression(gt);

        Assertions.assertTrue(result instanceof GreaterThan);
        GreaterThan rewrittenGt = (GreaterThan) result;

        // Left side should be Cast(ElementAt)
        Assertions.assertTrue(rewrittenGt.left() instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) rewrittenGt.left()).getDataType());

        // Right side should remain as ElementAt (no cast)
        Assertions.assertTrue(rewrittenGt.right() instanceof ElementAt);
        Assertions.assertFalse(rewrittenGt.right() instanceof Cast);
    }

    @Test
    public void testGlobPatternWithQuestionMark() {
        // Test glob pattern with ? (matches single character)
        VariantField field = new VariantField("val?", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // 'val1' should match 'val?'
        ElementAt matchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("val1"));
        Expression matchResult = rewriteExpression(matchingElementAt);
        Assertions.assertTrue(matchResult instanceof Cast);
        Assertions.assertEquals(BigIntType.INSTANCE, ((Cast) matchResult).getDataType());

        // 'val12' should NOT match 'val?' (? matches only one char)
        ElementAt nonMatchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("val12"));
        Expression nonMatchResult = rewriteExpression(nonMatchingElementAt);
        Assertions.assertTrue(nonMatchResult instanceof ElementAt);
        Assertions.assertFalse(nonMatchResult instanceof Cast);
    }

    @Test
    public void testGlobPatternWithBrackets() {
        // Test glob pattern with [...] (character class)
        VariantField field = new VariantField("type_[abc]", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));

        MockVariantSlot variantSlot = new MockVariantSlot(variantType);

        // 'type_a' should match 'type_[abc]'
        ElementAt matchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("type_a"));
        Expression matchResult = rewriteExpression(matchingElementAt);
        Assertions.assertTrue(matchResult instanceof Cast);

        // 'type_d' should NOT match 'type_[abc]'
        ElementAt nonMatchingElementAt = new ElementAt(variantSlot, new VarcharLiteral("type_d"));
        Expression nonMatchResult = rewriteExpression(nonMatchingElementAt);
        Assertions.assertTrue(nonMatchResult instanceof ElementAt);
        Assertions.assertFalse(nonMatchResult instanceof Cast);
    }

    /**
     * Mock Expression class for providing VariantType in tests.
     */
    private static class MockVariantSlot extends Expression {
        private final VariantType variantType;

        public MockVariantSlot(VariantType variantType) {
            super(Collections.emptyList());
            this.variantType = variantType;
        }

        @Override
        public DataType getDataType() {
            return variantType;
        }

        @Override
        public boolean nullable() {
            return true;
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return this;
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return visitor.visit(this, context);
        }

        @Override
        public int arity() {
            return 0;
        }

        @Override
        public Expression child(int index) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public List<Expression> children() {
            return Collections.emptyList();
        }
    }
}
