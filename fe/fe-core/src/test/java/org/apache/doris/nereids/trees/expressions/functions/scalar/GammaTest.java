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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GammaTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSignatureAndNullability() {
        Gamma gamma = new Gamma(new DoubleLiteral(2.5));

        Assertions.assertEquals(DoubleType.INSTANCE, gamma.getDataType());
        Assertions.assertEquals(1, gamma.getSignatures().size());
        FunctionSignature signature = gamma.getSignatures().get(0);
        Assertions.assertEquals(DoubleType.INSTANCE, signature.returnType);
        Assertions.assertEquals(ImmutableList.of(DoubleType.INSTANCE), signature.argumentsTypes);
        // gamma has poles at zero and at every negative integer, so the result stays nullable
        // however non-nullable the argument is
        Assertions.assertTrue(gamma.nullable());
        Assertions.assertEquals("gamma(2.5)", gamma.toSql());
    }

    @Test
    public void testAnalyzedArgumentIsCastToDouble() {
        Expression analyzed = typeCoercion(PARSER.parseExpression("gamma(5)"));

        Assertions.assertTrue(analyzed instanceof Gamma);
        Assertions.assertEquals(DoubleType.INSTANCE, analyzed.getDataType());
        Assertions.assertEquals(DoubleType.INSTANCE, analyzed.child(0).getDataType());
        Assertions.assertTrue(analyzed.nullable());
    }

    @Test
    public void testAcceptRebuildsThroughWithChildren() {
        Gamma gamma = new Gamma(new DoubleLiteral(2.5));

        Expression rewritten = gamma.accept(new DefaultExpressionRewriter<Void>() {
            @Override
            public Expression visitDoubleLiteral(DoubleLiteral doubleLiteral, Void context) {
                return new DoubleLiteral(doubleLiteral.getValue() + 1.0);
            }
        }, null);

        Assertions.assertEquals(new Gamma(new DoubleLiteral(3.5)), rewritten);
        Assertions.assertEquals(DoubleType.INSTANCE, rewritten.getDataType());
    }

    @Test
    public void testWithChildrenRejectsWrongArity() {
        Gamma gamma = new Gamma(new DoubleLiteral(2.5));

        // The arity guard is the only branch in this class, so the failing direction has to be
        // exercised as well: a partially covered line counts as uncovered for the increment
        // coverage gate.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> gamma.withChildren(ImmutableList.of(new DoubleLiteral(1.0), new DoubleLiteral(2.0))));
    }
}
