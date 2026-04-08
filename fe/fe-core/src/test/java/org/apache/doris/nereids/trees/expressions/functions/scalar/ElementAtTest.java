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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for ElementAt scalar function computeSignature behavior on VariantType.
 */
public class ElementAtTest {

    @Test
    public void testComputeSignatureSingleVariant() {
        VariantType variantType = new VariantType(100);
        ElementAt elementAt = new ElementAt(new MockVariantExpression(variantType), new VarcharLiteral("k"));
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE, VarcharType.SYSTEM_DEFAULT);

        signature = elementAt.computeSignature(signature);

        Assertions.assertTrue(signature.returnType instanceof VariantType);
        Assertions.assertEquals(100, ((VariantType) signature.returnType).getVariantMaxSubcolumnsCount());

        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(100, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());

        Assertions.assertTrue(signature.getArgType(1) instanceof VarcharType);
    }

    @Test
    public void testComputeSignatureWithNullLiteral() {
        ElementAt elementAt = new ElementAt(new NullLiteral(), new VarcharLiteral("k"));
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE, VarcharType.SYSTEM_DEFAULT);

        signature = elementAt.computeSignature(signature);

        Assertions.assertTrue(signature.returnType instanceof VariantType);
        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(0, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
    }

    @Test
    public void testComputeSignatureMultipleVariantsThrowsException() {
        VariantType variantType1 = new VariantType(150);
        VariantType variantType2 = new VariantType(250);
        ElementAt elementAt = new ElementAt(new MockVariantExpression(variantType1),
                new MockVariantExpression(variantType2));
        FunctionSignature signature = FunctionSignature.ret(VariantType.INSTANCE)
                .args(VariantType.INSTANCE, VariantType.INSTANCE);

        // New behavior: ElementAt only looks at the 1st argument's VariantType and keeps its max-subcolumns-count.
        signature = elementAt.computeSignature(signature);
        Assertions.assertTrue(signature.returnType instanceof VariantType);
        Assertions.assertEquals(150, ((VariantType) signature.returnType).getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(signature.getArgType(0) instanceof VariantType);
        Assertions.assertEquals(150, ((VariantType) signature.getArgType(0)).getVariantMaxSubcolumnsCount());
    }

    /**
     * Mock Expression class for providing VariantType in computeSignature tests.
     */
    private static class MockVariantExpression extends Expression {
        private final VariantType variantType;

        public MockVariantExpression(VariantType variantType) {
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
            throw new IndexOutOfBoundsException("MockVariantExpression has no children");
        }

        @Override
        public List<Expression> children() {
            return Collections.emptyList();
        }
    }
}


