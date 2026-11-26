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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TrimArrayTest {

    @Test
    void testPropertiesAndWithChildren() {
        ArrayLiteral array = new ArrayLiteral(ImmutableList.of(
                new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));
        BigIntLiteral size = new BigIntLiteral(1);
        TrimArray trimArray = new TrimArray(array, size);

        Assertions.assertEquals("trim_array", trimArray.getName());
        Assertions.assertEquals(2, trimArray.arity());
        Assertions.assertEquals(TrimArray.SIGNATURES, trimArray.getSignatures());
        Assertions.assertEquals(array, trimArray.child(0));
        Assertions.assertEquals(size, trimArray.child(1));
        Assertions.assertFalse(trimArray.nullable());

        ArrayLiteral newArray = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(4)));
        BigIntLiteral newSize = new BigIntLiteral(0);
        TrimArray rewritten = trimArray.withChildren(ImmutableList.of(newArray, newSize));
        Assertions.assertNotSame(trimArray, rewritten);
        Assertions.assertEquals(newArray, rewritten.child(0));
        Assertions.assertEquals(newSize, rewritten.child(1));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> trimArray.withChildren(ImmutableList.of(array)));
    }

    @Test
    void testVisitorDispatch() {
        TrimArray trimArray = new TrimArray(
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1))), new BigIntLiteral(0));
        ExpressionVisitor<String, Void> visitor = new ExpressionVisitor<String, Void>() {
            @Override
            public String visit(Expression expression, Void context) {
                return "expression";
            }

            @Override
            public String visitTrimArray(TrimArray function, Void context) {
                return function.getName();
            }
        };

        Assertions.assertEquals("trim_array", trimArray.accept(visitor, null));
    }
}
