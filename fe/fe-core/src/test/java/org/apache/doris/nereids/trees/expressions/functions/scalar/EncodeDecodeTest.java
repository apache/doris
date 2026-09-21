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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EncodeDecodeTest {

    @Test
    public void testEncodeExpressionContract() {
        StringLiteral source = new StringLiteral("hello");
        StringLiteral characterSet = new StringLiteral("UTF-8");
        Encode encode = new Encode(source, characterSet);

        Assertions.assertEquals("encode", encode.getName());
        Assertions.assertEquals(2, encode.arity());
        Assertions.assertSame(source, encode.child(0));
        Assertions.assertSame(characterSet, encode.child(1));

        FunctionSignature signature = encode.getSignatures().get(0);
        Assertions.assertEquals(VarBinaryType.INSTANCE, signature.returnType);
        Assertions.assertEquals(StringType.INSTANCE, signature.getArgType(0));
        Assertions.assertEquals(StringType.INSTANCE, signature.getArgType(1));

        StringLiteral replacementSource = new StringLiteral("world");
        StringLiteral replacementCharacterSet = new StringLiteral("UTF-16");
        Encode rewritten = encode.withChildren(
                ImmutableList.of(replacementSource, replacementCharacterSet));
        Assertions.assertNotSame(encode, rewritten);
        Assertions.assertSame(replacementSource, rewritten.child(0));
        Assertions.assertSame(replacementCharacterSet, rewritten.child(1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> encode.withChildren(ImmutableList.of(replacementSource)));
    }

    @Test
    public void testDecodeExpressionContract() {
        VarBinaryLiteral binary = new VarBinaryLiteral(new byte[] {0x68, 0x69});
        StringLiteral characterSet = new StringLiteral("UTF-8");
        Decode decode = new Decode(binary, characterSet);

        Assertions.assertEquals("decode", decode.getName());
        Assertions.assertEquals(2, decode.arity());
        Assertions.assertSame(binary, decode.child(0));
        Assertions.assertSame(characterSet, decode.child(1));

        FunctionSignature signature = decode.getSignatures().get(0);
        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(VarBinaryType.INSTANCE, signature.getArgType(0));
        Assertions.assertEquals(StringType.INSTANCE, signature.getArgType(1));

        VarBinaryLiteral replacementBinary = new VarBinaryLiteral(new byte[] {0x41});
        StringLiteral replacementCharacterSet = new StringLiteral("US-ASCII");
        Decode rewritten = decode.withChildren(
                ImmutableList.of(replacementBinary, replacementCharacterSet));
        Assertions.assertNotSame(decode, rewritten);
        Assertions.assertSame(replacementBinary, rewritten.child(0));
        Assertions.assertSame(replacementCharacterSet, rewritten.child(1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> decode.withChildren(ImmutableList.of(replacementBinary)));
    }

    @Test
    public void testVisitorDelegatesToScalarFunction() {
        Encode encode = new Encode(new StringLiteral("hello"), new StringLiteral("UTF-8"));
        Decode decode = new Decode(new VarBinaryLiteral(new byte[] {0x68, 0x69}),
                new StringLiteral("UTF-8"));
        ExpressionVisitor<Expression, Void> visitor = new ExpressionVisitor<Expression, Void>() {
            @Override
            public Expression visit(Expression expression, Void context) {
                return expression;
            }
        };

        Assertions.assertSame(encode, encode.accept(visitor, null));
        Assertions.assertSame(decode, decode.accept(visitor, null));
    }

    @Test
    public void testCharacterSetMustBeConstant() {
        SlotReference characterSetColumn = new SlotReference("charset", StringType.INSTANCE);
        Encode encode = new Encode(new StringLiteral("hello"), characterSetColumn);
        Decode decode = new Decode(new VarBinaryLiteral(new byte[] {0x68, 0x69}),
                characterSetColumn);

        AnalysisException encodeException = Assertions.assertThrows(
                AnalysisException.class, encode::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(encodeException.getMessage().contains(
                "second argument of function encode must be constant"));
        AnalysisException decodeException = Assertions.assertThrows(
                AnalysisException.class, decode::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(decodeException.getMessage().contains(
                "second argument of function decode must be constant"));

        Assertions.assertDoesNotThrow(new Encode(new StringLiteral("hello"),
                new Upper(new StringLiteral("utf-8")))::checkLegalityBeforeTypeCoercion);
        Assertions.assertDoesNotThrow(new Decode(new VarBinaryLiteral(new byte[] {0x68, 0x69}),
                new StringLiteral("UTF-8"))::checkLegalityBeforeTypeCoercion);
        Assertions.assertFalse(new Encode(new StringLiteral("hello"),
                new StringLiteral("UTF-8")).foldable());
        Assertions.assertFalse(new Decode(new VarBinaryLiteral(new byte[] {0x68, 0x69}),
                new StringLiteral("UTF-8")).foldable());
    }
}
