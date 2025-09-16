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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.VarBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class VarBinaryLiteralTest {

    private static byte[] bytes(String s) {
        return s.getBytes();
    }

    @Test
    public void testBasicProperties() {
        byte[] data = bytes("hello");
        VarBinaryLiteral vb = new VarBinaryLiteral(data);

        // type & value
        Assertions.assertEquals(VarBinaryType.INSTANCE, vb.getDataType());
        Assertions.assertTrue(Arrays.equals(data, (byte[]) vb.getValue()));

        // toString now returns upper-case hex representation of raw bytes
        Assertions.assertEquals("68656C6C6F".toUpperCase(), vb.toString());

        // equals same content
        VarBinaryLiteral vb2 = new VarBinaryLiteral(bytes("hello"));
        Assertions.assertEquals(vb, vb2);

        // getDouble is 0
        Assertions.assertEquals(0.0, vb.getDouble());
    }

    @Test
    public void testEqualsAndNotEquals() {
        VarBinaryLiteral a = new VarBinaryLiteral(bytes("abc"));
        VarBinaryLiteral b = new VarBinaryLiteral(bytes("abc"));
        VarBinaryLiteral c = new VarBinaryLiteral(bytes("abd"));

        Assertions.assertEquals(a, b);
        Assertions.assertNotEquals(a, c);
    }

    @Test
    public void testCompareTo() {
        VarBinaryLiteral a = new VarBinaryLiteral(bytes("x"));
        VarBinaryLiteral b = new VarBinaryLiteral(bytes("y"));
        Assertions.assertEquals(-1, a.compareTo(b));
        Assertions.assertEquals(0, a.compareTo(new VarBinaryLiteral(bytes("x"))));
    }

    @Test
    public void testLegacyLiteralRoundtrip() {
        VarBinaryLiteral vb = new VarBinaryLiteral(bytes("hello"));
        LiteralExpr legacy = vb.toLegacyLiteral();
        // legacy getStringValue() returns string with same bytes (ISO_8859_1), should be human readable here
        Assertions.assertEquals("hello", legacy.getStringValue());
        // toString is hex
        Assertions.assertEquals("68656C6C6F", vb.toString());
    }

    @Test
    public void testVisitorDispatch() {
        VarBinaryLiteral vb = new VarBinaryLiteral(bytes("hi"));
        ExpressionVisitor<String, Void> visitor = new ExpressionVisitor<String, Void>() {
            @Override
            public String visitVarBinaryLiteral(VarBinaryLiteral literal, Void context) {
                return "visited:" + literal.toString(); // hex
            }

            @Override
            public String visit(Expression expr, Void context) {
                return "default";
            }
        };
        Assertions.assertEquals("visited:6869", vb.accept(visitor, null));
    }

    @Test
    public void testEmbeddedNullBytes() {
        byte[] raw = new byte[] { 'a', 0, 'b', 0 }; // 61 00 62 00
        VarBinaryLiteral vb = new VarBinaryLiteral(raw);
        String hex = vb.toString();
        Assertions.assertEquals("61006200", hex);
        VarBinaryLiteral vb2 = new VarBinaryLiteral(new byte[] { 'a', 0, 'b', 0 });
        Assertions.assertEquals(vb, vb2);
    }
}
