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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Decode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Encode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UrlDecode;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StringArithmeticTest {

    @Test
    void testFieldMatchesFloatNaN() {
        IntegerLiteral result = (IntegerLiteral) StringArithmetic.fieldFloat(new FloatLiteral(Float.NaN),
                new FloatLiteral(Float.NaN));

        Assertions.assertEquals(1, result.getValue());
    }

    @Test
    void testFieldMatchesDoubleNaN() {
        IntegerLiteral result = (IntegerLiteral) StringArithmetic.fieldDouble(new DoubleLiteral(Double.NaN),
                new DoubleLiteral(Double.NaN));

        Assertions.assertEquals(1, result.getValue());
    }

    @Test
    void testFieldMatchesFloatSignedZero() {
        IntegerLiteral positiveZero = (IntegerLiteral) StringArithmetic.fieldFloat(new FloatLiteral(0.0f),
                new FloatLiteral(-0.0f));
        IntegerLiteral negativeZero = (IntegerLiteral) StringArithmetic.fieldFloat(new FloatLiteral(-0.0f),
                new FloatLiteral(0.0f));

        Assertions.assertEquals(1, positiveZero.getValue());
        Assertions.assertEquals(1, negativeZero.getValue());
    }

    @Test
    void testFieldMatchesDoubleSignedZero() {
        IntegerLiteral positiveZero = (IntegerLiteral) StringArithmetic.fieldDouble(new DoubleLiteral(0.0),
                new DoubleLiteral(-0.0));
        IntegerLiteral negativeZero = (IntegerLiteral) StringArithmetic.fieldDouble(new DoubleLiteral(-0.0),
                new DoubleLiteral(0.0));

        Assertions.assertEquals(1, positiveZero.getValue());
        Assertions.assertEquals(1, negativeZero.getValue());
    }

    @Test
    void testFieldComparesTimestampNsNanoseconds() {
        IntegerLiteral result = (IntegerLiteral) StringArithmetic.fieldTimeStampNs(
                new TimeStampNsLiteral("1970-01-01 00:00:00.000000002"),
                new TimeStampNsLiteral("1970-01-01 00:00:00.000000001"),
                new TimeStampNsLiteral("1970-01-01 00:00:00.000000002"));

        Assertions.assertEquals(2, result.getValue());
    }

    @Test
    void testUrlDecodeDoesNotFoldInvalidUtf8() {
        String[] invalidUtf8Values = {"%80", "%C0%AF", "%E0%80%80", "%ED%A0%80", "%FF"};
        for (String value : invalidUtf8Values) {
            UrlDecode urlDecode = new UrlDecode(new StringLiteral(value));
            Assertions.assertSame(urlDecode, ExpressionEvaluator.INSTANCE.eval(urlDecode), value);
        }
    }

    @Test
    void testUrlDecodeStillFoldsValidUtf8() {
        assertUrlDecodeValue("%E4%B8%AD+text", "中 text");
        assertUrlDecodeValue("%EF%BF%BD", "�");
    }

    @Test
    void testEncodeFoldsSupportedCharsets() {
        assertEncodeValue("A", "US-ASCII", new byte[] {0x41});
        assertEncodeValue("é", "ISO-8859-1", new byte[] {(byte) 0xE9});
        assertEncodeValue("中", "UTF-8", new byte[] {(byte) 0xE4, (byte) 0xB8, (byte) 0xAD});
        assertEncodeValue("中", "UTF-16BE", new byte[] {0x4E, 0x2D});
        assertEncodeValue("中", "UTF-16LE", new byte[] {0x2D, 0x4E});
        assertEncodeValue("中", "utf-16", new byte[] {(byte) 0xFE, (byte) 0xFF, 0x4E, 0x2D});
        assertEncodeValue("😀", "UTF-16BE", new byte[] {(byte) 0xD8, 0x3D, (byte) 0xDE, 0x00});
        assertEncodeValue("", "UTF-16", new byte[] {});
    }

    @Test
    void testDecodeFoldsSupportedCharsets() {
        assertDecodeValue(new byte[] {0x41}, "US-ASCII", "A");
        assertDecodeValue(new byte[] {(byte) 0xE9}, "ISO-8859-1", "é");
        assertDecodeValue(new byte[] {(byte) 0xE4, (byte) 0xB8, (byte) 0xAD}, "UTF-8", "中");
        assertDecodeValue(new byte[] {0x4E, 0x2D}, "UTF-16BE", "中");
        assertDecodeValue(new byte[] {0x2D, 0x4E}, "UTF-16LE", "中");
        assertDecodeValue(new byte[] {(byte) 0xFE, (byte) 0xFF, 0x4E, 0x2D}, "UTF-16", "中");
        assertDecodeValue(new byte[] {(byte) 0xFF, (byte) 0xFE, 0x2D, 0x4E}, "utf-16", "中");
        assertDecodeValue(new byte[] {0x4E, 0x2D}, "UTF-16", "中");
        assertDecodeValue(new byte[] {(byte) 0xFE, (byte) 0xFF}, "UTF-16", "");
        assertDecodeValue(new byte[] {(byte) 0xD8, 0x3D, (byte) 0xDE, 0x00}, "UTF-16BE", "😀");
        assertDecodeValue(new byte[] {}, "UTF-16", "");
    }

    @Test
    void testInvalidCharacterConversionDoesNotFold() {
        Encode unmappable = new Encode(new StringLiteral("中"), new StringLiteral("US-ASCII"));
        Decode malformed = new Decode(new VarBinaryLiteral(new byte[] {(byte) 0xE4, (byte) 0xB8}),
                new StringLiteral("UTF-8"));
        Encode unsupported = new Encode(new StringLiteral("text"), new StringLiteral("GBK"));

        Assertions.assertSame(unmappable, ExpressionEvaluator.INSTANCE.eval(unmappable));
        Assertions.assertSame(malformed, ExpressionEvaluator.INSTANCE.eval(malformed));
        Assertions.assertSame(unsupported, ExpressionEvaluator.INSTANCE.eval(unsupported));
    }

    @Test
    void testUnicodeCaseFoldedCharacterSetDoesNotFold() {
        Encode encode = new Encode(new StringLiteral("A"), new StringLiteral("U\u017F-ASCII"));
        Decode decode = new Decode(new VarBinaryLiteral(new byte[] {0x41}),
                new StringLiteral("U\u017F-ASCII"));

        Assertions.assertSame(encode, ExpressionEvaluator.INSTANCE.eval(encode));
        Assertions.assertSame(decode, ExpressionEvaluator.INSTANCE.eval(decode));
    }

    private void assertUrlDecodeValue(String encoded, String expected) {
        Expression result = ExpressionEvaluator.INSTANCE.eval(new UrlDecode(new StringLiteral(encoded)));
        Assertions.assertEquals(expected, ((StringLikeLiteral) result).getValue());
    }

    private void assertEncodeValue(String value, String characterSet, byte[] expected) {
        Expression result = ExpressionEvaluator.INSTANCE.eval(
                new Encode(new StringLiteral(value), new StringLiteral(characterSet)));
        Assertions.assertArrayEquals(expected, (byte[]) ((VarBinaryLiteral) result).getValue());
    }

    private void assertDecodeValue(byte[] value, String characterSet, String expected) {
        Expression result = ExpressionEvaluator.INSTANCE.eval(
                new Decode(new VarBinaryLiteral(value), new StringLiteral(characterSet)));
        Assertions.assertEquals(expected, ((StringLikeLiteral) result).getValue());
    }

    @Test
    void testParseUrlQueryStopsAtFragment() {
        // The only '?' is inside the fragment, so the url has no query component.
        assertParseUrlQueryIsNull("http://h/p#f?k=v");
        assertParseUrlQueryIsNull("http://h/p#f/?#k=v");
        // The query component starts at the first '?' and ends before the fragment.
        assertParseUrlQuery("http://h/p?k=1#f&k=2", "k=1");
        assertParseUrlQuery("http://h/p?a=1&k=2", "a=1&k=2");
    }

    @Test
    void testExtractUrlParameterStopsAtFragment() {
        // The only '?' is inside the fragment, so the url has no parameters.
        assertExtractUrlParameter("http://h/p#f?k=v", "k", "");
        // The parameters end before the fragment.
        assertExtractUrlParameter("http://h/p?k=1#f&k=2", "k", "1");
        assertExtractUrlParameter("http://h/p?k1=aa&k2=bb#f", "k2", "bb");
    }

    private void assertParseUrlQuery(String url, String expected) {
        Expression result = StringArithmetic.parseurl(
                new StringLiteral(url), new StringLiteral("QUERY"));
        Assertions.assertEquals(expected, ((StringLikeLiteral) result).getValue());
    }

    private void assertParseUrlQueryIsNull(String url) {
        Expression result = StringArithmetic.parseurl(
                new StringLiteral(url), new StringLiteral("QUERY"));
        Assertions.assertTrue(result instanceof NullLiteral, url);
    }

    private void assertExtractUrlParameter(String url, String parameter, String expected) {
        Expression result = StringArithmetic.extractUrlParameter(
                new StringLiteral(url), new StringLiteral(parameter));
        Assertions.assertEquals(expected, ((StringLikeLiteral) result).getValue());
    }
}
