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
import org.apache.doris.nereids.trees.expressions.functions.scalar.UrlDecode;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;

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

    private void assertUrlDecodeValue(String encoded, String expected) {
        Expression result = ExpressionEvaluator.INSTANCE.eval(new UrlDecode(new StringLiteral(encoded)));
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

    @Test
    void testParseUrlStopsAtTheAuthority() {
        // A ':' in the path is not a port separator, and an '@' in the path is not a
        // userinfo separator.
        assertParseUrl("http://example.com/a:b", "HOST", "example.com");
        assertParseUrlIsNull("http://example.com/a:b", "PORT");
        assertParseUrl("http://example.com/a:b", "AUTHORITY", "example.com");
        assertParseUrl("http://example.com/a@b:c", "HOST", "example.com");
        assertParseUrlIsNull("http://example.com/a@b:c", "USERINFO");
        // A ':' in the query or the fragment is not a port separator either.
        assertParseUrlIsNull("http://example.com/p?r=http:8080", "PORT");
        assertParseUrl("http://example.com#f:1", "HOST", "example.com");
        assertParseUrlIsNull("http://example.com#f:1", "PORT");
        assertParseUrl("http://example.com?x=1", "AUTHORITY", "example.com");
        // A real port and a real userinfo are still returned.
        assertParseUrl("http://user:pass@example.com:80/a:b", "HOST", "example.com");
        assertParseUrl("http://user:pass@example.com:80/a:b", "PORT", "80");
        assertParseUrl("http://user:pass@example.com:80/a:b", "USERINFO", "user:pass");
        assertParseUrl("http://user:pass@example.com:80/a:b", "AUTHORITY",
                "user:pass@example.com:80");
    }

    private void assertParseUrl(String url, String part, String expected) {
        Expression result = StringArithmetic.parseurl(
                new StringLiteral(url), new StringLiteral(part));
        Assertions.assertEquals(expected, ((StringLikeLiteral) result).getValue());
    }

    private void assertParseUrlIsNull(String url, String part) {
        Expression result = StringArithmetic.parseurl(
                new StringLiteral(url), new StringLiteral(part));
        Assertions.assertTrue(result instanceof NullLiteral, url + " " + part);
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
