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

import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonLiteralTest {

    @Test
    public void testCompareSameJsonLiterals() {
        JsonLiteral a = new JsonLiteral("[1,2,3]");
        JsonLiteral b = new JsonLiteral("[1,2,3]");
        Assertions.assertEquals(0, a.compareTo(b));
        Assertions.assertEquals(0, b.compareTo(a));
    }

    @Test
    public void testCompareDifferentJsonLiterals() {
        JsonLiteral a = new JsonLiteral("{\"a\":1}");
        JsonLiteral b = new JsonLiteral("{\"b\":2}");
        Assertions.assertTrue(a.compareTo(b) < 0);
        Assertions.assertTrue(b.compareTo(a) > 0);
    }

    @Test
    public void testCompareJsonObjectLiterals() {
        JsonLiteral a = new JsonLiteral("{\"key\":\"value1\"}");
        JsonLiteral b = new JsonLiteral("{\"key\":\"value2\"}");
        Assertions.assertTrue(a.compareTo(b) < 0);
        Assertions.assertTrue(b.compareTo(a) > 0);
    }

    @Test
    public void testCompareWithNullLiteral() {
        JsonLiteral json = new JsonLiteral("[1,2]");
        NullLiteral nullLit = NullLiteral.INSTANCE;
        Assertions.assertEquals(1, json.compareTo(nullLit));
    }

    @Test
    public void testCompareWithMaxLiteral() {
        JsonLiteral json = new JsonLiteral("[1,2]");
        MaxLiteral maxLit = new MaxLiteral(json.dataType);
        Assertions.assertEquals(-1, json.compareTo(maxLit));
    }

    @Test
    public void testInvalidJsonThrows() {
        Assertions.assertThrows(AnalysisException.class, () -> new JsonLiteral("not valid json"));
    }

    @Test
    public void testGetValue() {
        JsonLiteral json = new JsonLiteral("{\"a\":1}");
        Assertions.assertEquals("{\"a\":1}", json.getValue());
    }

    @Test
    public void testGetStringValue() {
        JsonLiteral json = new JsonLiteral("[1,2,3]");
        Assertions.assertEquals("[1,2,3]", json.getStringValue());
    }

    @Test
    public void testLegacyCompareLiteral() throws Exception {
        org.apache.doris.analysis.JsonLiteral a =
                new org.apache.doris.analysis.JsonLiteral("[1,2,3]");
        org.apache.doris.analysis.JsonLiteral b =
                new org.apache.doris.analysis.JsonLiteral("[1,2,3]");
        Assertions.assertEquals(0, a.compareLiteral(b));
    }

    @Test
    public void testLegacyCompareDifferent() throws Exception {
        org.apache.doris.analysis.JsonLiteral a =
                new org.apache.doris.analysis.JsonLiteral("{\"a\":1}");
        org.apache.doris.analysis.JsonLiteral b =
                new org.apache.doris.analysis.JsonLiteral("{\"b\":2}");
        Assertions.assertTrue(a.compareLiteral(b) < 0);
        Assertions.assertTrue(b.compareLiteral(a) > 0);
    }

    @Test
    public void testToLegacyAndCompare() {
        JsonLiteral a = new JsonLiteral("[1,2,3]");
        JsonLiteral b = new JsonLiteral("[1,2,3]");
        Assertions.assertEquals(0, a.toLegacyLiteral().compareLiteral(b.toLegacyLiteral()));
    }

    @Test
    public void testCompareWithDifferentTypeThrows() {
        JsonLiteral json = new JsonLiteral("[1,2]");
        StringLiteral str = new StringLiteral("[1,2]");
        Assertions.assertThrows(RuntimeException.class, () -> json.compareTo(str));
    }
}
