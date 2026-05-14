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

/**
 * Tests for JsonLiteral surrogate validation (RFC 8259 §8.2).
 */
public class JsonLiteralTest {

    // --- valid inputs ---

    @Test
    public void testValidAsciiString() {
        // plain ASCII string in JSON is always valid
        Assertions.assertDoesNotThrow(() -> new JsonLiteral("\"hello\""));
    }

    @Test
    public void testValidObject() {
        Assertions.assertDoesNotThrow(() -> new JsonLiteral("{\"key\":\"value\"}"));
    }

    @Test
    public void testValidArray() {
        Assertions.assertDoesNotThrow(() -> new JsonLiteral("[1, \"abc\", true]"));
    }

    @Test
    public void testValidSurrogatePair() {
        // \uD83D\uDE00 is a valid surrogate pair (U+1F600, 😀)
        // JSON escape: "\uD83D\uDE00"
        Assertions.assertDoesNotThrow(() -> new JsonLiteral("\"\\uD83D\\uDE00\""));
    }

    @Test
    public void testValidSurrogatePairInObject() {
        Assertions.assertDoesNotThrow(() -> new JsonLiteral("{\"emoji\":\"\\uD83D\\uDE00\"}"));
    }

    // --- lone high surrogate ---

    @Test
    public void testLoneHighSurrogateTopLevel() {
        // "\uD800" — lone high surrogate, no paired low surrogate
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("\"\\uD800\""));
        Assertions.assertTrue(ex.getMessage().contains("lone high surrogate"),
                "Expected 'lone high surrogate' in: " + ex.getMessage());
    }

    @Test
    public void testLoneHighSurrogateInObject() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("{\"k\":\"\\uD800\"}"));
        Assertions.assertTrue(ex.getMessage().contains("lone high surrogate"));
    }

    @Test
    public void testLoneHighSurrogateInArray() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("[\"\\uD800\"]"));
        Assertions.assertTrue(ex.getMessage().contains("lone high surrogate"));
    }

    @Test
    public void testHighSurrogateFollowedByNonLow() {
        // \uD800\u0041 — high surrogate followed by 'A', not a low surrogate
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("\"\\uD800A\""));
        Assertions.assertTrue(ex.getMessage().contains("lone high surrogate"));
    }

    // --- lone low surrogate ---

    @Test
    public void testLoneLowSurrogateTopLevel() {
        // "\uDC00" — lone low surrogate
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("\"\\uDC00\""));
        Assertions.assertTrue(ex.getMessage().contains("lone low surrogate"),
                "Expected 'lone low surrogate' in: " + ex.getMessage());
    }

    @Test
    public void testLoneLowSurrogateInObject() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new JsonLiteral("{\"k\":\"\\uDC00\"}"));
        Assertions.assertTrue(ex.getMessage().contains("lone low surrogate"));
    }
}
