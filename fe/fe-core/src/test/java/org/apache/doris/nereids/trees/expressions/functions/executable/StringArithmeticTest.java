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
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringArithmeticTest {
    @Test
    public void testLike() {
        // Basic pattern matching
        Assertions.assertTrue(likeTest("abc", "abc"), "Exact match");
        Assertions.assertTrue(likeTest("abc", "a%"), "Prefix wildcard");
        Assertions.assertTrue(likeTest("abc", "%c"), "Suffix wildcard");
        Assertions.assertTrue(likeTest("abc", "a%c"), "Prefix and suffix wildcard");
        Assertions.assertTrue(likeTest("abc", "ab%"), "Trailing wildcard");
        Assertions.assertFalse(likeTest("abc", "a_"), "Underscore with missing character");

        // Underscore matching (single character)
        Assertions.assertTrue(likeTest("a1c", "a_c"), "Underscore matches digit");
        Assertions.assertTrue(likeTest("a c", "a_c"), "Underscore matches space");
        Assertions.assertFalse(likeTest("ac", "a_c"), "Underscore requires exactly one character");

        // Escaped wildcards (% and _)
        Assertions.assertTrue(likeTest("a%c", "a\\%c"), "Escaped percent sign");
        Assertions.assertTrue(likeTest("a_c", "a\\_c"), "Escaped underscore");
        Assertions.assertFalse(likeTest("abc", "a\\%c"), "Escaped percent prevents wildcard");
        Assertions.assertFalse(likeTest("axc", "a\\_c"), "Escaped underscore prevents wildcard");
        Assertions.assertTrue(likeTest("100%_discount", "100\\%\\_discount"), "Mixed escaped wildcards");

        // Regular expression meta-character testing
        Assertions.assertTrue(likeTest("a.b", "a.b"), "Literal dot character");
        Assertions.assertFalse(likeTest("axb", "a.b"), "Dot matches only literal dot");
        Assertions.assertTrue(likeTest("a*b", "a*b"), "Literal asterisk");
        Assertions.assertTrue(likeTest("a*b", "a%*b"), "Mixed wildcards and asterisk");
        Assertions.assertTrue(likeTest("a?b", "a?b"), "Literal question mark");
        Assertions.assertFalse(likeTest("a_b", "a?b"), "Question mark doesn't match underscore");
        Assertions.assertTrue(likeTest("a[b]c", "a[b]c"), "Literal square brackets");
        Assertions.assertTrue(likeTest("a]c", "a]c"), "Literal closing bracket");
        Assertions.assertTrue(likeTest("a(b)c", "a(b)c"), "Literal parentheses");
        Assertions.assertTrue(likeTest("a{b}c", "a{b}c"), "Literal curly braces");
        Assertions.assertTrue(likeTest("a+b", "a+b"), "Literal plus sign");

        // Escaped meta-characters
        Assertions.assertTrue(likeTest("file.txt", "file\\.txt"), "Escaped dot in filename");
        Assertions.assertTrue(likeTest("*.doc?", "\\*\\.doc\\?"), "Mixed escaped meta-characters");
        Assertions.assertTrue(likeTest("regex$special", "regex\\$special"), "Escaped dollar sign");
        Assertions.assertTrue(likeTest("a\\b", "a\\\\b"), "Escaped backslash (matches literal backslash)");

        // Boundary cases
        Assertions.assertTrue(likeTest("", ""), "Empty string match");
        Assertions.assertTrue(likeTest("", "%"), "Wildcard matches empty string");
        Assertions.assertFalse(likeTest("", "_"), "Underscore requires one character");
        Assertions.assertTrue(likeTest("a", "_"), "Single character match");
        Assertions.assertFalse(likeTest("abc", "abcd"), "Partial match failure");
        Assertions.assertTrue(likeTest("abc", "abc%"), "Trailing wildcard with full match");

        // Null safety
        Assertions.assertThrows(NullPointerException.class, () -> likeTest(null, "%"), "Null text should throw");
        Assertions.assertThrows(NullPointerException.class, () -> likeTest("text", null), "Null pattern should throw");

        // Complex real-world patterns
        Assertions.assertTrue(likeTest("user@example.com", "%@%.com"), "Email pattern matching");
        Assertions.assertTrue(likeTest("/home/user/file.txt", "%/user/%.txt"), "File path pattern");
        Assertions.assertTrue(likeTest("2023-10-25", "____-__-__"), "Date underscore pattern");
        Assertions.assertTrue(likeTest("price: $19.99", "price: $%_._%"), "Price pattern");
        Assertions.assertTrue(likeTest("func(a, b)", "func%(%, %)%"), "Function call pattern");
        Assertions.assertTrue(likeTest("escaped[value]", "escaped\\[value\\]"), "Escaped brackets in value");

        // Wildcard edge cases
        Assertions.assertTrue(likeTest("any text", "%"), "Single percent matches everything");
        Assertions.assertFalse(likeTest("anything", "_"), "Single underscore fails for long strings");
        Assertions.assertTrue(likeTest("abc", "a%c%"), "Redundant wildcards");
        Assertions.assertTrue(likeTest("a%c", "a%%c"), "Adjacent wildcards");
    }


    private boolean likeTest(String input, String pattern) {
        Expression result = StringArithmetic.like(new StringLiteral(input), new StringLiteral(pattern));
        Assertions.assertInstanceOf(BooleanLiteral.class, result);
        return ((BooleanLiteral) result).getValue();
    }
}
