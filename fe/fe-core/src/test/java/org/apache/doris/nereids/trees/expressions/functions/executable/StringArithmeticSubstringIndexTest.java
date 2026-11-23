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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringArithmeticSubstringIndexTest {

    private String fold(String str, String delim, int count) {
        StringLiteral input = new StringLiteral(str);
        Expression result = StringArithmetic.substringIndex(
                input, new StringLiteral(delim), new IntegerLiteral(count));
        Assertions.assertEquals(StringType.INSTANCE, result.getDataType());
        return ((StringLiteral) result).getValue();
    }

    @Test
    public void testSubstringIndexNegativeCountOverlappingDelimiter() {
        // Negative counts search delimiters from right to left without overlap.
        Assertions.assertEquals("", fold("aaa", "aa", -1));
        Assertions.assertEquals("", fold("aaaaa", "aa", -1));
        Assertions.assertEquals("aa", fold("aaaaa", "aa", -2));
        Assertions.assertEquals("aa", fold("aaaa", "aa", -2));
    }

    @Test
    public void testSubstringIndexRegularCases() {
        Assertions.assertEquals("www", fold("www.doris.apache.org", ".", 1));
        Assertions.assertEquals("www.doris", fold("www.doris.apache.org", ".", 2));
        Assertions.assertEquals("apache.org", fold("www.doris.apache.org", ".", -2));
        Assertions.assertEquals("org", fold("www.doris.apache.org", ".", -1));
        // not enough delimiters: return the whole string
        Assertions.assertEquals("www.doris.apache.org", fold("www.doris.apache.org", ".", 5));
        Assertions.assertEquals("www.doris.apache.org", fold("www.doris.apache.org", ".", -5));
        Assertions.assertEquals("", fold("www.doris.apache.org", ".", 0));
        // empty delimiter returns the delimiter literal itself
        Assertions.assertEquals("", fold("www.doris.apache.org", "", 1));
        // overlapping delimiter, positive count (non-overlapping, from left) is unchanged
        Assertions.assertEquals("", fold("aaa", "aa", 1));
        Assertions.assertEquals("aa", fold("aaaaa", "aa", 2));
    }
}
