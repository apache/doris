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
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringArithmeticSplitByStringTest {

    private static ArrayLiteral makeArray(String... values) {
        List<Literal> items = Arrays.stream(values)
                .map(StringLiteral::new)
                .collect(Collectors.toList());
        return new ArrayLiteral(items);
    }

    private static ArrayLiteral makeEmptyArray() {
        return new ArrayLiteral(ImmutableList.of(), ArrayType.of(StringType.INSTANCE));
    }

    @Test
    public void testSplitByStringWithLimitBasic() {
        // limit < parts: "a,b,c,d" split by "," limit 2 -> ["a", "b,c,d"]
        Expression result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c,d"), new StringLiteral(","), new IntegerLiteral(2));
        Assertions.assertEquals(makeArray("a", "b,c,d"), result);

        // limit = 3
        result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c,d"), new StringLiteral(","), new IntegerLiteral(3));
        Assertions.assertEquals(makeArray("a", "b", "c,d"), result);

        // limit = 1: no split at all
        result = StringArithmetic.splitByString(
                new StringLiteral("one,two,three"), new StringLiteral(","), new IntegerLiteral(1));
        Assertions.assertEquals(makeArray("one,two,three"), result);

        // multi-char delimiter + limit
        result = StringArithmetic.splitByString(
                new StringLiteral("a::b::c::d"), new StringLiteral("::"), new IntegerLiteral(2));
        Assertions.assertEquals(makeArray("a", "b::c::d"), result);
    }

    @Test
    public void testSplitByStringWithLimitExceedParts() {
        // limit >= parts: "a,b,c" split by "," limit 10 -> ["a","b","c"]
        Expression result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c"), new StringLiteral(","), new IntegerLiteral(10));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);

        // limit == parts
        result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c"), new StringLiteral(","), new IntegerLiteral(3));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);
    }

    @Test
    public void testSplitByStringWithLimitZeroAndNegative() {
        // limit = 0 -> delegates to 2-arg version
        Expression result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c"), new StringLiteral(","), new IntegerLiteral(0));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);

        // limit = -1 -> delegates to 2-arg version
        result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c"), new StringLiteral(","), new IntegerLiteral(-1));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);

        // limit = -100 -> delegates to 2-arg version
        result = StringArithmetic.splitByString(
                new StringLiteral("a,b,c"), new StringLiteral(","), new IntegerLiteral(-100));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);
    }

    @Test
    public void testSplitByStringWithLimitEmptyFirst() {
        // empty source string -> empty array
        Expression result = StringArithmetic.splitByString(
                new StringLiteral(""), new StringLiteral(","), new IntegerLiteral(2));
        Assertions.assertEquals(makeEmptyArray(), result);

        result = StringArithmetic.splitByString(
                new StringLiteral(""), new StringLiteral(","), new IntegerLiteral(0));
        // limit <= 0 delegates to 2-arg, which also returns empty array for empty input
        Assertions.assertEquals(makeEmptyArray(), result);
    }

    @Test
    public void testSplitByStringWithLimitEmptyDelimiter() {
        // empty delimiter splits by character, with limit < chars
        Expression result = StringArithmetic.splitByString(
                new StringLiteral("abcde"), new StringLiteral(""), new IntegerLiteral(3));
        Assertions.assertEquals(makeArray("a", "b", "cde"), result);

        // limit = 1 -> entire string as single element
        result = StringArithmetic.splitByString(
                new StringLiteral("abcde"), new StringLiteral(""), new IntegerLiteral(1));
        Assertions.assertEquals(makeArray("abcde"), result);
    }

    @Test
    public void testSplitByStringWithLimitEmptyDelimiterExceed() {
        // empty delimiter + limit >= chars -> all characters
        Expression result = StringArithmetic.splitByString(
                new StringLiteral("abcde"), new StringLiteral(""), new IntegerLiteral(10));
        Assertions.assertEquals(makeArray("a", "b", "c", "d", "e"), result);

        // exact match
        result = StringArithmetic.splitByString(
                new StringLiteral("abc"), new StringLiteral(""), new IntegerLiteral(3));
        Assertions.assertEquals(makeArray("a", "b", "c"), result);
    }

    @Test
    public void testSplitByStringWithLimitConsecutiveDelimiters() {
        // consecutive delimiters produce empty strings
        Expression result = StringArithmetic.splitByString(
                new StringLiteral(",,,"), new StringLiteral(","), new IntegerLiteral(2));
        Assertions.assertEquals(makeArray("", ",,"), result);

        result = StringArithmetic.splitByString(
                new StringLiteral(",,a,b,c,"), new StringLiteral(","), new IntegerLiteral(3));
        Assertions.assertEquals(makeArray("", "", "a,b,c,"), result);
    }
}
