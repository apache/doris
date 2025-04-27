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

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CompareLiteralTest extends TestWithFeService {

    @Test
    public void testScalar() {
        // ip type
        checkCompareSameType(0, new IPv4Literal("170.0.0.100"), new IPv4Literal("170.0.0.100"));
        checkCompareSameType(1, new IPv4Literal("170.0.0.100"), new IPv4Literal("160.0.0.200"));
        checkCompareDiffType(new IPv4Literal("172.0.0.100"), new IPv6Literal("1080:0:0:0:8:800:200C:417A"));
        checkCompareSameType(0, new IPv6Literal("1080:0:0:0:8:800:200C:417A"), new IPv6Literal("1080:0:0:0:8:800:200C:417A"));
        checkCompareSameType(1, new IPv6Literal("1080:0:0:0:8:800:200C:417A"), new IPv6Literal("1000:0:0:0:8:800:200C:41AA"));
        IPv4Literal ipv4 = new IPv4Literal("170.0.0.100");
        Assertions.assertEquals(ipv4, new IPv4Literal(ipv4.toLegacyLiteral().getStringValue()));
        IPv6Literal ipv6 = new IPv6Literal("1080:0:0:0:8:800:200C:417A");
        Assertions.assertEquals(ipv6, new IPv6Literal(ipv6.toLegacyLiteral().getStringValue()));
    }

    @Test
    public void testComplex() throws Exception {
        // array type
        checkCompareSameType(0,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkCompareSameType(1,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(200))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkCompareSameType(1,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200), new IntegerLiteral(1))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkComparableNoException("select array(1,2) = array(1, 2)");
        checkComparableNoException("select array(1,2) > array(1, 2)");

        // json type
        checkNotComparable("select cast ('[1, 2]' as json) = cast('[1, 2]' as json)",
                "comparison predicate could not contains json type");
        checkNotComparable("select cast('[1, 2]' as json) > cast('[1, 2]' as json)",
                "comparison predicate could not contains json type");

        // map type
        checkNotComparable("select map(1, 2) = map(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select map(1, 2) > map(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select cast('(1, 2)' as map<int, int>) = cast('(1, 2)' as map<int, int>)",
                "comparison predicate could not contains complex type");

        // struct type
        checkNotComparable("select struct(1, 2) = struct(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select struct(1, 2) > struct(1, 2)",
                "comparison predicate could not contains complex type");
    }

    private void checkCompareSameType(int expect, Literal left, Literal right) {
        Assertions.assertEquals(expect, left.compareTo(right));
        Assertions.assertEquals(- expect, right.compareTo(left));
    }

    private void checkCompareDiffType(Literal left, Literal right) {
        Assertions.assertThrowsExactly(RuntimeException.class, () -> left.compareTo(right));
        Assertions.assertThrowsExactly(RuntimeException.class, () -> right.compareTo(left));
    }

    private void checkComparableNoException(String sql) throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> executeSql(sql));
    }

    private void checkNotComparable(String sql, String expectErrMsg) throws Exception {
        ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, expectErrMsg,
                () -> executeSql(sql));
    }
}
