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

import org.apache.doris.nereids.trees.expressions.literal.format.IntegerChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.function.Function;

public class IntegerLiteralTest {
    @Test
    public void testChecker() {
        assertValid(
                "1",
                "+1",
                "-1",
                "456"
        );

        assertInValid(
                "1.0",
                "1e3",
                "abc"
        );
    }

    private void assertValid(String...validString) {
        for (String str : validString) {
            check(str, s -> new IntegerLiteral(new BigInteger(s).intValueExact()));
        }
    }

    private void assertInValid(String...validString) {
        for (String str : validString) {
            Assertions.assertThrows(
                    Throwable.class,
                    () -> check(str, s -> new IntegerLiteral(new BigInteger(s).intValueExact()))
            );
        }
    }

    private <T extends IntegerLikeLiteral> T check(String str, Function<String, T> literalBuilder) {
        Assertions.assertTrue(IntegerChecker.isValidInteger(str), "Invalid integer: " + str);
        return literalBuilder.apply(str);
    }
}
