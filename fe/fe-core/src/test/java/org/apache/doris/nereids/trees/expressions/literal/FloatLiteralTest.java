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

import org.apache.doris.nereids.trees.expressions.literal.format.FloatChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.function.Function;

public class FloatLiteralTest {
    @Test
    public void testChecker() {
        assertValid(
                "123.45",
                "-34.56",
                "0",
                "0.01",
                "10000",
                "+1",
                "-1",
                "+1",
                "1.0",
                "-1.0",
                "-1.0e3",
                ".1",
                "1.",
                "1e3"
        );

        assertInValid(
                "e3",
                "abc",
                "12.34.56",
                "1,234.56"
        );

        Assertions.assertThrows(
                Throwable.class,
                () -> check("e3", s -> new FloatLiteral(new BigDecimal(s).floatValue()))
        );
    }

    private void assertValid(String...validString) {
        for (String str : validString) {
            check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()));
        }
    }

    private void assertInValid(String...validString) {
        for (String str : validString) {
            Assertions.assertThrows(
                    Throwable.class,
                    () -> check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()))
            );
        }
    }

    private <T extends FractionalLiteral> T check(String str, Function<String, T> literalBuilder) {
        Assertions.assertTrue(FloatChecker.isValidFloat(str), "Invalid fractional: " + str);
        return literalBuilder.apply(str);
    }
}
