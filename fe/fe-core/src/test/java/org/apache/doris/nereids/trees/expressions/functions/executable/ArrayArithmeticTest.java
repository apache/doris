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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArrayArithmeticTest {

    @Test
    void testTrimArray() {
        ArrayLiteral input = new ArrayLiteral(ImmutableList.of(
                new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));

        ArrayLiteral trimmed = (ArrayLiteral) ArrayArithmetic.trimArray(input, new BigIntLiteral(1));
        Assertions.assertEquals(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)),
                trimmed.getValue());
        Assertions.assertEquals(input.getDataType(), trimmed.getDataType());

        ArrayLiteral unchanged = (ArrayLiteral) ArrayArithmetic.trimArray(input, new BigIntLiteral(0));
        Assertions.assertEquals(input.getValue(), unchanged.getValue());

        ArrayLiteral empty = (ArrayLiteral) ArrayArithmetic.trimArray(input, new BigIntLiteral(3));
        Assertions.assertTrue(empty.getValue().isEmpty());
        Assertions.assertEquals(input.getDataType(), empty.getDataType());
    }

    @Test
    void testTrimArrayRejectsInvalidSize() {
        ArrayLiteral input = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1)));

        Assertions.assertThrows(AnalysisException.class,
                () -> ArrayArithmetic.trimArray(input, new BigIntLiteral(-1)));
        Assertions.assertThrows(AnalysisException.class,
                () -> ArrayArithmetic.trimArray(input, new BigIntLiteral(2)));
    }
}
