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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LiteralEqualTest {

    @Test
    void testEqual() {
        IntegerLiteral one = new IntegerLiteral(1);
        IntegerLiteral anotherOne = new IntegerLiteral(1);
        IntegerLiteral two = new IntegerLiteral(2);
        Assertions.assertNotEquals(one, two);
        Assertions.assertEquals(one, anotherOne);
        StringLiteral str1 = new StringLiteral("hello");
        Assertions.assertNotEquals(str1, one);
        Assertions.assertTrue(Literal.of("world") instanceof StringLiteral);
        Assertions.assertTrue(Literal.of(null) instanceof NullLiteral);
        Assertions.assertTrue(Literal.of(1) instanceof IntegerLiteral);
        Assertions.assertTrue(Literal.of(false) instanceof BooleanLiteral);
    }
}
