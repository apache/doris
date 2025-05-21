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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DoubleLiteralTest {

    @Test
    public void testGetStringValue() {
        Assertions.assertEquals("0", new DoubleLiteral(0).getStringValue());
        Assertions.assertEquals("0", new DoubleLiteral(0.0).getStringValue());
        Assertions.assertEquals("0", new DoubleLiteral(-0).getStringValue());
        Assertions.assertEquals("1", new DoubleLiteral(1).getStringValue());
        Assertions.assertEquals("1", new DoubleLiteral(1.0).getStringValue());
        Assertions.assertEquals("-1", new DoubleLiteral(-1).getStringValue());
        Assertions.assertEquals("1.554", new DoubleLiteral(1.554).getStringValue());
        Assertions.assertEquals("0.338", new DoubleLiteral(0.338).getStringValue());
        Assertions.assertEquals("-1", new DoubleLiteral(-1.0).getStringValue());
        Assertions.assertEquals("1e+100", new DoubleLiteral(1e100).getStringValue());
        Assertions.assertEquals("1e-100", new DoubleLiteral(1e-100).getStringValue());
        Assertions.assertEquals("10000000000000000", new DoubleLiteral(1.0E16).getStringValue());
        Assertions.assertEquals("-10000000000000000", new DoubleLiteral(-1.0E16).getStringValue());
        Assertions.assertEquals("1e+17", new DoubleLiteral(1.0E17).getStringValue());
        Assertions.assertEquals("-1e+17", new DoubleLiteral(-1.0E17).getStringValue());
        Assertions.assertEquals("0.0001", new DoubleLiteral(0.0001).getStringValue());
        Assertions.assertEquals("1e+308", new DoubleLiteral(1e308).getStringValue());
        Assertions.assertEquals("-1e+308", new DoubleLiteral(-1e308).getStringValue());
        Assertions.assertEquals("inf", new DoubleLiteral(Double.POSITIVE_INFINITY).getStringValue());
        Assertions.assertEquals("-inf", new DoubleLiteral(Double.NEGATIVE_INFINITY).getStringValue());
        Assertions.assertEquals("nan", new DoubleLiteral(Double.NaN).getStringValue());
    }
}
