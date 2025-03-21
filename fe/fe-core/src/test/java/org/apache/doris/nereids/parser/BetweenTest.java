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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BetweenTest {
    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void testBetween() {
        String expression = "A between 1 and 1"; //
        Expression result = PARSER.parseExpression(expression);
        Assertions.assertInstanceOf(EqualTo.class, result);

        expression = "A between 1 and 2";
        result = PARSER.parseExpression(expression);
        Assertions.assertInstanceOf(And.class, result);
    }
}
