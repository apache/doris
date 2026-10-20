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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MapConstructionFunctionsTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void testCreateMapRejectsComplexKey() {
        for (String sql : ImmutableList.of(
                "map(array(1, 2), 'v')",
                "map(named_struct('a', 1), 'v')",
                "map(map(1, 2), 'v')")) {
            AnalysisException complexKeyException = Assertions.assertThrows(
                    AnalysisException.class, () -> analyze(sql));
            Assertions.assertTrue(complexKeyException.getMessage().contains(
                    "MAP key type must be a primitive type"), complexKeyException::getMessage);
        }

        Assertions.assertDoesNotThrow(() -> analyze("map('k', array(1, 2))"));
        Assertions.assertDoesNotThrow(() -> analyze("map(NULL, 100)"));
    }

    private Expression analyze(String sql) {
        return ExpressionRewriteTestHelper.typeCoercion(PARSER.parseExpression(sql));
    }
}
