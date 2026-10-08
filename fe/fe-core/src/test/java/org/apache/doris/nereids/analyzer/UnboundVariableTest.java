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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** An unbound variable renders as SQL the parser reads back as the same variable. */
public class UnboundVariableTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @ParameterizedTest
    @CsvSource({
            "@authorized_region, USER",
            "@@k1_limit, DEFAULT",
            "@@session.k1_limit, SESSION",
            "@@global.k1_limit, GLOBAL"
    })
    public void testToSqlReadsBackAsTheSameVariable(String sql, VariableType type) {
        Expression parsed = PARSER.parseExpression(sql);
        Assertions.assertInstanceOf(UnboundVariable.class, parsed);
        Assertions.assertEquals(type, ((UnboundVariable) parsed).getType());

        String rendered = parsed.toSql();

        Assertions.assertEquals(sql, rendered);
        Assertions.assertEquals(parsed, PARSER.parseExpression(rendered));
    }
}
