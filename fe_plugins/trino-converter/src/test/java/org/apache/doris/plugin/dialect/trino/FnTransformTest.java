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

package org.apache.doris.plugin.dialect.trino;


import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import mockit.Mock;
import mockit.MockUp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


/**
 * Trino to Doris function mapping test.
 */
public class FnTransformTest {

    @BeforeAll
    public static void init() {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        ctx.setSessionVariable(sessionVariable);
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    @Test
    public void testStringFnTransform() {
        ParserContext parserContext = new ParserContext(Dialect.TRINO);
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT ascii('a') as b FROM t";
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        String dialectSql = "SELECT codepoint('a') as b FROM t";
        LogicalPlan dialectLogicalPlan = TrinoParser.parseSingle(dialectSql, parserContext);
        Assertions.assertEquals(dialectLogicalPlan, logicalPlan);
    }

    @Test
    public void testDateDiffFnTransform() {
        ParserContext parserContext = new ParserContext(Dialect.TRINO);
        String dialectSql = "SELECT date_diff('second', TIMESTAMP '2020-12-25 22:00:00', TIMESTAMP '2020-12-25 21:00:00')";
        LogicalPlan logicalPlan = TrinoParser.parseSingle(dialectSql, parserContext);
        Assertions.assertTrue(logicalPlan.toString().toLowerCase()
                    .contains("seconds_diff(2020-12-25 22:00:00, 2020-12-25 21:00:00)"));
    }
}
