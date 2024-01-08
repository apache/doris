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

package org.apache.doris.plugin.dialect.spark;

import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


/**
 * Spark-sql query tests.
 */
public class ParserTest {

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
    public void testParseCast1() {
        // doris parser will throw a ParseException when derived table does not have alias
        String sql1 = "select * from (select * from t1);";
        NereidsParser nereidsParser = new NereidsParser();
        Assertions.assertThrows(ParseException.class, () -> nereidsParser.parseSQL(sql1),
                "Every derived table must have its own alias");

        // test parse with spark-sql dialect
        LogicalPlan dialectLogicalPlan = nereidsParser.parseSingle(sql1, new SparkSql3LogicalPlanBuilder());
        Assertions.assertTrue(dialectLogicalPlan instanceof UnboundResultSink);
    }
}
