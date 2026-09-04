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

import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryOrganizationPlanTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    @Test
    void legacyModeAppliesClausesToSetOperands() {
        withAnsiMode(false, () -> {
            LogicalUnion unionWithRightLimit = Assertions.assertInstanceOf(LogicalUnion.class,
                    parseQuery("SELECT 1 UNION ALL SELECT 2 LIMIT 1"));
            Assertions.assertInstanceOf(LogicalLimit.class, unionWithRightLimit.child(1));

            LogicalUnion unionWithLeftSort = Assertions.assertInstanceOf(LogicalUnion.class,
                    parseQuery("SELECT 1 ORDER BY 1 UNION ALL SELECT 2"));
            Assertions.assertInstanceOf(LogicalSort.class, unionWithLeftSort.child(0));
        });
    }

    @Test
    void ansiModeAppliesClausesToWholeSetOperation() {
        withAnsiMode(true, () -> {
            LogicalLimit<?> limit = Assertions.assertInstanceOf(LogicalLimit.class,
                    parseQuery("SELECT 1 UNION ALL SELECT 2 LIMIT 1"));
            Assertions.assertInstanceOf(LogicalUnion.class, limit.child());
            Assertions.assertThrows(ParseException.class,
                    () -> parser.parseSingle("SELECT 1 ORDER BY 1 UNION ALL SELECT 2"));
        });
    }

    @Test
    void preservesParenthesizedAndInlineTableClauses() {
        for (boolean ansi : new boolean[] {false, true}) {
            withAnsiMode(ansi, () -> {
                Assertions.assertInstanceOf(LogicalLimit.class,
                        parseQuery("(SELECT 1 UNION ALL SELECT 2) LIMIT 1"));
                Assertions.assertInstanceOf(LogicalLimit.class,
                        parseQuery("VALUES (1), (2) ORDER BY 1 LIMIT 1"));
            });
        }
    }

    private Plan parseQuery(String sql) {
        LogicalPlan statement = parser.parseSingle(sql);
        return statement.child(0);
    }

    private void withAnsiMode(boolean enabled, Runnable test) {
        boolean previous = GlobalVariable.enable_ansi_query_organization_behavior;
        try {
            GlobalVariable.enable_ansi_query_organization_behavior = enabled;
            test.run();
        } finally {
            GlobalVariable.enable_ansi_query_organization_behavior = previous;
        }
    }
}
