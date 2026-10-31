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

package org.apache.doris.qe;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-88 review fix #4: the QUERY forwarding policy ({@code StmtExecutor#isForwardableQuery})
 * recognizes an EXPLAIN of a query, but the command dispatch only consulted
 * {@code isForwardToMaster()} for {@code Redirect} commands and {@code ExplainCommand} is a
 * {@code NoForward} one - so on an observer with forced forwarding the diagnostic ran LOCALLY and
 * printed a plan from a baseline cache that could predate the master's baseline, while the
 * immediately following SELECT was forwarded and used it.
 *
 * <p>The dispatch now routes a query EXPLAIN through the same path (see
 * StmtExecutor#isQueryExplain). These tests pin the classification the dispatch relies on.
 */
public class StmtExecutorExplainForwardTest {

    /** An EXPLAIN of a SELECT is a query EXPLAIN: it must take the query forwarding path. */
    @Test
    public void testExplainOfQueryIsForwardable() {
        Assertions.assertTrue(StmtExecutor.isQueryExplain(parse("EXPLAIN SELECT 1")),
                "an EXPLAIN of a query follows the query forwarding policy");
        Assertions.assertTrue(
                StmtExecutor.isQueryExplain(parse("EXPLAIN SELECT k FROM t1 WHERE k > 1")),
                "the classification does not depend on the plan's shape");
    }

    /**
     * An EXPLAIN of a DML / DDL stays a command: its output does not depend on this FE's baseline
     * cache, so it must NOT be forwarded as a query (the master's result is a DDL outcome, not a
     * query result set).
     */
    @Test
    public void testExplainOfCommandIsNotAQuery() {
        Assertions.assertFalse(
                StmtExecutor.isQueryExplain(parse("EXPLAIN INSERT INTO t1 VALUES (1)")),
                "an EXPLAIN of a DML keeps the command path");
    }

    /** A plain query is not an EXPLAIN plan at all (it goes through the query branch). */
    @Test
    public void testPlainQueryIsNotAnExplain() {
        Assertions.assertFalse(StmtExecutor.isQueryExplain(parse("SELECT 1")),
                "a plain query is handled by the query branch");
    }

    private static LogicalPlan parse(String sql) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        try {
            return (LogicalPlan) new NereidsParser().parseSingle(sql);
        } finally {
            ctx.setStatementContext(new StatementContext());
        }
    }
}
