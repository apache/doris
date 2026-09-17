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
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.planner.GroupCommitPlanner;

import java.util.Optional;

public class PreparedStatementContext {
    public PrepareCommand command;
    public ConnectContext ctx;
    private StatementContext statementContext;
    public String stmtString;
    public Optional<ShortCircuitQueryContext> shortCircuitQueryContext = Optional.empty();
    public Optional<GroupCommitPlanner> groupCommitPlanner = Optional.empty();

    // Timestamp in millisecond last command starts at
    protected volatile long startTime;

    public PreparedStatementContext(PrepareCommand command,
                ConnectContext ctx, StatementContext statementContext, String stmtString) {
        this.command = command;
        this.ctx = ctx;
        this.statementContext = statementContext;
        this.stmtString = stmtString;
    }

    public long getStartTime() {
        return startTime;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public void setStatementContext(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    /**
     * Allocate a fresh StatementContext for this EXECUTE and replace the previous one, so the
     * old context (with the per-statement state accumulated by prior executions: bound tables,
     * CTE maps, statistics, snapshots, ...) becomes unreachable and is promptly GC'd.
     *
     * <p>A prepared statement lives as long as its connection. Reusing one StatementContext
     * across all executions would keep growing those maps and could OOM long-lived connections,
     * so we create a new object per execution and carry over only the state that must survive
     * (placeholder bindings, comparison slots, id generator positions, short-circuit flags).
     *
     * @return the fresh StatementContext to use for the current execution
     */
    public StatementContext nextStatementContext() {
        // Close the outgoing context's per-statement connector scope before dropping it. The binary
        // COM_STMT_EXECUTE path has no per-statement StatementContext.close() finally (that only
        // runs for COM_QUERY), and coordinated scans may not have registered a query-finish
        // callback yet (connector commands and failures before scan registration have none).
        // Without this, the outgoing scope's closeable connector metadata / active connector
        // transactions would be abandoned, and GC cannot finalize them.
        statementContext.resetConnectorStatementScope();
        statementContext = statementContext.createNextExecuteContext();
        return statementContext;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }
}
