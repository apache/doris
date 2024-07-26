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

import java.util.Optional;

public class PreparedStatementContext {
    public PrepareCommand command;
    public ConnectContext ctx;
    StatementContext statementContext;
    public String stmtString;
    public Optional<ShortCircuitQueryContext> shortCircuitQueryContext = Optional.empty();

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

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }
}
