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
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.EmptyCommand;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class ConnectProcessorStatementTimeTest {

    @Test
    void testSqlCacheHitKeepsValidationTimestamp() {
        Instant validationTime = Instant.parse("2026-07-18T01:02:03Z");
        Instant laterExecutionTime = Instant.parse("2026-07-18T01:02:04Z");
        ConnectContext connectContext = new ConnectContext();
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select now()", 0), validationTime);
        LogicalPlanAdapter cachedStatement = new LogicalPlanAdapter(new EmptyCommand(), statementContext);
        TestConnectProcessor processor = new TestConnectProcessor(connectContext, laterExecutionTime);

        processor.prepareStatementExecutionTime(cachedStatement, true);

        Assertions.assertEquals(validationTime, statementContext.getStatementStartTime());
    }

    @Test
    void testNonCachedStatementUsesExecutionTimestamp() {
        Instant parseTime = Instant.parse("2026-07-18T01:02:03Z");
        Instant executionTime = Instant.parse("2026-07-18T01:02:04Z");
        ConnectContext connectContext = new ConnectContext();
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select now()", 0), parseTime);
        LogicalPlanAdapter parsedStatement = new LogicalPlanAdapter(new EmptyCommand(), statementContext);
        TestConnectProcessor processor = new TestConnectProcessor(connectContext, executionTime);

        processor.prepareStatementExecutionTime(parsedStatement, false);

        Assertions.assertEquals(executionTime, statementContext.getStatementStartTime());
    }

    private static class TestConnectProcessor extends ConnectProcessor {
        private final Instant statementStartTime;

        TestConnectProcessor(ConnectContext context, Instant statementStartTime) {
            super(context);
            this.statementStartTime = statementStartTime;
        }

        @Override
        protected Instant currentStatementStartTime() {
            return statementStartTime;
        }
    }
}
