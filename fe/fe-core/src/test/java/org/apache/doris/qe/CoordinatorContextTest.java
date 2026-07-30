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
import org.apache.doris.thrift.TQueryGlobals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class CoordinatorContextTest {

    @Test
    void testQueryGlobalsUseStatementStartTime() {
        Instant statementStartTime = Instant.parse("2026-06-11T09:30:45.123456Z");
        ConnectContext connectContext = new ConnectContext();
        connectContext.setStatementContext(
                new StatementContext(connectContext, new OriginStatement("select now(6)", 0), statementStartTime));

        TQueryGlobals queryGlobals = CoordinatorContext.createQueryGlobals(connectContext);

        Assertions.assertEquals(statementStartTime.toEpochMilli(), queryGlobals.getTimestampMs());
        Assertions.assertEquals(statementStartTime.getNano(), queryGlobals.getNanoSeconds());
        Assertions.assertEquals("2026-06-11 17:30:45", queryGlobals.getNowString());
    }

    @Test
    void testQueryGlobalsUseCanonicalStatementTimeZone() {
        Instant statementStartTime = Instant.parse("2026-06-10T04:34:56.123456Z");
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setTimeZone("PST");
        connectContext.setStatementContext(
                new StatementContext(connectContext, new OriginStatement("select now(6)", 0), statementStartTime));

        TQueryGlobals queryGlobals = CoordinatorContext.createQueryGlobals(connectContext);

        Assertions.assertEquals("America/Los_Angeles", queryGlobals.getTimeZone());
        Assertions.assertEquals("2026-06-09 21:34:56", queryGlobals.getNowString());
        Assertions.assertEquals(statementStartTime.toEpochMilli(), queryGlobals.getTimestampMs());
        Assertions.assertEquals(statementStartTime.getNano(), queryGlobals.getNanoSeconds());
    }

    @Test
    void testQueryGlobalsWithoutStatementContext() {
        ConnectContext connectContext = new ConnectContext();

        TQueryGlobals queryGlobals = CoordinatorContext.createQueryGlobals(connectContext);

        Assertions.assertTrue(queryGlobals.isSetTimestampMs());
        Assertions.assertTrue(queryGlobals.isSetNanoSeconds());
    }
}
