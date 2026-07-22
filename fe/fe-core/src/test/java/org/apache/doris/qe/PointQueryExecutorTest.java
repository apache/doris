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
import org.apache.doris.proto.InternalService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class PointQueryExecutorTest {

    @Test
    void testSetQueryGlobalsFromStatementContext() {
        Instant statementStartTime = Instant.parse("2026-06-15T02:03:04.123456Z");
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setTimeZone("CST");
        connectContext.setStatementContext(
                new StatementContext(connectContext, null, statementStartTime));
        InternalService.PTabletKeyLookupRequest.Builder requestBuilder =
                InternalService.PTabletKeyLookupRequest.newBuilder();

        PointQueryExecutor.setQueryGlobals(requestBuilder, connectContext);

        InternalService.PTabletKeyLookupRequest request = requestBuilder.buildPartial();
        Assertions.assertEquals("Asia/Shanghai", request.getTimeZone());
        Assertions.assertEquals(statementStartTime.toEpochMilli(), request.getTimestampMs());
        Assertions.assertEquals(statementStartTime.getNano(), request.getNanoSeconds());
    }

    @Test
    void testSetQueryGlobalsUsesCanonicalCapturedTimeZone() {
        Instant statementStartTime = Instant.parse("2026-06-15T02:03:04.123456Z");
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setTimeZone("PST");
        connectContext.setStatementContext(
                new StatementContext(connectContext, null, statementStartTime));
        connectContext.getSessionVariable().setTimeZone("+08:00");
        InternalService.PTabletKeyLookupRequest.Builder requestBuilder =
                InternalService.PTabletKeyLookupRequest.newBuilder();

        PointQueryExecutor.setQueryGlobals(requestBuilder, connectContext);

        InternalService.PTabletKeyLookupRequest request = requestBuilder.buildPartial();
        Assertions.assertEquals("America/Los_Angeles", request.getTimeZone());
        Assertions.assertEquals(statementStartTime.toEpochMilli(), request.getTimestampMs());
        Assertions.assertEquals(statementStartTime.getNano(), request.getNanoSeconds());
    }
}
