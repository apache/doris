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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * ConnectProcessor.finalizeCommand() is the only place a forwarded statement's status and result set
 * are replayed to the client, and it is MySQL-only (it opens with a Preconditions.checkState on the
 * connect type). carryForwardedOutcomeToFlightSession() is its Arrow Flight SQL counterpart. Without
 * it, ctx.getState() stays at the OK that executeQuery() set with reset() and the FlightSqlChannel
 * stays empty, so DorisFlightSqlProducer answers with addOKResult()'s synthesized StatusResult=0:
 * success for a statement that failed on the master, and that same row instead of the rows a
 * forwarded SHOW produced.
 */
public class ConnectProcessorFlightForwardOutcomeTest {
    private boolean savedRunningUnitTest;
    private FlightSqlConnectContext context;
    private ConnectProcessor processor;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
        context = new FlightSqlConnectContext("test-peer-identity");
        processor = new TestConnectProcessor(context);
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testMasterFailureIsReportedToTheFlightClient() throws Exception {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        // e.g. CREATE TABLE on a table that already exists.
        Mockito.when(executor.getProxyStatusCode()).thenReturn(1050);
        Mockito.when(executor.getProxyErrMsg()).thenReturn("Table 'tbl' already exists");

        processor.carryForwardedOutcomeToFlightSession(executor);

        Assertions.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, context.getState().getErrorCode());
        // The master's own error code has no ErrorCode enum on this side, so it travels in the text.
        Assertions.assertTrue(context.getState().getErrorMessage().contains("1050"),
                context.getState().getErrorMessage());
        Assertions.assertTrue(context.getState().getErrorMessage().contains("Table 'tbl' already exists"),
                context.getState().getErrorMessage());
        // A statement that failed must not also replay a result set.
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any());
    }

    @Test
    public void testForwardedResultSetIsReplayedToTheFlightChannel() throws Exception {
        ShowResultSet resultSet = new ShowResultSet(
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                        .build(),
                Lists.<List<String>>newArrayList(Lists.newArrayList("10086")));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        Mockito.when(executor.getShowResultSet()).thenReturn(resultSet);

        processor.carryForwardedOutcomeToFlightSession(executor);

        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        // sendResultSet()'s ARROW_FLIGHT_SQL branch puts the rows into the FlightSqlChannel, which is
        // what DorisFlightSqlProducer hands back instead of the synthesized StatusResult row.
        Mockito.verify(executor).sendResultSet(resultSet);
    }

    @Test
    public void testForwardedDdlKeepsTheSynthesizedOkResult() throws Exception {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        // A forwarded DDL carries no result set, and StatusResult=0 is the right answer for it.
        Mockito.when(executor.getShowResultSet()).thenReturn(null);

        processor.carryForwardedOutcomeToFlightSession(executor);

        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(0L, context.getFlightSqlChannel().resultNum());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any());
    }

    private static class TestConnectProcessor extends ConnectProcessor {
        private TestConnectProcessor(ConnectContext context) {
            super(context);
        }
    }
}
