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

package org.apache.doris.arrowflight.protocol;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * MysqlProtocolAdapter.finishCommand() replays a forwarded statement's status and result set to a
 * MySQL client. FlightProtocolAdapter.finishStatement() is its Arrow Flight SQL counterpart. Without
 * it, ctx.getState() stays at the OK that executeQuery() set with reset() and the FlightSqlChannel
 * stays empty, so DorisFlightSqlProducer answers with addOKResult()'s synthesized StatusResult=0:
 * success for a statement that failed on the master, and that same row instead of the rows a
 * forwarded SHOW produced.
 */
public class FlightForwardedOutcomeTest {
    private boolean savedRunningUnitTest;
    private ConnectContext context;
    private FlightProtocolAdapter adapter;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
        context = ConnectContext.forFlight("test-peer-identity");
        adapter = FlightProtocolAdapter.of(context);
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testMasterFailureIsReportedToTheFlightClient() throws Exception {
        StmtExecutor executor = forwardedExecutor();
        // e.g. CREATE TABLE on a table that already exists.
        Mockito.when(executor.getProxyStatusCode()).thenReturn(1050);
        Mockito.when(executor.getProxyErrMsg()).thenReturn("Table 'tbl' already exists");

        Assertions.assertTrue(adapter.finishStatement(context, executor, 0, 1));

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
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        Mockito.when(executor.getShowResultSet()).thenReturn(resultSet);

        Assertions.assertTrue(adapter.finishStatement(context, executor, 0, 1));

        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        // sendResultSet() puts the rows into the FlightSqlChannel through the FlightResultSender,
        // which is what DorisFlightSqlProducer hands back instead of the synthesized StatusResult row.
        Mockito.verify(executor).sendResultSet(resultSet);
    }

    @Test
    public void testForwardedDdlKeepsTheSynthesizedOkResult() throws Exception {
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        // A forwarded DDL carries no result set, and StatusResult=0 is the right answer for it.
        Mockito.when(executor.getShowResultSet()).thenReturn(null);

        Assertions.assertTrue(adapter.finishStatement(context, executor, 0, 1));

        Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(0L, context.getFlightSqlChannel().resultNum());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any());
    }

    @Test
    public void testStatementNotForwardedIsLeftAlone() throws Exception {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.hasForwardedToMaster()).thenReturn(false);

        Assertions.assertTrue(adapter.finishStatement(context, executor, 0, 1));

        Mockito.verify(executor, Mockito.never()).getProxyStatusCode();
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any());
    }

    private StmtExecutor forwardedExecutor() {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.hasForwardedToMaster()).thenReturn(true);
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        return executor;
    }
}
