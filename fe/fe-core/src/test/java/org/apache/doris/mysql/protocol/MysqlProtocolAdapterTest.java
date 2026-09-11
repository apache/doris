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

package org.apache.doris.mysql.protocol;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.ProxyMysqlChannel;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.arrowflight.FlightProtocolAdapter;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

/**
 * A MySQL connection, a proxy context on the master and an internal context are all a
 * ConnectContext bound to a MysqlProtocolAdapter; the channel tells them apart.
 */
public class MysqlProtocolAdapterTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testInternalContextIsAMysqlContextWithoutAClient() {
        ConnectContext ctx = new ConnectContext();

        Assertions.assertEquals(ConnectType.MYSQL, ctx.getConnectType());
        Assertions.assertEquals(TResultSinkType.MYSQL_PROTOCOL, ctx.getResultSinkType());
        Assertions.assertTrue(ctx.getMysqlChannel() instanceof DummyMysqlChannel);
        Assertions.assertSame(ctx.getMysqlChannel(), MysqlProtocolAdapter.of(ctx).getChannel());
        Assertions.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        Assertions.assertFalse(ctx.isProxy());

        // What only an Arrow Flight SQL session has is absent, not stubbed.
        Assertions.assertNull(ctx.getPeerIdentity());
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());
        ctx.closeFlightSqlDeferredExecutors();
        Assertions.assertThrows(IllegalStateException.class, ctx::getFlightSqlChannel);
        Assertions.assertThrows(IllegalStateException.class, () -> FlightProtocolAdapter.of(ctx));
    }

    @Test
    public void testProxyContextCollectsThePacketsOfTheForwardedStatement() {
        ConnectContext ctx = ConnectContext.forMysqlProxy("session-1");

        Assertions.assertEquals(ConnectType.MYSQL, ctx.getConnectType());
        Assertions.assertTrue(ctx.isProxy());
        Assertions.assertEquals("session-1", ctx.getSessionId());
        Assertions.assertTrue(ctx.getMysqlChannel() instanceof ProxyMysqlChannel);
    }

    @Test
    public void testConnectionRegistersItsTraceIdInTheMysqlPool() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = new ConnectContext();
        ctx.setConnectScheduler(scheduler);
        ctx.setTraceId("trace-1");
        TUniqueId queryId = new TUniqueId(1, 2);

        ctx.setQueryId(queryId);

        Assertions.assertEquals(DebugUtil.printId(queryId), scheduler.getConnectPoolMgr().getQueryIdByTraceId("trace-1"));
        Assertions.assertEquals("", scheduler.getFlightSqlConnectPoolMgr().getQueryIdByTraceId("trace-1"));
    }

    @Test
    public void testCursorTerminatorFollowsTheExecuteFlagAndTheClient() {
        ConnectContext ctx = new ConnectContext();
        MysqlProtocolAdapter protocol = MysqlProtocolAdapter.of(ctx);
        ctx.setConnectAttributes(ImmutableMap.of("_client_name", "MySQL Connector/J", "_client_version", "8.2.0"));

        // Only a COM_STMT_EXECUTE that asked for a cursor ...
        Assertions.assertFalse(protocol.clientConsumesCursorMetadataTerminator(ctx));
        ctx.setCursorFetchRequested(true);
        Assertions.assertTrue(ctx.isCursorFetchRequested());
        Assertions.assertTrue(protocol.clientConsumesCursorMetadataTerminator(ctx));
        // ... from a Connector/J release that swallows the terminator.
        ctx.setConnectAttributes(ImmutableMap.of("_client_name", "MySQL Connector/J", "_client_version", "9.5.0"));
        Assertions.assertFalse(protocol.clientConsumesCursorMetadataTerminator(ctx));

        // The flag belongs to the statement: it is gone once the statement is over.
        ctx.clear();
        Assertions.assertFalse(ctx.isCursorFetchRequested());
    }

    @Test
    public void testConnectionLifecycleGoesThroughTheChannel() throws Exception {
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        ConnectContext ctx = new ConnectContext(new MysqlProtocolAdapter(channel));
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        MysqlProtocolAdapter protocol = MysqlProtocolAdapter.of(ctx);

        protocol.startAcceptQuery(ctx, processor);
        protocol.suspendAcceptQuery();
        protocol.resumeAcceptQuery();
        protocol.stopAcceptQuery();
        ctx.cleanup();

        InOrder inOrder = Mockito.inOrder(channel);
        inOrder.verify(channel).startAcceptQuery(ctx, processor);
        inOrder.verify(channel).suspendAcceptQuery();
        inOrder.verify(channel).resumeAcceptQuery();
        inOrder.verify(channel).stopAcceptQuery();
        inOrder.verify(channel).close();
    }
}
