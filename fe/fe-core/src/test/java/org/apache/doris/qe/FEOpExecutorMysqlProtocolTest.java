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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FEOpExecutorMysqlProtocolTest {
    @Test
    public void testForwardRequestCarriesMysqlProtocolContext() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext context = createContext();
            context.getMysqlChannel().setClientDeprecatedEOF();
            context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
            context.setCursorFetchRequested(true);
            context.setConnectAttributes(ImmutableMap.of(
                    "_client_name", "MySQL Connector/J", "_client_version", "8.2.0"));

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assert.assertTrue(request.isClientDeprecatedEOF());
            Assert.assertTrue(request.isCursorFetchRequested());
            Assert.assertEquals("8.2.0", request.getConnectAttributes().get("_client_version"));
        }
    }

    @Test
    public void testForwardResponseRequiresExplicitProtocolConfirmation() {
        TestFEOpExecutor executor = new TestFEOpExecutor(createContext());
        executor.setResult(new TMasterOpResult());
        Assert.assertFalse(executor.isClientDeprecatedEofApplied());
        Assert.assertFalse(executor.hasQueryResultPackets());

        TMasterOpResult confirmed = new TMasterOpResult();
        confirmed.setClientDeprecatedEofApplied(true);
        confirmed.setQueryResultBufList(Collections.singletonList(ByteBuffer.wrap(new byte[] {1})));
        confirmed.setAffectedRows(7);
        executor.setResult(confirmed);
        Assert.assertTrue(executor.isClientDeprecatedEofApplied());
        Assert.assertTrue(executor.hasQueryResultPackets());
    }

    @Test
    public void testArrowForwardRequestDoesNotAccessMysqlChannel() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            FlightSqlConnectContext context = new FlightSqlConnectContext("alice");
            context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
            context.setRemoteIP("127.0.0.1");
            TMasterOpRequest request = new TestFEOpExecutor(context).build();
            Assert.assertFalse(request.isSetClientDeprecatedEOF());
            Assert.assertFalse(request.isSetMysqlCapability());
        }
    }

    @Test
    public void testForwardedCapabilityAndMissingCursorFlag() throws Exception {
        int legacyFlags = MysqlCapability.DEFAULT_CAPABILITY.getFlags()
                & ~MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit();
        TMasterOpRequest request = new TMasterOpRequest();
        request.setMysqlCapability(legacyFlags);
        ConnectContext context = createContext();
        ConnectProcessor.restoreForwardedMysqlContext(context, request);
        Assert.assertEquals(legacyFlags, context.getCapability().getFlags());
        Assert.assertFalse(context.getMysqlChannel().getSerializer().getCapability().isDeprecatedEOF());

        request = new TMasterOpRequest();
        request.setClientDeprecatedEOF(true);
        request.setPrepareExecuteBuffer(new byte[] {0});
        context = createContext();
        context.setConnectAttributes(ImmutableMap.of("_client_name", "MySQL Connector/J", "_client_version", "8.2.0"));
        ConnectProcessor.restoreForwardedMysqlContext(context, request);
        Assert.assertFalse(context.isCursorFetchRequested());
        Assert.assertTrue(context.getCapability().isDeprecatedEOF());
        request.setCursorFetchRequested(true);
        ConnectProcessor.restoreForwardedMysqlContext(context, request);
        Assert.assertTrue(context.isCursorFetchRequested());
        request.setCursorFetchRequested(false);
        ConnectProcessor.restoreForwardedMysqlContext(context, request);
        Assert.assertFalse(context.isCursorFetchRequested());
    }

    @Test
    public void testOldMasterPacketMatrix() {
        for (boolean legacyMaster : new boolean[] {false, true}) {
            for (boolean cursor : new boolean[] {false, true}) {
                for (String version : new String[] {"8.2.0", "9.4.0", "9.5.0"}) {
                    for (boolean rows : new boolean[] {false, true}) {
                        ConnectContext context = createContext();
                        context.getMysqlChannel().setClientDeprecatedEOF();
                        context.setCursorFetchRequested(cursor);
                        context.setConnectAttributes(ImmutableMap.of(
                                "_client_name", "MySQL Connector/J", "_client_version", version));
                        TestFEOpExecutor executor = new TestFEOpExecutor(context);
                        List<ByteBuffer> packets = new ArrayList<>();
                        packets.add(ByteBuffer.wrap(new byte[] {1})); // column count
                        packets.add(ByteBuffer.wrap(new byte[] {3, 'd', 'e', 'f'})); // opaque column definition
                        if (legacyMaster) {
                            packets.add(ByteBuffer.wrap(new byte[] {(byte) 0xFE, 3, 0, 2, 0}));
                        }
                        ByteBuffer row = ByteBuffer.wrap(new byte[] {0, 0, 42});
                        if (rows) {
                            packets.add(row);
                        }
                        TMasterOpResult result = new TMasterOpResult();
                        result.setQueryResultBufList(packets);
                        result.setStatus("EOF");
                        result.setStatusCode(1105); // production successful SELECT mapping
                        result.setPacket(ByteBuffer.wrap(legacyMaster
                                ? new byte[] {(byte) 0xFE, 3, 0, 2, 0}
                                : new byte[] {(byte) 0xFE, 0, 0, 2, 0, 3, 0, 0}));
                        executor.setResult(result);
                        executor.prepareQueryResultForClient();
                        boolean shim = cursor && !version.equals("9.5.0");
                        Assert.assertEquals(2 + (shim ? 1 : 0) + (rows ? 1 : 0),
                                executor.getQueryResultBufList().size());
                        if (shim) {
                            Assert.assertEquals(8, executor.getQueryResultBufList().get(2).remaining());
                        }
                        if (rows) {
                            Assert.assertSame(row, executor.getQueryResultBufList().get(shim ? 3 : 2));
                        }
                        ByteBuffer end = executor.getOutputPacket().duplicate();
                        Assert.assertEquals(0xFE, MysqlProto.readInt1(end));
                        Assert.assertEquals(0, MysqlProto.readVInt(end));
                        Assert.assertEquals(0, MysqlProto.readVInt(end));
                        Assert.assertEquals(2, MysqlProto.readInt2(end));
                        Assert.assertEquals(3, MysqlProto.readInt2(end));
                        List<ByteBuffer> normalized = executor.getQueryResultBufList();
                        executor.prepareQueryResultForClient();
                        Assert.assertSame(normalized, executor.getQueryResultBufList());
                    }
                }
            }
        }
    }

    @Test
    public void testOkAndErrorAreNotRebuilt() {
        ConnectContext context = createContext();
        context.getMysqlChannel().setClientDeprecatedEOF();
        TestFEOpExecutor executor = new TestFEOpExecutor(context);
        for (byte[] bytes : Arrays.asList(new byte[] {0, 7, 0, 2, 0, 3, 0, 4, 'i', 'n', 'f', 'o'},
                new byte[] {(byte) 0xFF, 1, 2})) {
            TMasterOpResult result = new TMasterOpResult();
            ByteBuffer packet = ByteBuffer.wrap(bytes);
            result.setPacket(packet);
            executor.setResult(result);
            executor.prepareQueryResultForClient();
            Assert.assertEquals(packet, executor.getOutputPacket());
        }
    }

    private ConnectContext createContext() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        context.setRemoteIP("127.0.0.1");
        context.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
        return context;
    }

    private static class TestFEOpExecutor extends FEOpExecutor {
        private TestFEOpExecutor(ConnectContext context) {
            super(new TNetworkAddress("127.0.0.1", 9010), new OriginStatement("select 1", 0), context, true);
        }

        private TMasterOpRequest build() throws AnalysisException {
            return buildStmtForwardParams();
        }

        private void setResult(TMasterOpResult result) {
            this.result = result;
        }
    }
}
