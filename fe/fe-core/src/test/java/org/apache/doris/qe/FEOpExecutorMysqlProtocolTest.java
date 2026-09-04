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
import org.apache.doris.mysql.MysqlCommand;
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
import java.util.Collections;

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
        Assert.assertEquals(0L, executor.getAffectedRows());

        TMasterOpResult confirmed = new TMasterOpResult();
        confirmed.setClientDeprecatedEofApplied(true);
        confirmed.setQueryResultBufList(Collections.singletonList(ByteBuffer.wrap(new byte[] {1})));
        confirmed.setAffectedRows(7);
        executor.setResult(confirmed);
        Assert.assertTrue(executor.isClientDeprecatedEofApplied());
        Assert.assertTrue(executor.hasQueryResultPackets());
        Assert.assertEquals(7L, executor.getAffectedRows());
    }

    private ConnectContext createContext() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        context.setRemoteIP("127.0.0.1");
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
