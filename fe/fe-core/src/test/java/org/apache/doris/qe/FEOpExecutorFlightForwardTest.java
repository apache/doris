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
import org.apache.doris.common.FeConstants;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Building the forward request for the master FE must not reach for a MysqlChannel unconditionally:
 * an Arrow Flight SQL session has none, so every statement a Flight connection forwards (any DDL on
 * a non-master FE, or anything at all under force_forward_all_queries) used to fail with
 * "getMysqlChannel not in mysql connection". CLIENT_DEPRECATE_EOF is a MySQL protocol capability and
 * is only carried for MySQL connections.
 */
public class FEOpExecutorFlightForwardTest {
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
    public void testFlightSessionForwardsWithoutMysqlChannel() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            FlightSqlConnectContext context = new FlightSqlConnectContext("test-peer-identity");
            prepare(context);

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertFalse(request.isSetClientDeprecatedEOF());
            Assertions.assertEquals("select 1", request.getSql());
        }
    }

    @Test
    public void testMysqlSessionKeepsCarryingDeprecatedEof() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext context = new ConnectContext();
            prepare(context);
            context.getMysqlChannel().setClientDeprecatedEOF();

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertTrue(request.isSetClientDeprecatedEOF());
            Assertions.assertTrue(request.isClientDeprecatedEOF());
        }
    }

    @Test
    public void testMysqlSessionWithoutDeprecatedEof() throws Exception {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext context = new ConnectContext();
            prepare(context);

            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertTrue(request.isSetClientDeprecatedEOF());
            Assertions.assertFalse(request.isClientDeprecatedEOF());
        }
    }

    private static MockedStatic<Env> mockSelfNode() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }

    private static void prepare(ConnectContext context) {
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        context.setRemoteIP("127.0.0.1");
    }

    private static class TestFEOpExecutor extends FEOpExecutor {
        private TestFEOpExecutor(ConnectContext context) {
            super(new TNetworkAddress("127.0.0.1", 9010), new OriginStatement("select 1", 0), context, true);
        }

        private TMasterOpRequest build() throws AnalysisException {
            return buildStmtForwardParams();
        }
    }
}
