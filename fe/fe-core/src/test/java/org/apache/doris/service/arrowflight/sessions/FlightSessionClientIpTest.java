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

package org.apache.doris.service.arrowflight.sessions;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

/**
 * An Arrow Flight SQL session reported 0.0.0.0 everywhere a client address is shown - the Host column
 * of SHOW PROCESSLIST and information_schema.processlist, the audit log's client_ip, and the
 * kill/timeout warnings - because FlightSqlChannel had those accessors stubbed out. The real address
 * is resolved when the bearer token is issued (FlightRemoteIpServerStreamTracer) and stored on the
 * context, so that is what the session reports.
 */
public class FlightSessionClientIpTest {
    private static final String CLIENT_IP = "10.26.20.3";
    private static final String UNKNOWN_IP = "0.0.0.0";

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
    public void testProcesslistAndAuditSeeTheAuthenticatedClientIp() {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
            ctx.setRemoteIP(CLIENT_IP);

            Assertions.assertEquals(CLIENT_IP, ctx.getRemoteHostPortString());
            // AuditLogHelper and LineageUtils read the address through getClientIP().
            Assertions.assertEquals(CLIENT_IP, ctx.getClientIP());

            List<String> row = ctx.toThreadInfo(false).toRow(1, System.currentTimeMillis(), Optional.empty());
            // Host is the fourth column of SHOW PROCESSLIST / information_schema.processlist.
            Assertions.assertEquals(CLIENT_IP, row.get(3));
        }
    }

    @Test
    public void testFallsBackWhenTheAddressWasNotResolved() {
        try (MockedStatic<Env> mockedEnv = mockSelfNode()) {
            ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");

            Assertions.assertEquals(UNKNOWN_IP, ctx.getRemoteHostPortString());
            ctx.setRemoteIP("");
            Assertions.assertEquals(UNKNOWN_IP, ctx.getRemoteHostPortString());
        }
    }

    private static MockedStatic<Env> mockSelfNode() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }
}
