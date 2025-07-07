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

package org.apache.doris.mysql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsWithTokenManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenDetails;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;
import org.xnio.StreamConnection;

public class ConnectionExceedTest {
    @Mocked
    private Auth mockAuth;

    @Mocked
    private Env mockEnv;

    @Mocked
    private StreamConnection mockConnection;

    @Mocked
    private MysqlProto mysqlProto;

    @Mocked
    private FlightTokenManager mockTokenManager;

    @Mocked
    private ExecuteEnv mockExecuteEnv;

    @Test
    public void testHandleConnectionExceed() throws Exception {
        // Create a scheduler with small max connections
        ConnectScheduler scheduler = new ConnectScheduler(2);

        // Setup expectations
        new Expectations() {
            {
                mockEnv.getAuth();
                result = mockAuth;

                mockAuth.getMaxConn("test_user");
                result = 2;

                // Mock MysqlProto.negotiate to return true to simulate successful authentication
                MysqlProto.negotiate((ConnectContext) any);
                result = true;
            }
        };

        // Create first context and register
        ConnectContext context1 = new ConnectContext();
        context1.setEnv(mockEnv);
        context1.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        Assert.assertTrue(scheduler.submit(context1));
        Assert.assertEquals(-1, scheduler.getConnectPoolMgr().registerConnection(context1));

        // Create second context and register
        ConnectContext context2 = new ConnectContext();
        context2.setEnv(mockEnv);
        context2.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        Assert.assertTrue(scheduler.submit(context2));
        Assert.assertEquals(-1, scheduler.getConnectPoolMgr().registerConnection(context2));

        // Create third context and try to register - should fail
        ConnectContext context3 = new ConnectContext();
        context3.setEnv(mockEnv);
        context3.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        Assert.assertTrue(scheduler.submit(context3));

        // Create AcceptListener and handle the connection
        AcceptListener listener = new AcceptListener(scheduler);
        listener.handleConnection(context3, mockConnection);
        String expectedMsg = String.format(
                "Reach limit of connections. Total: %d, User: %d, Current: %d",
                scheduler.getConnectPoolMgr().getMaxConnections(),
                2, // Mocked user connection limit
                scheduler.getConnectionNum());
        Assert.assertEquals(expectedMsg, context3.getState().getErrorMessage());
        Assert.assertEquals(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, context3.getState().getErrorCode());
    }

    @Test
    public void testFlightSessionConnectionExceed() throws Exception {
        // Create a scheduler with small max connections
        ConnectScheduler scheduler = new ConnectScheduler(1000, 2);

        // Setup expectations
        new Expectations() {
            {
                // Arrow flight sql not check the number of user connections.
                // mockEnv.getAuth();
                // result = mockAuth;
                // mockAuth.getMaxConn("test_user");
                // result = 2;

                mockExecuteEnv.getScheduler();
                result = scheduler;

                UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
                FlightTokenDetails tokenDetails = new FlightTokenDetails(
                        "test_token",
                        "test_user",
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3600000, // expires in 1 hour
                        userIdentity,
                        "127.0.0.1"
                );
                mockTokenManager.validateToken("test_token");
                result = tokenDetails;
            }
        };

        // Create first context and register
        ConnectContext context1 = new ConnectContext();
        context1.setEnv(mockEnv);
        context1.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        Assert.assertTrue(scheduler.submit(context1));
        Assert.assertEquals(-1, scheduler.getFlightSqlConnectPoolMgr().registerConnection(context1));

        // Create second context and register
        ConnectContext context2 = new ConnectContext();
        context2.setEnv(mockEnv);
        context2.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
        Assert.assertTrue(scheduler.submit(context2));
        Assert.assertEquals(-1, scheduler.getFlightSqlConnectPoolMgr().registerConnection(context2));

        // Create FlightSessionsWithTokenManager and try to create a new connection
        FlightSessionsWithTokenManager manager = new FlightSessionsWithTokenManager(mockTokenManager);
        try {
            manager.createConnectContext("test_token");
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Verify error message is set correctly
            String expectedMsg = String.format(
                    "Register arrow flight sql connection failed, Unknown Error, the number of arrow flight "
                            + "bearer tokens should be equal to arrow flight sql max connections, "
                            + "max connections: %d, used: %d.",
                    scheduler.getFlightSqlConnectPoolMgr().getMaxConnections(),
                    scheduler.getConnectionNum());
            Assert.assertEquals(expectedMsg, e.getMessage());
        }
    }
}
