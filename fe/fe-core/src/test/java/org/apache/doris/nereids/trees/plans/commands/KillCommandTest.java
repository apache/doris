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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.utils.KillUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit test for {@link KillConnectionCommand} and {@link KillQueryCommand}.
 */
public class KillCommandTest {

    private ConnectContext mockContext;
    private StmtExecutor mockExecutor;
    private OriginStatement mockOriginStmt;

    @BeforeEach
    public void setUp() {
        mockContext = Mockito.mock(ConnectContext.class);
        mockExecutor = Mockito.mock(StmtExecutor.class);
        mockOriginStmt = Mockito.mock(OriginStatement.class);

        // Setup the getOriginStmt method to return our mock
        Mockito.when(mockExecutor.getOriginStmt()).thenReturn(null);
    }

    /**
     * Test the doRun method's exception case with negative connectionId for KillConnectionCommand.
     */
    @Test
    public void testKillConnectionWithNegativeConnectionId() {
        // Create command with a negative connection ID
        KillConnectionCommand command = new KillConnectionCommand(-1);

        // Verify the expected exception is thrown
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> command.doRun(mockContext, mockExecutor)
        );

        // Verify the exception message
        Assertions.assertTrue(exception.getMessage().contains("Please specify connection id which >= 0 to kill"));
    }

    /**
     * Test that KillUtils.kill is called and leads to calling KillUtils.killByConnectionId.
     */
    @Test
    public void testKillConnectionCallsKillByConnectionId() throws Exception {
        // Set up a valid connection ID
        final int connectionId = 123;

        // Create a command with the valid connection ID
        KillConnectionCommand command = new KillConnectionCommand(connectionId);

        // Use Mockito's static mocking to intercept KillUtils methods
        try (MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class)) {
            // Execute the command
            command.doRun(mockContext, mockExecutor);

            // Verify KillUtils.kill was called with correct parameters
            mockedKillUtils.verify(() -> KillUtils.kill(
                    Mockito.eq(mockContext),
                    Mockito.eq(true),
                    Mockito.isNull(),
                    Mockito.eq(connectionId),
                    Mockito.isNull()
                )
            );
        }
    }

    /**
     * Test that KillConnectionCommand ultimately calls killByConnectionId through KillUtils.kill.
     */
    @Test
    public void testKillConnectionChainToKillByConnectionId() throws Exception {
        // Set up a valid connection ID
        final int connectionId = 123;

        // Create a command with the valid connection ID
        KillConnectionCommand command = new KillConnectionCommand(connectionId);

        // Track whether killByConnectionId was called
        final boolean[] killByConnectionIdCalled = {false};

        // Use Mockito's static mocking to intercept KillUtils methods
        try (MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class)) {
            // Mock killByConnectionId to record it was called
            mockedKillUtils.when(() -> KillUtils.killByConnectionId(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyBoolean(),
                    Mockito.anyInt()
            )).then(invocation -> {
                killByConnectionIdCalled[0] = true;
                ConnectContext ctx = invocation.getArgument(0);
                boolean killConn = invocation.getArgument(1);
                int connId = invocation.getArgument(2);

                // Verify correct parameters
                Assertions.assertSame(mockContext, ctx);
                Assertions.assertTrue(killConn);
                Assertions.assertEquals(connectionId, connId);
                return null;
            });

            // Mock the kill method to call the real killByConnectionId
            mockedKillUtils.when(() -> KillUtils.kill(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyBoolean(),
                    Mockito.any(),
                    Mockito.anyInt(),
                    Mockito.any()
            )).then(invocation -> {
                ConnectContext ctx = invocation.getArgument(0);
                boolean killConn = invocation.getArgument(1);
                int connId = invocation.getArgument(3);

                if (killConn) {
                    KillUtils.killByConnectionId(ctx, killConn, connId);
                }
                return null;
            });

            // Execute the command
            command.doRun(mockContext, mockExecutor);

            // Verify killByConnectionId was called
            Assertions.assertTrue(killByConnectionIdCalled[0], "KillUtils.killByConnectionId should have been called");
        }
    }

    /**
     * Test the doRun method's exception case with empty parameters for KillQueryCommand.
     */
    @Test
    public void testKillQueryWithEmptyParameters() {
        // Create command with empty queryId and negative connectionId
        KillQueryCommand command = new KillQueryCommand(null, -1);

        // Verify the expected exception is thrown
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> command.doRun(mockContext, mockExecutor)
        );

        // Verify the exception message
        Assertions.assertTrue(exception.getMessage().contains(
                "Please specify a non empty query id or connection id which >= 0 to kill"));
    }

    /**
     * Test KillQueryCommand when using queryId calls killQueryByQueryId.
     */
    @Test
    public void testKillQueryWithQueryId() throws Exception {
        // Set up a valid query ID
        final String queryId = "test_query_id";

        // Create a command with the valid query ID
        KillQueryCommand command = new KillQueryCommand(queryId, -1);

        // Track whether killQueryByQueryId was called
        final boolean[] killQueryByQueryIdCalled = {false};

        // Use Mockito's static mocking to intercept KillUtils methods
        try (MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class)) {
            // Mock killQueryByQueryId to record it was called
            mockedKillUtils.when(() -> KillUtils.killQueryByQueryId(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyString(),
                    Mockito.any()
            )).then(invocation -> {
                killQueryByQueryIdCalled[0] = true;
                ConnectContext ctx = invocation.getArgument(0);
                String qId = invocation.getArgument(1);

                // Verify correct parameters
                Assertions.assertSame(mockContext, ctx);
                Assertions.assertEquals(queryId, qId);
                return null;
            });

            // Mock the kill method to call the real killQueryByQueryId
            mockedKillUtils.when(() -> KillUtils.kill(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyBoolean(),
                    Mockito.anyString(),
                    Mockito.anyInt(),
                    Mockito.any()
            )).then(invocation -> {
                ConnectContext ctx = invocation.getArgument(0);
                boolean killConn = invocation.getArgument(1);
                String qId = invocation.getArgument(2);
                OriginStatement stmt = invocation.getArgument(4);

                if (!killConn && qId != null) {
                    KillUtils.killQueryByQueryId(ctx, qId, stmt);
                }
                return null;
            });

            // Execute the command
            command.doRun(mockContext, mockExecutor);

            // Verify killQueryByQueryId was called
            Assertions.assertTrue(killQueryByQueryIdCalled[0], "KillUtils.killQueryByQueryId should have been called");

            // Verify KillUtils.kill was called with correct parameters
            mockedKillUtils.verify(() -> KillUtils.kill(
                    Mockito.eq(mockContext),
                    Mockito.eq(false),
                    Mockito.eq(queryId),
                    Mockito.eq(-1),
                    Mockito.isNull()
                )
            );
        }
    }

    /**
     * Test KillQueryCommand when using connectionId calls killByConnectionId.
     */
    @Test
    public void testKillQueryWithConnectionId() throws Exception {
        // Set up a valid connection ID
        final int connectionId = 123;

        // Create a command with null queryId and valid connectionId
        KillQueryCommand command = new KillQueryCommand(null, connectionId);

        // Track whether killByConnectionId was called
        final boolean[] killByConnectionIdCalled = {false};

        // Use Mockito's static mocking to intercept KillUtils methods
        try (MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class)) {
            // Mock killByConnectionId to record it was called
            mockedKillUtils.when(() -> KillUtils.killByConnectionId(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyBoolean(),
                    Mockito.anyInt()
            )).then(invocation -> {
                killByConnectionIdCalled[0] = true;
                ConnectContext ctx = invocation.getArgument(0);
                boolean killConn = invocation.getArgument(1);
                int connId = invocation.getArgument(2);

                // Verify correct parameters
                Assertions.assertSame(mockContext, ctx);
                Assertions.assertFalse(killConn);
                Assertions.assertEquals(connectionId, connId);
                return null;
            });

            // Mock the kill method to call the real killByConnectionId
            mockedKillUtils.when(() -> KillUtils.kill(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyBoolean(),
                    Mockito.any(),
                    Mockito.anyInt(),
                    Mockito.any()
            )).then(invocation -> {
                ConnectContext ctx = invocation.getArgument(0);
                boolean killConn = invocation.getArgument(1);
                String qId = invocation.getArgument(2);
                int connId = invocation.getArgument(3);

                if (!killConn && qId == null) {
                    KillUtils.killByConnectionId(ctx, killConn, connId);
                }
                return null;
            });

            // Execute the command
            command.doRun(mockContext, mockExecutor);

            // Verify killByConnectionId was called
            Assertions.assertTrue(killByConnectionIdCalled[0], "KillUtils.killByConnectionId should have been called");

            // Verify KillUtils.kill was called with correct parameters
            mockedKillUtils.verify(() -> KillUtils.kill(
                    Mockito.eq(mockContext),
                    Mockito.eq(false),
                    Mockito.isNull(),
                    Mockito.eq(connectionId),
                    Mockito.isNull()
                )
            );
        }
    }
}
