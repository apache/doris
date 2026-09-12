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
import org.apache.doris.arrowflight.FlightSqlConnectProcessor;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The execution layer does not branch on the protocol a connection speaks; it asks the
 * connection's {@code ProtocolAdapter}. The adapter tests pin what each adapter answers; these
 * tests drive real statements through the processors and the executor and check that the answer
 * is what decides, where a unit test of the adapter alone cannot show it.
 */
public class ProtocolCapabilityWiringTest extends TestWithFeService {
    private static final String DB_NAME = "protocol_capability_wiring_db";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse(DB_NAME);
    }

    // On a follower a query is forwarded to the master when the session asks for it
    // (ForceForwardAllQueriesTest); an Arrow Flight SQL session cannot take the master's answer
    // and is refused before any rpc, a MySQL connection goes on to forward.
    @Test
    public void testForwardedQueryIsRefusedOnAFlightSessionBeforeTheRpc() throws Exception {
        Env env = Env.getCurrentEnv();
        FrontendNodeType originalFeType = env.getFeType();
        AtomicBoolean canRead = Deencapsulation.getField(env, "canRead");
        boolean originalCanRead = canRead.get();
        boolean originalForceForward = Config.force_forward_all_queries;
        Deencapsulation.setField(env, "feType", FrontendNodeType.FOLLOWER);
        canRead.set(true);
        Config.force_forward_all_queries = false;
        try {
            ConnectContext flight = flightContext();
            flight.getSessionVariable().forceForwardAllQueries = true;
            StmtExecutor refused = new StmtExecutor(flight, analyzeAndGetStmtByNereids("select 1", flight));

            UserException e = Assertions.assertThrows(UserException.class, refused::execute);

            Assertions.assertTrue(e.getMessage().contains("not supported on an Arrow Flight SQL connection"),
                    e.getMessage());
            Assertions.assertFalse(refused.hasForwardedToMaster());
            Assertions.assertEquals(MysqlStateType.ERR, flight.getState().getStateType());

            ConnectContext mysql = createDefaultCtx();
            mysql.setDatabase(DB_NAME);
            mysql.setThreadLocalInfo();
            mysql.getSessionVariable().forceForwardAllQueries = true;
            StmtExecutor forwarded = new StmtExecutor(mysql, analyzeAndGetStmtByNereids("select 1", mysql));
            try {
                forwarded.execute();
            } catch (Exception expected) {
                // There is no master to reach from a unit test; what matters is that the
                // statement was handed to the forwarding path instead of being refused.
            }
            Assertions.assertTrue(forwarded.hasForwardedToMaster());
        } finally {
            Deencapsulation.setField(env, "feType", originalFeType);
            canRead.set(originalCanRead);
            Config.force_forward_all_queries = originalForceForward;
            connectContext.setThreadLocalInfo();
        }
    }

    // The previous statement of a Flight request left its result on a backend; the next one is
    // answered by this frontend again, because the processor tells the adapter where a statement
    // starts.
    @Test
    public void testAStatementStartsOnTheFrontendWhateverThePreviousOneDid() throws Exception {
        ConnectContext flight = flightContext();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(flight);
        adapter.beforeQuery(flight);
        Assertions.assertFalse(flight.isReturnResultFromLocal());
        try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(flight)) {
            processor.handleQuery("show variables like 'wait_timeout'");

            Assertions.assertNotEquals(MysqlStateType.ERR, flight.getState().getStateType(),
                    flight.getState().getErrorMessage());
            Assertions.assertTrue(flight.isReturnResultFromLocal());
            Assertions.assertEquals(1, adapter.getChannel().resultNum());
        } finally {
            adapter.getChannel().close();
            connectContext.setThreadLocalInfo();
        }
    }

    // The master answers a forwarded query for the client of the frontend that forwarded it: the
    // packets it collects and the response that ends them follow the capabilities the request
    // carries, restored into the proxy context by the adapter.
    @Test
    public void testMasterAnswersAForwardedQueryWithTheClientsCapabilities() throws Exception {
        for (boolean deprecateEof : new boolean[] {true, false}) {
            int flags = MysqlCapability.DEFAULT_CAPABILITY.getFlags();
            if (!deprecateEof) {
                flags &= ~MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit();
            }
            TMasterOpRequest request = new TMasterOpRequest();
            request.setDb(DB_NAME);
            request.setUser("root");
            request.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
            request.setSql("select 1");
            request.setMysqlCapability(flags);
            request.setClientDeprecatedEOF(deprecateEof);
            ConnectContext proxy = ConnectContext.forMysqlProxy("session-1");
            try {
                TMasterOpResult result = new MysqlConnectProcessor(proxy).proxyExecute(request);

                // A query's state is EOF, the end of its result set; only an OK state sets statusCode 0.
                Assertions.assertEquals(MysqlStateType.EOF.name(), result.getStatus(),
                        proxy.getState().getErrorMessage());
                // column count, one column definition, the terminator the client expects, one row
                List<ByteBuffer> packets = result.getQueryResultBufList();
                Assertions.assertEquals(deprecateEof ? 3 : 4, packets.size());
                Assertions.assertEquals(1, MysqlProto.readVInt(packets.get(0).duplicate()));
                if (!deprecateEof) {
                    ByteBuffer eof = packets.get(2);
                    Assertions.assertEquals(0xFE, Byte.toUnsignedInt(eof.get(eof.position())));
                    Assertions.assertEquals(5, eof.remaining());
                }
                // and the packet that ends the result set is an EOF, or an OK with the 0xFE header
                ByteBuffer end = result.packet;
                Assertions.assertEquals(0xFE, Byte.toUnsignedInt(end.get(end.position())));
                Assertions.assertEquals(deprecateEof, end.remaining() > 5);
                Assertions.assertEquals(deprecateEof, result.isClientDeprecatedEofApplied());
            } finally {
                connectContext.setThreadLocalInfo();
            }
        }
    }

    private ConnectContext flightContext() {
        ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setDatabase(DB_NAME);
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
