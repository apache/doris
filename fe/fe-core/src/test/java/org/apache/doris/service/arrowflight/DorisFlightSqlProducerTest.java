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

package org.apache.doris.service.arrowflight;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class DorisFlightSqlProducerTest {

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259):
    // executeAndSendResult() registers it as a deferred executor on the ConnectContext right after
    // submitting it to the BE. GetFlightInfo then still has to fetch the Arrow schema from the BE.
    // If that fetch fails (timeout / non-OK / empty / mismatched schema / RPC error), no FlightInfo
    // is returned, so no DoGet will ever pull this query's results. The deferred coordinator must be
    // finalized on this error path; otherwise its external-table batch SplitSource, query queue slot
    // and query registration leak until the next query starts or the connection is torn down.
    @Test
    public void testGetFlightInfoFinalizesDeferredExecutorWhenSchemaFetchFails() throws Exception {
        // A flight ConnectContext whose getFlightSqlChannel() works (the base context throws).
        ConnectContext ctx = Mockito.spy(new ConnectContext());
        Mockito.doReturn(Mockito.mock(FlightSqlChannel.class)).when(ctx).getFlightSqlChannel();

        // Stands in for the just-planned external-table query whose results DoGet would pull from BE.
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);

        FlightSessionsManager sessionsManager = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessionsManager.getConnectContext(Mockito.anyString())).thenReturn(ctx);

        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("token");

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager);
        try (MockedConstruction<FlightSqlConnectProcessor> mocked = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (mock, context) -> {
                    // handleQuery plans + submits to BE and defers the coordinator (coordBase == coord),
                    // exactly as executeAndSendResult() does for an Arrow Flight external-table scan.
                    Mockito.doAnswer(invocation -> {
                        ctx.setReturnResultFromLocal(false);
                        ctx.addFlightSqlDeferredExecutor(deferred);
                        return null;
                    }).when(mock).handleQuery(Mockito.anyString());
                    // The Arrow schema fetch fails after the coordinator was already deferred.
                    Mockito.doThrow(new RuntimeException("fetch arrow flight schema timeout"))
                            .when(mock).fetchArrowFlightSchema(Mockito.anyInt());
                })) {
            CommandStatementQuery request = CommandStatementQuery.newBuilder().setQuery("select 1").build();
            FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);

            try {
                producer.getFlightInfoStatement(request, callContext, descriptor);
                Assert.fail("expected the schema fetch failure to propagate as a CallStatus");
            } catch (Throwable expected) {
                // GetFlightInfo is expected to fail; the point of the test is what happens to the
                // deferred coordinator, not the thrown status itself.
            }

            // The deferred coordinator of the failed query is finalized on the error path instead of
            // leaking until the next query / connection teardown.
            Mockito.verify(deferred).finalizeArrowFlightQuery();

            // It is also removed from the deferred list, so a later teardown does not finalize it
            // again (no double-close, no retained reference).
            ctx.closeFlightSqlDeferredExecutors();
            Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
        } finally {
            producer.close();
        }
    }
}
