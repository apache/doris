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

import org.apache.doris.arrowflight.results.FlightSqlResultCacheEntry;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A frontend-side result of an Arrow Flight SQL session is cached for the client's DoGet; a
 * backend result never passes through the frontend.
 */
public class FlightResultSenderTest {
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
    public void testResultSetIsCachedUnderTheQueryId() throws Exception {
        ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        TUniqueId queryId = new TUniqueId(3, 4);
        ctx.setQueryId(queryId);
        ctx.setRunningQuery("show variables");
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("wait_timeout", "28800"));
        rows.add(Lists.newArrayList("x", null));
        ResultSet resultSet = new CommonResultSet(
                new CommonResultSetMetaData(Lists.newArrayList(
                        new Column("Variable_name", PrimitiveType.VARCHAR), new Column("Value", PrimitiveType.VARCHAR))),
                rows);

        ctx.getResultSender().sendResultSet(resultSet, null, false);

        Assertions.assertEquals(1, adapter.getChannel().resultNum());
        FlightSqlResultCacheEntry entry = adapter.getChannel().getResult(DebugUtil.printId(queryId));
        Assertions.assertNotNull(entry);
        Assertions.assertEquals("show variables", entry.getQuery());
        VectorSchemaRoot root = entry.getVectorSchemaRoot();
        Assertions.assertEquals(2, root.getRowCount());
        Assertions.assertEquals("28800",
                new String(((VarCharVector) root.getVector("Value")).get(0), StandardCharsets.UTF_8));
        Assertions.assertTrue(root.getVector("Value").isNull(1));
        adapter.getChannel().close();
    }

    @Test
    public void testBackendResultsNeverPassThroughTheFrontend() {
        ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");

        Assertions.assertThrows(IllegalStateException.class, () -> ctx.getResultSender()
                .sendFields(Lists.newArrayList("k1"), null, Lists.newArrayList(Type.INT)));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ctx.getResultSender().sendRow(ByteBuffer.wrap(new byte[] {1, 49})));
        // Nothing is pending on a Flight session when a query starts.
        ctx.getResultSender().reset();
        Assertions.assertEquals(0, FlightProtocolAdapter.of(ctx).getChannel().resultNum());
    }
}
