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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.protocol.ResultSender;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Delivers the results of an Arrow Flight SQL session. A result the frontend materialized is
 * cached on the session's {@code FlightSqlChannel} under the statement's query id, and
 * {@code DorisFlightSqlProducer} hands it out when the client's DoGet arrives. A result a backend
 * produces never passes through the frontend: the client pulls it from the backend with the
 * endpoints GetFlightInfo returned, so this sender has no row stream.
 *
 * <p>Every column of a cached result is a {@code Utf8} vector for now; typing them by the result
 * set's column types is a later step.
 */
public class FlightResultSender implements ResultSender {
    private final ConnectContext ctx;
    private final FlightProtocolAdapter adapter;

    FlightResultSender(ConnectContext ctx, FlightProtocolAdapter adapter) {
        this.ctx = ctx;
        this.adapter = adapter;
    }

    @Override
    public void sendResultSet(ResultSet resultSet, List<FieldInfo> fieldInfos, boolean binaryRows) {
        adapter.getChannel().addResult(DebugUtil.printId(ctx.queryId()), adapter.getRunningQuery(), resultSet);
        // The statement's result is on this frontend, whatever the query path decided earlier: an
        // EXPLAIN goes through the query path, which marks the result as coming from the backend
        // before it knows the statement will not run there.
        adapter.setReturnResultFromLocal(true);
    }

    @Override
    public void sendFields(List<String> colNames, List<FieldInfo> fieldInfos, List<Type> types) {
        throw new IllegalStateException(
                "an Arrow Flight SQL client pulls query results from the backend, not through the frontend");
    }

    @Override
    public void sendRow(ByteBuffer row) {
        throw new IllegalStateException(
                "an Arrow Flight SQL client pulls query results from the backend, not through the frontend");
    }

    @Override
    public void reset() {
        // Results are cached per query id and the cache is cleared when the next request of the
        // session starts (DorisFlightSqlProducer.executeQueryStatement); nothing is pending here.
    }
}
