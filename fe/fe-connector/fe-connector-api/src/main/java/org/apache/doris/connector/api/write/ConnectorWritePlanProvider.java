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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;

/**
 * Plans the write (sink) for a connector table: produces the opaque
 * {@link org.apache.doris.thrift.TDataSink} that BE uses to write data.
 *
 * <p>This is the write-side analogue of
 * {@link org.apache.doris.connector.api.scan.ConnectorScanPlanProvider}. A
 * connector with write capability returns an implementation from
 * {@link org.apache.doris.connector.api.Connector#getWritePlanProvider()}; the
 * engine calls {@link #planWrite} when translating a physical table sink, then
 * dispatches the resulting Thrift data sink to BE unchanged.</p>
 */
public interface ConnectorWritePlanProvider {

    /**
     * Builds the data sink for the given bound write request.
     *
     * @param session the current session
     * @param handle  the bound write request (target table, columns, overwrite, context)
     * @return a {@link ConnectorSinkPlan} wrapping the Thrift data sink
     */
    ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle);

    /**
     * Appends connector-specific EXPLAIN detail for the write (e.g. the generated INSERT SQL,
     * sink dialect, target format). Write-side analogue of the scan provider's
     * {@code appendExplainInfo}: the engine emits the generic plugin-driven sink line, then calls
     * this so the connector can surface its own write details without the engine knowing the
     * sink dialect.
     *
     * <p>This runs when the plan's EXPLAIN string is generated, which is <i>before</i> the write
     * plan is bound (the sink's {@code planWrite} has not run yet for an EXPLAIN). The connector
     * therefore derives the detail from the {@code handle} and may consult the source for metadata
     * (e.g. remote column names). Default: no extra EXPLAIN info.</p>
     *
     * @param output  the EXPLAIN string being built
     * @param prefix  the current indentation prefix
     * @param session the current session (may be consulted for session-scoped write options)
     * @param handle  the write request (target table handle and write columns)
     */
    default void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Default: no extra EXPLAIN info
    }
}
