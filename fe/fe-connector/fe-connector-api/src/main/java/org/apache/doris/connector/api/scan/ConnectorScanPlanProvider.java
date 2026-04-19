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

package org.apache.doris.connector.api.scan;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Plans the set of scan ranges (splits) needed to read a connector table.
 *
 * <p>This is a core SPI interface that every connector with scan capability
 * must implement. The engine calls {@link #planScan} to obtain scan ranges,
 * which are then converted to Thrift structures and dispatched to BE.</p>
 */
public interface ConnectorScanPlanProvider {

    /**
     * Returns the scan range type this provider produces.
     *
     * <p>The engine uses this to determine which Thrift scan range structure
     * to generate. For example, {@link ConnectorScanRangeType#FILE_SCAN}
     * produces TFileScanRange, while {@link ConnectorScanRangeType#ES_SCAN}
     * produces TEsScanRange.</p>
     *
     * @return the scan range type (default: FILE_SCAN)
     */
    default ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    /**
     * Plans the scan for the given table, returning a list of scan ranges.
     *
     * @param session the current session
     * @param handle  the table handle to scan (may have been updated by applyFilter/applyProjection)
     * @param columns the columns to read
     * @param filter  an optional filter expression (remaining after pushdown)
     * @return a list of scan ranges that cover the requested data
     */
    List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter);

    /**
     * Plans the scan with an optional row limit.
     *
     * <p>Some connectors (e.g., JDBC) can push the limit into the remote query
     * to reduce data transfer. The default delegates to the 4-arg planScan,
     * ignoring the limit.</p>
     *
     * @param session the current session
     * @param handle  the table handle
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @param limit   the maximum number of rows to return, or -1 for no limit
     * @return a list of scan ranges
     */
    default List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit) {
        return planScan(session, handle, columns, filter);
    }

    /**
     * Returns scan-node-level properties shared across all scan ranges.
     *
     * <p>Unlike per-range properties in {@link ConnectorScanRange#getProperties()},
     * these properties apply to the entire scan node. For example, ES connectors
     * return the query DSL, authentication info, and field context mappings here,
     * since they are shared across all shard scan ranges.</p>
     *
     * @param session the current session
     * @param handle  the table handle (may have been updated by applyFilter)
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @return node-level properties (default: empty map)
     */
    default Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return Collections.emptyMap();
    }

    /**
     * Estimates the number of scan ranges for parallelism planning.
     * Returns -1 if the estimate is unknown.
     *
     * <p>The engine may use this to pre-allocate resources or decide
     * scan parallelism before calling {@link #planScan}.</p>
     */
    default long estimateScanRangeCount(ConnectorSession session,
            ConnectorTableHandle handle) {
        return -1;
    }
}
