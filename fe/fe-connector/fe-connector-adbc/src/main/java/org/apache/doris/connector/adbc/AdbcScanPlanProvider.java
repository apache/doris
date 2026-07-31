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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Turns a scan into the single remote statement that serves it.
 *
 * <p>One range, one statement. The design's distributed read splits a query into the driver's own partition
 * descriptors and hands one to each backend, but BE has no reader for those yet, so planning several ranges
 * would produce work nothing can run. The shape here is the one the JDBC connector has always had, and the
 * partitioned path replaces this method's body without changing anything around it.
 */
public class AdbcScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(AdbcScanPlanProvider.class);

    /** Names the reader on BE. Kept next to {@link AdbcScanRange#getFileFormat()}, which must agree. */
    private static final String FILE_FORMAT = "arrow";

    private final Map<String, String> properties;
    private final Path driverPath;
    private final AdbcDialectSelector dialectSelector;
    private final Supplier<AdbcClient> clientSupplier;

    public AdbcScanPlanProvider(Map<String, String> properties, Path driverPath,
            AdbcDialectSelector dialectSelector, Supplier<AdbcClient> clientSupplier) {
        this.properties = properties;
        this.driverPath = driverPath;
        this.dialectSelector = dialectSelector;
        this.clientSupplier = clientSupplier;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        AdbcTableHandle handle = adbcHandle(request.getTableHandle());
        AdbcQueryBuilder.BuiltQuery query = buildQuery(handle, request.getColumns(),
                request.getFilter(), request.getLimit());
        LOG.debug("ADBC scan of {}.{}: {}", handle.getDorisDbName(), handle.getRemoteTable(),
                query.getSql());
        return Collections.singletonList(newRange(query.getSql()));
    }

    /**
     * The properties the scan node reads before it has any ranges: the reader to use, and the statement to
     * show in {@code EXPLAIN}.
     *
     * <p>It regenerates the statement rather than reusing one, because {@code EXPLAIN} never calls
     * {@link #planScan}. The two must agree or {@code EXPLAIN} describes a query that will not be run; a
     * test pins that they do.
     */
    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        Map<String, String> props = new HashMap<>();
        props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, FILE_FORMAT);
        // No row limit here: EXPLAIN has none to show, and planScan applies its own.
        props.put(ScanNodePropertyKeys.REMOTE_QUERY,
                buildQuery(adbcHandle(handle), columns, filter, -1L).getSql());
        return props;
    }

    private AdbcQueryBuilder.BuiltQuery buildQuery(AdbcTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter, long limit) {
        return AdbcQueryBuilder.build(dialectSelector.select(clientSupplier), handle, columns,
                filter, limit);
    }

    private AdbcScanRange newRange(String sql) {
        return new AdbcScanRange.Builder()
                .driverPath(driverPath.toString())
                .driverEntrypoint(properties.get(AdbcConnectorProperties.DRIVER_ENTRYPOINT))
                .uri(AdbcConnectorProperties.require(properties, AdbcConnectorProperties.URI))
                .username(properties.get(AdbcConnectorProperties.USER))
                .password(properties.get(AdbcConnectorProperties.PASSWORD))
                .querySql(sql)
                .driverOptions(AdbcConnectorProperties.driverOptions(properties))
                .build();
    }

    private static AdbcTableHandle adbcHandle(ConnectorTableHandle handle) {
        if (handle instanceof AdbcTableHandle) {
            return (AdbcTableHandle) handle;
        }
        // Reached by a passthrough-SQL table handle (a table-valued function forwarding raw SQL), which
        // this connector does not accept yet. Naming the handle keeps the message useful when it does.
        throw new DorisConnectorException("An adbc catalog cannot scan through a "
                + handle.getClass().getSimpleName() + "; only its own tables are readable.");
    }
}
