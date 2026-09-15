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


package org.apache.doris.job.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPassthroughSqlOps;
import org.apache.doris.connector.spi.ConnectorQueryResult;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.connector.converter.ConnectorColumnConverter;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The streaming (CDC) framework's view of a source database, served by the source's connector plugin
 * through the connector SPI: the tables of a remote database, their columns as Doris columns, their
 * primary keys, and the small probe queries the framework runs before it starts a job.
 *
 * <p>One instance is one temporary connector — its own connection pool, its own driver classloader
 * (shared per driver url with every catalog) — opened for one piece of work and closed with it, exactly
 * as the framework used its own JDBC client before. Metadata is acquired through the engine's
 * {@link PluginDrivenMetadata} funnel like every other caller; nothing here knows what a JDBC driver is.
 * The source properties are the job's ({@code jdbc_url}, {@code user}, {@code password},
 * {@code driver_url}, {@code driver_class}, ...), which are exactly the property names a JDBC catalog
 * takes; the connector ignores the streaming-only keys.</p>
 */
public class StreamingSourceClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(StreamingSourceClient.class);

    private final DataSourceType sourceType;
    private final Connector connector;
    private final ConnectorSession session;
    private final Map<String, ConnectorTableHandle> handles = new HashMap<>();

    StreamingSourceClient(DataSourceType sourceType, Connector connector, ConnectorSession session) {
        this.sourceType = sourceType;
        this.connector = connector;
        this.session = session;
    }

    /**
     * Opens a client over {@code sourceProps} through the connector plugin that serves {@code sourceType}.
     *
     * @throws JobException when that plugin is not installed, or the connector rejects the properties
     */
    public static StreamingSourceClient open(DataSourceType sourceType, Map<String, String> sourceProps)
            throws JobException {
        String connectorType = sourceType.connectorType();
        Connector connector;
        try {
            connector = ConnectorFactory.createConnector(connectorType, sourceProps,
                    new DefaultConnectorContext(sourceType.name(), -1L));
        } catch (RuntimeException e) {
            throw new JobException("Failed to open streaming source " + sourceType + ": " + e.getMessage(), e);
        }
        if (connector == null) {
            throw new JobException("Streaming source " + sourceType + " requires the '" + connectorType
                    + "' connector plugin, which is not installed");
        }
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogId(-1L)
                .withCatalogName(sourceType.name())
                .withCatalogProperties(sourceProps)
                .withStatementScope(ConnectorStatementScope.NONE)
                .build();
        return new StreamingSourceClient(sourceType, connector, session);
    }

    /** The tables of a remote database (a MySQL database, a PostgreSQL schema). */
    public List<String> listTables(String remoteDb) {
        return onPluginClassLoader(() -> metadata().listTableNames(session, remoteDb));
    }

    public boolean tableExists(String remoteDb, String table) {
        return onPluginClassLoader(() -> handle(remoteDb, table).isPresent());
    }

    /**
     * The table's columns as Doris columns, converted from the connector's schema exactly as a catalog
     * table's are.
     *
     * @throws JobException when the table does not exist
     */
    public List<Column> getColumns(String remoteDb, String table) throws JobException {
        ConnectorTableHandle handle = requireHandle(remoteDb, table);
        return onPluginClassLoader(() -> ConnectorColumnConverter.convertColumns(
                metadata().getTableSchema(session, handle).getColumns()));
    }

    /**
     * The table's primary-key column names in key order; empty when it has none.
     *
     * @throws JobException when the table does not exist
     */
    public List<String> getPrimaryKeys(String remoteDb, String table) throws JobException {
        ConnectorTableHandle handle = requireHandle(remoteDb, table);
        return onPluginClassLoader(() -> metadata().getPrimaryKeys(session, handle));
    }

    /**
     * Runs a read-only probe query on the source with positional parameters and returns its rows.
     *
     * @throws JobException when the source's connector cannot run queries
     */
    public ConnectorQueryResult executeQuery(String sql, List<Object> params) throws JobException {
        ConnectorMetadata metadata = onPluginClassLoader(this::metadata);
        if (!(metadata instanceof ConnectorPassthroughSqlOps)) {
            throw new JobException("Streaming source " + sourceType + " does not support probe queries");
        }
        return onPluginClassLoader(() -> ((ConnectorPassthroughSqlOps) metadata).executeQuery(session, sql, params));
    }

    @Override
    public void close() {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(connector.getClass().getClassLoader());
        try {
            connector.close();
        } catch (IOException e) {
            LOG.warn("Failed to close streaming source {}", sourceType, e);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private ConnectorMetadata metadata() {
        return PluginDrivenMetadata.get(session, connector);
    }

    private ConnectorTableHandle requireHandle(String remoteDb, String table) throws JobException {
        Optional<ConnectorTableHandle> handle = onPluginClassLoader(() -> handle(remoteDb, table));
        if (!handle.isPresent()) {
            throw new JobException("Table " + remoteDb + "." + table + " does not exist in streaming source "
                    + sourceType);
        }
        return handle.get();
    }

    private Optional<ConnectorTableHandle> handle(String remoteDb, String table) {
        String key = remoteDb + "." + table;
        ConnectorTableHandle cached = handles.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        Optional<ConnectorTableHandle> handle = metadata().getTableHandle(session, remoteDb, table);
        handle.ifPresent(h -> handles.put(key, h));
        return handle;
    }

    /**
     * Runs {@code body} with the thread-context classloader pinned to the connector's plugin loader, the
     * engine-side convention at every plugin boundary (a plugin's by-name reflection resolves against the
     * context loader; unpinned it would find fe-core's copies of shared classes).
     */
    private <T> T onPluginClassLoader(Supplier<T> body) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(connector.getClass().getClassLoader());
        try {
            return body.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

}
