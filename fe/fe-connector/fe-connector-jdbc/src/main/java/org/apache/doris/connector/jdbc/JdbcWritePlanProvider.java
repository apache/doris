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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TJdbcTableSink;
import org.apache.doris.thrift.TOdbcTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Write plan provider for JDBC sources.
 *
 * <p>Builds the opaque {@link TJdbcTableSink} for a bound INSERT: it resolves the
 * remote table / column names, generates the parameterized INSERT SQL, and stamps
 * the JDBC connection configuration. JDBC writes are auto-committed by BE per row,
 * so there is no FE-side transaction work here (see
 * {@link JdbcConnectorMetadata#beginTransaction}, which returns a degenerate no-op
 * transaction).</p>
 *
 * <p>Ported byte-for-byte from the legacy config-bag write path — the deleted
 * {@code JdbcConnectorMetadata.getWriteConfig} (property bag) plus
 * {@code PluginDrivenTableSink.bindJdbcWriteSink} (Thrift assembly) — fused into
 * this single {@code planWrite} call (P6.3-T02 / RFC OQ-1). The connection-pool
 * values come off {@link JdbcCatalogProperties}, which holds the same defaults
 * {@code getWriteConfig} used to compute them with.</p>
 */
public class JdbcWritePlanProvider implements ConnectorWritePlanProvider {

    private final JdbcConnectorClient client;
    private final JdbcCatalogProperties props;

    public JdbcWritePlanProvider(JdbcConnectorClient client, JdbcCatalogProperties props) {
        this.client = client;
        this.props = props;
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle.getTableHandle();
        String insertSql = buildInsertSql(session, jdbcHandle, handle.getColumns());

        TJdbcTable tJdbcTable = new TJdbcTable();
        tJdbcTable.setJdbcUrl(props.getJdbcUrl());
        tJdbcTable.setJdbcUser(props.getUser());
        tJdbcTable.setJdbcPassword(props.getPassword());
        tJdbcTable.setJdbcDriverUrl(props.getDriverUrl());
        tJdbcTable.setJdbcDriverClass(props.getDriverClass());
        tJdbcTable.setJdbcDriverChecksum(
                props.getDriverChecksum());
        tJdbcTable.setJdbcTableName(jdbcHandle.getRemoteTableName());
        tJdbcTable.setJdbcResourceName("");
        tJdbcTable.setCatalogId(session.getCatalogId());
        tJdbcTable.setConnectionPoolMinSize(props.getConnectionPoolMinSize());
        tJdbcTable.setConnectionPoolMaxSize(props.getConnectionPoolMaxSize());
        tJdbcTable.setConnectionPoolMaxWaitTime(props.getConnectionPoolMaxWaitTime());
        tJdbcTable.setConnectionPoolMaxLifeTime(props.getConnectionPoolMaxLifeTime());
        tJdbcTable.setConnectionPoolKeepAlive(props.isConnectionPoolKeepAlive());

        TJdbcTableSink jdbcSink = new TJdbcTableSink();
        jdbcSink.setJdbcTable(tJdbcTable);
        jdbcSink.setInsertSql(insertSql);
        jdbcSink.setUseTransaction(useTransaction(session));
        // dbType.name() is never empty and maps onto TOdbcTableType (legacy parity: a name with
        // no matching enum throws here, exactly as the legacy bindJdbcWriteSink did).
        jdbcSink.setTableType(TOdbcTableType.valueOf(client.getDbType().name()));

        TDataSink dataSink = new TDataSink(TDataSinkType.JDBC_TABLE_SINK);
        dataSink.setJdbcTableSink(jdbcSink);
        return new ConnectorSinkPlan(dataSink);
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Surface the connector-specific write detail the unified plugin-driven sink line cannot
        // (the sink is source-agnostic). Mirrors the legacy jdbc EXPLAIN block (table type /
        // generated INSERT SQL / use-transaction).
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle.getTableHandle();
        output.append(prefix).append("  TABLE TYPE: ")
                .append(client.getDbType().name()).append("\n");
        output.append(prefix).append("  INSERT SQL: ")
                .append(buildInsertSql(session, jdbcHandle, handle.getColumns())).append("\n");
        output.append(prefix).append("  USE TRANSACTION: ")
                .append(useTransaction(session)).append("\n");
    }

    /**
     * Builds the parameterized INSERT SQL for the bound write. Shared by {@link #planWrite} and
     * {@link #appendExplainInfo} so the EXPLAIN SQL is identical to the one sent to BE. Resolves
     * the local -> remote column name mapping via the metadata column handles (same client +
     * properties as the legacy {@code getWriteConfig}, which called its own getColumnHandles).
     *
     * <p>The {@code new JdbcConnectorMetadata} is a cheap stateless wrapper; its
     * {@link JdbcConnectorMetadata#getColumnHandles} resolves the raw remote-column fetch through the
     * per-statement scope memo ({@code JdbcConnectorMetadata.COLUMNS_NAMESPACE}), so it shares the single
     * {@code getJdbcColumnsInfo} round-trip with the read path and with the sibling
     * planWrite/appendExplainInfo call of the same statement — EXPLAIN INSERT no longer double-fetches.
     */
    private String buildInsertSql(ConnectorSession session, JdbcTableHandle jdbcHandle,
            List<ConnectorColumn> columns) {
        List<String> columnNames = columns.stream()
                .map(ConnectorColumn::getName)
                .collect(Collectors.toList());
        Map<String, ConnectorColumnHandle> colHandles =
                new JdbcConnectorMetadata(client, props).getColumnHandles(session, jdbcHandle);
        Map<String, String> remoteColumnNames = new HashMap<>();
        for (Map.Entry<String, ConnectorColumnHandle> entry : colHandles.entrySet()) {
            JdbcColumnHandle ch = (JdbcColumnHandle) entry.getValue();
            remoteColumnNames.put(ch.getLocalName(), ch.getRemoteName());
        }
        return JdbcIdentifierQuoter.buildInsertSql(client.getDbType(),
                jdbcHandle.getRemoteDbName(), jdbcHandle.getRemoteTableName(),
                remoteColumnNames, columnNames);
    }

    private boolean useTransaction(ConnectorSession session) {
        return Boolean.parseBoolean(session.getSessionProperties()
                .getOrDefault("enable_odbc_transcation", "false"));
    }
}
