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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan plan provider for JDBC connectors.
 *
 * <p>JDBC scans always produce exactly one scan range. The scan range carries
 * the SQL query and JDBC connection parameters needed by BE's JdbcJniReader.
 * Filter pushdown converts {@link ConnectorExpression} trees into SQL WHERE
 * clauses with database-specific formatting.</p>
 */
public class JdbcScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(JdbcScanPlanProvider.class);

    private final JdbcDbType dbType;
    private final JdbcCatalogProperties props;
    private final long catalogId;

    public JdbcScanPlanProvider(JdbcDbType dbType, JdbcCatalogProperties props, long catalogId) {
        this.dbType = dbType;
        this.props = props;
        this.catalogId = catalogId;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        ConnectorTableHandle handle = request.getTableHandle();
        List<ConnectorColumnHandle> columns = request.getColumns();
        Optional<ConnectorExpression> filter = request.getFilter();
        long limit = request.getLimit();
        String querySql;
        if (handle instanceof PassthroughQueryTableHandle) {
            // Query passthrough from TVF — use the raw SQL directly
            querySql = ((PassthroughQueryTableHandle) handle).getQuery();
        } else {
            JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
            String remoteDbName = jdbcHandle.getRemoteDbName();
            String remoteTableName = jdbcHandle.getRemoteTableName();

            // Build function pushdown config from catalog properties + session
            Map<String, String> sessionProps = session.getSessionProperties();
            String functionRulesJson = props.getFunctionRules();
            boolean extFuncPushdown = Boolean.parseBoolean(
                    sessionProps.getOrDefault("enable_ext_func_pred_pushdown", "true"));
            JdbcFunctionPushdownConfig functionConfig = JdbcFunctionPushdownConfig.create(
                    dbType, functionRulesJson, extFuncPushdown);

            // Build the SQL query with database-specific formatting
            JdbcQueryBuilder queryBuilder = new JdbcQueryBuilder(dbType, functionConfig, sessionProps);
            querySql = queryBuilder.buildQuery(
                    remoteDbName, remoteTableName, columns, filter, limit);

            LOG.debug("JDBC scan query for {}.{}: {}", remoteDbName, remoteTableName, querySql);
        }

        // Build the scan range with all JDBC connection parameters
        String jdbcUrl = props.getJdbcUrl();
        String user = props.getUser();
        String password = props.getPassword();
        String driverClass = props.getDriverClass();
        String driverUrl = props.getDriverUrl();
        String checksum = props.getDriverChecksum();

        int poolMinSize = props.getConnectionPoolMinSize();
        int poolMaxSize = props.getConnectionPoolMaxSize();
        int poolMaxWaitTime = props.getConnectionPoolMaxWaitTime();
        int poolMaxLifeTime = props.getConnectionPoolMaxLifeTime();
        boolean poolKeepAlive = props.isConnectionPoolKeepAlive();

        JdbcScanRange scanRange = new JdbcScanRange.Builder()
                .querySql(querySql)
                .jdbcUrl(jdbcUrl)
                .jdbcUser(user)
                .jdbcPassword(password)
                .driverClass(driverClass)
                .driverUrl(driverUrl)
                .driverChecksum(checksum)
                .catalogId(catalogId)
                .tableType(dbType)
                .connectionPoolMinSize(poolMinSize)
                .connectionPoolMaxSize(poolMaxSize)
                .connectionPoolMaxWaitTime(poolMaxWaitTime)
                .connectionPoolMaxLifeTime(poolMaxLifeTime)
                .connectionPoolKeepAlive(poolKeepAlive)
                .build();

        return Collections.singletonList(scanRange);
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        // Build the same query SQL that planScan() would produce, for EXPLAIN output
        String querySql;
        if (handle instanceof PassthroughQueryTableHandle) {
            querySql = ((PassthroughQueryTableHandle) handle).getQuery();
        } else {
            JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
            Map<String, String> sessionProps = session.getSessionProperties();
            String functionRulesJson = props.getFunctionRules();
            boolean extFuncPushdown = Boolean.parseBoolean(
                    sessionProps.getOrDefault("enable_ext_func_pred_pushdown", "true"));
            JdbcFunctionPushdownConfig functionConfig = JdbcFunctionPushdownConfig.create(
                    dbType, functionRulesJson, extFuncPushdown);
            JdbcQueryBuilder queryBuilder = new JdbcQueryBuilder(dbType, functionConfig, sessionProps);
            querySql = queryBuilder.buildQuery(
                    jdbcHandle.getRemoteDbName(), jdbcHandle.getRemoteTableName(),
                    columns, filter, -1);
        }
        Map<String, String> props = new HashMap<>();
        props.put(ScanNodePropertyKeys.REMOTE_QUERY, querySql);
        return props;
    }
}
