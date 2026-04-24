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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

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
    private final Map<String, String> catalogProperties;
    private final long catalogId;

    public JdbcScanPlanProvider(JdbcDbType dbType, Map<String, String> catalogProperties, long catalogId) {
        this.dbType = dbType;
        this.catalogProperties = catalogProperties;
        this.catalogId = catalogId;
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return planScan(session, handle, columns, filter, -1);
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit) {
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
            String functionRulesJson = catalogProperties.getOrDefault(
                    JdbcConnectorProperties.FUNCTION_RULES, "");
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
        String jdbcUrl = getProperty(JdbcConnectorProperties.JDBC_URL, "");
        String user = getProperty(JdbcConnectorProperties.USER, "");
        String password = getProperty(JdbcConnectorProperties.PASSWORD, "");
        String driverClass = getProperty(JdbcConnectorProperties.DRIVER_CLASS, "");
        String driverUrl = getProperty(JdbcConnectorProperties.DRIVER_URL, "");
        String checksum = getProperty("checksum", "");

        int poolMinSize = JdbcConnectorProperties.getInt(
                catalogProperties, JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE);
        int poolMaxSize = JdbcConnectorProperties.getInt(
                catalogProperties, JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE);
        int poolMaxWaitTime = JdbcConnectorProperties.getInt(
                catalogProperties, JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME);
        int poolMaxLifeTime = JdbcConnectorProperties.getInt(
                catalogProperties, JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME);
        boolean poolKeepAlive = Boolean.parseBoolean(
                getProperty(JdbcConnectorProperties.CONNECTION_POOL_KEEP_ALIVE,
                        String.valueOf(JdbcConnectorProperties.DEFAULT_POOL_KEEP_ALIVE)));

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
    public long estimateScanRangeCount(ConnectorSession session, ConnectorTableHandle handle) {
        return 1;
    }

    private String getProperty(String key, String defaultValue) {
        return catalogProperties.getOrDefault(key, defaultValue);
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
            String functionRulesJson = catalogProperties.getOrDefault(
                    JdbcConnectorProperties.FUNCTION_RULES, "");
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
        props.put("query", querySql);
        return props;
    }
}
