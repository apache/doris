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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionRequest;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Getter
public class JdbcExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalCatalog.class);

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            JdbcResource.JDBC_URL,
            JdbcResource.DRIVER_URL,
            JdbcResource.DRIVER_CLASS
    );

    // Must add "transient" for Gson to ignore this field,
    // or Gson will throw exception with HikariCP
    private transient JdbcClient jdbcClient;

    public JdbcExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment)
            throws DdlException {
        super(catalogId, name, InitCatalogLog.Type.JDBC, comment);
        this.catalogProperty = new CatalogProperty(resource, processCompatibleProperties(props));
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }

        JdbcResource.checkBooleanProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, getOnlySpecifiedDatabase());
        JdbcResource.checkBooleanProperty(JdbcResource.LOWER_CASE_META_NAMES, getLowerCaseMetaNames());
        JdbcResource.checkBooleanProperty(JdbcResource.CONNECTION_POOL_KEEP_ALIVE,
                String.valueOf(isConnectionPoolKeepAlive()));
        JdbcResource.checkBooleanProperty(JdbcResource.TEST_CONNECTION, String.valueOf(isTestConnection()));
        JdbcResource.checkDatabaseListProperties(getOnlySpecifiedDatabase(), getIncludeDatabaseMap(),
                getExcludeDatabaseMap());
        JdbcResource.checkConnectionPoolProperties(getConnectionPoolMinSize(), getConnectionPoolMaxSize(),
                getConnectionPoolMaxWaitTime(), getConnectionPoolMaxLifeTime());
    }

    @Override
    public void setDefaultPropsIfMissing(boolean isReplay) {
        super.setDefaultPropsIfMissing(isReplay);
        // Modify lower_case_table_names to lower_case_meta_names if it exists
        if (catalogProperty.getProperties().containsKey("lower_case_table_names") && isReplay) {
            String lowerCaseTableNamesValue = catalogProperty.getProperties().get("lower_case_table_names");
            catalogProperty.addProperty("lower_case_meta_names", lowerCaseTableNamesValue);
            catalogProperty.deleteProperty("lower_case_table_names");
            LOG.info("Modify lower_case_table_names to lower_case_meta_names, value: {}", lowerCaseTableNamesValue);
        } else if (catalogProperty.getProperties().containsKey("lower_case_table_names") && !isReplay) {
            throw new IllegalArgumentException("Jdbc catalog property lower_case_table_names is not supported,"
                    + " please use lower_case_meta_names instead.");
        }
    }

    @Override
    public void onRefresh(boolean invalidCache) {
        super.onRefresh(invalidCache);
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    @Override
    public void onRefreshCache(boolean invalidCache) {
        onRefresh(invalidCache);
    }

    @Override
    public void onClose() {
        super.onClose();
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    protected Map<String, String> processCompatibleProperties(Map<String, String> props)
            throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            properties.put(StringUtils.removeStart(kv.getKey(), JdbcResource.JDBC_PROPERTIES_PREFIX), kv.getValue());
        }
        String jdbcUrl = properties.getOrDefault(JdbcResource.JDBC_URL, "");
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            jdbcUrl = JdbcResource.handleJdbcUrl(jdbcUrl);
            properties.put(JdbcResource.JDBC_URL, jdbcUrl);
        }
        return properties;
    }

    public String getDatabaseTypeName() {
        return jdbcClient.getDbType();
    }

    public String getJdbcUser() {
        return catalogProperty.getOrDefault(JdbcResource.USER, "");
    }

    public String getJdbcPasswd() {
        return catalogProperty.getOrDefault(JdbcResource.PASSWORD, "");
    }

    public String getJdbcUrl() {
        return catalogProperty.getOrDefault(JdbcResource.JDBC_URL, "");
    }

    public String getDriverUrl() {
        return catalogProperty.getOrDefault(JdbcResource.DRIVER_URL, "");
    }

    public String getDriverClass() {
        return catalogProperty.getOrDefault(JdbcResource.DRIVER_CLASS, "");
    }

    public String getCheckSum() {
        return catalogProperty.getOrDefault(JdbcResource.CHECK_SUM, "");
    }

    public String getOnlySpecifiedDatabase() {
        return catalogProperty.getOrDefault(JdbcResource.ONLY_SPECIFIED_DATABASE, JdbcResource.getDefaultPropertyValue(
                JdbcResource.ONLY_SPECIFIED_DATABASE));
    }

    public String getLowerCaseMetaNames() {
        return catalogProperty.getOrDefault(JdbcResource.LOWER_CASE_META_NAMES, JdbcResource.getDefaultPropertyValue(
                JdbcResource.LOWER_CASE_META_NAMES));
    }

    public String getMetaNamesMapping() {
        return catalogProperty.getOrDefault(JdbcResource.META_NAMES_MAPPING, JdbcResource.getDefaultPropertyValue(
                JdbcResource.META_NAMES_MAPPING));
    }

    public int getConnectionPoolMinSize() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MIN_SIZE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MIN_SIZE)));
    }

    public int getConnectionPoolMaxSize() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_SIZE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_SIZE)));
    }

    public int getConnectionPoolMaxWaitTime() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_WAIT_TIME, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_WAIT_TIME)));
    }

    public int getConnectionPoolMaxLifeTime() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_LIFE_TIME, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_LIFE_TIME)));
    }

    public boolean isConnectionPoolKeepAlive() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_KEEP_ALIVE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_KEEP_ALIVE)));
    }

    public boolean isTestConnection() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(JdbcResource.TEST_CONNECTION, JdbcResource
                .getDefaultPropertyValue(JdbcResource.TEST_CONNECTION)));
    }

    @Override
    protected void initLocalObjectsImpl() {
        JdbcClientConfig jdbcClientConfig = new JdbcClientConfig()
                .setCatalog(this.name)
                .setUser(getJdbcUser())
                .setPassword(getJdbcPasswd())
                .setJdbcUrl(getJdbcUrl())
                .setDriverUrl(getDriverUrl())
                .setDriverClass(getDriverClass())
                .setOnlySpecifiedDatabase(getOnlySpecifiedDatabase())
                .setIsLowerCaseMetaNames(getLowerCaseMetaNames())
                .setMetaNamesMapping(getMetaNamesMapping())
                .setIncludeDatabaseMap(getIncludeDatabaseMap())
                .setExcludeDatabaseMap(getExcludeDatabaseMap())
                .setConnectionPoolMinSize(getConnectionPoolMinSize())
                .setConnectionPoolMaxSize(getConnectionPoolMaxSize())
                .setConnectionPoolMaxLifeTime(getConnectionPoolMaxLifeTime())
                .setConnectionPoolMaxWaitTime(getConnectionPoolMaxWaitTime())
                .setConnectionPoolKeepAlive(isConnectionPoolKeepAlive());

        jdbcClient = JdbcClient.createJdbcClient(jdbcClientConfig);
    }

    protected List<String> listDatabaseNames() {
        return jdbcClient.getDatabaseNameList();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return jdbcClient.getTablesNameList(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return jdbcClient.isTableExist(dbName, tblName);
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        super.checkWhenCreating();
        Map<String, String> properties = catalogProperty.getProperties();
        if (properties.containsKey(JdbcResource.DRIVER_URL)) {
            String computedChecksum = JdbcResource.computeObjectChecksum(properties.get(JdbcResource.DRIVER_URL));
            if (properties.containsKey(JdbcResource.CHECK_SUM)) {
                String providedChecksum = properties.get(JdbcResource.CHECK_SUM);
                if (!providedChecksum.equals(computedChecksum)) {
                    throw new DdlException(
                            "The provided checksum (" + providedChecksum
                                    + ") does not match the computed checksum (" + computedChecksum
                                    + ") for the driver_url."
                    );
                }
            } else {
                catalogProperty.addProperty(JdbcResource.CHECK_SUM, computedChecksum);
            }
        }
        testJdbcConnection();
    }

    /**
     * Execute stmt direct via jdbc
     *
     * @param stmt, the raw stmt string
     */
    public void executeStmt(String stmt) {
        makeSureInitialized();
        jdbcClient.executeStmt(stmt);
    }

    /**
     * Get columns from query
     *
     * @param query, the query string
     * @return the columns
     */
    public List<Column> getColumnsFromQuery(String query) {
        makeSureInitialized();
        return jdbcClient.getColumnsFromQuery(query);
    }

    public void configureJdbcTable(JdbcTable jdbcTable, String tableName) {
        jdbcTable.setCatalogId(this.getId());
        jdbcTable.setExternalTableName(tableName);
        jdbcTable.setJdbcTypeName(this.getDatabaseTypeName());
        jdbcTable.setJdbcUrl(this.getJdbcUrl());
        jdbcTable.setJdbcUser(this.getJdbcUser());
        jdbcTable.setJdbcPasswd(this.getJdbcPasswd());
        jdbcTable.setDriverClass(this.getDriverClass());
        jdbcTable.setDriverUrl(this.getDriverUrl());
        jdbcTable.setCheckSum(this.getCheckSum());
        jdbcTable.setResourceName(this.getResource());
        jdbcTable.setConnectionPoolMinSize(this.getConnectionPoolMinSize());
        jdbcTable.setConnectionPoolMaxSize(this.getConnectionPoolMaxSize());
        jdbcTable.setConnectionPoolMaxLifeTime(this.getConnectionPoolMaxLifeTime());
        jdbcTable.setConnectionPoolMaxWaitTime(this.getConnectionPoolMaxWaitTime());
        jdbcTable.setConnectionPoolKeepAlive(this.isConnectionPoolKeepAlive());
    }

    private void testJdbcConnection() throws DdlException {
        if (FeConstants.runningUnitTest) {
            // skip test connection in unit test
            return;
        }
        if (isTestConnection()) {
            try {
                initLocalObjectsImpl();
                testFeToJdbcConnection();
                testBeToJdbcConnection();
            } finally {
                if (jdbcClient != null) {
                    jdbcClient.closeClient();
                    jdbcClient = null;
                }
            }
        }
    }

    private void testFeToJdbcConnection() throws DdlException {
        try {
            jdbcClient.testConnection();
        } catch (JdbcClientException e) {
            String errorMessage = "Test FE Connection to JDBC Failed: " + e.getMessage();
            LOG.error(errorMessage, e);
            throw new DdlException(errorMessage, e);
        }
    }

    private void testBeToJdbcConnection() throws DdlException {
        Backend aliveBe = null;
        try {
            for (Backend be : Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values()) {
                if (be.isAlive()) {
                    aliveBe = be;
                }
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (aliveBe == null) {
            throw new DdlException("Test BE Connection to JDBC Failed: No Alive backends");
        }
        TNetworkAddress address = new TNetworkAddress(aliveBe.getHost(), aliveBe.getBrpcPort());
        try {
            JdbcTable jdbcTable = getTestConnectionJdbcTable();
            PJdbcTestConnectionRequest request = InternalService.PJdbcTestConnectionRequest.newBuilder()
                    .setJdbcTable(ByteString.copyFrom(new TSerializer().serialize(jdbcTable.toThrift())))
                    .setJdbcTableType(jdbcTable.getJdbcTableType().getValue())
                    .setQueryStr(jdbcClient.getTestQuery()).build();
            InternalService.PJdbcTestConnectionResult result = null;
            Future<PJdbcTestConnectionResult> future = BackendServiceProxy.getInstance()
                    .testJdbcConnection(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new DdlException("Test BE Connection to JDBC Failed: " + result.getStatus().getErrorMsgs(0));
            }
        } catch (TException | RpcException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private JdbcTable getTestConnectionJdbcTable() throws DdlException {
        JdbcTable jdbcTable = new JdbcTable(0, "test_jdbc_connection", Lists.newArrayList(),
                TableType.JDBC_EXTERNAL_TABLE);
        this.configureJdbcTable(jdbcTable, "test_jdbc_connection");

        // Special checksum computation
        jdbcTable.setCheckSum(JdbcResource.computeObjectChecksum(this.getDriverUrl()));

        return jdbcTable;
    }
}
