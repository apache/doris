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

package org.apache.doris.datasource.fluss;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.transaction.TransactionManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlussExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(FlussExternalCatalog.class);

    public static final String FLUSS_COORDINATOR_URI = "fluss.coordinator.uri";
    public static final String FLUSS_BOOTSTRAP_SERVERS = "bootstrap.servers";

    protected Connection flussConnection;
    protected Admin flussAdmin;

    public FlussExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.FLUSS, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        String coordinatorUri = catalogProperty.getOrDefault(FLUSS_COORDINATOR_URI, null);
        String bootstrapServers = catalogProperty.getOrDefault(FLUSS_BOOTSTRAP_SERVERS, null);
        if (StringUtils.isEmpty(coordinatorUri) && StringUtils.isEmpty(bootstrapServers)) {
            throw new DdlException("Missing required property: " + FLUSS_COORDINATOR_URI
                    + " or " + FLUSS_BOOTSTRAP_SERVERS);
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        Configuration conf = createFlussConfiguration();
        flussConnection = ConnectionFactory.createConnection(conf);
        flussAdmin = flussConnection.getAdmin();
        initPreExecutionAuthenticator();
        FlussMetadataOps ops = ExternalMetadataOperations.newFlussMetadataOps(this, flussConnection);
        threadPoolWithPreAuth = ThreadPoolManager.newDaemonFixedThreadPoolWithPreAuth(
                ICEBERG_CATALOG_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE,
                String.format("fluss_catalog_%s_executor_pool", name),
                true,
                executionAuthenticator);
        metadataOps = ops;
    }

    private Configuration createFlussConfiguration() {
        Configuration conf = new Configuration();
        Map<String, String> props = catalogProperty.getProperties();
        
        // Set bootstrap.servers or coordinator URI
        String coordinatorUri = props.get(FLUSS_COORDINATOR_URI);
        String bootstrapServers = props.get(FLUSS_BOOTSTRAP_SERVERS);
        if (StringUtils.isNotEmpty(bootstrapServers)) {
            conf.setString(FLUSS_BOOTSTRAP_SERVERS, bootstrapServers);
        } else if (StringUtils.isNotEmpty(coordinatorUri)) {
            // If coordinator URI is provided, use it as bootstrap servers
            conf.setString(FLUSS_BOOTSTRAP_SERVERS, coordinatorUri);
        }
        
        // Copy other Fluss client properties (with fluss. prefix removed)
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("fluss.") && !key.equals(FLUSS_COORDINATOR_URI)) {
                String flussKey = key.substring("fluss.".length());
                conf.setString(flussKey, entry.getValue());
            }
        }
        
        return conf;
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = new org.apache.doris.common.security.authentication.ExecutionAuthenticator() {};
        }
    }

    public Connection getFlussConnection() {
        makeSureInitialized();
        return flussConnection;
    }

    public Admin getFlussAdmin() {
        makeSureInitialized();
        return flussAdmin;
    }

    @Override
    protected List<String> listDatabaseNames() {
        makeSureInitialized();
        try {
            CompletableFuture<List<String>> future = flussAdmin.listDatabases();
            List<String> databases = future.get();
            return databases != null ? databases : new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list databases, catalog name: " + getName(), e);
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> {
                try {
                    org.apache.fluss.metadata.TablePath tablePath = 
                            org.apache.fluss.metadata.TablePath.of(dbName, tblName);
                    CompletableFuture<org.apache.fluss.metadata.TableInfo> future = 
                            flussAdmin.getTableInfo(tablePath);
                    future.get(); // Will throw exception if table doesn't exist
                    return true;
                } catch (Exception e) {
                    if (ExceptionUtils.getRootCause(e) instanceof TableNotExistException) {
                        return false;
                    }
                    throw new RuntimeException("Failed to check table existence", e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to check table existence, catalog name: " + getName()
                    + ", error message: " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> {
                try {
                    CompletableFuture<List<String>> future = flussAdmin.listTables(dbName);
                    List<String> tables = future.get();
                    return tables != null ? tables : new ArrayList<>();
                } catch (Exception e) {
                    LOG.warn("Failed to list tables for database: " + dbName, e);
                    return new ArrayList<>();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list table names, catalog name: " + getName(), e);
        }
    }

    @Override
    public void close() {
        if (flussConnection != null) {
            try {
                flussConnection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Fluss connection", e);
            }
            flussConnection = null;
            flussAdmin = null;
        }
        super.close();
    }
}

