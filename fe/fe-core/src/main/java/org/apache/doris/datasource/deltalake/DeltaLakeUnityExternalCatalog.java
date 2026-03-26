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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.deltalake.unity.UnityCatalogRestClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * External catalog for Delta Lake data sources backed by Databricks Unity Catalog.
 *
 * <p>Uses Unity Catalog REST API for metadata discovery (schemas, tables)
 * and optionally uses Unity Catalog's Credential Vending API for
 * temporaryservice credentials to access cloud storage.
 *
 * <p>Example usage:
 * <pre>{@code
 * CREATE CATALOG my_databricks PROPERTIES (
 *     'type' = 'deltalake',
 *     'deltalake.catalog.type' = 'unity',
 *     'databricks.host' = 'https://dbc-xxx.cloud.databricks.com',
 *     'databricks.token' = 'dapi_xxxxxxxxxxxx',
 *     'databricks.unity.catalog.name' = 'main',
 *     'databricks.credential.vending.enabled' = 'true'
 * );
 * }</pre>
 */
public class DeltaLakeUnityExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeUnityExternalCatalog.class);

    public static final String DATABRICKS_HOST = "databricks.host";
    public static final String DATABRICKS_TOKEN = "databricks.token";
    public static final String UNITY_CATALOG_NAME = "databricks.unity.catalog.name";
    public static final String CREDENTIAL_VENDING_ENABLED = "databricks.credential.vending.enabled";
    public static final String HTTP_CONNECT_TIMEOUT_MS = "databricks.http.timeout.connect.ms";
    public static final String HTTP_READ_TIMEOUT_MS = "databricks.http.timeout.read.ms";

    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10000;
    private static final long DEFAULT_READ_TIMEOUT_MS = 30000;

    private volatile UnityCatalogRestClient restClient;
    private volatile boolean credentialVendingEnabled;
    private volatile String unityCatalogName;

    public DeltaLakeUnityExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.DELTALAKE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        String host = catalogProperty.getOrDefault(DATABRICKS_HOST, null);
        String token = catalogProperty.getOrDefault(DATABRICKS_TOKEN, null);
        String catalogName = catalogProperty.getOrDefault(UNITY_CATALOG_NAME, null);

        if (host == null || host.isEmpty()) {
            throw new DdlException("Missing required property '" + DATABRICKS_HOST
                    + "' for Unity Catalog Delta Lake catalog.");
        }
        if (token == null || token.isEmpty()) {
            throw new DdlException("Missing required property '" + DATABRICKS_TOKEN
                    + "' for Unity Catalog Delta Lake catalog.");
        }
        if (catalogName == null || catalogName.isEmpty()) {
            throw new DdlException("Missing required property '" + UNITY_CATALOG_NAME
                    + "' for Unity Catalog Delta Lake catalog.");
        }
    }

    // Unity Catalog uses PAT token authentication via HTTP, not Kerberos.
    // The default no-op ExecutionAuthenticator from ExternalCatalog base class is sufficient.

    @Override
    protected void initLocalObjectsImpl() {
        String host = catalogProperty.getOrDefault(DATABRICKS_HOST, "");
        String token = catalogProperty.getOrDefault(DATABRICKS_TOKEN, "");
        this.unityCatalogName = catalogProperty.getOrDefault(UNITY_CATALOG_NAME, "");
        this.credentialVendingEnabled = Boolean.parseBoolean(
                catalogProperty.getOrDefault(CREDENTIAL_VENDING_ENABLED, "false"));

        long connectTimeout = Long.parseLong(
                catalogProperty.getOrDefault(HTTP_CONNECT_TIMEOUT_MS,
                        String.valueOf(DEFAULT_CONNECT_TIMEOUT_MS)));
        long readTimeout = Long.parseLong(
                catalogProperty.getOrDefault(HTTP_READ_TIMEOUT_MS,
                        String.valueOf(DEFAULT_READ_TIMEOUT_MS)));

        this.restClient = new UnityCatalogRestClient(host, token, connectTimeout, readTimeout);
        initPreExecutionAuthenticator();
        metadataOps = new DeltaLakeUnityMetadataOps(this, restClient, unityCatalogName);
    }

    @Override
    public void onClose() {
        super.onClose();
        if (metadataOps != null) {
            metadataOps.close();
            metadataOps = null;
        }
        // restClient is closed via metadataOps.close()
        restClient = null;
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        return metadataOps.listTableNames(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return metadataOps.tableExist(dbName, tblName);
    }

    /**
     * Returns whether Credential Vending is enabled for this catalog.
     */
    public boolean isCredentialVendingEnabled() {
        makeSureInitialized();
        return credentialVendingEnabled;
    }

    /**
     * Returns the Unity Catalog REST client.
     */
    public UnityCatalogRestClient getRestClient() {
        makeSureInitialized();
        return restClient;
    }

    /**
     * Returns the Unity Catalog name this Doris catalog is mapped to.
     */
    public String getUnityCatalogName() {
        makeSureInitialized();
        return unityCatalogName;
    }
}
