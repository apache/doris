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

import org.apache.doris.datasource.deltalake.unity.UnityCatalogRestClient;
import org.apache.doris.datasource.deltalake.unity.UnityCatalogRestClient.TableInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unity Catalog-based metadata operations for Delta Lake catalog.
 * Uses Databricks Unity Catalog REST API for namespace (schema/table) discovery.
 * Delta Kernel Engine management is inherited from {@link AbstractDeltaLakeMetadataOps}.
 *
 * <p>Unity Catalog namespace mapping:
 * <ul>
 *   <li>Unity Schema → Doris Database</li>
 *   <li>Unity Table → Doris Table</li>
 * </ul>
 *
 * <p>Only tables with {@code data_source_format = "DELTA"} are visible.
 */
public class DeltaLakeUnityMetadataOps extends AbstractDeltaLakeMetadataOps {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeUnityMetadataOps.class);

    private static final String DELTA_FORMAT = "DELTA";

    private final UnityCatalogRestClient restClient;
    private final String unityCatalogName;

    public DeltaLakeUnityMetadataOps(DeltaLakeUnityExternalCatalog catalog,
            UnityCatalogRestClient restClient, String unityCatalogName) {
        super(catalog);
        this.restClient = restClient;
        this.unityCatalogName = unityCatalogName;
    }

    @Override
    public List<String> listDatabaseNames() {
        // Unity Catalog schemas map to Doris databases
        return restClient.listSchemas(unityCatalogName);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        // Only return Delta format tables
        return restClient.listTableNames(unityCatalogName, dbName, DELTA_FORMAT);
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        String fullName = buildFullTableName(dbName, tblName);
        return restClient.tableExists(fullName);
    }

    @Override
    public boolean databaseExist(String dbName) {
        return restClient.schemaExists(unityCatalogName, dbName);
    }

    @Override
    public String getTableLocation(String dbName, String tblName) {
        String fullName = buildFullTableName(dbName, tblName);
        TableInfo tableInfo = restClient.getTable(fullName);
        return tableInfo.getStorageLocation();
    }

    /**
     * Get the table_id (UUID) from Unity Catalog, required for Credential Vending.
     *
     * @param dbName  the database (schema) name
     * @param tblName the table name
     * @return the table UUID
     */
    public String getTableId(String dbName, String tblName) {
        String fullName = buildFullTableName(dbName, tblName);
        TableInfo tableInfo = restClient.getTable(fullName);
        return tableInfo.getTableId();
    }

    /**
     * Get temporary storage credentials for accessing table data files.
     * Used in Credential Vending mode.
     *
     * @param dbName  the database (schema) name
     * @param tblName the table name
     * @return map of Hadoop configuration properties for accessing the storage
     */
    public Map<String, String> getStorageCredentials(String dbName, String tblName) {
        String tableId = getTableId(dbName, tblName);
        UnityCatalogRestClient.TemporaryCredentials creds =
                restClient.generateTempTableCredentials(tableId, "READ");

        Map<String, String> hadoopProps = new HashMap<>();

        if (creds.getAwsTempCredentials() != null) {
            UnityCatalogRestClient.AwsTempCredentials aws = creds.getAwsTempCredentials();
            hadoopProps.put("fs.s3a.access.key", aws.getAccessKeyId());
            hadoopProps.put("fs.s3a.secret.key", aws.getSecretAccessKey());
            hadoopProps.put("fs.s3a.session.token", aws.getSessionToken());
            hadoopProps.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        } else if (creds.getAzureUserDelegationSas() != null) {
            // Azure SAS token handling
            UnityCatalogRestClient.AzureUserDelegationSas azure = creds.getAzureUserDelegationSas();
            hadoopProps.put("fs.azure.sas.token", azure.getSasToken());
        } else if (creds.getGcpOAuthToken() != null) {
            // GCP OAuth token handling
            UnityCatalogRestClient.GcpOAuthToken gcp = creds.getGcpOAuthToken();
            hadoopProps.put("fs.gs.auth.access.token", gcp.getOauthToken());
        }

        return hadoopProps;
    }

    @Override
    public void close() {
        if (restClient != null) {
            restClient.close();
        }
    }

    private String buildFullTableName(String dbName, String tblName) {
        return unityCatalogName + "." + dbName + "." + tblName;
    }
}
