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

package org.apache.doris.datasource;

import org.apache.doris.datasource.property.constants.BigQueryProperties;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BigQueryExternalCatalog extends ExternalCatalog {
    private BigQuery bigQuery;
    @SerializedName(value = "projectId")
    private String projectId;
    @SerializedName(value = "credentials_file")
    private String credentialsFile;
    @SerializedName(value = "credentials_key")
    private String credentialsKey;

    public BigQueryExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.BIGQUERY, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();
        String projectId = props.get(BigQueryProperties.PROJECT);
        String credentialsFile = props.get(BigQueryProperties.CREDENTIALS_FILE);
        String credentialsKey = props.get(BigQueryProperties.CREDENTIALS_KEY);
        if (Strings.isNullOrEmpty(projectId)) {
            throw new IllegalArgumentException("Missing required property: " + BigQueryProperties.PROJECT);
        }
        GoogleCredentials credentials = null;
        if (!Strings.isNullOrEmpty(credentialsFile)) {
            // If credentialsFile is provided, use it to create GoogleCredentials
            try (FileInputStream serviceAccountStream = new FileInputStream(credentialsFile)) {
                credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read credentials file: " + credentialsFile, e);
            }
        } else if (!Strings.isNullOrEmpty(credentialsKey)) {
            // Handle credentialsKey here
            // ...
        } else {
            throw new IllegalArgumentException("Missing required property: " + BigQueryProperties.CREDENTIALS_FILE
                    + " or " + BigQueryProperties.CREDENTIALS_KEY);
        }

        if (credentials == null) {
            throw new RuntimeException("Unsupported authentication method");
        }

        this.projectId = projectId;
        this.bigQuery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
    }

    public DatasetId getDatasetId(String project, String dataset) {
        return DatasetId.of(project, dataset);
    }

    public TableId getTableId(String project, String dataset, String table) {
        return TableId.of(project, dataset, table);
    }

    public BigInteger getTotalRows(String projectId, String datasetName, String tableName) {
        TableId tableId = getTableId(projectId, datasetName, tableName);
        Table table = bigQuery.getTable(tableId);
        return table.getNumRows();
    }

    public BigQuery getClient() {
        makeSureInitialized();
        return bigQuery;
    }

    public List<String> listDatabaseNames() {
        List<String> result = new ArrayList<>();
        Page<Dataset> datasets = bigQuery.listDatasets(projectId);

        if (datasets == null) {
            System.out.println("Project does not contain any datasets");
            return result;
        }

        datasets.iterateAll().forEach(
                dataset -> result.add(dataset.getDatasetId().getDataset())
        );
        return result;
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<String> result = new ArrayList<>();
        Page<Table> tables = bigQuery.listTables(getDatasetId(projectId, dbName));

        if (tables == null) {
            System.out.println("Dataset does not contain any tables");
            return result;
        }

        tables.iterateAll().forEach(
                table -> result.add(table.getTableId().getTable())
        );

        return result;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        TableId tableId = getTableId(projectId, dbName, tblName);
        return bigQuery.getTable(tableId).exists();
    }

}
