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

package org.apache.doris.datasource.iceberg.dlf.client;

import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.CreateDatabaseRequest;
import com.aliyun.datalake20200710.models.CreateDatabaseResponse;
import com.aliyun.datalake20200710.models.Database;
import com.aliyun.datalake20200710.models.DatabaseInput;
import com.aliyun.datalake20200710.models.DeleteDatabaseRequest;
import com.aliyun.datalake20200710.models.DeleteDatabaseResponse;
import com.aliyun.datalake20200710.models.DeleteTableRequest;
import com.aliyun.datalake20200710.models.DeleteTableResponse;
import com.aliyun.datalake20200710.models.GetDatabaseRequest;
import com.aliyun.datalake20200710.models.GetDatabaseResponse;
import com.aliyun.datalake20200710.models.GetTableRequest;
import com.aliyun.datalake20200710.models.GetTableResponse;
import com.aliyun.datalake20200710.models.ListDatabasesRequest;
import com.aliyun.datalake20200710.models.ListDatabasesResponse;
import com.aliyun.datalake20200710.models.ListTablesRequest;
import com.aliyun.datalake20200710.models.ListTablesResponse;
import com.aliyun.datalake20200710.models.RenameTableRequest;
import com.aliyun.datalake20200710.models.RenameTableResponse;
import com.aliyun.datalake20200710.models.Table;
import com.aliyun.datalake20200710.models.TableInput;
import com.aliyun.datalake20200710.models.UpdateDatabaseRequest;
import com.aliyun.datalake20200710.models.UpdateDatabaseResponse;
import com.aliyun.teaopenapi.models.Config;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DLFHttpProxy {

    private final Client client;

    public DLFHttpProxy(String accessId, String secretId, String endpoint, String region) {
        try {
            Config config = new Config();
            config.setType("access_key");
            config.setAccessKeyId(accessId);
            config.setAccessKeySecret(secretId);
            if (endpoint.startsWith("http")) {
                endpoint = endpoint.split("://")[1];
            }
            config.setEndpoint(endpoint);
            config.setRegionId(region);
            this.client = new Client(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Table> listTables(String catalogId, String databaseName) {
        try {
            ListTablesRequest request = new ListTablesRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(databaseName);
            ListTablesResponse response = client.listTables(request);
            return response.getBody().getTables();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean dropTable(String catalogId, String dbName, String tableName) {
        try {
            DeleteTableRequest request = new DeleteTableRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(dbName);
            request.setTableName(tableName);
            DeleteTableResponse response = client.deleteTable(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean renameTable(String catalogId, String dbName, String sourceName, String targetName, boolean isAsync) {
        try {
            RenameTableRequest request = new RenameTableRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(dbName);
            TableInput tableInput = new TableInput();
            tableInput.setTableName(sourceName);
            request.setTableInput(tableInput);
            request.setTableName(targetName);
            request.setIsAsync(isAsync);
            RenameTableResponse response = client.renameTable(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Table getTable(String catalogId, String databaseName, String tableName) {
        try {
            GetTableRequest request = new GetTableRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(databaseName);
            request.setTableName(tableName);
            GetTableResponse response = client.getTable(request);
            return response.getBody().getTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean createDatabase(String catalogId, String dbName, String locationUri, Map<String, String> props) {
        try {
            CreateDatabaseRequest request = new CreateDatabaseRequest();
            request.setCatalogId(catalogId);
            DatabaseInput dbInput = new DatabaseInput();
            dbInput.setName(dbName);
            dbInput.setLocationUri(locationUri);
            dbInput.setDescription(props.getOrDefault("description", ""));
            dbInput.setParameters(props);
            request.setDatabaseInput(dbInput);
            CreateDatabaseResponse response = client.createDatabase(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Database> listDatabases(String catalogId) throws NoSuchNamespaceException {
        try {
            ListDatabasesRequest request = new ListDatabasesRequest();
            request.setCatalogId(catalogId);
            ListDatabasesResponse response = client.listDatabases(request);
            return response.getBody().getDatabases();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Database getDatabase(String catalogId, String dbName) throws NoSuchNamespaceException {
        try {
            GetDatabaseRequest request = new GetDatabaseRequest();
            request.setCatalogId(catalogId);
            request.setName(dbName);
            GetDatabaseResponse response = client.getDatabase(new GetDatabaseRequest());
            return response.getBody().getDatabase();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean dropDatabase(String catalogId, String dbName, boolean cascade) throws NamespaceNotEmptyException {
        try {
            DeleteDatabaseRequest request = new DeleteDatabaseRequest();
            request.setCatalogId(catalogId);
            request.setName(dbName);
            request.setCascade(cascade);
            DeleteDatabaseResponse response = client.deleteDatabase(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean addDbProperties(String catalogId, String dbName, Map<String, String> map)
            throws NoSuchNamespaceException {
        try {
            UpdateDatabaseRequest request = new UpdateDatabaseRequest();
            request.setCatalogId(catalogId);
            request.setName(dbName);
            DatabaseInput dbInput = new DatabaseInput();
            dbInput.setParameters(map);
            request.setDatabaseInput(dbInput);
            UpdateDatabaseResponse response = client.updateDatabase(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean removeDbProperties(String catalogId, String dbName, Set<String> pNames)
            throws NoSuchNamespaceException {
        try {
            UpdateDatabaseRequest request = new UpdateDatabaseRequest();
            request.setCatalogId(catalogId);
            request.setName(dbName);
            DatabaseInput dbInput = new DatabaseInput();
            Map<String, String> props = new HashMap<>();
            for (String deleteProps : pNames) {
                props.put(deleteProps, null);
            }
            dbInput.setParameters(props);
            request.setDatabaseInput(dbInput);
            UpdateDatabaseResponse response = client.updateDatabase(request);
            return response.getBody().getSuccess();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
