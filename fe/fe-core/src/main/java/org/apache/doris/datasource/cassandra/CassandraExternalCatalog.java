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

package org.apache.doris.datasource.cassandra;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.CassandraProperties;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CassandraExternalCatalog extends ExternalCatalog {

    @SerializedName(value = "contactPoints")
    private String contactPoints;

    @SerializedName(value = "userName")
    private String userName;

    @SerializedName(value = "password")
    private String password;

    @SerializedName(value = "datacenter")
    private String datacenter;

    public CassandraExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.CASSANDRA, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    protected List<String> listDatabaseNames() {
        List<String> databaseNames = new ArrayList<>();
        try (CqlSession session = CassandraClient.getCqlSessionBuilder(contactPoints, userName, password, datacenter)
                .build()) {
            session.getMetadata().getKeyspaces().forEach(
                    (cqlIdentifier, keyspaceMetadata) -> databaseNames.add(keyspaceMetadata.getName().asInternal()));
        }
        return databaseNames;
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String keyspaceName) {

        List<String> tableNames = new ArrayList<>();

        try (CqlSession session = CqlSession.builder().build()) {

            session.getMetadata().getKeyspace(keyspaceName).ifPresent(keyspaceMetadata -> keyspaceMetadata.getTables()
                    .forEach((cqlIdentifier, tableMetadata) -> tableNames.add(tableMetadata.getName().asInternal())));

            return tableNames;
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String keyspaceName, String tableName) {

        AtomicBoolean tableExists = new AtomicBoolean(false);
        try (CqlSession session = CqlSession.builder().build()) {
            session.getMetadata().getKeyspace(keyspaceName)
                    .flatMap(keyspaceMetadata -> keyspaceMetadata.getTable(tableName))
                    .ifPresent(tableMetadata -> tableExists.set(true));
            return tableExists.get();
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();
        contactPoints = props.get(CassandraProperties.contactPoints);
        userName = props.get(CassandraProperties.USERNAME);
        password = props.get(CassandraProperties.PASSWORD);
        datacenter = props.get(CassandraProperties.DATACENTER);
    }

    public TableMetadata getTable(String keyspaceName, String tableName) {
        try (CqlSession session = CqlSession.builder().build()) {
            return session.getMetadata().getKeyspace(keyspaceName)
                    .flatMap(keyspaceMetadata -> keyspaceMetadata.getTable(tableName))
                    .orElseThrow(() -> new RuntimeException("Table not found"));
        }
    }

    public String getContactPoints() {
        makeSureInitialized();
        return contactPoints;
    }

    public String getUserName() {
        makeSureInitialized();
        return userName;
    }

    public String getPassword() {
        makeSureInitialized();
        return password;
    }

    public String getDatacenter() {
        makeSureInitialized();
        return datacenter;
    }
}
