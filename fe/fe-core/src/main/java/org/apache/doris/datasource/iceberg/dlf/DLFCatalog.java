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

package org.apache.doris.datasource.iceberg.dlf;

import org.apache.doris.catalog.S3Resource;
import org.apache.doris.datasource.iceberg.dlf.client.DLFCachedClientPool;
import org.apache.doris.datasource.iceberg.dlf.client.DLFHttpProxy;

import com.aliyun.datalake20200710.models.Database;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DLFCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    private Configuration conf;
    private DLFCachedClientPool clients;
    private FileIO fileIO;
    private String uid;
    private DLFHttpProxy delegate;

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new DLFTableOperations(this.conf, this.clients, this.fileIO, this.uid, dbName, tableName);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        return null;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.uid = name;
        this.fileIO = new HadoopFileIO(conf);
        this.clients = new DLFCachedClientPool(this.conf, properties);
        this.delegate = new DLFHttpProxy(
                conf.get(S3Resource.S3_ACCESS_KEY),
                conf.get(S3Resource.S3_SECRET_KEY),
                conf.get(S3Resource.S3_ENDPOINT),
                conf.get(S3Resource.S3_REGION));
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        String dbName = namespace.level(0);
        return delegate.listTables(uid, dbName)
            .stream()
            .map(e -> TableIdentifier.of(dbName, e.getTableName()))
            .collect(Collectors.toList());
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        try {
            String dbName = tableIdentifier.namespace().level(0);
            delegate.dropTable(uid, dbName, tableIdentifier.name());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public void renameTable(TableIdentifier sourceTbl, TableIdentifier targetTbl) {
        try {
            String sourceDbName = sourceTbl.namespace().level(0);
            String targetDbName = targetTbl.namespace().level(0);
            if (!sourceDbName.equals(targetDbName)) {
                throw new RuntimeException("The two table not belong to a database.");
            }
            delegate.renameTable(uid, sourceDbName, sourceTbl.name(), targetTbl.name(), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> props) {
        try {
            String dbName = namespace.level(0);
            String locationUri = props.get("location");
            if (locationUri.isEmpty()) {
                throw new RuntimeException("");
            }
            delegate.createDatabase(uid, dbName, locationUri, props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        List<Namespace> namespaces = new ArrayList<>();
        List<Database> databases = delegate.listDatabases(uid);
        for (Database database : databases) {
            namespaces.add(Namespace.of(database.getName()));
        }
        return namespaces;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        String dbName = namespace.level(0);
        Database database = delegate.getDatabase(uid, dbName);
        Map<String, String> metaProperties = new HashMap<>(database.getParameters());
        metaProperties.put("createBy", database.getCreatedBy());
        return metaProperties;
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        String dbName = namespace.level(0);
        try {
            delegate.dropDatabase(uid, dbName, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> props) throws NoSuchNamespaceException {
        String dbName = namespace.level(0);
        try {
            delegate.addDbProperties(uid, dbName, props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> pNames) throws NoSuchNamespaceException {
        String dbName = namespace.level(0);
        try {
            delegate.removeDbProperties(uid, dbName, pNames);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
