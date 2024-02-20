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

package org.apache.doris.datasource.iceberg;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class HiveCompatibleCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    protected Configuration conf;
    protected ClientPool<IMetaStoreClient, TException> clients;
    protected FileIO fileIO;
    protected String uid;

    public void initialize(String name, FileIO fileIO,
                           ClientPool<IMetaStoreClient, TException> clients) {
        this.uid = name;
        this.fileIO = fileIO;
        this.clients = clients;
    }

    protected FileIO initializeFileIO(Map<String, String> properties, Configuration hadoopConf) {
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        if (fileIOImpl == null) {
            /* when use the S3FileIO, we need some custom configurations,
             * so HadoopFileIO is used in the superclass by default
             * we can add better implementations to derived class just like the implementation in DLFCatalog.
             */
            FileIO io = new HadoopFileIO(hadoopConf);
            io.initialize(properties);
            return io;
        } else {
            return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
        }
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        return null;
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    protected boolean isValidNamespace(Namespace namespace) {
        return namespace.levels().length != 1;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        if (isValidNamespace(namespace)) {
            throw new NoSuchTableException("Invalid namespace: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            return clients.run(client -> client.getAllTables(dbName))
                .stream()
                .map(tbl -> TableIdentifier.of(dbName, tbl))
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        throw new UnsupportedOperationException(
            "Cannot drop table " + tableIdentifier + " : dropTable is not supported");
    }

    @Override
    public void renameTable(TableIdentifier sourceTbl, TableIdentifier targetTbl) {
        throw new UnsupportedOperationException(
            "Cannot rename table " + sourceTbl + " : renameTable is not supported");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> props) {
        throw new UnsupportedOperationException(
            "Cannot create namespace " + namespace + " : createNamespace is not supported");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        if (isValidNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        if (!namespace.isEmpty()) {
            return new ArrayList<>();
        }
        List<Namespace> namespaces = new ArrayList<>();
        List<String> databases;
        try {
            databases = clients.run(client -> client.getAllDatabases());
            for (String database : databases) {
                namespaces.add(Namespace.of(database));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return namespaces;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        if (isValidNamespace(namespace)) {
            throw new NoSuchTableException("Invalid namespace: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            return clients.run(client -> client.getDatabase(dbName)).getParameters();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        throw new UnsupportedOperationException(
            "Cannot drop namespace " + namespace + " : dropNamespace is not supported");
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> props) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
            "Cannot set namespace properties " + namespace + " : setProperties is not supported");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> pNames) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
            "Cannot remove properties " + namespace + " : removeProperties is not supported");
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
