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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ThirdPartyCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

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

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        return null;
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    protected boolean isValidNamespace(Namespace namespace) {
        return namespace.levels().length == 1;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        if (!isValidNamespace(namespace)) {
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
        if (!isValidIdentifier(tableIdentifier)) {
            throw new NoSuchTableException("Invalid identifier: %s", tableIdentifier);
        }
        try {
            String dbName = tableIdentifier.namespace().level(0);
            clients.run(client -> {
                client.dropTable(dbName, tableIdentifier.name(),
                        false /* do not delete data */,
                        false /* throw NoSuchObjectException if the table doesn't exist */);
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public void renameTable(TableIdentifier sourceTbl, TableIdentifier targetTbl) {
        if (!isValidIdentifier(sourceTbl)) {
            throw new NoSuchTableException("Invalid identifier: %s", sourceTbl);
        }
        try {
            String sourceDbName = sourceTbl.namespace().level(0);
            String targetDbName = targetTbl.namespace().level(0);
            if (!sourceDbName.equals(targetDbName)) {
                throw new RuntimeException("The two table not belong to a database.");
            }
            Table table = clients.run(client -> client.getTable(sourceDbName, sourceTbl.name()));
            validateTableIsIceberg(table, fullTableName(sourceDbName, sourceTbl));
            clients.run(client -> {
                MetastoreUtil.alterTable(client, sourceDbName, sourceTbl.name(), table);
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Fail to renameTable.", e);
        }
    }

    static void validateTableIsIceberg(Table table, String fullName) {
        String type = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
        NoSuchIcebergTableException.check(
                type != null && type.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
                "Not an iceberg table: %s (type=%s)",
                fullName,
                type);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> props) {
        throw new UnsupportedOperationException(
            "Cannot create namespace " + namespace + " : createNamespace is not supported");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        if (!isValidNamespace(namespace) && !namespace.isEmpty()) {
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
        if (!isValidNamespace(namespace)) {
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
        if (!isValidNamespace(namespace)) {
            throw new NoSuchTableException("Invalid namespace: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            clients.run(client -> {
                client.dropDatabase(dbName);
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
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

