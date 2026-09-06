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

package org.apache.doris.connector.iceberg.dlf;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog.TableBuilder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Base catalog for Hive-compatible metastores that need a custom client pool. */
public abstract class HiveCompatibleCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    protected Configuration conf;
    protected ClientPool<IMetaStoreClient, TException> clients;
    protected FileIO fileIO;
    protected String catalogName;
    private boolean listAllTables;
    private boolean closed;

    public void initialize(String name, FileIO fileIO, ClientPool<IMetaStoreClient, TException> clients) {
        initialize(name, fileIO, clients, Map.of());
    }

    public void initialize(String name, FileIO fileIO, ClientPool<IMetaStoreClient, TException> clients,
            Map<String, String> properties) {
        this.catalogName = name;
        this.fileIO = fileIO;
        this.clients = clients;
        this.listAllTables = Boolean.parseBoolean(properties.getOrDefault(
                HiveCatalog.LIST_ALL_TABLES, HiveCatalog.LIST_ALL_TABLES_DEFAULT));
    }

    protected FileIO initializeFileIO(Map<String, String> properties, Configuration hadoopConf) {
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        if (fileIOImpl == null) {
            FileIO io = new HadoopFileIO(hadoopConf);
            io.initialize(properties);
            return io;
        }
        return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
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
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            List<String> tableNames = clients.run(client -> client.getAllTables(dbName));
            if (listAllTables) {
                return tableNames.stream()
                        .map(table -> TableIdentifier.of(dbName, table))
                        .collect(Collectors.toList());
            }
            // DLF namespaces are format-shared; publishing non-Iceberg names creates unusable Doris tables.
            List<Table> tables = clients.run(client -> client.getTableObjectsByName(dbName, tableNames));
            return tables.stream()
                    .filter(table -> table.getParameters() != null
                            && BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
                                    table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)))
                    .map(Table::getTableName)
                    .map(table -> TableIdentifier.of(dbName, table))
                    .collect(Collectors.toList());
        } catch (UnknownDBException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        } catch (TException e) {
            throw new RuntimeException("Failed to list tables under namespace " + namespace, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listTables", e);
        }
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        // DLF metadata writes were never supported; reject before BaseMetastoreCatalog builds a null location.
        throw new UnsupportedOperationException("Cannot create table " + identifier + ": not supported");
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        throw new UnsupportedOperationException("Cannot drop table " + tableIdentifier + ": not supported");
    }

    @Override
    public void renameTable(TableIdentifier source, TableIdentifier target) {
        throw new UnsupportedOperationException("Cannot rename table " + source + ": not supported");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> properties) {
        throw new UnsupportedOperationException("Cannot create namespace " + namespace + ": not supported");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        if (!isValidNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        if (!namespace.isEmpty()) {
            return new ArrayList<>();
        }
        try {
            return clients.run(IMetaStoreClient::getAllDatabases).stream()
                    .map(Namespace::of)
                    .collect(Collectors.toList());
        } catch (TException e) {
            throw new RuntimeException("Failed to list namespaces", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listNamespaces", e);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        if (!isValidNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        try {
            Database database = clients.run(client -> client.getDatabase(namespace.level(0)));
            Map<String, String> metadata = new HashMap<>();
            if (database.getParameters() != null) {
                metadata.putAll(database.getParameters());
            }
            // Iceberg consumes the reserved location key rather than Hive's locationUri field directly.
            if (database.getLocationUri() != null) {
                metadata.put("location", database.getLocationUri());
            }
            if (database.getDescription() != null) {
                metadata.put("comment", database.getDescription());
            }
            if (database.getOwnerName() != null) {
                metadata.put(HiveCatalog.HMS_DB_OWNER, database.getOwnerName());
                if (database.getOwnerType() != null) {
                    metadata.put(HiveCatalog.HMS_DB_OWNER_TYPE, database.getOwnerType().name());
                }
            }
            return metadata;
        } catch (NoSuchObjectException | UnknownDBException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        } catch (TException e) {
            throw new RuntimeException("Failed to load namespace " + namespace, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to loadNamespaceMetadata", e);
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        throw new UnsupportedOperationException("Cannot drop namespace " + namespace + ": not supported");
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Cannot set namespace properties " + namespace + ": not supported");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
                "Cannot remove namespace properties " + namespace + ": not supported");
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        // Mark closed first so cleanup remains idempotent even when partially initialized resources fail to close.
        closed = true;
        IOException failure = null;
        try {
            if (fileIO != null) {
                fileIO.close();
            }
        } catch (Exception e) {
            failure = new IOException("Failed to close DLF FileIO", e);
        }
        try {
            if (clients instanceof AutoCloseable) {
                ((AutoCloseable) clients).close();
            }
        } catch (Exception e) {
            IOException closeFailure = new IOException("Failed to close DLF client pool", e);
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        try {
            super.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
