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

package org.apache.doris.datasource.iceberg.hive;

import org.apache.doris.datasource.iceberg.hadoop.IcebergHadoopFileIO;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Preconditions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergHiveCatalog extends HiveCompatibleCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveCatalog.class);
    private ClientPool<IMetaStoreClient, TException> clients;
    private Configuration conf;
    private String name;
    private FileIO fileIO;
    private boolean listAllTables = false;

    public void initialize(String name, Map<String, String> properties) {
        this.name = name;
        if (this.conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = getConf();
        }
        if (properties.containsKey("uri")) {
            this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get("uri"));
        }
        if (properties.containsKey("warehouse")) {
            this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash(properties.get("warehouse")));
        }

        this.listAllTables = Boolean.parseBoolean(properties.getOrDefault("list-all-tables", "false"));
        String fileIOImpl = properties.get("io-impl");
        DFSFileSystem fs = new DFSFileSystem(properties);
        fileIO = fileIOImpl == null ? new IcebergHadoopFileIO(this.conf, fs)
                : CatalogUtil.loadFileIO(fileIOImpl, properties, this.conf);
        this.clients = new HCachedClientPool(name, this.conf, properties);
    }

    public String name() {
        return this.name;
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new IcebergHiveTableOperations(this.conf, this.clients, this.fileIO, this.name(), dbName, tableName);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        try {
            Database databaseData = this.clients.run((client) ->
                    client.getDatabase(tableIdentifier.namespace().levels()[0]));
            if (databaseData.getLocationUri() != null) {
                return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
            }
        } catch (TException e) {
            throw new RuntimeException(String.format("Metastore operation failed for %s", tableIdentifier), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during commit", e);
        }

        String databaseLocation = this.databaseLocation(tableIdentifier.namespace().levels()[0]);
        return String.format("%s/%s", databaseLocation, tableIdentifier.name());
    }

    private String databaseLocation(String databaseName) {
        String warehouseLocation = this.conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
        Preconditions.checkNotNull(warehouseLocation,
                "Warehouse location is not set: hive.metastore.warehouse.dir=null");
        warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
        return String.format("%s/%s.db", warehouseLocation, databaseName);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(!namespace.isEmpty(),
                "Cannot create namespace with invalid name: %s", namespace);
        Preconditions.checkArgument(this.isValidateNamespace(namespace),
                "Cannot support multi part namespace in Hive Metastore: %s", namespace);
        Preconditions.checkArgument(meta.get("hive.metastore.database.owner-type") == null
                || meta.get("hive.metastore.database.owner") != null,
                "Create namespace setting %s without setting %s is not allowed",
                "hive.metastore.database.owner-type",
                "hive.metastore.database.owner");
        try {
            this.clients.run((client) -> {
                client.createDatabase(convertToDatabase(namespace, meta));
                return null;
            });
            LOG.info("Created namespace: {}", namespace);
        } catch (TException e) {
            throw new RuntimeException("Failed to create namespace " + namespace + " in Hive Metastore", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to createDatabase(name) "
                    + namespace + " in Hive Metastore", e);
        }
    }

    Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
        if (!this.isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } else {
            Database database = new Database();
            Map<String, String> parameter = Maps.newHashMap();
            database.setName(namespace.level(0));
            database.setLocationUri(this.databaseLocation(namespace.level(0)));
            meta.forEach((key, value) -> {
                if (key.equals("comment")) {
                    database.setDescription(value);
                } else if (key.equals("location")) {
                    database.setLocationUri(value);
                } else if (key.equals("hive.metastore.database.owner")) {
                    database.setOwnerName(value);
                } else if (key.equals("hive.metastore.database.owner-type") && value != null) {
                    database.setOwnerType(PrincipalType.valueOf(value));
                } else if (value != null) {
                    parameter.put(key, value);
                }
            });
            if (database.getOwnerName() == null) {
                database.setOwnerName(HiveHadoopUtil.currentUser());
                database.setOwnerType(PrincipalType.USER);
            }
            database.setParameters(parameter);
            return database;
        }
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        if (!this.isValidateNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } else if (!namespace.isEmpty()) {
            return ImmutableList.of();
        } else {
            try {
                List<Namespace> namespaces = this.clients.run(IMetaStoreClient::getAllDatabases).stream()
                        .map(Namespace::of).collect(Collectors.toList());
                LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
                return namespaces;
            } catch (TException e) {
                throw new RuntimeException("Failed to list all namespace: " + namespace + " in Hive Metastore", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to getAllDatabases() "
                        + namespace + " in Hive Metastore", e);
            }
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        if (!this.isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } else {
            try {
                Database database = this.clients.run((client) -> {
                    try {
                        return client.getDatabase(namespace.level(0));
                    } catch (NoSuchObjectException e) {
                        return null;
                    }
                });
                if (database == null) {
                    throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
                }
                Map<String, String> metadata = this.convertToMetadata(database);
                LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
                return metadata;
            } catch (UnknownDBException | NoSuchObjectException e) {
                throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
            } catch (TException e) {
                throw new RuntimeException("Failed to list namespace under namespace: "
                        + namespace + " in Hive Metastore", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to getDatabase(name) "
                        + namespace + " in Hive Metastore", e);
            }
        }
    }

    private Map<String, String> convertToMetadata(Database database) {
        Map<String, String> meta = Maps.newHashMap();
        meta.putAll(database.getParameters());
        meta.put("location", database.getLocationUri());
        if (database.getDescription() != null) {
            meta.put("comment", database.getDescription());
        }
        if (database.getOwnerName() != null) {
            meta.put("hive.metastore.database.owner", database.getOwnerName());
            if (database.getOwnerType() != null) {
                meta.put("hive.metastore.database.owner-type", database.getOwnerType().name());
            }
        }
        return meta;
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if (!this.isValidateNamespace(namespace)) {
            return false;
        } else {
            try {
                this.clients.run((client) -> {
                    client.dropDatabase(namespace.level(0), false, false, false);
                    return null;
                });
                LOG.info("Dropped namespace: {}", namespace);
                return true;
            } catch (InvalidOperationException e) {
                throw new NamespaceNotEmptyException(e,
                        "Namespace %s is not empty. One or more tables exist.", namespace);
            } catch (NoSuchObjectException e) {
                return false;
            } catch (TException e) {
                throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to drop dropDatabase(name) "
                        + namespace + " in Hive Metastore", e);
            }
        }
    }

    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!this.isValidIdentifier(identifier)) {
            return false;
        } else {
            String database = identifier.namespace().level(0);
            TableOperations ops = this.newTableOps(identifier);
            TableMetadata lastMetadata = null;
            if (purge) {
                try {
                    lastMetadata = ops.current();
                } catch (NotFoundException e) {
                    LOG.warn("Failed to load table metadata for table: {}, continuing drop without purge",
                            identifier, e);
                }
            }
            try {
                this.clients.run((client) -> {
                    client.dropTable(database, identifier.name(), false, false);
                    return null;
                });
                if (purge && lastMetadata != null) {
                    CatalogUtil.dropTableData(ops.io(), lastMetadata);
                }
                LOG.info("Dropped table: {}", identifier);
                return true;
            } catch (NoSuchObjectException | NoSuchTableException e) {
                LOG.info("Skipping drop, table does not exist: {}", identifier, e);
                return false;
            } catch (TException e) {
                throw new RuntimeException("Failed to drop " + identifier, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to dropTable", e);
            }
        }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
        if (!this.isValidIdentifier(from)) {
            throw new NoSuchTableException("Invalid identifier: %s", from);
        } else {
            TableIdentifier to = this.removeCatalogName(originalTo);
            Preconditions.checkArgument(this.isValidIdentifier(to), "Invalid identifier: %s", to);
            String toDatabase = to.namespace().level(0);
            String fromDatabase = from.namespace().level(0);
            String fromName = from.name();

            try {
                Table table = this.clients.run((client) -> client.getTable(fromDatabase, fromName));
                validateTableIsIceberg(table, fullTableName(this.name, from));
                table.setDbName(toDatabase);
                table.setTableName(to.name());
                this.clients.run((client) -> {
                    MetastoreUtil.alterTable(client, fromDatabase, fromName, table);
                    return null;
                });
                LOG.info("Renamed table from {}, to {}", from, to);
            } catch (NoSuchObjectException e) {
                throw new NoSuchTableException("Table does not exist: %s", from);
            } catch (AlreadyExistsException e) {
                throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);
            } catch (TException e) {
                throw new RuntimeException("Failed to rename " + from + " to " + to, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to rename", e);
            }
        }
    }

    private TableIdentifier removeCatalogName(TableIdentifier to) {
        if (this.isValidIdentifier(to)) {
            return to;
        } else {
            return to.namespace().levels().length == 2
                    && this.name().equalsIgnoreCase(to.namespace().level(0))
                    ? TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name()) : to;
        }
    }

    static void validateTableIsIceberg(Table table, String fullName) {
        String tableType = table.getParameters().get("table_type");
        NoSuchIcebergTableException.check(tableType != null && tableType.equalsIgnoreCase("iceberg"),
                "Not an iceberg table: %s (type=%s)", fullName, tableType);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        Preconditions.checkArgument(properties.get("hive.metastore.database.owner-type") == null
                    && properties.get("hive.metastore.database.owner") == null,
                "Setting %s and %s has to be performed together or not at all",
                "hive.metastore.database.owner-type",
                "hive.metastore.database.owner");
        Map<String, String> parameter = Maps.newHashMap();
        parameter.putAll(this.loadNamespaceMetadata(namespace));
        parameter.putAll(properties);
        Database database = this.convertToDatabase(namespace, parameter);
        this.alterHiveDataBase(namespace, database);
        LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        Preconditions.checkArgument(properties.contains("hive.metastore.database.owner-type")
                == properties.contains("hive.metastore.database.owner"),
                "Removing %s and %s has to be performed together or not at all",
                "hive.metastore.database.owner-type",
                "hive.metastore.database.owner");
        Map<String, String> parameter = Maps.newHashMap();
        parameter.putAll(this.loadNamespaceMetadata(namespace));
        properties.forEach((key) -> parameter.put(key, null));
        Database database = this.convertToDatabase(namespace, parameter);
        this.alterHiveDataBase(namespace, database);
        LOG.debug("Successfully removed properties {} from {}", properties, namespace);
        return true;
    }

    private void alterHiveDataBase(Namespace namespace, Database database) {
        try {
            this.clients.run((client) -> {
                client.alterDatabase(namespace.level(0), database);
                return null;
            });
        } catch (UnknownDBException | NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        } catch (TException e) {
            throw new RuntimeException("Failed to list namespace under namespace: "
                    + namespace + " in Hive Metastore", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to getDatabase(name) "
                    + namespace + " in Hive Metastore", e);
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(this.isValidateNamespace(namespace),
                "Missing database in namespace: %s", namespace);
        String database = namespace.level(0);
        try {
            List<String> tableNames = this.clients.run((client) -> client.getAllTables(database));
            List<TableIdentifier> tableIdentifiers;
            if (this.listAllTables) {
                tableIdentifiers = tableNames.stream()
                        .map((t) -> TableIdentifier.of(namespace, t))
                        .collect(Collectors.toList());
            } else {
                List<Table> tableObjects = this.clients.run((client) ->
                        client.getTableObjectsByName(database, tableNames));
                tableIdentifiers = tableObjects.stream()
                        .filter((table) ->
                                table.getParameters() != null
                                        && "iceberg".equalsIgnoreCase(table.getParameters().get("table_type")))
                        .map((table) -> TableIdentifier.of(namespace, table.getTableName()))
                        .collect(Collectors.toList());
            }

            LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, tableIdentifiers);
            return tableIdentifiers;
        } catch (UnknownDBException e) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables under namespace " + namespace, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listTables", e);
        }
    }

    private boolean isValidateNamespace(Namespace namespace) {
        return namespace.levels().length == 1;
    }
}
