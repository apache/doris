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

import org.apache.doris.datasource.iceberg.hive.CachedClientPool;
import org.apache.doris.datasource.iceberg.hive.HiveTableOperations;
import org.apache.doris.datasource.iceberg.io.AuthHadoopFileIO;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
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

public class HiveIcebergCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
    public static final String LIST_ALL_TABLES = "list-all-tables";
    public static final String LIST_ALL_TABLES_DEFAULT = "false";
    public static final String HMS_TABLE_OWNER = "hive.metastore.table.owner";
    public static final String HMS_DB_OWNER = "hive.metastore.database.owner";
    public static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";
    static final String HIVE_CONF_CATALOG = "metastore.catalog.default";
    private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergCatalog.class);
    private String name;
    private Configuration conf;
    private FileIO fileIO;
    private ClientPool<IMetaStoreClient, TException> clients;
    private boolean listAllTables = false;
    private Map<String, String> catalogProperties;

    public HiveIcebergCatalog() {
    }

    public void initialize(String inputName, Map<String, String> properties) {
        this.catalogProperties = ImmutableMap.copyOf(properties);
        this.name = inputName;
        if (this.conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }

        if (properties.containsKey("uri")) {
            this.conf.set(ConfVars.METASTOREURIS.varname, properties.get("uri"));
        }

        if (properties.containsKey("warehouse")) {
            this.conf.set(ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash(properties.get("warehouse")));
        }

        this.listAllTables = Boolean.parseBoolean(properties.getOrDefault("list-all-tables", "false"));
        String fileIOImpl = properties.get("io-impl");
        this.fileIO = (fileIOImpl == null ? new AuthHadoopFileIO(this.conf) : CatalogUtil.loadFileIO(fileIOImpl,
            properties, this.conf));
        this.clients = new CachedClientPool(this.conf, properties);
    }

    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(this.isValidateNamespace(namespace),
                "Missing database in namespace: %s", namespace);
        String database = namespace.level(0);

        try {
            List<String> tableNames = this.clients.run((client) -> {
                return client.getAllTables(database);
            });
            List tableIdentifiers;
            if (this.listAllTables) {
                tableIdentifiers = tableNames.stream().map((t) -> TableIdentifier.of(namespace, t)).collect(
                    Collectors.toList());
            } else {
                List<Table> tableObjects = this.clients.run((client) -> client.getTableObjectsByName(database,
                        tableNames));
                tableIdentifiers = tableObjects.stream().filter((table) -> table.getParameters() != null
                    && "iceberg".equalsIgnoreCase(table.getParameters().get("table_type"))
                ).map((table) -> TableIdentifier.of(namespace, table.getTableName())).collect(Collectors.toList());
            }

            LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, tableIdentifiers);
            return tableIdentifiers;
        } catch (UnknownDBException var6) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", new Object[]{namespace});
        } catch (TException var7) {
            throw new RuntimeException("Failed to list all tables under namespace " + namespace, var7);
        } catch (InterruptedException var8) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to listTables", var8);
        }
    }

    public String name() {
        return this.name;
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
                } catch (NotFoundException var10) {
                    LOG.warn("Failed to load table metadata for table: {}, continuing drop without purge",
                            identifier, var10);
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
            } catch (NoSuchObjectException | NoSuchTableException var7) {
                LOG.info("Skipping drop, table does not exist: {}", identifier, var7);
                return false;
            } catch (TException var8) {
                throw new RuntimeException("Failed to drop " + identifier, var8);
            } catch (InterruptedException var9) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to dropTable", var9);
            }
        }
    }

    public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
        if (!this.isValidIdentifier(from)) {
            throw new NoSuchTableException("Invalid identifier: %s", new Object[]{from});
        } else {
            TableIdentifier to = this.removeCatalogName(originalTo);
            Preconditions.checkArgument(this.isValidIdentifier(to), "Invalid identifier: %s", to);
            String toDatabase = to.namespace().level(0);
            String fromDatabase = from.namespace().level(0);
            String fromName = from.name();

            try {
                Table table = this.clients.run((client) -> client.getTable(fromDatabase, fromName));
                HiveTableOperations.validateTableIsIceberg(table, fullTableName(this.name, from));
                table.setDbName(toDatabase);
                table.setTableName(to.name());
                this.clients.run((client) -> {
                    MetastoreUtil.alterTable(client, fromDatabase, fromName, table);
                    return null;
                });
                LOG.info("Renamed table from {}, to {}", from, to);
            } catch (NoSuchObjectException var8) {
                throw new NoSuchTableException("Table does not exist: %s", from);
            } catch (AlreadyExistsException var9) {
                throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);
            } catch (TException var10) {
                throw new RuntimeException("Failed to rename " + from + " to " + to, var10);
            } catch (InterruptedException var11) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to rename", var11);
            }
        }
    }

    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(!namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        Preconditions.checkArgument(this.isValidateNamespace(namespace),
                "Cannot support multi part namespace in Hive Metastore: %s", namespace);
        Preconditions.checkArgument(meta.get("hive.metastore.database.owner-type") == null
                || meta.get("hive.metastore.database.owner") != null,
                "Create namespace setting %s without setting %s is not allowed",
                "hive.metastore.database.owner-type", "hive.metastore.database.owner");

        try {
            this.clients.run((client) -> {
                client.createDatabase(this.convertToDatabase(namespace, meta));
                return null;
            });
            LOG.info("Created namespace: {}", namespace);
        } catch (AlreadyExistsException var4) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException(var4, "Namespace '%s' already exists!",
                namespace);
        } catch (TException var5) {
            throw new RuntimeException("Failed to create namespace " + namespace + " in Hive Metastore", var5);
        } catch (InterruptedException var6) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to createDatabase(name) "
                + namespace + " in Hive Metastore", var6);
        }
    }

    public List<Namespace> listNamespaces(Namespace namespace) {
        if (!this.isValidateNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } else if (!namespace.isEmpty()) {
            return ImmutableList.of();
        } else {
            try {
                List<Namespace> namespaces = (this.clients.run(IMetaStoreClient::getAllDatabases))
                        .stream()
                        .map(db -> Namespace.of(new String[]{db}))
                        .collect(Collectors.toList());
                LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
                return namespaces;
            } catch (TException var3) {
                throw new RuntimeException("Failed to list all namespace: " + namespace + " in Hive Metastore", var3);
            } catch (InterruptedException var4) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to getAllDatabases() "
                    + namespace + " in Hive Metastore", var4);
            }
        }
    }

    public boolean dropNamespace(Namespace namespace) {
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
            } catch (InvalidOperationException var3) {
                throw new NamespaceNotEmptyException(var3, "Namespace %s is not empty. One or more tables exist.",
                    namespace);
            } catch (NoSuchObjectException var4) {
                return false;
            } catch (TException var5) {
                throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", var5);
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to drop dropDatabase(name) "
                    + namespace + " in Hive Metastore", var6);
            }
        }
    }

    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        Preconditions.checkArgument(properties.get("hive.metastore.database.owner-type") == null
                == (properties.get("hive.metastore.database.owner") == null),
                "Setting %s and %s has to be performed together or not at all",
                "hive.metastore.database.owner-type", "hive.metastore.database.owner");
        Map<String, String> parameter = Maps.newHashMap();
        parameter.putAll(this.loadNamespaceMetadata(namespace));
        parameter.putAll(properties);
        Database database = this.convertToDatabase(namespace, parameter);
        this.alterHiveDataBase(namespace, database);
        LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
        return true;
    }

    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        Preconditions.checkArgument(properties.contains("hive.metastore.database.owner-type")
                == properties.contains("hive.metastore.database.owner"),
                "Removing %s and %s has to be performed together or not at all",
                "hive.metastore.database.owner-type", "hive.metastore.database.owner");
        Map<String, String> parameter = Maps.newHashMap();
        parameter.putAll(this.loadNamespaceMetadata(namespace));
        properties.forEach(key -> parameter.put(key, null));
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
        } catch (UnknownDBException | NoSuchObjectException var4) {
            throw new NoSuchNamespaceException(var4, "Namespace does not exist: %s", namespace);
        } catch (TException var5) {
            throw new RuntimeException("Failed to list namespace under namespace: "
                + namespace + " in Hive Metastore", var5);
        } catch (InterruptedException var6) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in call to getDatabase(name) "
                + namespace + " in Hive Metastore", var6);
        }
    }

    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        if (!this.isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        } else {
            try {
                Database database = this.clients.run((client) -> client.getDatabase(namespace.level(0)));
                Map<String, String> metadata = this.convertToMetadata(database);
                LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
                return metadata;
            } catch (UnknownDBException | NoSuchObjectException var4) {
                throw new NoSuchNamespaceException(var4, "Namespace does not exist: %s", namespace);
            } catch (TException var5) {
                throw new RuntimeException("Failed to list namespace under namespace: "
                    + namespace + " in Hive Metastore", var5);
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted in call to getDatabase(name) "
                    + namespace + " in Hive Metastore", var6);
            }
        }
    }

    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    private TableIdentifier removeCatalogName(TableIdentifier to) {
        if (this.isValidIdentifier(to)) {
            return to;
        } else {
            return to.namespace().levels().length == 2 && this.name().equalsIgnoreCase(to.namespace().level(0))
                ? TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name()) : to;
        }
    }

    private boolean isValidateNamespace(Namespace namespace) {
        return namespace.levels().length == 1;
    }

    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new HiveTableOperations(this.conf, this.clients, this.fileIO, this.name, dbName, tableName);
    }

    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        try {
            Database databaseData = this.clients.run((client) ->
                    client.getDatabase(tableIdentifier.namespace().levels()[0]));
            if (databaseData.getLocationUri() != null) {
                return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
            }
        } catch (TException var3) {
            throw new RuntimeException(String.format("Metastore operation failed for %s", tableIdentifier), var3);
        } catch (InterruptedException var4) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during commit", var4);
        }

        String databaseLocation = this.databaseLocation(tableIdentifier.namespace().levels()[0]);
        return String.format("%s/%s", databaseLocation, tableIdentifier.name());
    }

    private String databaseLocation(String databaseName) {
        String warehouseLocation = this.conf.get(ConfVars.METASTOREWAREHOUSE.varname);
        Preconditions.checkNotNull(warehouseLocation,
                "Warehouse location is not set: hive.metastore.warehouse.dir=null");
        warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
        return String.format("%s/%s.db", warehouseLocation, databaseName);
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

    public String toString() {
        return MoreObjects.toStringHelper(this).add("name", this.name).add("uri", this.conf == null
            ? "" : this.conf.get(ConfVars.METASTOREURIS.varname)).toString();
    }

    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    public Configuration getConf() {
        return this.conf;
    }

    protected Map<String, String> properties() {
        return this.catalogProperties == null ? ImmutableMap.of() : this.catalogProperties;
    }

    @VisibleForTesting
    void setListAllTables(boolean listAllTables) {
        this.listAllTables = listAllTables;
    }

    @VisibleForTesting
    ClientPool<IMetaStoreClient, TException> clientPool() {
        return this.clients;
    }
}
