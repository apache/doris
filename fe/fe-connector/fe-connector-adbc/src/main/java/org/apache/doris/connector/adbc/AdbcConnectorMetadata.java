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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Serves {@code SHOW DATABASES} / {@code SHOW TABLES} / {@code DESC} for an ADBC catalog.
 *
 * <p>Created fresh per statement by the engine's metadata funnel, so it holds no cross-statement state.
 * Everything worth keeping longer lives on the connector and is reached from here: which call a driver
 * answers schemas with (re-probing it per table would cost one failed remote call per table forever), and
 * the answers themselves ({@link AdbcMetadataCache}).
 */
public class AdbcConnectorMetadata implements ConnectorMetadata {

    /**
     * Only base tables are surfaced; ADBC catalogs expose no views in Doris. Asking for the type is not
     * enough to get it -- a Doris source answers this filter with everything it has -- so what actually
     * holds the guarantee is {@code AdbcObjectsReader}, which drops whatever the answer calls a view.
     */
    private static final String[] TABLE_TYPES = {"table"};

    private final AdbcClient client;
    private final AdbcSchemaStrategy schemaStrategy;
    private final Supplier<AdbcDialect> dialect;
    private final AdbcMetadataCache cache;

    /**
     * @param dialect resolved lazily, because only the {@code executeSchema} fallback needs it and
     *                resolving it can cost a remote call
     * @param cache   the catalog's, shared with every other statement
     */
    public AdbcConnectorMetadata(AdbcClient client, AdbcSchemaStrategy schemaStrategy,
            Supplier<AdbcDialect> dialect, AdbcMetadataCache cache) {
        this.client = client;
        this.schemaStrategy = schemaStrategy;
        this.dialect = dialect;
        this.cache = cache;
    }

    // ========= ConnectorSchemaOps =========

    /**
     * <b>Reads the source, never the cache</b>, and refreshes the cache with what it read.
     *
     * <p>This is not just a report for {@code SHOW DATABASES}: the engine's own name cache is loaded from it
     * and then decides whether a database exists at all -- including its last-chance re-list for a name it
     * has never seen. Answering that from a remembered listing would make the re-list pointless and leave a
     * database created a moment ago unreachable until an entry expired.
     */
    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        List<String> names = new ArrayList<>();
        for (AdbcNamespace namespace : cache.reloadNamespaces(this::readNamespaces)) {
            names.add(namespace.dorisDatabaseName());
        }
        return names;
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return findNamespace(dbName).isPresent();
    }

    // ========= ConnectorTableMetadataOps =========

    /** Reads the source, never the cache. Same reason as {@link #listDatabaseNames}, one level down. */
    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        Optional<AdbcNamespace> namespace = findNamespace(dbName);
        if (!namespace.isPresent()) {
            return List.of();
        }
        return cache.reloadTableNames(namespace.get(), () -> readTableNames(namespace.get()));
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        Optional<AdbcNamespace> namespace = findNamespace(dbName);
        if (!namespace.isPresent()) {
            return Optional.empty();
        }
        if (!tableExists(namespace.get(), tableName)) {
            return Optional.empty();
        }
        return Optional.of(new AdbcTableHandle(namespace.get(), tableName));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        AdbcTableHandle adbcHandle = (AdbcTableHandle) handle;
        Schema arrowSchema = arrowSchemaOf(session, adbcHandle);

        List<ConnectorColumn> columns = new ArrayList<>(arrowSchema.getFields().size());
        for (Field field : arrowSchema.getFields()) {
            columns.add(new ConnectorColumn(field.getName(),
                    AdbcTypeMapper.toDorisType(field.getName(), field),
                    null, field.isNullable(), null, true));
        }
        // No table properties. The catalog's own map is what used to be handed over here, and it holds
        // the password: SHOW CREATE TABLE renders table properties unmasked (Env.getDdlStmt), and the
        // only thing keeping that rendering away from an adbc table is the SUPPORTS_SHOW_CREATE_DDL
        // capability this connector does not declare -- a single gate between a credential and a
        // user-visible string. Nothing reads these entries for an adbc table, so there is nothing to
        // send.
        return new ConnectorTableSchema(adbcHandle.getRemoteTable(), columns, "ADBC",
                Collections.emptyMap());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        Schema arrowSchema = arrowSchemaOf(session, (AdbcTableHandle) handle);
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(arrowSchema.getFields().size());
        for (Field field : arrowSchema.getFields()) {
            handles.put(field.getName(), new NamedColumnHandle(field.getName()));
        }
        return handles;
    }

    /**
     * ADBC has no descriptor of its own, so it borrows the Hive one, as the other connectors that read
     * through the generic file-scan path do.
     *
     * <p>Leaving the SPI default (null) is not neutral: fe-core would fall back to
     * {@code TTableType.SCHEMA_TABLE} and BE would build a {@code SchemaTableDescriptor} instead of the
     * descriptor the scan path expects.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        THiveTable hiveTable = new THiveTable(dbName, tableName, new HashMap<>());
        TTableDescriptor descriptor = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        descriptor.setHiveTable(hiveTable);
        return descriptor;
    }

    // ========= internals =========

    private List<AdbcNamespace> readNamespaces() {
        return client.withConnection(connection -> {
            try (ArrowReader reader = connection.getObjects(
                    AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
                return AdbcObjectsReader.readNamespaces(reader);
            }
        });
    }

    /**
     * Resolves a Doris database name, asking the source again before deciding there is no such database.
     *
     * <p>This is the path a query takes -- every {@code getTableHandle} starts here -- so it reads what was
     * remembered. The second read is what keeps that from being able to say "no": a database created since
     * the listing was cached is found rather than denied, and the extra remote call falls only on the path
     * that was about to fail anyway.
     */
    private Optional<AdbcNamespace> findNamespace(String dbName) {
        Optional<AdbcNamespace> found = match(cache.namespaces(this::readNamespaces), dbName);
        if (found.isPresent()) {
            return found;
        }
        return match(cache.reloadNamespaces(this::readNamespaces), dbName);
    }

    private static Optional<AdbcNamespace> match(List<AdbcNamespace> namespaces, String dbName) {
        for (AdbcNamespace namespace : namespaces) {
            if (namespace.dorisDatabaseName().equals(dbName)) {
                return Optional.of(namespace);
            }
        }
        return Optional.empty();
    }

    /** The table-level counterpart of {@link #findNamespace}; same reason, same cost. */
    private boolean tableExists(AdbcNamespace namespace, String tableName) {
        return cache.tableNames(namespace, () -> readTableNames(namespace)).contains(tableName)
                || cache.reloadTableNames(namespace, () -> readTableNames(namespace)).contains(tableName);
    }

    private List<String> readTableNames(AdbcNamespace namespace) {
        return client.withConnection(connection -> {
            try (ArrowReader reader = connection.getObjects(AdbcConnection.GetObjectsDepth.TABLES,
                    emptyToNull(namespace.getRemoteCatalog()), emptyToNull(namespace.getRemoteDbSchema()),
                    null, TABLE_TYPES, null)) {
                return AdbcObjectsReader.readTableNames(reader, namespace);
            }
        });
    }

    /**
     * Two layers, and neither makes the other redundant: the statement scope folds the two SPI calls one
     * statement makes for the same table ({@code getTableSchema} then {@code getColumnHandles}) into one
     * lookup, while the catalog cache carries the answer to the next statement.
     */
    private Schema arrowSchemaOf(ConnectorSession session, AdbcTableHandle handle) {
        return AdbcStatementScope.sharedTableSchema(session, handle,
                () -> cache.tableSchema(handle, () -> fetchArrowSchema(handle)));
    }

    /**
     * Resolves one table's Arrow schema, falling back across the ways a driver may offer it.
     *
     * <p>Neither call is guaranteed. {@code getTableSchema} has a default implementation in the ADBC API
     * that throws {@code NOT_IMPLEMENTED}, and drivers do leave it there -- the Java Flight SQL driver, for
     * one. {@code executeSchema} is no safer in the other direction: the SQLite driver answers it with
     * {@code NOT_IMPLEMENTED} while implementing {@code getTableSchema} fine. So each is the other's
     * fallback, and the strategy is remembered per catalog rather than re-probed per table.
     *
     * <p><b>Only {@code NOT_IMPLEMENTED} triggers the fallback.</b> Any other status means the driver did
     * try and the table is at fault -- a missing table answers {@code NOT_FOUND} -- and falling back there
     * would swap a precise "no such table" for a misleading "this driver implements neither method".
     *
     * <p>The column layer of {@code getObjects} is deliberately not a third fallback: it reports XDBC
     * integer type codes, not Arrow types, so it would answer with a different type system than the one BE
     * reads the data in.
     */
    private Schema fetchArrowSchema(AdbcTableHandle handle) {
        return client.withConnection(connection -> {
            if (schemaStrategy.get() == AdbcSchemaStrategy.Kind.EXECUTE_SCHEMA) {
                return executeSchema(connection, handle);
            }
            try {
                Schema schema = connection.getTableSchema(
                        emptyToNull(handle.getRemoteCatalog()), emptyToNull(handle.getRemoteDbSchema()),
                        handle.getRemoteTable());
                schemaStrategy.set(AdbcSchemaStrategy.Kind.GET_TABLE_SCHEMA);
                return schema;
            } catch (AdbcException e) {
                if (e.getStatus() != AdbcStatusCode.NOT_IMPLEMENTED) {
                    throw AdbcClient.translate(e, "Failed to read the schema of "
                            + handle.getDorisDbName() + "." + handle.getRemoteTable());
                }
                Schema schema = executeSchemaOrExplain(connection, handle, e);
                schemaStrategy.set(AdbcSchemaStrategy.Kind.EXECUTE_SCHEMA);
                return schema;
            }
        });
    }

    private Schema executeSchemaOrExplain(AdbcConnection connection, AdbcTableHandle handle,
            AdbcException getTableSchemaFailure) {
        try {
            return executeSchema(connection, handle);
        } catch (Exception e) {
            throw new DorisConnectorException("Cannot determine the schema of "
                    + handle.getDorisDbName() + "." + handle.getRemoteTable()
                    + ": this ADBC driver implements neither getTableSchema (status="
                    + getTableSchemaFailure.getStatus() + ") nor executeSchema (" + e.getMessage()
                    + "). Doris cannot map the table's columns without one of them.", e);
        }
    }

    /**
     * Asks for the shape of a row without fetching any. {@code WHERE 1 = 0} is the most portable way to
     * say "no rows"; the table name comes from the dialect so this path and a scan address the same table
     * the same way -- two spellings of one name is exactly how a source ends up working for queries and
     * failing for {@code DESC}.
     */
    private Schema executeSchema(AdbcConnection connection, AdbcTableHandle handle) {
        String sql = "SELECT * FROM " + dialect.get().qualifiedTableName(handle) + " WHERE 1 = 0";
        try (AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            return statement.executeSchema();
        } catch (AdbcException e) {
            throw AdbcClient.translate(e, "executeSchema failed for: " + sql);
        } catch (Exception e) {
            throw new DorisConnectorException("executeSchema failed for: " + sql, e);
        }
    }

    /**
     * ADBC treats null as "any" for a catalog or schema filter, and a source without that level reports it
     * as the empty string. Passing the empty string through would ask for a level literally named "".
     */
    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
