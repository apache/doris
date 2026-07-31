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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Serves {@code SHOW DATABASES} / {@code SHOW TABLES} / {@code DESC} for an ADBC catalog.
 *
 * <p>Created fresh per statement by the engine's metadata funnel, so it holds no cross-statement state; the
 * one piece of knowledge worth keeping longer -- which call a driver answers schemas with -- lives on the
 * connector, because re-probing it per table would cost one failed remote call per table forever.
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
    private final Map<String, String> properties;
    private final Supplier<AdbcDialect> dialect;

    /**
     * @param dialect resolved lazily, because only the {@code executeSchema} fallback needs it and
     *                resolving it can cost a remote call
     */
    public AdbcConnectorMetadata(AdbcClient client, AdbcSchemaStrategy schemaStrategy,
            Map<String, String> properties, Supplier<AdbcDialect> dialect) {
        this.client = client;
        this.schemaStrategy = schemaStrategy;
        this.properties = properties;
        this.dialect = dialect;
    }

    // ========= ConnectorSchemaOps =========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        List<String> names = new ArrayList<>();
        for (AdbcNamespace namespace : listNamespaces()) {
            names.add(namespace.dorisDatabaseName());
        }
        return names;
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return findNamespace(dbName).isPresent();
    }

    // ========= ConnectorTableMetadataOps =========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        Optional<AdbcNamespace> namespace = findNamespace(dbName);
        if (!namespace.isPresent()) {
            return List.of();
        }
        return listTableNames(namespace.get());
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        Optional<AdbcNamespace> namespace = findNamespace(dbName);
        if (!namespace.isPresent()) {
            return Optional.empty();
        }
        if (!listTableNames(namespace.get()).contains(tableName)) {
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
        return new ConnectorTableSchema(adbcHandle.getRemoteTable(), columns, "ADBC", properties);
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

    private List<AdbcNamespace> listNamespaces() {
        return client.withConnection(connection -> {
            try (ArrowReader reader = connection.getObjects(
                    AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
                return AdbcObjectsReader.readNamespaces(reader);
            }
        });
    }

    private Optional<AdbcNamespace> findNamespace(String dbName) {
        for (AdbcNamespace namespace : listNamespaces()) {
            if (namespace.dorisDatabaseName().equals(dbName)) {
                return Optional.of(namespace);
            }
        }
        return Optional.empty();
    }

    private List<String> listTableNames(AdbcNamespace namespace) {
        return client.withConnection(connection -> {
            try (ArrowReader reader = connection.getObjects(AdbcConnection.GetObjectsDepth.TABLES,
                    emptyToNull(namespace.getRemoteCatalog()), emptyToNull(namespace.getRemoteDbSchema()),
                    null, TABLE_TYPES, null)) {
                return AdbcObjectsReader.readTableNames(reader, namespace);
            }
        });
    }

    private Schema arrowSchemaOf(ConnectorSession session, AdbcTableHandle handle) {
        return AdbcStatementScope.sharedTableSchema(session, handle, () -> fetchArrowSchema(handle));
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
