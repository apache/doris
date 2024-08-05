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

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;
import org.apache.doris.thrift.TTrinoConnectorTable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class TrinoConnectorExternalTable extends ExternalTable {

    public TrinoConnectorExternalTable(long id, String name, String dbName, TrinoConnectorExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.TRINO_CONNECTOR_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        // 1. Get necessary objects
        TrinoConnectorExternalCatalog trinoConnectorCatalog = (TrinoConnectorExternalCatalog) catalog;
        CatalogHandle catalogHandle = trinoConnectorCatalog.getTrinoCatalogHandle();
        Connector connector = trinoConnectorCatalog.getConnector();
        Session trinoSession = trinoConnectorCatalog.getTrinoSession();
        ConnectorSession connectorSession = trinoSession.toConnectorSession(catalogHandle);

        // 2. Begin transaction and get ConnectorMetadata
        ConnectorTransactionHandle connectorTransactionHandle = connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata connectorMetadata = connector.getMetadata(connectorSession, connectorTransactionHandle);

        // 3. Get ConnectorTableHandle
        Optional<ConnectorTableHandle> connectorTableHandle = Optional.empty();
        QualifiedObjectName qualifiedTable = new QualifiedObjectName(trinoConnectorCatalog.getName(), dbName,
                name);
        if (!qualifiedTable.getCatalogName().isEmpty()
                && !qualifiedTable.getSchemaName().isEmpty()
                && !qualifiedTable.getObjectName().isEmpty()) {
            connectorTableHandle = Optional.ofNullable(connectorMetadata.getTableHandle(connectorSession,
                    qualifiedTable.asSchemaTableName(), Optional.empty(), Optional.empty()));
        }
        if (!connectorTableHandle.isPresent()) {
            throw new RuntimeException(String.format("Table does not exist: %s.%s.%s", qualifiedTable));
        }

        // 4. Get ColumnHandle
        Map<String, ColumnHandle> handles = connectorMetadata.getColumnHandles(connectorSession,
                connectorTableHandle.get());
        ImmutableMap.Builder<String, ColumnHandle> columnHandleMapBuilder = ImmutableMap.builder();
        for (Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            columnHandleMapBuilder.put(mapEntry.getKey().toLowerCase(Locale.ENGLISH), mapEntry.getValue());
        }
        Map<String, ColumnHandle> columnHandleMap = columnHandleMapBuilder.buildOrThrow();

        // 5. Get ColumnMetadata
        ImmutableMap.Builder<String, ColumnMetadata> columnMetadataMapBuilder = ImmutableMap.builder();
        List<Column> columns = Lists.newArrayListWithCapacity(columnHandleMap.size());
        for (ColumnHandle columnHandle : columnHandleMap.values()) {
            ColumnMetadata columnMetadata = connectorMetadata.getColumnMetadata(connectorSession,
                    connectorTableHandle.get(), columnHandle);
            if (columnMetadata.isHidden()) {
                continue;
            }
            columnMetadataMapBuilder.put(columnMetadata.getName(), columnMetadata);

            Column column = new Column(columnMetadata.getName(),
                    trinoConnectorTypeToDorisType(columnMetadata.getType()),
                    true,
                    null,
                    true,
                    columnMetadata.getComment(),
                    !columnMetadata.isHidden(),
                    Column.COLUMN_UNIQUE_ID_INIT_VALUE);
            columns.add(column);
        }
        Map<String, ColumnMetadata> columnMetadataMap = columnMetadataMapBuilder.buildOrThrow();
        return Optional.of(
                new TrinoSchemaCacheValue(columns, connectorMetadata, connectorTableHandle, connectorTransactionHandle,
                        columnHandleMap, columnMetadataMap));
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TTrinoConnectorTable tTrinoConnectorTable = new TTrinoConnectorTable();
        tTrinoConnectorTable.setDbName(dbName);
        tTrinoConnectorTable.setTableName(name);
        tTrinoConnectorTable.setProperties(new HashMap<>());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(),
                TTableType.TRINO_CONNECTOR_TABLE, schema.size(), 0, getName(), dbName);
        tTableDescriptor.setTrinoConnectorTable(tTrinoConnectorTable);
        return tTableDescriptor;
    }

    private Type trinoConnectorTypeToDorisType(io.trino.spi.type.Type type) {
        if (type instanceof BooleanType) {
            return Type.BOOLEAN;
        } else if (type instanceof TinyintType) {
            return Type.TINYINT;
        } else if (type instanceof SmallintType) {
            return Type.SMALLINT;
        } else if (type instanceof IntegerType) {
            return Type.INT;
        } else if (type instanceof BigintType) {
            return Type.BIGINT;
        } else if (type instanceof RealType) {
            return Type.FLOAT;
        } else if (type instanceof DoubleType) {
            return Type.DOUBLE;
        } else if (type instanceof CharType) {
            return Type.CHAR;
        } else if (type instanceof VarcharType) {
            return Type.STRING;
            // } else if (type instanceof BinaryType) {
            //     return Type.STRING;
        } else if (type instanceof VarbinaryType) {
            return Type.STRING;
        } else if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
        } else if (type instanceof TimeType) {
            return Type.STRING;
        } else if (type instanceof DateType) {
            return ScalarType.createDateV2Type();
        } else if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            return ScalarType.createDatetimeV2Type(timestampType.getPrecision());
        } else if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
            return ScalarType.createDatetimeV2Type(timestampWithTimeZoneType.getPrecision());
        } else if (type instanceof io.trino.spi.type.ArrayType) {
            Type elementType = trinoConnectorTypeToDorisType(
                    ((io.trino.spi.type.ArrayType) type).getElementType());
            return ArrayType.create(elementType, true);
        } else if (type instanceof io.trino.spi.type.MapType) {
            Type keyType = trinoConnectorTypeToDorisType(
                    ((io.trino.spi.type.MapType) type).getKeyType());
            Type valueType = trinoConnectorTypeToDorisType(
                    ((io.trino.spi.type.MapType) type).getValueType());
            return new MapType(keyType, valueType, true, true);
        } else if (type instanceof RowType) {
            ArrayList<StructField> dorisFields = Lists.newArrayList();
            for (Field field : ((RowType) type).getFields()) {
                Type childType = trinoConnectorTypeToDorisType(field.getType());
                if (field.getName().isPresent()) {
                    dorisFields.add(new StructField(field.getName().get(), childType));
                } else {
                    dorisFields.add(new StructField(childType));
                }
            }
            return new StructType(dorisFields);
        } else {
            throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }

    public ConnectorTableHandle getConnectorTableHandle() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((TrinoSchemaCacheValue) value).getConnectorTableHandle().get())
                .orElse(null);
    }

    public ConnectorMetadata getConnectorMetadata() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((TrinoSchemaCacheValue) value).getConnectorMetadata()).orElse(null);
    }

    public ConnectorTransactionHandle getConnectorTransactionHandle() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((TrinoSchemaCacheValue) value).getConnectorTransactionHandle())
                .orElse(null);
    }

    public Map<String, ColumnHandle> getColumnHandleMap() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((TrinoSchemaCacheValue) value).getColumnHandleMap()).orElse(null);
    }

    public Map<String, ColumnMetadata> getColumnMetadataMap() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((TrinoSchemaCacheValue) value).getColumnMetadataMap()).orElse(null);
    }
}
