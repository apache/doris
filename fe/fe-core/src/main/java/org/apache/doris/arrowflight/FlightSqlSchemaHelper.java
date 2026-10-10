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

package org.apache.doris.arrowflight;

import org.apache.doris.arrow.DorisArrowTypeMapping;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TDescribeTablesResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TTableStatus;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlightSqlSchemaHelper {
    private static final Logger LOG = LogManager.getLogger(FlightSqlSchemaHelper.class);
    private final ConnectContext ctx;
    private final FrontendServiceImpl impl;
    private boolean includeSchema;
    private String catalogFilterPattern = null;
    private String dbSchemaFilterPattern = null;
    private String tableNameFilterPattern = null;
    private List<String> tableTypesList = null;

    public FlightSqlSchemaHelper(ConnectContext context) {
        ctx = context;
        impl = new FrontendServiceImpl(ExecuteEnv.getInstance());
    }

    private static final byte[] EMPTY_SERIALIZED_SCHEMA = getSerializedSchema(Collections.emptyList());

    static Field withDorisTypeMetadata(Field field, Type type) {
        if (type.isVariantType()) {
            if (field.getMetadata() == null
                    || !"arrow.parquet.variant".equals(field.getMetadata().get("ARROW:extension:name"))) {
                throw CallStatus.UNIMPLEMENTED.withDescription(
                        "Backend returned a non-native Variant schema; use Variant V2 "
                                + "or cast the result to STRING").toRuntimeException();
            }
        }
        List<Field> children = new ArrayList<>(field.getChildren());
        if (type.isArrayType()) {
            children.set(0, withDorisTypeMetadata(children.get(0), ((ArrayType) type).getItemType()));
        } else if (type.isMapType()) {
            Field entries = children.get(0);
            List<Field> pair = new ArrayList<>(entries.getChildren());
            pair.set(0, withDorisTypeMetadata(pair.get(0), ((MapType) type).getKeyType()));
            pair.set(1, withDorisTypeMetadata(pair.get(1), ((MapType) type).getValueType()));
            children.set(0, new Field(entries.getName(), entries.getFieldType(), pair));
        } else if (type.isStructType()) {
            StructType struct = (StructType) type;
            for (int i = 0; i < children.size(); i++) {
                children.set(i, withDorisTypeMetadata(children.get(i), struct.getFields().get(i).getType()));
            }
        }
        String marker = null;
        switch (type.getPrimitiveType()) {
            case LARGEINT:
            case IPV4:
            case IPV6:
            case UUID:
            case VARIANT:
                marker = type.getPrimitiveType().name();
                break;
            case JSONB:
                marker = "JSON";
                break;
            default:
                break;
        }
        FieldType fieldType = field.getFieldType();
        if (marker != null && !field.getMetadata().containsKey("doris_type")) {
            Map<String, String> metadata = new HashMap<>(field.getMetadata());
            metadata.put("doris_type", marker);
            fieldType = new FieldType(field.isNullable(), field.getType(), field.getDictionary(), metadata);
        }
        // Old BEs omit these markers. Fill only missing ones from the planned Doris type;
        // preserve conflicting markers and all physical properties for the strict schema comparison.
        return new Field(field.getName(), fieldType, children);
    }

    protected static byte[] getSerializedSchema(List<Field> fields) {
        if (EMPTY_SERIALIZED_SCHEMA == null && fields == null) {
            fields = Collections.emptyList();
        } else if (fields == null) {
            return Arrays.copyOf(EMPTY_SERIALIZED_SCHEMA, EMPTY_SERIALIZED_SCHEMA.length);
        }

        final ByteArrayOutputStream columnOutputStream = new ByteArrayOutputStream();
        final Schema schema = new Schema(fields);

        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(columnOutputStream)), schema);
        } catch (final IOException e) {
            throw new RuntimeException("IO Error when serializing schema '" + schema + "'.", e);
        }

        return columnOutputStream.toByteArray();
    }

    /**
     * Set in the Tables request object the parameter that user passed via CommandGetTables.
     */
    public void setParameterForGetTables(CommandGetTables command) {
        includeSchema = command.getIncludeSchema();
        catalogFilterPattern = command.hasCatalog() ? command.getCatalog() : "internal";
        dbSchemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        tableNameFilterPattern = command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;
        tableTypesList = command.getTableTypesList().isEmpty() ? null : command.getTableTypesList();
    }

    /**
     * Set in the Schemas request object the parameter that user passed via CommandGetDbSchemas.
     */
    public void setParameterForGetDbSchemas(CommandGetDbSchemas command) {
        catalogFilterPattern = command.hasCatalog() ? command.getCatalog() : "internal";
        dbSchemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
    }

    /**
     * Call FrontendServiceImpl->getDbNames.
     */
    private TGetDbsResult getDbNames() throws TException {
        TGetDbsParams getDbsParams = new TGetDbsParams();
        if (catalogFilterPattern != null) {
            getDbsParams.setCatalog(catalogFilterPattern);
        }
        if (dbSchemaFilterPattern != null) {
            getDbsParams.setPattern(dbSchemaFilterPattern);
        }
        getDbsParams.setCurrentUserIdent(ctx.getCurrentUserIdentity().toThrift());
        return impl.getDbNames(getDbsParams);
    }

    /**
     * Call FrontendServiceImpl->listTableStatus.
     */
    private TListTableStatusResult listTableStatus(String dbName, String catalogName) throws TException {
        TGetTablesParams getTablesParams = new TGetTablesParams();
        getTablesParams.setDb(dbName);
        if (!catalogName.isEmpty()) {
            getTablesParams.setCatalog(catalogName);
        }
        if (tableNameFilterPattern != null) {
            getTablesParams.setPattern(tableNameFilterPattern);
        }
        if (tableTypesList != null) {
            getTablesParams.setType(tableTypesList.get(0)); // currently only one type is supported.
        }
        getTablesParams.setCurrentUserIdent(ctx.getCurrentUserIdentity().toThrift());
        return impl.listTableStatus(getTablesParams);
    }

    /**
     * Call FrontendServiceImpl->describeTables.
     */
    private TDescribeTablesResult describeTables(String dbName, String catalogName, List<String> tablesName)
            throws TException {
        TDescribeTablesParams describeTablesParams = new TDescribeTablesParams();
        describeTablesParams.setDb(dbName);
        if (!catalogName.isEmpty()) {
            describeTablesParams.setCatalog(catalogName);
        }
        describeTablesParams.setTablesName(tablesName);
        describeTablesParams.setCurrentUserIdent(ctx.getCurrentUserIdentity().toThrift());
        return impl.describeTables(describeTablesParams);
    }

    /**
     * Construct <tableName, List<ArrowType>>
     */
    private Map<String, List<Field>> buildTableToFields(String dbName, TDescribeTablesResult describeTablesResult,
            List<String> tablesName) {
        Map<String, List<Field>> tableToFields = new HashMap<>();
        int columnIndex = 0;
        for (int tableIndex = 0; tableIndex < describeTablesResult.getTablesOffsetSize(); tableIndex++) {
            String tableName = tablesName.get(tableIndex);
            final List<Field> fields = new ArrayList<>();
            Integer tableOffset = describeTablesResult.getTablesOffset().get(tableIndex);
            for (; columnIndex < tableOffset; columnIndex++) {
                TColumnDef columnDef = describeTablesResult.getColumns().get(columnIndex);
                fields.add(buildField(dbName, tableName, columnDef.getColumnDesc()));
            }
            tableToFields.put(tableName, fields);
        }
        return tableToFields;
    }

    private static Field buildField(String dbName, String tableName, TColumnDesc desc) {
        return withNativeVariant(DorisArrowTypeMapping.toField(dbName, tableName, desc), desc);
    }

    private static Field withNativeVariant(Field field, TColumnDesc desc) {
        if (desc.getColumnType() == TPrimitiveType.VARIANT) {
            return nativeVariantField(field.getName(), field.isNullable(), field.getMetadata());
        }
        if (!desc.isSetChildren() || desc.getChildren().isEmpty()) {
            return field;
        }
        // Preserve the shared mapper's physical types, nullability and column metadata.
        // Only Variant leaves need the Flight-specific native storage representation.
        List<Field> children = new ArrayList<>(field.getChildren());
        if (desc.getColumnType() == TPrimitiveType.MAP && desc.getChildrenSize() == 2) {
            Field entries = children.get(0);
            List<Field> pair = new ArrayList<>(entries.getChildren());
            for (int i = 0; i < pair.size(); i++) {
                pair.set(i, withNativeVariant(pair.get(i), desc.getChildren().get(i)));
            }
            children.set(0, new Field(entries.getName(), entries.getFieldType(), pair));
        } else if (desc.getColumnType() == TPrimitiveType.STRUCT
                || (desc.getColumnType() == TPrimitiveType.ARRAY && desc.getChildrenSize() == 1)) {
            for (int i = 0; i < children.size(); i++) {
                children.set(i, withNativeVariant(children.get(i), desc.getChildren().get(i)));
            }
        }
        return new Field(field.getName(), field.getFieldType(), children);
    }

    static Field nativeVariantField(String name, boolean nullable, Map<String, String> columnMetadata) {
        Map<String, String> metadata = new HashMap<>(columnMetadata);
        // Discovery and execution must share the extension metadata as well as its storage type.
        metadata.put("ARROW:extension:name", "arrow.parquet.variant");
        metadata.put("ARROW:extension:metadata", "");
        return new Field(name, new FieldType(nullable, new ArrowType.Struct(), null, metadata),
                Arrays.asList(Field.notNullable("metadata", new ArrowType.Binary()),
                        Field.notNullable("value", new ArrowType.Binary())));
    }

    /**
     * for FlightSqlProducer Schemas.GET_CATALOGS_SCHEMA
     */
    public void getCatalogs(VectorSchemaRoot vectorSchemaRoot) throws TException {
        VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");

        Set<String> catalogsSet = new LinkedHashSet<>();
        catalogsSet.add("internal"); // An ordered Set with "internal" first.
        for (CatalogIf catalog : Env.getCurrentEnv().getCatalogMgr().listCatalogs()) {
            catalogsSet.add(catalog.getName());
        }

        int catalogIndex = 0;
        for (String catalog : catalogsSet) {
            catalogNameVector.setSafe(catalogIndex, new Text(catalog));
            catalogIndex++;
        }
        vectorSchemaRoot.setRowCount(catalogIndex);
    }

    /**
     * for FlightSqlProducer Schemas.GET_SCHEMAS_SCHEMA
     */
    public void getSchemas(VectorSchemaRoot vectorSchemaRoot) throws TException {
        VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
        VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("db_schema_name");

        TGetDbsResult getDbsResult = getDbNames();
        for (int dbIndex = 0; dbIndex < getDbsResult.getDbs().size(); dbIndex++) {
            String dbName = getDbsResult.getDbs().get(dbIndex);
            String catalogName = getDbsResult.isSetCatalogs() ? getDbsResult.getCatalogs().get(dbIndex) : "";
            catalogNameVector.setSafe(dbIndex, new Text(catalogName));
            schemaNameVector.setSafe(dbIndex, new Text(dbName));
        }
        vectorSchemaRoot.setRowCount(getDbsResult.getDbs().size());
    }

    /**
     * for FlightSqlProducer Schemas.GET_TABLES_SCHEMA_NO_SCHEMA and Schemas.GET_TABLES_SCHEMA
     */
    public void getTables(VectorSchemaRoot vectorSchemaRoot) throws TException {
        VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");
        VarCharVector schemaNameVector = (VarCharVector) vectorSchemaRoot.getVector("db_schema_name");
        VarCharVector tableNameVector = (VarCharVector) vectorSchemaRoot.getVector("table_name");
        VarCharVector tableTypeVector = (VarCharVector) vectorSchemaRoot.getVector("table_type");
        VarBinaryVector schemaVector = (VarBinaryVector) vectorSchemaRoot.getVector("table_schema");

        int tablesCount = 0;
        TGetDbsResult getDbsResult = getDbNames();
        for (int dbIndex = 0; dbIndex < getDbsResult.getDbs().size(); dbIndex++) {
            String dbName = getDbsResult.getDbs().get(dbIndex);
            String catalogName = getDbsResult.isSetCatalogs() ? getDbsResult.getCatalogs().get(dbIndex) : "";
            TListTableStatusResult listTableStatusResult = listTableStatus(dbName, catalogName);

            Map<String, List<Field>> tableToFields;
            if (includeSchema) {
                List<String> tablesName = new ArrayList<>();
                for (TTableStatus tableStatus : listTableStatusResult.getTables()) {
                    tablesName.add(tableStatus.getName());
                }
                TDescribeTablesResult describeTablesResult = describeTables(dbName, catalogName, tablesName);
                tableToFields = buildTableToFields(dbName, describeTablesResult, tablesName);
            } else {
                tableToFields = null;
            }

            for (TTableStatus tableStatus : listTableStatusResult.getTables()) {
                catalogNameVector.setSafe(tablesCount, new Text(catalogName));
                schemaNameVector.setSafe(tablesCount, new Text(dbName));
                tableNameVector.setSafe(tablesCount, new Text(tableStatus.getName()));
                tableTypeVector.setSafe(tablesCount, new Text(tableStatus.getType()));
                if (includeSchema) {
                    List<Field> fields = tableToFields.get(tableStatus.getName());
                    schemaVector.setSafe(tablesCount, getSerializedSchema(fields));
                }
                tablesCount++;
            }
        }
        vectorSchemaRoot.setRowCount(tablesCount);
    }
}
