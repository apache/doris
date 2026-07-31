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

package org.apache.doris.service.arrowflight;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
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
import org.apache.doris.thrift.TTableStatus;

import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
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

    /**
     * Convert Doris data type to an arrowType.
     * <p>
     * Ref: `convert_to_arrow_type` in be/src/util/arrow/row_batch.cpp.
     * which is consistent with the type of Arrow data returned by Doris Arrow Flight Sql query.
     */
    private static ArrowType getArrowType(PrimitiveType primitiveType, Integer precision, Integer scale,
            String timeZone) {
        switch (primitiveType) {
            case BOOLEAN:
                return new ArrowType.Bool();
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INT:
            case IPV4:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case LARGEINT:
            case VARCHAR:
            case STRING:
            case CHAR:
            case DATETIME:
            case DATE:
            case JSONB:
            case IPV6:
            case VARIANT:
                return new ArrowType.Utf8();
            case DATEV2:
                // DAY, not MILLISECOND: BE writes a DATEV2 column as arrow::Date32Type (a day number),
                // so a MILLISECOND unit here describes the metadata as date64 while the data that
                // follows is date32. A client that trusts this schema -- one reading through the ADBC
                // Flight SQL driver does -- then types the column as a datetime and fails the read.
                return new ArrowType.Date(DateUnit.DAY);
            case DATETIMEV2:
                if (scale > 3) {
                    return new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZone);
                } else if (scale > 0) {
                    return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZone);
                } else {
                    return new ArrowType.Timestamp(TimeUnit.SECOND, timeZone);
                }
            case TIMESTAMP_NS:
                throw new IllegalArgumentException("TIMESTAMP_NS is not supported by Arrow Flight SQL");
            case TIMESTAMPTZ:
                if (scale > 3) {
                    return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
                } else if (scale > 0) {
                    return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
                } else {
                    return new ArrowType.Timestamp(TimeUnit.SECOND, "UTC");
                }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new ArrowType.Decimal(precision, scale, 128);
            case DECIMAL256:
                return new ArrowType.Decimal(precision, scale, 256);
            case DECIMALV2:
                return new ArrowType.Decimal(27, 9, 128);
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
                return new ArrowType.Binary();
            case MAP:
                return new ArrowType.Map(false);
            case ARRAY:
                return new ArrowType.List();
            case STRUCT:
                return new ArrowType.Struct();
            default:
                return new ArrowType.Null();
        }
    }

    private static ArrowType columnDescToArrowType(final TColumnDesc desc) {
        PrimitiveType primitiveType = PrimitiveType.fromThrift(desc.getColumnType());
        Integer precision = desc.isSetColumnPrecision() ? desc.getColumnPrecision() : null;
        Integer scale = desc.isSetColumnScale() ? desc.getColumnScale() : null;
        // TODO there is no timezone in TColumnDesc, so use current timezone.
        String timeZone = JdbcToArrowUtils.getUtcCalendar().getTimeZone().getID();
        return getArrowType(primitiveType, precision, scale, timeZone);
    }

    private static Map<String, String> createFlightSqlColumnMetadata(final String dbName, final String tableName,
            final TColumnDesc desc) {
        final FlightSqlColumnMetadata.Builder columnMetadataBuilder = new FlightSqlColumnMetadata.Builder().schemaName(
                        dbName).tableName(tableName).typeName(PrimitiveType.fromThrift(desc.getColumnType()).toString())
                .isAutoIncrement(false).isCaseSensitive(false).isReadOnly(true).isSearchable(true);

        if (desc.isSetColumnPrecision()) {
            columnMetadataBuilder.precision(desc.getColumnPrecision());
        }
        if (desc.isSetColumnScale()) {
            columnMetadataBuilder.scale(desc.getColumnScale());
        }
        return columnMetadataBuilder.build().getMetadataMap();
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

    /** One column, with its nested types described down to the leaves. */
    private static Field buildField(String dbName, String tableName, TColumnDesc desc) {
        ArrowType arrowType = columnDescToArrowType(desc);
        return new Field(desc.getColumnName(),
                new FieldType(desc.isIsAllowNull(), arrowType, null,
                        createFlightSqlColumnMetadata(dbName, tableName, desc)),
                arrowChildren(dbName, tableName, desc, arrowType));
    }

    /**
     * The Arrow children of a complex column, built from the descriptor's own children.
     *
     * <p>These are not decoration. An Arrow ARRAY/MAP/STRUCT type carries its element types in its
     * children and nowhere else, so a placeholder child says the column is an array OF NOTHING --
     * and BE emits the real element type in the data ({@code convert_to_arrow_type}: ListType(item),
     * MapType(key, value), StructType(fields)), which leaves the schema describing one thing and the
     * batch carrying another. A client that types its columns from this schema (one reading through
     * the ADBC Flight SQL driver does) then rejects the column outright.
     *
     * <p>{@code describeTables} already reports the tree -- {@code Column.createChildrenColumn} names
     * an array's element "item" and a map's pair "key"/"value", which is what Arrow calls them too.
     * When it reports none, the old placeholders are kept rather than an empty child list: a source
     * that cannot describe its nested types is no worse off than before.
     */
    private static List<Field> arrowChildren(String dbName, String tableName, TColumnDesc desc,
            ArrowType arrowType) {
        List<TColumnDesc> children = desc.isSetChildren() ? desc.getChildren() : Collections.emptyList();
        switch (arrowType.getTypeID()) {
            case List:
            case LargeList:
            case FixedSizeList:
                if (children.size() != 1) {
                    return Collections.singletonList(
                            Field.notNullable(BaseRepeatedValueVector.DATA_VECTOR_NAME,
                                    ZeroVector.INSTANCE.getField().getType()));
                }
                return Collections.singletonList(buildField(dbName, tableName, children.get(0)));
            case Map:
                // Arrow spells a map as list<entries: struct<key, value>>, with the entries struct and
                // the key both non-nullable -- the descriptor's key nullability is not carried over,
                // because an Arrow map with a nullable key is not a valid schema.
                if (children.size() != 2) {
                    return Collections.singletonList(
                            Field.notNullable(MapVector.DATA_VECTOR_NAME, new ArrowType.List()));
                }
                Field key = buildField(dbName, tableName, children.get(0));
                Field value = buildField(dbName, tableName, children.get(1));
                Field entries = new Field(MapVector.DATA_VECTOR_NAME,
                        new FieldType(false, new ArrowType.Struct(), null),
                        Arrays.asList(new Field(key.getName(),
                                        new FieldType(false, key.getType(), null), key.getChildren()),
                                value));
                return Collections.singletonList(entries);
            case Struct:
                if (children.isEmpty()) {
                    return Collections.emptyList();
                }
                List<Field> structFields = new ArrayList<>(children.size());
                for (TColumnDesc child : children) {
                    structFields.add(buildField(dbName, tableName, child));
                }
                return structFields;
            default:
                return null;
        }
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
