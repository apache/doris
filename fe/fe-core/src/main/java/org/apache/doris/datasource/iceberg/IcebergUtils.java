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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.Lists;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Unbound;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Iceberg utils
 */
public class IcebergUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergUtils.class);
    private static ThreadLocal<Integer> columnIdThreadLocal = new ThreadLocal<Integer>() {
        @Override
        public Integer initialValue() {
            return 0;
        }
    };
    // https://iceberg.apache.org/spec/#schemas-and-data-types
    // All time and timestamp values are stored with microsecond precision
    private static final int ICEBERG_DATETIME_SCALE_MS = 6;
    private static final String PARQUET_NAME = "parquet";
    private static final String ORC_NAME = "orc";

    public static final String TOTAL_RECORDS = "total-records";
    public static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    public static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    // nickname in flink and spark
    public static final String WRITE_FORMAT = "write-format";
    public static final String COMPRESSION_CODEC = "compression-codec";

    // nickname in spark
    public static final String SPARK_SQL_COMPRESSION_CODEC = "spark.sql.iceberg.compression-codec";

    public static Expression convertToIcebergExpr(Expr expr, Schema schema) {
        if (expr == null) {
            return null;
        }

        Expression expression = null;
        // BoolLiteral
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            boolean value = boolLiteral.getValue();
            if (value) {
                expression = Expressions.alwaysTrue();
            } else {
                expression = Expressions.alwaysFalse();
            }
        } else if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1), schema);
                    if (left != null && right != null) {
                        expression = Expressions.and(left, right);
                    } else if (left != null) {
                        return left;
                    } else if (right != null) {
                        return right;
                    }
                    break;
                }
                case OR: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1), schema);
                    if (left != null && right != null) {
                        expression = Expressions.or(left, right);
                    }
                    break;
                }
                case NOT: {
                    Expression child = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    if (child != null) {
                        expression = Expressions.not(child);
                    }
                    break;
                }
                default:
                    return null;
            }
        } else if (expr instanceof BinaryPredicate) {
            TExprOpcode opCode = expr.getOpcode();
            switch (opCode) {
                case EQ:
                case NE:
                case GE:
                case GT:
                case LE:
                case LT:
                case EQ_FOR_NULL:
                    BinaryPredicate eq = (BinaryPredicate) expr;
                    SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                    LiteralExpr literalExpr = null;
                    if (slotRef == null && eq.getChild(0).isLiteral()) {
                        literalExpr = (LiteralExpr) eq.getChild(0);
                        slotRef = convertDorisExprToSlotRef(eq.getChild(1));
                    } else if (eq.getChild(1).isLiteral()) {
                        literalExpr = (LiteralExpr) eq.getChild(1);
                    }
                    if (slotRef == null || literalExpr == null) {
                        return null;
                    }
                    String colName = slotRef.getColumnName();
                    Types.NestedField nestedField = schema.caseInsensitiveFindField(colName);
                    colName = nestedField.name();
                    Object value = extractDorisLiteral(nestedField.type(), literalExpr);
                    if (value == null) {
                        if (opCode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                            expression = Expressions.isNull(colName);
                        } else {
                            return null;
                        }
                    } else {
                        switch (opCode) {
                            case EQ:
                            case EQ_FOR_NULL:
                                expression = Expressions.equal(colName, value);
                                break;
                            case NE:
                                expression = Expressions.not(Expressions.equal(colName, value));
                                break;
                            case GE:
                                expression = Expressions.greaterThanOrEqual(colName, value);
                                break;
                            case GT:
                                expression = Expressions.greaterThan(colName, value);
                                break;
                            case LE:
                                expression = Expressions.lessThanOrEqual(colName, value);
                                break;
                            case LT:
                                expression = Expressions.lessThan(colName, value);
                                break;
                            default:
                                return null;
                        }
                    }
                    break;
                default:
                    return null;
            }
        } else if (expr instanceof InPredicate) {
            // InPredicate, only support a in (1,2,3)
            InPredicate inExpr = (InPredicate) expr;
            if (inExpr.contains(Subquery.class)) {
                return null;
            }
            SlotRef slotRef = convertDorisExprToSlotRef(inExpr.getChild(0));
            if (slotRef == null) {
                return null;
            }
            String colName = slotRef.getColumnName();
            Types.NestedField nestedField = schema.caseInsensitiveFindField(colName);
            colName = nestedField.name();
            List<Object> valueList = new ArrayList<>();
            for (int i = 1; i < inExpr.getChildren().size(); ++i) {
                if (!(inExpr.getChild(i) instanceof LiteralExpr)) {
                    return null;
                }
                LiteralExpr literalExpr = (LiteralExpr) inExpr.getChild(i);
                Object value = extractDorisLiteral(nestedField.type(), literalExpr);
                if (value == null) {
                    return null;
                }
                valueList.add(value);
            }
            if (inExpr.isNotIn()) {
                // not in
                expression = Expressions.notIn(colName, valueList);
            } else {
                // in
                expression = Expressions.in(colName, valueList);
            }
        }

        return checkConversion(expression, schema);
    }

    private static Expression checkConversion(Expression expression, Schema schema) {
        if (expression == null) {
            return null;
        }
        switch (expression.op()) {
            case AND: {
                And andExpr = (And) expression;
                Expression left = checkConversion(andExpr.left(), schema);
                Expression right = checkConversion(andExpr.right(), schema);
                if (left != null && right != null) {
                    return andExpr;
                } else if (left != null) {
                    return left;
                } else if (right != null) {
                    return right;
                } else {
                    return null;
                }
            }
            case OR: {
                Or orExpr = (Or) expression;
                Expression left = checkConversion(orExpr.left(), schema);
                Expression right = checkConversion(orExpr.right(), schema);
                if (left == null || right == null) {
                    return null;
                } else {
                    return orExpr;
                }
            }
            case NOT: {
                Not notExpr = (Not) expression;
                Expression child = checkConversion(notExpr.child(), schema);
                if (child == null) {
                    return null;
                } else {
                    return notExpr;
                }
            }
            case TRUE:
            case FALSE:
                return expression;
            default:
                if (!(expression instanceof Unbound)) {
                    return null;
                }
                try {
                    ((Unbound<?, ?>) expression).bind(schema.asStruct(), true);
                    return expression;
                } catch (Exception e) {
                    LOG.debug("Failed to check expression: {}", e.getMessage());
                    return null;
                }
        }
    }

    public static Object extractDorisLiteral(org.apache.iceberg.types.Type icebergType, Expr expr) {
        TypeID icebergTypeID = icebergType.typeId();
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            switch (icebergTypeID) {
                case BOOLEAN:
                    return boolLiteral.getValue();
                case STRING:
                    return boolLiteral.getStringValue();
                default:
                    return null;
            }
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            switch (icebergTypeID) {
                case STRING:
                case DATE:
                    return dateLiteral.getStringValue();
                case TIMESTAMP:
                    if (((Types.TimestampType) icebergType).shouldAdjustToUTC()) {
                        return dateLiteral.getUnixTimestampWithMicroseconds(TimeUtils.getTimeZone());
                    } else {
                        return dateLiteral.getUnixTimestampWithMicroseconds(TimeUtils.getUTCTimeZone());
                    }
                default:
                    return null;
            }
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            switch (icebergTypeID) {
                case DECIMAL:
                    return decimalLiteral.getValue();
                case STRING:
                    return decimalLiteral.getStringValue();
                case DOUBLE:
                    return decimalLiteral.getDoubleValue();
                default:
                    return null;
            }
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            if (floatLiteral.getType() == Type.FLOAT) {
                switch (icebergTypeID) {
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                        return floatLiteral.getValue();
                    default:
                        return null;
                }
            } else {
                switch (icebergTypeID) {
                    case DOUBLE:
                    case DECIMAL:
                        return floatLiteral.getValue();
                    default:
                        return null;
                }
            }
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            Type type = intLiteral.getType();
            if (type.isInteger32Type()) {
                switch (icebergTypeID) {
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case DECIMAL:
                        return (int) intLiteral.getValue();
                    default:
                        return null;
                }
            } else {
                // only PrimitiveType.BIGINT
                switch (icebergTypeID) {
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case TIME:
                    case TIMESTAMP:
                    case DATE:
                    case DECIMAL:
                        return intLiteral.getValue();
                    default:
                        return null;
                }
            }
        } else if (expr instanceof StringLiteral) {
            String value = expr.getStringValue();
            switch (icebergTypeID) {
                case DATE:
                case TIME:
                case TIMESTAMP:
                case STRING:
                case UUID:
                case DECIMAL:
                    return value;
                case INTEGER:
                    try {
                        return Integer.parseInt(value);
                    } catch (Exception e) {
                        return null;
                    }
                case LONG:
                    try {
                        return Long.parseLong(value);
                    } catch (Exception e) {
                        return null;
                    }
                default:
                    return null;
            }
        }
        return null;
    }

    private static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    public static PartitionSpec solveIcebergPartitionSpec(PartitionDesc partitionDesc, Schema schema)
            throws UserException {
        if (partitionDesc == null) {
            return PartitionSpec.unpartitioned();
        }

        ArrayList<Expr> partitionExprs = partitionDesc.getPartitionExprs();
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (Expr expr : partitionExprs) {
            if (expr instanceof SlotRef) {
                builder.identity(((SlotRef) expr).getColumnName());
            } else if (expr instanceof FunctionCallExpr) {
                String exprName = expr.getExprName();
                List<Expr> params = ((FunctionCallExpr) expr).getParams().exprs();
                switch (exprName.toLowerCase()) {
                    case "bucket":
                        builder.bucket(params.get(1).getExprName(), Integer.parseInt(params.get(0).getStringValue()));
                        break;
                    case "year":
                    case "years":
                        builder.year(params.get(0).getExprName());
                        break;
                    case "month":
                    case "months":
                        builder.month(params.get(0).getExprName());
                        break;
                    case "date":
                    case "day":
                    case "days":
                        builder.day(params.get(0).getExprName());
                        break;
                    case "date_hour":
                    case "hour":
                    case "hours":
                        builder.hour(params.get(0).getExprName());
                        break;
                    case "truncate":
                        builder.truncate(params.get(1).getExprName(), Integer.parseInt(params.get(0).getStringValue()));
                        break;
                    default:
                        throw new UserException("unsupported partition for " + exprName);
                }
            }
        }
        return builder.build();
    }

    private static Type icebergPrimitiveTypeToDorisType(org.apache.iceberg.types.Type.PrimitiveType primitive) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case LONG:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
            case BINARY:
            case UUID:
                return Type.STRING;
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) primitive;
                return ScalarType.createCharType(fixed.length());
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return ScalarType.createDecimalV3Type(decimal.precision(), decimal.scale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP:
                return ScalarType.createDatetimeV2Type(ICEBERG_DATETIME_SCALE_MS);
            case TIME:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + primitive);
        }
    }

    public static Type icebergTypeToDorisType(org.apache.iceberg.types.Type type) {
        if (type.isPrimitiveType()) {
            return icebergPrimitiveTypeToDorisType((org.apache.iceberg.types.Type.PrimitiveType) type);
        }
        switch (type.typeId()) {
            case LIST:
                Types.ListType list = (Types.ListType) type;
                return ArrayType.create(icebergTypeToDorisType(list.elementType()), true);
            case MAP:
                Types.MapType map = (Types.MapType) type;
                return new MapType(
                        icebergTypeToDorisType(map.keyType()),
                        icebergTypeToDorisType(map.valueType())
                );
            case STRUCT:
                Types.StructType struct = (Types.StructType) type;
                ArrayList<StructField> nestedTypes = struct.fields().stream().map(
                        x -> new StructField(x.name(), icebergTypeToDorisType(x.type()))
                ).collect(Collectors.toCollection(ArrayList::new));
                return new StructType(nestedTypes);
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }


    public static org.apache.iceberg.Table getIcebergTable(ExternalCatalog catalog, String dbName, String tblName) {
        return getIcebergTableInternal(catalog, dbName, tblName, false);
    }

    public static org.apache.iceberg.Table getAndCloneTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
        return getIcebergTableInternal(catalog, tableInfo.getDbName(), tableInfo.getTbName(), true);
    }

    public static org.apache.iceberg.Table getRemoteTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getRemoteTable(catalog, tableInfo.getDbName(), tableInfo.getTbName());
    }

    private static org.apache.iceberg.Table getIcebergTableInternal(ExternalCatalog catalog, String dbName,
            String tblName,
            boolean isClone) {
        IcebergMetadataCache metadataCache = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache();
        return isClone ? metadataCache.getAndCloneTable(catalog, dbName, tblName)
                : metadataCache.getIcebergTable(catalog, dbName, tblName);
    }

    /**
     * Get iceberg schema from catalog and convert them to doris schema
     */
    public static List<Column> getSchema(ExternalCatalog catalog, String dbName, String name) {
        return HiveMetaStoreClientHelper.ugiDoAs(catalog.getConfiguration(), () -> {
            org.apache.iceberg.Table icebergTable = getIcebergTable(catalog, dbName, name);
            Schema schema = icebergTable.schema();
            List<Types.NestedField> columns = schema.columns();
            List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
            for (Types.NestedField field : columns) {
                tmpSchema.add(new Column(field.name().toLowerCase(Locale.ROOT),
                        IcebergUtils.icebergTypeToDorisType(field.type()), true, null, true, field.doc(), true,
                        schema.caseInsensitiveFindField(field.name()).fieldId()));
            }
            return tmpSchema;
        });
    }


    /**
     * Estimate iceberg table row count.
     * Get the row count by adding all task file recordCount.
     *
     * @return estimated row count
     */
    public static long getIcebergRowCount(ExternalCatalog catalog, String dbName, String tbName) {
        // the table may be null when the iceberg metadata cache is not loaded.But I don't think it's a problem,
        // because the NPE would be caught in the caller and return the default value -1.
        // Meanwhile, it will trigger iceberg metadata cache to load the table, so we can get it next time.
        Table icebergTable = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(catalog, dbName, tbName);
        Snapshot snapshot = icebergTable.currentSnapshot();
        if (snapshot == null) {
            // empty table
            return 0;
        }
        Map<String, String> summary = snapshot.summary();
        return Long.parseLong(summary.get(TOTAL_RECORDS)) - Long.parseLong(summary.get(TOTAL_POSITION_DELETES));
    }


    public static FileFormat getFileFormat(Table icebergTable) {
        Map<String, String> properties = icebergTable.properties();
        String fileFormatName;
        if (properties.containsKey(WRITE_FORMAT)) {
            fileFormatName = properties.get(WRITE_FORMAT);
        } else {
            fileFormatName = properties.getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, PARQUET_NAME);
        }
        FileFormat fileFormat;
        if (fileFormatName.toLowerCase().contains(ORC_NAME)) {
            fileFormat = FileFormat.ORC;
        } else if (fileFormatName.toLowerCase().contains(PARQUET_NAME)) {
            fileFormat = FileFormat.PARQUET;
        } else {
            throw new RuntimeException("Unsupported input format type: " + fileFormatName);
        }
        return fileFormat;
    }


    public static String getFileCompress(Table table) {
        Map<String, String> properties = table.properties();
        if (properties.containsKey(COMPRESSION_CODEC)) {
            return properties.get(COMPRESSION_CODEC);
        } else if (properties.containsKey(SPARK_SQL_COMPRESSION_CODEC)) {
            return properties.get(SPARK_SQL_COMPRESSION_CODEC);
        }
        FileFormat fileFormat = getFileFormat(table);
        if (fileFormat == FileFormat.PARQUET) {
            return properties.getOrDefault(
                    TableProperties.PARQUET_COMPRESSION, TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        } else if (fileFormat == FileFormat.ORC) {
            return properties.getOrDefault(
                    TableProperties.ORC_COMPRESSION, TableProperties.ORC_COMPRESSION_DEFAULT);
        }
        throw new NotSupportedException("Unsupported file format: " + fileFormat);
    }

    public static String dataLocation(Table table) {
        Map<String, String> properties = table.properties();
        if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new NotSupportedException(
                    "Table " + table.name() + " specifies " + properties
                            .get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)
                            + " as a location provider. "
                            + "Writing to Iceberg tables with custom location provider is not supported.");
        }
        String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
            if (dataLocation == null) {
                dataLocation = String.format("%s/data", LocationUtil.stripTrailingSlash(table.location()));
            }
        }
        return dataLocation;
    }
}
