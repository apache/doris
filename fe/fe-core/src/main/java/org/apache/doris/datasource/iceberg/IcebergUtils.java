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
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.thrift.TExprOpcode;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.Unbound;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
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

    public static final long UNKNOWN_SNAPSHOT_ID = -1;  // means an empty table
    public static final long NEWEST_SCHEMA_ID = -1;

    public static final String YEAR = "year";
    public static final String MONTH = "month";
    public static final String DAY = "day";
    public static final String HOUR = "hour";
    public static final String IDENTITY = "identity";
    public static final int PARTITION_DATA_ID_START = 1000; // org.apache.iceberg.PartitionSpec

    private static final Pattern SNAPSHOT_ID = Pattern.compile("\\d+");

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

    public static Table getIcebergTable(ExternalTable dorisTable) {
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache().getIcebergTable(dorisTable);
    }

    public static org.apache.iceberg.types.Type dorisTypeToIcebergType(Type type) {
        DorisTypeToIcebergType visitor = type.isStructType() ? new DorisTypeToIcebergType((StructType) type)
                : new DorisTypeToIcebergType();
        return DorisTypeToIcebergType.visit(type, visitor);
    }

    public static Literal<?> parseIcebergLiteral(String value, org.apache.iceberg.types.Type type) {
        if (value == null) {
            return null;
        }
        switch (type.typeId()) {
            case BOOLEAN:
                try {
                    return Literal.of(Boolean.parseBoolean(value));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid Boolean string: " + value, e);
                }
            case INTEGER:
            case DATE:
                try {
                    return Literal.of(Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Int string: " + value, e);
                }
            case LONG:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
                try {
                    return Literal.of(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Long string: " + value, e);
                }
            case FLOAT:
                try {
                    return Literal.of(Float.parseFloat(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Float string: " + value, e);
                }
            case DOUBLE:
                try {
                    return Literal.of(Double.parseDouble(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Double string: " + value, e);
                }
            case STRING:
                return Literal.of(value);
            case UUID:
                try {
                    return Literal.of(UUID.fromString(value));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid UUID string: " + value, e);
                }
            case FIXED:
            case BINARY:
            case GEOMETRY:
            case GEOGRAPHY:
                return Literal.of(ByteBuffer.wrap(value.getBytes()));
            case DECIMAL:
                try {
                    return Literal.of(new BigDecimal(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid Decimal string: " + value, e);
                }
            default:
                throw new IllegalArgumentException("Cannot parse unknown type: " + type);
        }
    }

    private static void updateIcebergColumnUniqueId(Column column, Types.NestedField icebergField) {
        column.setUniqueId(icebergField.fieldId());
        List<NestedField> icebergFields = Lists.newArrayList();
        switch (icebergField.type().typeId()) {
            case LIST:
                icebergFields = ((Types.ListType) icebergField.type()).fields();
                break;
            case MAP:
                icebergFields =  ((Types.MapType) icebergField.type()).fields();
                break;
            case STRUCT:
                icebergFields = ((Types.StructType) icebergField.type()).fields();
                break;
            default:
                return;
        }

        List<Column> childColumns = column.getChildren();
        for (int idx = 0; idx < childColumns.size(); idx++) {
            updateIcebergColumnUniqueId(childColumns.get(idx), icebergFields.get(idx));
        }
    }

    /**
     * Get iceberg schema from catalog and convert them to doris schema
     */
    private static List<Column> getSchema(ExternalTable dorisTable, long schemaId, boolean isView) {
        try {
            return dorisTable.getCatalog().getExecutionAuthenticator().execute(() -> {
                Schema schema;
                if (isView) {
                    View icebergView = getIcebergView(dorisTable);
                    if (schemaId == NEWEST_SCHEMA_ID) {
                        schema = icebergView.schema();
                    } else {
                        schema = icebergView.schemas().get((int) schemaId);
                    }
                } else {
                    Table icebergTable = getIcebergTable(dorisTable);
                    if (schemaId == NEWEST_SCHEMA_ID || icebergTable.currentSnapshot() == null) {
                        schema = icebergTable.schema();
                    } else {
                        schema = icebergTable.schemas().get((int) schemaId);
                    }
                }
                String type = isView ? "view" : "table";
                Preconditions.checkNotNull(schema,
                        "Schema for " + type + " " + dorisTable.getCatalog().getName()
                                + "." + dorisTable.getDbName() + "." + dorisTable.getName() + " is null");
                return parseSchema(schema);
            });
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * Parse iceberg schema to doris schema
     */
    public static List<Column> parseSchema(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        List<Column> resSchema = Lists.newArrayListWithCapacity(columns.size());
        for (Types.NestedField field : columns) {
            Column column =  new Column(field.name().toLowerCase(Locale.ROOT),
                            IcebergUtils.icebergTypeToDorisType(field.type()), true, null,
                            true, field.doc(), true, -1);
            updateIcebergColumnUniqueId(column, field);
            if (field.type().isPrimitiveType() && field.type().typeId() == TypeID.TIMESTAMP) {
                Types.TimestampType timestampType = (Types.TimestampType) field.type();
                if (timestampType.shouldAdjustToUTC()) {
                    column.setWithTZExtraInfo();
                }
            }
            resSchema.add(column);
        }
        return resSchema;
    }

    /**
     * Estimate iceberg table row count.
     * Get the row count by adding all task file recordCount.
     *
     * @return estimated row count
     */
    public static long getIcebergRowCount(ExternalTable tbl) {
        // the table may be null when the iceberg metadata cache is not loaded.But I don't think it's a problem,
        // because the NPE would be caught in the caller and return the default value -1.
        // Meanwhile, it will trigger iceberg metadata cache to load the table, so we can get it next time.
        Table icebergTable = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(tbl);
        Snapshot snapshot = icebergTable.currentSnapshot();
        if (snapshot == null) {
            LOG.info("Iceberg table {}.{}.{} is empty, return -1.",
                    tbl.getCatalog().getName(), tbl.getDbName(), tbl.getName());
            // empty table
            return TableIf.UNKNOWN_ROW_COUNT;
        }
        Map<String, String> summary = snapshot.summary();
        long rows = Long.parseLong(summary.get(TOTAL_RECORDS)) - Long.parseLong(summary.get(TOTAL_POSITION_DELETES));
        LOG.info("Iceberg table {}.{}.{} row count in summary is {}",
                tbl.getCatalog().getName(), tbl.getDbName(), tbl.getName(), rows);
        return rows;
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

    public static HiveCatalog createIcebergHiveCatalog(ExternalCatalog externalCatalog, String name) {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(externalCatalog.getConfiguration());

        Map<String, String> catalogProperties = externalCatalog.getProperties();
        if (!catalogProperties.containsKey(HiveCatalog.LIST_ALL_TABLES)) {
            // This configuration will display all tables (including non-Iceberg type tables),
            // which can save the time of obtaining table objects.
            // Later, type checks will be performed when loading the table.
            catalogProperties.put(HiveCatalog.LIST_ALL_TABLES, "true");
        }
        String metastoreUris = catalogProperties.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
        catalogProperties.put(CatalogProperties.URI, metastoreUris);
        hiveCatalog.initialize(name, catalogProperties);
        return hiveCatalog;
    }

    // Retrieve the manifest files that match the query based on partitions in filter
    public static CloseableIterable<ManifestFile> getMatchingManifest(
                List<ManifestFile> dataManifests,
                Map<Integer, PartitionSpec> specsById,
                Expression dataFilter) {
        LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder()
                .build(
                        specId -> {
                            PartitionSpec spec = specsById.get(specId);
                            return ManifestEvaluator.forPartitionFilter(
                                    Expressions.and(
                                            Expressions.alwaysTrue(),
                                            Projections.inclusive(spec, true).project(dataFilter)),
                                    spec,
                                    true);
                        });

        CloseableIterable<ManifestFile> matchingManifests = CloseableIterable.filter(
                CloseableIterable.withNoopClose(dataManifests),
                manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

        matchingManifests =
                CloseableIterable.filter(
                        matchingManifests,
                        manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());

        return matchingManifests;
    }

    // get snapshot id from query like 'for version/time as of' or '@branch/@tag'
    public static IcebergTableQueryInfo getQuerySpecSnapshot(
            Table table,
            Optional<TableSnapshot> queryTableSnapshot,
            Optional<TableScanParams> scanParams) throws UserException {

        Preconditions.checkArgument(
                queryTableSnapshot.isPresent() || isIcebergBranchOrTag(scanParams),
                "should spec version or time or branch or tag");

        // not support `select * from tb@branch/tag(b) for version/time as of ...`
        Preconditions.checkArgument(
                !(queryTableSnapshot.isPresent() && isIcebergBranchOrTag(scanParams)),
                "could not spec a version/time with tag/branch");

        // solve @branch/@tag
        if (scanParams.isPresent()) {
            String refName;
            TableScanParams params = scanParams.get();
            if (!params.getMapParams().isEmpty()) {
                refName = params.getMapParams().get("name");
            } else {
                refName = params.getListParams().get(0);
            }
            SnapshotRef snapshotRef = table.refs().get(refName);
            if (params.isBranch()) {
                if (snapshotRef == null || !snapshotRef.isBranch()) {
                    throw new UserException("Table " + table.name() + " does not have branch named " + refName);
                }
            } else {
                if (snapshotRef == null || !snapshotRef.isTag()) {
                    throw new UserException("Table " + table.name() + " does not have tag named " + refName);
                }
            }
            return new IcebergTableQueryInfo(
                snapshotRef.snapshotId(),
                refName,
                SnapshotUtil.schemaFor(table, refName).schemaId());
        }

        // solve version/time as of
        String value = queryTableSnapshot.get().getValue();
        TableSnapshot.VersionType type = queryTableSnapshot.get().getType();
        if (type == TableSnapshot.VersionType.VERSION) {
            if (SNAPSHOT_ID.matcher(value).matches()) {
                long snapshotId = Long.parseLong(value);
                Snapshot snapshot = table.snapshot(snapshotId);
                if (snapshot == null) {
                    throw new UserException("Table " + table.name() + " does not have snapshotId " + value);
                }
                return new IcebergTableQueryInfo(
                    snapshotId,
                    null,
                    snapshot.schemaId()
                );
            }

            if (!table.refs().containsKey(value)) {
                throw new UserException("Table " + table.name() + " does not have tag or branch named " + value);
            }
            return new IcebergTableQueryInfo(
                table.refs().get(value).snapshotId(),
                value,
                SnapshotUtil.schemaFor(table, value).schemaId()
            );
        } else {
            long timestamp = TimeUtils.timeStringToLong(value, TimeUtils.getTimeZone());
            if (timestamp < 0) {
                throw new DateTimeException("can't parse time: " + value);
            }
            long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, timestamp);
            return new IcebergTableQueryInfo(
                snapshotId,
                null,
                table.snapshot(snapshotId).schemaId()
                );
        }
    }

    public static boolean isIcebergBranchOrTag(Optional<TableScanParams> scanParams) {
        if (scanParams == null || !scanParams.isPresent()) {
            return false;
        }
        TableScanParams params = scanParams.get();
        if (params.isBranch() || params.isTag()) {
            if (!params.getMapParams().isEmpty()) {
                Preconditions.checkArgument(
                        params.getMapParams().containsKey("name"),
                        "must contain key 'name' in params"
                );
            } else {
                Preconditions.checkArgument(
                        params.getListParams().size() == 1
                                && params.getListParams().get(0) != null,
                        "must contain a branch/tag name in params"
                );
            }
            return true;
        }
        return false;
    }

    // read schema from external schema cache
    public static IcebergSchemaCacheValue getSchemaCacheValue(ExternalTable dorisTable, long schemaId) {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(dorisTable.getCatalog());
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(
                new IcebergSchemaCacheKey(dorisTable.getOrBuildNameMapping(), schemaId));
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to getSchema for: %s.%s.%s.%s",
                    null, dorisTable.getCatalog().getName(), dorisTable.getDbName(), dorisTable.getName(), schemaId);
        }
        return (IcebergSchemaCacheValue) schemaCacheValue.get();
    }

    public static IcebergSnapshot getLastedIcebergSnapshot(ExternalTable dorisTable) {
        Table table = IcebergUtils.getIcebergTable(dorisTable);
        Snapshot snapshot = table.currentSnapshot();
        long snapshotId = snapshot == null ? IcebergUtils.UNKNOWN_SNAPSHOT_ID : snapshot.snapshotId();
        return new IcebergSnapshot(snapshotId, table.schema().schemaId());
    }

    public static IcebergPartitionInfo loadPartitionInfo(ExternalTable dorisTable, long snapshotId)
            throws AnalysisException {
        // snapshotId == UNKNOWN_SNAPSHOT_ID means this is an empty table, haven't contained any snapshot yet.
        if (snapshotId == IcebergUtils.UNKNOWN_SNAPSHOT_ID) {
            return IcebergPartitionInfo.empty();
        }
        Table table = getIcebergTable(dorisTable);
        List<IcebergPartition> icebergPartitions = loadIcebergPartition(table, snapshotId);
        Map<String, IcebergPartition> nameToPartition = Maps.newHashMap();
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();

        List<Column> partitionColumns = IcebergUtils.getSchemaCacheValue(
                dorisTable, table.snapshot(snapshotId).schemaId()).getPartitionColumns();
        for (IcebergPartition partition : icebergPartitions) {
            nameToPartition.put(partition.getPartitionName(), partition);
            String transform = table.specs().get(partition.getSpecId()).fields().get(0).transform().toString();
            Range<PartitionKey> partitionRange = getPartitionRange(
                    partition.getPartitionValues().get(0), transform, partitionColumns);
            PartitionItem item = new RangePartitionItem(partitionRange);
            nameToPartitionItem.put(partition.getPartitionName(), item);
        }
        Map<String, Set<String>> partitionNameMap = mergeOverlapPartitions(nameToPartitionItem);
        return new IcebergPartitionInfo(nameToPartitionItem, nameToPartition, partitionNameMap);
    }

    private static List<IcebergPartition> loadIcebergPartition(Table table, long snapshotId) {
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils
                .createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
        List<IcebergPartition> partitions = Lists.newArrayList();
        try (CloseableIterable<FileScanTask> tasks = partitionsTable.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                CloseableIterable<StructLike> rows = task.asDataTask().rows();
                for (StructLike row : rows) {
                    partitions.add(generateIcebergPartition(table, row));
                }
            }
        } catch (IOException e) {
            LOG.warn("Failed to get Iceberg table {} partition info.", table.name(), e);
        }
        return partitions;
    }

    private static IcebergPartition generateIcebergPartition(Table table, StructLike row) {
        // row format :
        // 0. partitionData,
        // 1. spec_id,
        // 2. record_count,
        // 3. file_count,
        // 4. total_data_file_size_in_bytes,
        // 5. position_delete_record_count,
        // 6. position_delete_file_count,
        // 7. equality_delete_record_count,
        // 8. equality_delete_file_count,
        // 9. last_updated_at,
        // 10. last_updated_snapshot_id
        Preconditions.checkState(!table.spec().fields().isEmpty(), table.name() + " is not a partition table.");
        int specId = row.get(1, Integer.class);
        PartitionSpec partitionSpec = table.specs().get(specId);
        StructProjection partitionData = row.get(0, StructProjection.class);
        StringBuilder sb = new StringBuilder();
        List<String> partitionValues = Lists.newArrayList();
        List<String> transforms = Lists.newArrayList();
        for (int i = 0; i < partitionSpec.fields().size(); ++i) {
            PartitionField partitionField = partitionSpec.fields().get(i);
            Class<?> fieldClass = partitionSpec.javaClasses()[i];
            int fieldId = partitionField.fieldId();
            // Iceberg partition field id starts at PARTITION_DATA_ID_START,
            // So we can get the field index in partitionData using fieldId - PARTITION_DATA_ID_START
            int index = fieldId - PARTITION_DATA_ID_START;
            Object o = partitionData.get(index, fieldClass);
            String fieldValue = o == null ? null : o.toString();
            String fieldName = partitionField.name();
            sb.append(fieldName);
            sb.append("=");
            sb.append(fieldValue);
            sb.append("/");
            partitionValues.add(fieldValue);
            transforms.add(partitionField.transform().toString());
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        String partitionName = sb.toString();
        long recordCount = row.get(2, Long.class);
        long fileCount = row.get(3, Integer.class);
        long fileSizeInBytes = row.get(4, Long.class);
        long lastUpdateTime = row.get(9, Long.class);
        long lastUpdateSnapShotId = row.get(10, Long.class);
        return new IcebergPartition(partitionName, specId, recordCount, fileSizeInBytes, fileCount,
            lastUpdateTime, lastUpdateSnapShotId, partitionValues, transforms);
    }

    @VisibleForTesting
    public static Range<PartitionKey> getPartitionRange(String value, String transform, List<Column> partitionColumns)
            throws AnalysisException {
        // For NULL value, create a minimum partition for it.
        if (value == null) {
            PartitionKey nullLowKey = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue("0000-01-01")), partitionColumns);
            PartitionKey nullUpKey = nullLowKey.successor();
            return Range.closedOpen(nullLowKey, nullUpKey);
        }
        LocalDateTime epoch = Instant.EPOCH.atZone(ZoneId.of("UTC")).toLocalDateTime();
        LocalDateTime target;
        LocalDateTime lower;
        LocalDateTime upper;
        long longValue = Long.parseLong(value);
        switch (transform) {
            case HOUR:
                target = epoch.plusHours(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(),
                    target.getHour(), 0, 0);
                upper = lower.plusHours(1);
                break;
            case DAY:
                target = epoch.plusDays(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(), 0, 0, 0);
                upper = lower.plusDays(1);
                break;
            case MONTH:
                target = epoch.plusMonths(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), 1, 0, 0, 0);
                upper = lower.plusMonths(1);
                break;
            case YEAR:
                target = epoch.plusYears(longValue);
                lower = LocalDateTime.of(target.getYear(), Month.JANUARY, 1, 0, 0, 0);
                upper = lower.plusYears(1);
                break;
            default:
                throw new RuntimeException("Unsupported transform " + transform);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        Column c = partitionColumns.get(0);
        Preconditions.checkState(c.getDataType().isDateType(), "Only support date type partition column");
        if (c.getType().isDate() || c.getType().isDateV2()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        }
        PartitionValue lowerValue = new PartitionValue(lower.format(formatter));
        PartitionValue upperValue = new PartitionValue(upper.format(formatter));
        PartitionKey lowKey = PartitionKey.createPartitionKey(Lists.newArrayList(lowerValue), partitionColumns);
        PartitionKey upperKey =  PartitionKey.createPartitionKey(Lists.newArrayList(upperValue), partitionColumns);
        return Range.closedOpen(lowKey, upperKey);
    }

    /**
     * Merge overlapped iceberg partitions into one Doris partition.
     */
    @VisibleForTesting
    public static Map<String, Set<String>> mergeOverlapPartitions(Map<String, PartitionItem> originPartitions) {
        List<Map.Entry<String, PartitionItem>> entries = sortPartitionMap(originPartitions);
        Map<String, Set<String>> map = Maps.newHashMap();
        for (int i = 0; i < entries.size() - 1; i++) {
            Range<PartitionKey> firstValue = entries.get(i).getValue().getItems();
            String firstKey = entries.get(i).getKey();
            Range<PartitionKey> secondValue = entries.get(i + 1).getValue().getItems();
            String secondKey = entries.get(i + 1).getKey();
            // If the first entry enclose the second one, remove the second entry and keep a record in the return map.
            // So we can track the iceberg partitions those contained by one Doris partition.
            while (i < entries.size() && firstValue.encloses(secondValue)) {
                originPartitions.remove(secondKey);
                map.putIfAbsent(firstKey, Sets.newHashSet(firstKey));
                String finalSecondKey = secondKey;
                map.computeIfPresent(firstKey, (key, value) -> {
                    value.add(finalSecondKey);
                    return value;
                });
                i++;
                if (i >= entries.size() - 1) {
                    break;
                }
                secondValue = entries.get(i + 1).getValue().getItems();
                secondKey = entries.get(i + 1).getKey();
            }
        }
        return map;
    }

    /**
     * Sort the given map entries by PartitionItem Range(LOW, HIGH)
     * When comparing two ranges, the one with smaller LOW value is smaller than the other one.
     * If two ranges have same values of LOW, the one with larger HIGH value is smaller.
     *
     * For now, we only support year, month, day and hour,
     * so it is impossible to have two partially intersect partitions.
     * One range is either enclosed by another or has no intersection at all with another.
     *
     *
     * For example, we have these 4 ranges:
     * [10, 20), [30, 40), [0, 30), [10, 15)
     *
     * After sort, they become:
     * [0, 30), [10, 20), [10, 15), [30, 40)
     */
    @VisibleForTesting
    public static List<Map.Entry<String, PartitionItem>> sortPartitionMap(Map<String, PartitionItem> originPartitions) {
        List<Map.Entry<String, PartitionItem>> entries = new ArrayList<>(originPartitions.entrySet());
        entries.sort(new RangeComparator());
        return entries;
    }

    public static class RangeComparator implements Comparator<Map.Entry<String, PartitionItem>> {
        @Override
        public int compare(Map.Entry<String, PartitionItem> p1, Map.Entry<String, PartitionItem> p2) {
            PartitionItem value1 = p1.getValue();
            PartitionItem value2 = p2.getValue();
            if (value1 instanceof RangePartitionItem && value2 instanceof RangePartitionItem) {
                Range<PartitionKey> items1 = value1.getItems();
                Range<PartitionKey> items2 = value2.getItems();
                if (!items1.hasLowerBound()) {
                    return -1;
                }
                if (!items2.hasLowerBound()) {
                    return 1;
                }
                PartitionKey upper1 = items1.upperEndpoint();
                PartitionKey lower1 = items1.lowerEndpoint();
                PartitionKey upper2 = items2.upperEndpoint();
                PartitionKey lower2 = items2.lowerEndpoint();
                int compareLow = lower1.compareTo(lower2);
                return compareLow == 0 ? upper2.compareTo(upper1) : compareLow;
            }
            return 0;
        }
    }

    public static IcebergSnapshotCacheValue getIcebergSnapshotCacheValue(
            Optional<TableSnapshot> tableSnapshot,
            ExternalTable dorisTable,
            Optional<TableScanParams> scanParams) {
        if (tableSnapshot.isPresent() || IcebergUtils.isIcebergBranchOrTag(scanParams)) {
            // If a snapshot is specified,
            // use the specified snapshot and the corresponding schema(not the latest schema).
            Table icebergTable = getIcebergTable(dorisTable);
            IcebergTableQueryInfo info;
            try {
                info = getQuerySpecSnapshot(icebergTable, tableSnapshot, scanParams);
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
            return new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(),
                    new IcebergSnapshot(info.getSnapshotId(), info.getSchemaId()));
        } else {
            // Otherwise, use the latest snapshot and the latest schema.
            return Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()
                    .getSnapshotCache(dorisTable);
        }
    }

    public static List<Column> getIcebergSchema(ExternalTable dorisTable) {
        Optional<MvccSnapshot> snapshotFromContext = MvccUtil.getSnapshotFromContext(dorisTable);
        IcebergSnapshotCacheValue cacheValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshotFromContext, dorisTable);
        return IcebergUtils.getSchemaCacheValue(dorisTable, cacheValue.getSnapshot().getSchemaId())
                .getSchema();
    }

    public static IcebergSnapshotCacheValue getOrFetchSnapshotCacheValue(
            Optional<MvccSnapshot> snapshot,
            ExternalTable dorisTable) {
        if (snapshot.isPresent()) {
            return ((IcebergMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        } else {
            return IcebergUtils.getIcebergSnapshotCacheValue(Optional.empty(), dorisTable, Optional.empty());
        }
    }

    public static View getIcebergView(ExternalTable dorisTable) {
        IcebergMetadataCache metadataCache = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache();
        return metadataCache.getIcebergView(dorisTable);
    }

    public static Optional<SchemaCacheValue> loadSchemaCacheValue(
            ExternalTable dorisTable, long schemaId, boolean isView) {
        List<Column> schema = IcebergUtils.getSchema(dorisTable, schemaId, isView);
        List<Column> tmpColumns = Lists.newArrayList();
        if (!isView) {
            // get table partition column info
            Table table = IcebergUtils.getIcebergTable(dorisTable);
            PartitionSpec spec = table.spec();
            for (PartitionField field : spec.fields()) {
                Types.NestedField col = table.schema().findField(field.sourceId());
                for (Column c : schema) {
                    if (c.getName().equalsIgnoreCase(col.name())) {
                        tmpColumns.add(c);
                        break;
                    }
                }
            }
        }
        return Optional.of(new IcebergSchemaCacheValue(schema, tmpColumns));
    }

    public static String showCreateView(IcebergExternalTable icebergExternalTable) {
        return String.format("CREATE VIEW `%s` AS ", icebergExternalTable.getName())
                +
                icebergExternalTable.getViewText();
    }

}
