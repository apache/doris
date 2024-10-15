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

package org.apache.doris.datasource.lakesoul;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.thrift.TExprOpcode;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LakeSoulUtils {

    public static final String FILE_NAMES = "file_paths";
    public static final String PRIMARY_KEYS = "primary_keys";
    public static final String SCHEMA_JSON = "table_schema";
    public static final String PARTITION_DESC = "partition_descs";
    public static final String REQUIRED_FIELDS = "required_fields";
    public static final String OPTIONS = "options";
    public static final String SUBSTRAIT_PREDICATE = "substrait_predicate";
    public static final String CDC_COLUMN = "lakesoul_cdc_change_column";
    public static final String LIST_DELIM = ";";
    public static final String PARTITIONS_KV_DELIM = "=";
    public static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    public static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String FS_S3A_REGION = "fs.s3a.endpoint.region";
    public static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";

    private static final OffsetDateTime EPOCH;
    private static final LocalDate EPOCH_DAY;

    static {
        EPOCH = Instant.ofEpochSecond(0L).atOffset(ZoneOffset.UTC);
        EPOCH_DAY = EPOCH.toLocalDate();
    }

    public static List<PartitionInfo> applyPartitionFilters(
            List<PartitionInfo> allPartitionInfo,
            String tableName,
            Schema partitionArrowSchema,
            Map<String, ColumnRange> columnNameToRange
    ) throws IOException {

        Expression conjunctionFilter = null;
        for (Field field : partitionArrowSchema.getFields()) {
            ColumnRange columnRange = columnNameToRange.get(field.getName());
            if (columnRange != null) {
                Expression expr = columnRangeToSubstraitFilter(field, columnRange);
                if (expr != null) {
                    if (conjunctionFilter == null) {
                        conjunctionFilter = expr;
                    } else {
                        conjunctionFilter = SubstraitUtil.and(conjunctionFilter, expr);
                    }
                }
            }
        }
        return SubstraitUtil.applyPartitionFilters(
            allPartitionInfo,
            partitionArrowSchema,
            SubstraitUtil.substraitExprToProto(conjunctionFilter, tableName)
        );
    }

    public static Expression columnRangeToSubstraitFilter(
            Field columnField,
            ColumnRange columnRange
    ) throws IOException {
        Optional<RangeSet<ColumnBound>> rangeSetOpt = columnRange.getRangeSet();
        if (columnRange.hasConjunctiveIsNull() || !rangeSetOpt.isPresent()) {
            return SubstraitUtil.CONST_TRUE;
        } else {
            RangeSet<ColumnBound> rangeSet = rangeSetOpt.get();
            if (rangeSet.isEmpty()) {
                return SubstraitUtil.CONST_TRUE;
            } else {
                Expression conjunctionFilter = null;
                for (Range range : rangeSet.asRanges()) {
                    Expression expr = rangeToSubstraitFilter(columnField, range);
                    if (expr != null) {
                        if (conjunctionFilter == null) {
                            conjunctionFilter = expr;
                        } else {
                            conjunctionFilter = SubstraitUtil.or(conjunctionFilter, expr);
                        }
                    }
                }
                return conjunctionFilter;
            }
        }
    }

    public static Expression rangeToSubstraitFilter(Field columnField, Range range) throws IOException {
        if (!range.hasLowerBound() && !range.hasUpperBound()) {
            // Range.all()
            return SubstraitUtil.CONST_TRUE;
        } else {
            Expression upper = SubstraitUtil.CONST_TRUE;
            if (range.hasUpperBound()) {
                String func = range.upperBoundType() == BoundType.OPEN ? "lt:any_any" : "lte:any_any";
                Expression left = SubstraitUtil.arrowFieldToSubstraitField(columnField);
                Expression right = SubstraitUtil.anyToSubstraitLiteral(
                        SubstraitUtil.arrowFieldToSubstraitType(columnField),
                        ((ColumnBound) range.upperEndpoint()).getValue().getRealValue());
                upper = SubstraitUtil.makeBinary(
                        left,
                        right,
                        DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
                        func,
                        TypeCreator.NULLABLE.BOOLEAN
                );
            }
            Expression lower = SubstraitUtil.CONST_TRUE;
            if (range.hasLowerBound()) {
                String func = range.lowerBoundType() == BoundType.OPEN ? "gt:any_any" : "gte:any_any";
                Expression left = SubstraitUtil.arrowFieldToSubstraitField(columnField);
                Expression right = SubstraitUtil.anyToSubstraitLiteral(
                        SubstraitUtil.arrowFieldToSubstraitType(columnField),
                        ((ColumnBound) range.lowerEndpoint()).getValue().getRealValue());
                lower = SubstraitUtil.makeBinary(
                        left,
                        right,
                        DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
                        func,
                        TypeCreator.NULLABLE.BOOLEAN
                );
            }
            return SubstraitUtil.and(upper, lower);
        }
    }

    public static io.substrait.proto.Plan getPushPredicate(
            List<Expr> conjuncts,
            String tableName,
            Schema tableSchema,
            Schema partitionArrowSchema,
            Map<String, String> properties,
            boolean incRead
    ) throws IOException {

        Set<String> partitionColumn =
                partitionArrowSchema
                    .getFields()
                    .stream()
                    .map(Field::getName)
                    .collect(Collectors.toSet());
        Expression conjunctionFilter = null;
        String cdcColumn = properties.get(CDC_COLUMN);
        if (cdcColumn != null && !incRead) {
            conjunctionFilter = SubstraitUtil.cdcColumnMergeOnReadFilter(tableSchema.findField(cdcColumn));
        }
        for (Expr expr : conjuncts) {
            if (!isAllPartitionPredicate(expr, partitionColumn)) {
                Expression predicate = convertToSubstraitExpr(expr, tableSchema);
                if (predicate != null) {
                    if (conjunctionFilter == null) {
                        conjunctionFilter = predicate;
                    } else {
                        conjunctionFilter = SubstraitUtil.and(conjunctionFilter, predicate);
                    }
                }
            }
        }
        if (conjunctionFilter == null) {
            return null;
        }
        return SubstraitUtil.substraitExprToProto(conjunctionFilter, tableName);
    }

    public static boolean isAllPartitionPredicate(Expr predicate, Set<String> partitionColumns) {
        if (predicate == null) {
            return false;
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            return isAllPartitionPredicate(compoundPredicate.getChild(0), partitionColumns)
                && isAllPartitionPredicate(compoundPredicate.getChild(1), partitionColumns);
        }
        // Make sure the col slot is always first
        SlotRef slotRef = convertDorisExprToSlotRef(predicate.getChild(0));
        LiteralExpr literalExpr = convertDorisExprToLiteralExpr(predicate.getChild(1));
        if (slotRef == null || literalExpr == null) {
            return false;
        }
        String colName = slotRef.getColumnName();
        return partitionColumns.contains(colName);

    }

    public static SlotRef convertDorisExprToSlotRef(Expr expr) {
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

    public static LiteralExpr convertDorisExprToLiteralExpr(Expr expr) {
        LiteralExpr literalExpr = null;
        if (expr instanceof LiteralExpr) {
            literalExpr = (LiteralExpr) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof LiteralExpr) {
                literalExpr = (LiteralExpr) expr.getChild(0);
            }
        }
        return literalExpr;
    }

    public static Expression convertToSubstraitExpr(Expr predicate, Schema tableSchema) throws IOException {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) predicate;
            boolean value = boolLiteral.getValue();
            if (value) {
                return SubstraitUtil.CONST_TRUE;
            } else {
                return SubstraitUtil.CONST_FALSE;
            }
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    Expression left = convertToSubstraitExpr(compoundPredicate.getChild(0), tableSchema);
                    Expression right = convertToSubstraitExpr(compoundPredicate.getChild(1), tableSchema);
                    if (left != null && right != null) {
                        return SubstraitUtil.and(left, right);
                    } else if (left != null) {
                        return left;
                    } else {
                        return right;
                    }
                }
                case OR: {
                    Expression left = convertToSubstraitExpr(compoundPredicate.getChild(0), tableSchema);
                    Expression right = convertToSubstraitExpr(compoundPredicate.getChild(1), tableSchema);
                    if (left != null && right != null) {
                        return SubstraitUtil.or(left, right);
                    }
                    return null;
                }
                case NOT: {
                    Expression child = convertToSubstraitExpr(compoundPredicate.getChild(0), tableSchema);
                    if (child != null) {
                        return SubstraitUtil.not(child);
                    }
                    return null;
                }
                default:
                    return null;
            }
        } else if (predicate instanceof InPredicate) {
            InPredicate inExpr = (InPredicate) predicate;
            if (inExpr.contains(Subquery.class)) {
                return null;
            }
            SlotRef slotRef = convertDorisExprToSlotRef(inExpr.getChild(0));
            if (slotRef == null) {
                return null;
            }
            String colName = slotRef.getColumnName();
            Field field = tableSchema.findField(colName);
            Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(field);

            colName = field.getName();
            Type type = field.getType().accept(
                new SubstraitUtil.ArrowTypeToSubstraitTypeConverter(field.isNullable())
            );
            List<Expression.Literal> valueList = new ArrayList<>();
            for (int i = 1; i < inExpr.getChildren().size(); ++i) {
                if (!(inExpr.getChild(i) instanceof LiteralExpr)) {
                    return null;
                }
                LiteralExpr literalExpr = (LiteralExpr) inExpr.getChild(i);
                Object value = extractDorisLiteral(type, literalExpr);
                if (value == null) {
                    return null;
                }
                valueList.add(SubstraitUtil.anyToSubstraitLiteral(type, value));
            }
            if (inExpr.isNotIn()) {
                // not in
                return SubstraitUtil.notIn(fieldRef, valueList);
            } else {
                // in
                return SubstraitUtil.in(fieldRef, valueList);
            }
        }
        return convertBinaryExpr(predicate, tableSchema);
    }

    private static Expression convertBinaryExpr(Expr dorisExpr, Schema tableSchema) throws IOException {
        TExprOpcode opcode = dorisExpr.getOpcode();
        // Make sure the col slot is always first
        SlotRef slotRef = convertDorisExprToSlotRef(dorisExpr.getChild(0));
        LiteralExpr literalExpr = convertDorisExprToLiteralExpr(dorisExpr.getChild(1));
        if (slotRef == null || literalExpr == null) {
            return null;
        }
        String colName = slotRef.getColumnName();
        Field field = tableSchema.findField(colName);
        Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(field);

        Type type = field.getType().accept(
            new SubstraitUtil.ArrowTypeToSubstraitTypeConverter(field.isNullable())
        );
        Object value = extractDorisLiteral(type, literalExpr);
        if (value == null) {
            if (opcode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                return SubstraitUtil.makeUnary(
                        fieldRef,
                        DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
                        "is_null:any",
                        TypeCreator.NULLABLE.BOOLEAN);
            } else {
                return null;
            }
        }
        Expression literal = SubstraitUtil.anyToSubstraitLiteral(
                type,
                value
        );

        String namespace;
        String func;
        switch (opcode) {
            case EQ:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "equal:any_any";
                break;
            case EQ_FOR_NULL:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "is_null:any";
                break;
            case NE:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "not_equal:any_any";
                break;
            case GE:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "gte:any_any";
                break;
            case GT:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "gt:any_any";
                break;
            case LE:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "lte:any_any";
                break;
            case LT:
                namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                func = "lt:any_any";
                break;
            case INVALID_OPCODE:
                if (dorisExpr instanceof IsNullPredicate) {
                    if (((IsNullPredicate) dorisExpr).isNotNull()) {
                        namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                        func = "is_not_null:any";
                    } else {
                        namespace = DefaultExtensionCatalog.FUNCTIONS_COMPARISON;
                        func = "is_null:any";
                    }
                    break;
                }
                return null;
            default:
                return null;
        }
        return SubstraitUtil.makeBinary(fieldRef, literal, namespace, func, TypeCreator.NULLABLE.BOOLEAN);
    }

    public static Object extractDorisLiteral(Type type, LiteralExpr expr) {

        if (expr instanceof BoolLiteral) {
            if (type instanceof Type.Bool) {
                return ((BoolLiteral) expr).getValue();
            }
            if (type instanceof Type.Str) {
                return expr.getStringValue();
            }
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            if (type instanceof Type.Date) {
                if (dateLiteral.getType().isDatetimeV2() || dateLiteral.getType().isDatetime()) {
                    return null;
                }
                return (int) LocalDate.of((int) dateLiteral.getYear(),
                        (int) dateLiteral.getMonth(),
                        (int) dateLiteral.getDay()).toEpochDay();
            }
            if (type instanceof Type.TimestampTZ || type instanceof Type.Timestamp) {
                return dateLiteral.getLongValue();
            }
            if (type instanceof Type.Str) {
                return expr.getStringValue();
            }
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            if (type instanceof Type.Decimal) {
                return decimalLiteral.getValue();
            } else if (type instanceof Type.FP64) {
                return decimalLiteral.getDoubleValue();
            }
            if (type instanceof Type.Str) {
                return expr.getStringValue();
            }
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;

            if (floatLiteral.getType() == org.apache.doris.catalog.Type.FLOAT) {
                return type instanceof Type.FP32
                    || type instanceof Type.FP64
                    || type instanceof Type.Decimal ? ((FloatLiteral) expr).getValue() : null;
            } else {
                return type instanceof Type.FP64
                    || type instanceof Type.Decimal ? ((FloatLiteral) expr).getValue() : null;
            }
        } else if (expr instanceof IntLiteral) {
            if (type instanceof Type.I8
                    || type instanceof Type.I16
                    || type instanceof Type.I32
                    || type instanceof Type.I64
                    || type instanceof Type.FP32
                    || type instanceof Type.FP64
                    || type instanceof Type.Decimal
                    || type instanceof Type.Date
            ) {
                return expr.getRealValue();
            }
            if (!expr.getType().isInteger32Type()) {
                if (type instanceof Type.Time || type instanceof Type.Timestamp || type instanceof Type.TimestampTZ) {
                    return expr.getLongValue();
                }
            }

        } else if (expr instanceof StringLiteral) {
            String value = expr.getStringValue();
            if (type instanceof Type.Str) {
                return value;
            }
            if (type instanceof Type.Date) {
                try {
                    return (int) ChronoUnit.DAYS.between(
                        EPOCH_DAY,
                        LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE));
                } catch (DateTimeParseException e) {
                    return null;
                }
            }
            if (type instanceof Type.Timestamp || type instanceof Type.TimestampTZ) {
                try {
                    return ChronoUnit.MICROS.between(
                        EPOCH,
                        OffsetDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME));
                } catch (DateTimeParseException e) {
                    return null;
                }
            }
        }
        return null;
    }

}
