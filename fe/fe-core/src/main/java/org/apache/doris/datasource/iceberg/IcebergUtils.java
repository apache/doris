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
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Iceberg utils
 */
public class IcebergUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergUtils.class);
    private static long MILLIS_TO_NANO_TIME = 1000;
    // https://iceberg.apache.org/spec/#schemas-and-data-types
    // All time and timestamp values are stored with microsecond precision
    private static final int ICEBERG_DATETIME_SCALE_MS = 6;

    public static Expression convertToIcebergExpr(Expr expr, Schema schema) {
        if (expr == null) {
            return null;
        }

        // BoolLiteral
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            boolean value = boolLiteral.getValue();
            if (value) {
                return Expressions.alwaysTrue();
            } else {
                return Expressions.alwaysFalse();
            }
        }

        // CompoundPredicate
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1), schema);
                    if (left != null && right != null) {
                        return Expressions.and(left, right);
                    }
                    return null;
                }
                case OR: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1), schema);
                    if (left != null && right != null) {
                        return Expressions.or(left, right);
                    }
                    return null;
                }
                case NOT: {
                    Expression child = convertToIcebergExpr(compoundPredicate.getChild(0), schema);
                    if (child != null) {
                        return Expressions.not(child);
                    }
                    return null;
                }
                default:
                    return null;
            }
        }

        // BinaryPredicate
        if (expr instanceof BinaryPredicate) {
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
                    Object value = extractDorisLiteral(literalExpr);
                    if (value == null) {
                        if (opCode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                            return Expressions.isNull(colName);
                        } else {
                            return null;
                        }
                    }
                    switch (opCode) {
                        case EQ:
                        case EQ_FOR_NULL:
                            return Expressions.equal(colName, value);
                        case NE:
                            return Expressions.not(Expressions.equal(colName, value));
                        case GE:
                            return Expressions.greaterThanOrEqual(colName, value);
                        case GT:
                            return Expressions.greaterThan(colName, value);
                        case LE:
                            return Expressions.lessThanOrEqual(colName, value);
                        case LT:
                            return Expressions.lessThan(colName, value);
                        default:
                            return null;
                    }
                default:
                    return null;
            }
        }

        // InPredicate, only support a in (1,2,3)
        if (expr instanceof InPredicate) {
            InPredicate inExpr = (InPredicate) expr;
            if (inExpr.contains(Subquery.class)) {
                return null;
            }
            SlotRef slotRef = convertDorisExprToSlotRef(inExpr.getChild(0));
            if (slotRef == null) {
                return null;
            }
            List<Object> valueList = new ArrayList<>();
            for (int i = 1; i < inExpr.getChildren().size(); ++i) {
                if (!(inExpr.getChild(i) instanceof LiteralExpr)) {
                    return null;
                }
                LiteralExpr literalExpr = (LiteralExpr) inExpr.getChild(i);
                Object value = extractDorisLiteral(literalExpr);
                valueList.add(value);
            }
            String colName = slotRef.getColumnName();
            Types.NestedField nestedField = schema.caseInsensitiveFindField(colName);
            colName = nestedField.name();
            if (inExpr.isNotIn()) {
                // not in
                return Expressions.notIn(colName, valueList);
            } else {
                // in
                return Expressions.in(colName, valueList);
            }
        }

        return null;
    }

    private static Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            if (dateLiteral.isDateType()) {
                return dateLiteral.getStringValue();
            } else {
                return dateLiteral.unixTimestamp(TimeUtils.getTimeZone()) * MILLIS_TO_NANO_TIME;
            }
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            return decimalLiteral.getValue();
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return stringLiteral.getStringValue();
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
            case STRUCT:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }

    /**
     * Get iceberg schema from catalog and convert them to doris schema
     */
    public static List<Column> getSchema(ExternalCatalog catalog, String dbName, String name) {
        return HiveMetaStoreClientHelper.ugiDoAs(catalog.getConfiguration(), () -> {
            org.apache.iceberg.Table icebergTable = Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getIcebergMetadataCache()
                    .getIcebergTable(catalog, dbName, name);
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
}
