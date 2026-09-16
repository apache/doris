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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DefaultValueUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts a Paimon schema default into the equivalent Doris planner expression. */
public final class PaimonDefaultValue {
    private PaimonDefaultValue() {
    }

    public static Expression toDorisExpression(DataField field, DataType targetType) {
        if (field.defaultValue() == null) {
            return new NullLiteral(targetType);
        }
        try {
            Object value = DefaultValueUtils.convertDefaultValue(field.type(), field.defaultValue());
            return toDorisExpression(field.type(), value, targetType);
        } catch (Exception e) {
            throw new AnalysisException(String.format(
                    "Failed to convert Paimon default value %s for column '%s' of type %s",
                    field.defaultValue(), field.name(), field.type()), e);
        }
    }

    @VisibleForTesting
    static Expression toDorisExpression(
            org.apache.paimon.types.DataType paimonType, Object value, DataType targetType) {
        if (value == null) {
            return new NullLiteral(targetType);
        }
        switch (paimonType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new StringLiteral(((BinaryString) value).toString());
            case BOOLEAN:
                return BooleanLiteral.of((Boolean) value);
            case BINARY:
            case VARBINARY:
                return targetType instanceof VarBinaryType
                        ? new VarBinaryLiteral(targetType, (byte[]) value)
                        : new StringLiteral(new String((byte[]) value, StandardCharsets.UTF_8));
            case DECIMAL:
                return new DecimalV3Literal(
                        (DecimalV3Type) targetType, ((Decimal) value).toBigDecimal());
            case TINYINT:
                return new TinyIntLiteral((Byte) value);
            case SMALLINT:
                return new SmallIntLiteral((Short) value);
            case INTEGER:
                return new IntegerLiteral((Integer) value);
            case BIGINT:
                return new BigIntLiteral((Long) value);
            case FLOAT:
                return new FloatLiteral((Float) value);
            case DOUBLE:
                return new DoubleLiteral((Double) value);
            case DATE:
                LocalDate date = LocalDate.ofEpochDay(((Integer) value).longValue());
                return new DateV2Literal(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampLiteral(((Timestamp) value).toLocalDateTime(), targetType);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Instant instant = value instanceof LocalZoneTimestamp
                        ? ((LocalZoneTimestamp) value).toInstant()
                        : ((Timestamp) value).toInstant();
                return timestampLiteral(LocalDateTime.ofInstant(instant, TimeUtils.getDorisZoneId()), targetType);
            case ARRAY:
                return arrayLiteral((org.apache.paimon.types.ArrayType) paimonType,
                        (InternalArray) value, targetType);
            case MAP:
                return mapLiteral((org.apache.paimon.types.MapType) paimonType,
                        (InternalMap) value, targetType);
            case ROW:
                return rowLiteral((RowType) paimonType, (InternalRow) value, targetType);
            default:
                throw new AnalysisException("Unsupported Paimon write-default type: " + paimonType);
        }
    }

    private static Literal timestampLiteral(LocalDateTime value, DataType targetType) {
        DateTimeV2Type type = (DateTimeV2Type) targetType;
        return new DateTimeV2Literal(type, value.getYear(), value.getMonthValue(), value.getDayOfMonth(),
                value.getHour(), value.getMinute(), value.getSecond(), value.getNano() / 1_000);
    }

    private static Literal arrayLiteral(org.apache.paimon.types.ArrayType paimonType,
            InternalArray value, DataType targetType) {
        ArrayType arrayType = (ArrayType) targetType;
        org.apache.paimon.types.DataType elementType = paimonType.getElementType();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
        List<Literal> elements = new ArrayList<>(value.size());
        for (int i = 0; i < value.size(); i++) {
            elements.add((Literal) toDorisExpression(
                    elementType, getter.getElementOrNull(value, i), arrayType.getItemType()));
        }
        return new ArrayLiteral(elements, targetType);
    }

    private static Literal mapLiteral(org.apache.paimon.types.MapType paimonType,
            InternalMap value, DataType targetType) {
        MapType mapType = (MapType) targetType;
        org.apache.paimon.types.DataType keyType = paimonType.getKeyType();
        org.apache.paimon.types.DataType valueType = paimonType.getValueType();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        Map<Literal, Literal> entries = new LinkedHashMap<>();
        for (int i = 0; i < value.size(); i++) {
            entries.put(
                    (Literal) toDorisExpression(
                            keyType, keyGetter.getElementOrNull(value.keyArray(), i), mapType.getKeyType()),
                    (Literal) toDorisExpression(
                            valueType, valueGetter.getElementOrNull(value.valueArray(), i), mapType.getValueType()));
        }
        return new MapLiteral(entries, targetType);
    }

    private static Literal rowLiteral(RowType paimonType, InternalRow value, DataType targetType) {
        StructType structType = (StructType) targetType;
        List<Literal> fields = new ArrayList<>(paimonType.getFieldCount());
        for (int i = 0; i < paimonType.getFieldCount(); i++) {
            org.apache.paimon.types.DataType fieldType = paimonType.getTypeAt(i);
            Object fieldValue = InternalRow.createFieldGetter(fieldType, i).getFieldOrNull(value);
            fields.add((Literal) toDorisExpression(
                    fieldType, fieldValue, structType.getFields().get(i).getDataType()));
        }
        return new StructLiteral(fields, targetType);
    }
}
