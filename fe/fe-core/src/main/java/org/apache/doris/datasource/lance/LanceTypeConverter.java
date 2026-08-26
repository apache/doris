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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/** Arrow schema conversion shared by Lance external-table metadata. */
public final class LanceTypeConverter {
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final String ARROW_EXTENSION_NAME = "ARROW:extension:name";
    private static final String ARROW_JSON_EXTENSION = "arrow.json";
    private static final String LANCE_JSON_EXTENSION = "lance.json";
    private static final String LANCE_BFLOAT16_EXTENSION = "lance.bfloat16";
    private static final String LANCE_BLOB_V2_EXTENSION = "lance.blob.v2";

    private LanceTypeConverter() {
    }

    /** 将 Lance 暴露的 Arrow 字段递归转换为 Doris 类型。 */
    public static Type toDorisType(Field field) {
        // TODO(lance): Dataset.getSchema() currently erases the Dictionary marker, while
        // Dataset.getLanceSchema() fails to convert a schema containing Dictionary in the
        // Lance 9.1.0-beta.3 Java SDK. Reject physical Dictionary columns after that SDK
        // conversion is fixed; an unmarked Int16 field cannot be distinguished safely here.
        String extensionName = field.getMetadata() == null
                ? null : field.getMetadata().get(ARROW_EXTENSION_NAME);
        if (field.getDictionary() != null) {
            return Type.UNSUPPORTED;
        }
        if (extensionName != null && !extensionName.isEmpty()) {
            return extensionType(field, extensionName);
        }

        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Null:
                return Type.NULL;
            case Bool:
                return Type.BOOLEAN;
            case Int:
                return integerType((ArrowType.Int) arrowType);
            case FloatingPoint:
                FloatingPointPrecision precision =
                        ((ArrowType.FloatingPoint) arrowType).getPrecision();
                if (precision == FloatingPointPrecision.HALF
                        || precision == FloatingPointPrecision.SINGLE) {
                    return Type.FLOAT;
                }
                if (precision == FloatingPointPrecision.DOUBLE) {
                    return Type.DOUBLE;
                }
                return Type.UNSUPPORTED;
            case Utf8:
            case LargeUtf8:
                return Type.STRING;
            case Binary:
            case LargeBinary:
                return ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH);
            case FixedSizeBinary:
                int byteWidth = ((ArrowType.FixedSizeBinary) arrowType).getByteWidth();
                return byteWidth >= 0 && byteWidth <= ScalarType.MAX_VARBINARY_LENGTH
                        ? ScalarType.createVarbinaryType(byteWidth)
                        : Type.UNSUPPORTED;
            case Date:
                return dateType((ArrowType.Date) arrowType);
            case Time:
                return timeType((ArrowType.Time) arrowType);
            case Timestamp:
                return timestampType((ArrowType.Timestamp) arrowType);
            case Duration:
                return Type.BIGINT;
            case Decimal:
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                if (decimal.getPrecision() <= 0 || decimal.getPrecision() > MAX_DECIMAL_PRECISION
                        || decimal.getScale() < 0 || decimal.getScale() > decimal.getPrecision()) {
                    return Type.UNSUPPORTED;
                }
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case List:
            case LargeList:
            case FixedSizeList:
                requireChildren(field, 1);
                Type itemType = toDorisType(field.getChildren().get(0));
                return itemType.isSupported() ? new ArrayType(itemType) : Type.UNSUPPORTED;
            case Map:
                requireChildren(field, 1);
                Field entries = field.getChildren().get(0);
                requireChildren(entries, 2);
                Field key = entries.getChildren().get(0);
                Field value = entries.getChildren().get(1);
                Type keyType = toDorisType(key);
                Type valueType = toDorisType(value);
                return keyType.isSupported() && valueType.isSupported()
                        ? new MapType(keyType, valueType, key.isNullable(), value.isNullable())
                        : Type.UNSUPPORTED;
            case Struct:
                List<StructField> fields = new ArrayList<>();
                for (Field child : field.getChildren()) {
                    Type childType = toDorisType(child);
                    if (!childType.isSupported()) {
                        return Type.UNSUPPORTED;
                    }
                    fields.add(new StructField(child.getName(), childType,
                            child.getMetadata() == null
                                    ? null : child.getMetadata().get("comment"),
                            child.isNullable()));
                }
                return new StructType(new ArrayList<>(fields));
            default:
                return Type.UNSUPPORTED;
        }
    }

    /** 根据扩展名及其物理存储类型返回对应的 Doris 类型。 */
    private static Type extensionType(Field field, String extensionName) {
        ArrowType storageType = field.getType();
        switch (extensionName) {
            case ARROW_JSON_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.Utf8
                                || storageType.getTypeID() == ArrowType.ArrowTypeID.LargeUtf8
                        ? Type.JSONB : Type.UNSUPPORTED;
            case LANCE_JSON_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.LargeBinary
                        ? Type.JSONB : Type.UNSUPPORTED;
            case LANCE_BFLOAT16_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.FixedSizeBinary
                                && ((ArrowType.FixedSizeBinary) storageType).getByteWidth() == 2
                        ? Type.FLOAT : Type.UNSUPPORTED;
            case LANCE_BLOB_V2_EXTENSION:
                return isBlobV2Storage(field)
                        ? ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH)
                        : Type.UNSUPPORTED;
            default:
                return Type.UNSUPPORTED;
        }
    }

    /** 校验 Blob v2 的逻辑结构，兼容最小字段集和带范围信息的完整字段集。 */
    private static boolean isBlobV2Storage(Field field) {
        if (field.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
            return false;
        }
        List<Field> children = field.getChildren();
        if (children.size() != 2 && children.size() != 4) {
            return false;
        }
        if (!isField(children.get(0), "data", ArrowType.ArrowTypeID.LargeBinary)
                || !isField(children.get(1), "uri", ArrowType.ArrowTypeID.Utf8)) {
            return false;
        }
        return children.size() == 2
                || (isUnsignedIntegerField(children.get(2), "position", 64)
                        && isUnsignedIntegerField(children.get(3), "size", 64));
    }

    /** 校验字段名称和 Arrow 类型标识。 */
    private static boolean isField(Field field, String name, ArrowType.ArrowTypeID typeId) {
        return name.equals(field.getName()) && field.getType().getTypeID() == typeId;
    }

    /** 校验指定名称和位宽的无符号整数字段。 */
    private static boolean isUnsignedIntegerField(Field field, String name, int bitWidth) {
        if (!name.equals(field.getName())
                || field.getType().getTypeID() != ArrowType.ArrowTypeID.Int) {
            return false;
        }
        ArrowType.Int integer = (ArrowType.Int) field.getType();
        return !integer.getIsSigned() && integer.getBitWidth() == bitWidth;
    }

    /** 将 Arrow 整数按有符号性和位宽映射到可无损容纳它的 Doris 类型。 */
    private static Type integerType(ArrowType.Int type) {
        if (type.getIsSigned()) {
            switch (type.getBitWidth()) {
                case 8:
                    return Type.TINYINT;
                case 16:
                    return Type.SMALLINT;
                case 32:
                    return Type.INT;
                case 64:
                    return Type.BIGINT;
                default:
                    return Type.UNSUPPORTED;
            }
        }
        switch (type.getBitWidth()) {
            case 8:
                return Type.SMALLINT;
            case 16:
                return Type.INT;
            case 32:
                return Type.BIGINT;
            case 64:
                return Type.LARGEINT;
            default:
                return Type.UNSUPPORTED;
        }
    }

    /** 将 Arrow 时间戳映射到对应精度和时区语义的 Doris 类型。 */
    private static Type timestampType(ArrowType.Timestamp type) {
        TimeUnit unit = type.getUnit();
        int scale;
        switch (unit) {
            case SECOND:
                scale = 0;
                break;
            case MILLISECOND:
                scale = 3;
                break;
            case MICROSECOND:
            case NANOSECOND:
                scale = 6;
                break;
            default:
                return Type.UNSUPPORTED;
        }
        String timezone = type.getTimezone();
        return timezone == null || timezone.isEmpty()
                ? ScalarType.createDatetimeV2Type(scale)
                : ScalarType.createTimeStampTzType(scale);
    }

    /** 将 Arrow 日内时间映射到 Doris TIMEV2。 */
    private static Type timeType(ArrowType.Time type) {
        TimeUnit unit = type.getUnit();
        switch (unit) {
            case SECOND:
                return type.getBitWidth() == 32
                        ? ScalarType.createTimeV2Type(0) : Type.UNSUPPORTED;
            case MILLISECOND:
                return type.getBitWidth() == 32
                        ? ScalarType.createTimeV2Type(3) : Type.UNSUPPORTED;
            case MICROSECOND:
            case NANOSECOND:
                return type.getBitWidth() == 64
                        ? ScalarType.createTimeV2Type(6) : Type.UNSUPPORTED;
            default:
                return Type.UNSUPPORTED;
        }
    }

    /** 将 Arrow 日期单位映射到 Doris DATEV2。 */
    private static Type dateType(ArrowType.Date type) {
        DateUnit unit = type.getUnit();
        switch (unit) {
            case DAY:
            case MILLISECOND:
                return ScalarType.createDateV2Type();
            default:
                return Type.UNSUPPORTED;
        }
    }

    /** 校验嵌套 Arrow 字段的子字段数量。 */
    private static void requireChildren(Field field, int expected) {
        if (field.getChildren().size() != expected) {
            throw new IllegalArgumentException("Invalid Arrow children for Lance field '" + field.getName()
                    + "': expected " + expected + " but found " + field.getChildren().size());
        }
    }
}
