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

    private LanceTypeConverter() {
    }

    public static Type toDorisType(Field field) {
        // Arrow Java exposes unknown extension types through their storage type and field
        // metadata. Treating the storage type as the logical type would make DESC report
        // Blob, JSON, or BFloat16 as supported even though the scanner cannot decode their
        // extension semantics. Dictionary arrays are likewise not decoded by the BE reader.
        // TODO(lance): Dataset.getSchema() currently erases the Dictionary marker, while
        // Dataset.getLanceSchema() fails to convert a schema containing Dictionary in the
        // Lance 9.1.0-beta.3 Java SDK. Reject physical Dictionary columns after that SDK
        // conversion is fixed; an unmarked Int16 field cannot be distinguished safely here.
        String extensionName = field.getMetadata() == null
                ? null : field.getMetadata().get(ARROW_EXTENSION_NAME);
        if (field.getDictionary() != null
                || (extensionName != null && !extensionName.isEmpty())) {
            return Type.UNSUPPORTED;
        }

        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
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

    private static void requireChildren(Field field, int expected) {
        if (field.getChildren().size() != expected) {
            throw new IllegalArgumentException("Invalid Arrow children for Lance field '" + field.getName()
                    + "': expected " + expected + " but found " + field.getChildren().size());
        }
    }
}
