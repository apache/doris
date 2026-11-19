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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Maps a Lance dataset's Arrow schema onto Doris columns.
 *
 * <p>The mapping is the one Lance catalogs have shown since 4.1. A type Doris cannot read is not an error
 * for the table: its column stays visible as {@code UNSUPPORTED} and only a query that reads it fails.
 * Unsigned integers widen to the next signed type so no value changes, binary maps to VARBINARY, a
 * timestamp with a time zone is an instant (TIMESTAMPTZ) and one without is a wall-clock DATETIMEV2.
 */
final class LanceTypeMapping {
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final String ARROW_EXTENSION_NAME = "ARROW:extension:name";
    private static final String ARROW_JSON_EXTENSION = "arrow.json";
    private static final String LANCE_JSON_EXTENSION = "lance.json";
    private static final String LANCE_BFLOAT16_EXTENSION = "lance.bfloat16";
    private static final String COMMENT_METADATA_KEY = "comment";

    private static final ConnectorType UNSUPPORTED = ConnectorType.of("UNSUPPORTED");

    private LanceTypeMapping() {
    }

    /** One column per top-level field, keeping Arrow nullability and the field's {@code comment} metadata. */
    static List<ConnectorColumn> toColumns(Schema schema) {
        List<ConnectorColumn> columns = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            columns.add(new ConnectorColumn(field.getName(), toConnectorType(field), commentOf(field),
                    field.isNullable(), null));
        }
        return columns;
    }

    /**
     * Whether reading this field needs the BE Lance materialization that came with these types (Arrow Null
     * and Duration, the JSON and bfloat16 extensions), anywhere in the field's children. A BE from before
     * them cannot read the column, which is what the scan's smooth-upgrade fence is for.
     */
    static boolean requiresCurrentBeReader(Field field) {
        ArrowType.ArrowTypeID typeId = field.getType().getTypeID();
        if (typeId == ArrowType.ArrowTypeID.Null
                || typeId == ArrowType.ArrowTypeID.Duration) {
            return true;
        }
        String extensionName = extensionNameOf(field);
        if (ARROW_JSON_EXTENSION.equals(extensionName)
                || LANCE_JSON_EXTENSION.equals(extensionName)
                || LANCE_BFLOAT16_EXTENSION.equals(extensionName)) {
            return true;
        }
        for (Field child : field.getChildren()) {
            if (requiresCurrentBeReader(child)) {
                return true;
            }
        }
        return false;
    }

    /** Converts one Arrow field, including Null leaves in complex types. */
    static ConnectorType toConnectorType(Field field) {
        // TODO(lance): Dataset.getSchema() currently erases the Dictionary marker, while
        // Dataset.getLanceSchema() fails to convert a schema containing Dictionary in the
        // Lance Java SDK. Reject physical Dictionary columns after that SDK
        // conversion is fixed; an unmarked Int16 field cannot be distinguished safely here.
        if (field.getDictionary() != null) {
            return UNSUPPORTED;
        }
        String extensionName = extensionNameOf(field);
        if (extensionName != null && !extensionName.isEmpty()) {
            return extensionType(field, extensionName);
        }

        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Null:
                // Required Null leaves cannot use the nullable SerDe that handles Arrow NA buffers.
                return field.isNullable() ? ConnectorType.of("NULL_TYPE") : UNSUPPORTED;
            case Bool:
                return ConnectorType.of("BOOLEAN");
            case Int:
                return integerType((ArrowType.Int) arrowType);
            case FloatingPoint:
                FloatingPointPrecision precision =
                        ((ArrowType.FloatingPoint) arrowType).getPrecision();
                if (precision == FloatingPointPrecision.HALF
                        || precision == FloatingPointPrecision.SINGLE) {
                    return ConnectorType.of("FLOAT");
                }
                if (precision == FloatingPointPrecision.DOUBLE) {
                    return ConnectorType.of("DOUBLE");
                }
                return UNSUPPORTED;
            case Utf8:
            case LargeUtf8:
                return ConnectorType.of("STRING");
            case Binary:
            case LargeBinary:
                // No length: the engine maps an unbounded VARBINARY to its maximum length.
                return ConnectorType.of("VARBINARY");
            case FixedSizeBinary:
                // A zero width carries no length the engine can keep; it becomes the unbounded VARBINARY.
                int byteWidth = ((ArrowType.FixedSizeBinary) arrowType).getByteWidth();
                return byteWidth >= 0 ? ConnectorType.of("VARBINARY", byteWidth, 0) : UNSUPPORTED;
            case Date:
                // Both units hold whole days in Lance datasets; Date64 is milliseconds at midnight.
                return ConnectorType.of("DATEV2");
            case Time:
                return timeType((ArrowType.Time) arrowType);
            case Timestamp:
                return timestampType((ArrowType.Timestamp) arrowType);
            case Duration:
                return ConnectorType.of("BIGINT");
            case Decimal:
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                if (decimal.getPrecision() <= 0 || decimal.getPrecision() > MAX_DECIMAL_PRECISION
                        || decimal.getScale() < 0 || decimal.getScale() > decimal.getPrecision()) {
                    return UNSUPPORTED;
                }
                return ConnectorType.of("DECIMALV3", decimal.getPrecision(), decimal.getScale());
            case List:
            case LargeList:
            case FixedSizeList:
                requireChildren(field, 1);
                ConnectorType itemType = toConnectorType(field.getChildren().get(0));
                return isUnsupported(itemType) ? UNSUPPORTED : ConnectorType.arrayOf(itemType);
            case Map:
                requireChildren(field, 1);
                Field entries = field.getChildren().get(0);
                requireChildren(entries, 2);
                Field key = entries.getChildren().get(0);
                Field value = entries.getChildren().get(1);
                ConnectorType keyType = toConnectorType(key);
                ConnectorType valueType = toConnectorType(value);
                if (isUnsupported(keyType) || isUnsupported(valueType)) {
                    return UNSUPPORTED;
                }
                return new ConnectorType("MAP", -1, -1, Arrays.asList(keyType, valueType),
                        Collections.emptyList(), Arrays.asList(key.isNullable(), value.isNullable()),
                        Collections.emptyList());
            case Struct:
                return structType(field);
            default:
                return UNSUPPORTED;
        }
    }

    private static ConnectorType structType(Field field) {
        List<Field> children = field.getChildren();
        if (children.isEmpty()) {
            // Doris has no empty STRUCT to show it as.
            return UNSUPPORTED;
        }
        List<String> names = new ArrayList<>(children.size());
        List<ConnectorType> types = new ArrayList<>(children.size());
        List<Boolean> nullable = new ArrayList<>(children.size());
        List<String> comments = new ArrayList<>(children.size());
        for (Field child : children) {
            ConnectorType childType = toConnectorType(child);
            if (isUnsupported(childType)) {
                return UNSUPPORTED;
            }
            // The engine lowercases struct field names itself, as Doris always has for these columns.
            names.add(child.getName());
            types.add(childType);
            nullable.add(child.isNullable());
            comments.add(commentOf(child));
        }
        return ConnectorType.structOf(names, types, nullable, comments);
    }

    /** Maps a known extension and validates its storage type. */
    private static ConnectorType extensionType(Field field, String extensionName) {
        ArrowType storageType = field.getType();
        switch (extensionName) {
            case ARROW_JSON_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.Utf8
                                || storageType.getTypeID() == ArrowType.ArrowTypeID.LargeUtf8
                        ? ConnectorType.of("JSONB") : UNSUPPORTED;
            case LANCE_JSON_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.LargeBinary
                        ? ConnectorType.of("JSONB") : UNSUPPORTED;
            case LANCE_BFLOAT16_EXTENSION:
                return storageType.getTypeID() == ArrowType.ArrowTypeID.FixedSizeBinary
                                && ((ArrowType.FixedSizeBinary) storageType).getByteWidth() == 2
                        ? ConnectorType.of("FLOAT") : UNSUPPORTED;
            default:
                return UNSUPPORTED;
        }
    }

    /** Maps an Arrow integer to the narrowest lossless Doris type. */
    private static ConnectorType integerType(ArrowType.Int type) {
        if (type.getIsSigned()) {
            switch (type.getBitWidth()) {
                case 8:
                    return ConnectorType.of("TINYINT");
                case 16:
                    return ConnectorType.of("SMALLINT");
                case 32:
                    return ConnectorType.of("INT");
                case 64:
                    return ConnectorType.of("BIGINT");
                default:
                    return UNSUPPORTED;
            }
        }
        switch (type.getBitWidth()) {
            case 8:
                return ConnectorType.of("SMALLINT");
            case 16:
                return ConnectorType.of("INT");
            case 32:
                return ConnectorType.of("BIGINT");
            case 64:
                return ConnectorType.of("LARGEINT");
            default:
                return UNSUPPORTED;
        }
    }

    /**
     * Maps an Arrow timestamp while preserving precision and time zone semantics. The scale rides in the
     * precision slot, which is where the engine reads it for DATETIMEV2 and TIMESTAMPTZ.
     */
    private static ConnectorType timestampType(ArrowType.Timestamp type) {
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
                return UNSUPPORTED;
        }
        String timezone = type.getTimezone();
        return ConnectorType.of(timezone == null || timezone.isEmpty() ? "DATETIMEV2" : "TIMESTAMPTZ", scale, 0);
    }

    /** Maps an Arrow time of day to Doris TIMEV2, the scale in the precision slot as for DATETIMEV2. */
    private static ConnectorType timeType(ArrowType.Time type) {
        TimeUnit unit = type.getUnit();
        switch (unit) {
            case SECOND:
                return type.getBitWidth() == 32 ? ConnectorType.of("TIMEV2", 0, 0) : UNSUPPORTED;
            case MILLISECOND:
                return type.getBitWidth() == 32 ? ConnectorType.of("TIMEV2", 3, 0) : UNSUPPORTED;
            case MICROSECOND:
            case NANOSECOND:
                return type.getBitWidth() == 64 ? ConnectorType.of("TIMEV2", 6, 0) : UNSUPPORTED;
            default:
                return UNSUPPORTED;
        }
    }

    private static boolean isUnsupported(ConnectorType type) {
        return UNSUPPORTED.getTypeName().equals(type.getTypeName());
    }

    private static String extensionNameOf(Field field) {
        return field.getMetadata() == null ? null : field.getMetadata().get(ARROW_EXTENSION_NAME);
    }

    private static String commentOf(Field field) {
        return field.getMetadata() == null ? null : field.getMetadata().get(COMMENT_METADATA_KEY);
    }

    /** Checks the child count of a nested Arrow field. */
    private static void requireChildren(Field field, int expected) {
        if (field.getChildren().size() != expected) {
            throw new DorisConnectorException("Invalid Arrow children for Lance field '" + field.getName()
                    + "': expected " + expected + " but found " + field.getChildren().size());
        }
    }
}
