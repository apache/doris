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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Maps an Arrow type onto the Doris type Doris will present for the column.
 *
 * <p>This runs on the FE side of a path whose BE side reads the very same Arrow arrays, so the mapping is
 * the contract between the two. Two consequences shape it:
 *
 * <ul>
 *   <li><b>Unsupported types fail here, not later.</b> A column Doris has no type for is rejected while the
 *       table is being described, naming the column and the Arrow type. Mapping it to something lossy would
 *       turn a clear error into wrong data, and deferring it to BE would produce an error with no column
 *       name in it.</li>
 *   <li><b>Unsigned integers widen.</b> Doris has no unsigned types, so a {@code uint32} maps to BIGINT
 *       rather than INT: the narrow choice silently wraps every value above 2^31.</li>
 * </ul>
 *
 * <p>Encoding variants map by their decoded value type, matching the BE normalizer that decodes them before
 * handing arrays to the serde. Dictionary encoding needs no case of its own: an Arrow {@code Field} carries
 * the dictionary's VALUE type as its own type and keeps the index type in its {@code DictionaryEncoding}
 * metadata, so a dictionary column already arrives here as the type Doris will see. Run-end encoding does
 * need one, because there the value type is a child.
 */
public final class AdbcTypeMapper {

    /** Doris DATETIMEV2/TIMESTAMPTZ scale is capped at 6 digits; Arrow nanosecond precision has 9. */
    private static final int MAX_DATETIME_SCALE = 6;
    private static final int MAX_DECIMAL128_PRECISION = 38;
    private static final int MAX_DECIMAL256_PRECISION = 76;

    private AdbcTypeMapper() {
    }

    /** Maps one top-level column. {@code columnName} only ever appears in error messages. */
    public static ConnectorType toDorisType(String columnName, Field field) {
        return toDorisType(columnName, field, field.getType());
    }

    private static ConnectorType toDorisType(String columnName, Field field, ArrowType arrowType) {
        switch (arrowType.getTypeID()) {
            case Bool:
                return ConnectorType.of("BOOLEAN");
            case Int:
                return intType(columnName, (ArrowType.Int) arrowType);
            case FloatingPoint:
                return floatType((ArrowType.FloatingPoint) arrowType);
            case Decimal:
                return decimalType(columnName, (ArrowType.Decimal) arrowType);
            case Date:
                // DATEDAY is a calendar date; DATEMILLI carries a time-of-day and would lose it as DATEV2.
                return ((ArrowType.Date) arrowType).getUnit() == org.apache.arrow.vector.types.DateUnit.DAY
                        ? ConnectorType.of("DATEV2")
                        : ConnectorType.of("DATETIMEV2", 3, 0);
            case Timestamp:
                return timestampType((ArrowType.Timestamp) arrowType);
            case Utf8:
            case LargeUtf8:
            case Utf8View:
                return ConnectorType.of("STRING");
            case Binary:
            case LargeBinary:
            case BinaryView:
            case FixedSizeBinary:
                // Doris has no general binary column type on the external-table path.
                return ConnectorType.of("STRING");
            case List:
            case LargeList:
            case FixedSizeList:
            case ListView:
            case LargeListView:
                return ConnectorType.arrayOf(childType(columnName, field, 0));
            case Struct:
                return structType(columnName, field);
            case Map:
                return mapType(columnName, field);
            case RunEndEncoded:
                // Children are (run_ends, values); only the value type reaches Doris.
                return childType(columnName, field, field.getChildren().size() - 1);
            default:
                throw unsupported(columnName, arrowType);
        }
    }

    private static ConnectorType intType(String columnName, ArrowType.Int type) {
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
                    throw unsupported(columnName, type);
            }
        }
        // Widen by one step: Doris has no unsigned integers, and the same-width type would wrap the upper
        // half of the range into negatives without any error.
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
                throw unsupported(columnName, type);
        }
    }

    private static ConnectorType floatType(ArrowType.FloatingPoint type) {
        switch (type.getPrecision()) {
            case HALF:
            case SINGLE:
                return ConnectorType.of("FLOAT");
            default:
                return ConnectorType.of("DOUBLE");
        }
    }

    private static ConnectorType decimalType(String columnName, ArrowType.Decimal type) {
        int limit = type.getBitWidth() > 128 ? MAX_DECIMAL256_PRECISION : MAX_DECIMAL128_PRECISION;
        if (type.getPrecision() > limit) {
            throw new DorisConnectorException("Column '" + columnName + "' has Arrow type "
                    + type + ", whose precision " + type.getPrecision()
                    + " exceeds the maximum Doris DECIMALV3 precision " + limit);
        }
        return ConnectorType.of("DECIMALV3", type.getPrecision(), type.getScale());
    }

    private static ConnectorType timestampType(ArrowType.Timestamp type) {
        int scale;
        switch (type.getUnit()) {
            case SECOND:
                scale = 0;
                break;
            case MILLISECOND:
                scale = 3;
                break;
            case NANOSECOND:
                // Truncated rather than rejected: sub-microsecond precision is rare in stored data, and
                // refusing the column would make whole tables unreadable over a fractional digit.
                scale = MAX_DATETIME_SCALE;
                break;
            default:
                scale = MAX_DATETIME_SCALE;
                break;
        }
        // A zoned Arrow timestamp is an instant, and TIMESTAMPTZ is the only Doris type that keeps one:
        // DATETIMEV2 would drop the zone and leave a wall clock whose meaning depends on who reads it.
        //
        // This is where the catalog property enable.mapping.timestamp_tz would belong -- the JDBC
        // catalog and the file formats consult it and default to DATETIMEV2 -- and it is deliberately
        // NOT consulted: ExternalCatalog.setDefaultPropsIfMissing stamps the property as "false" into
        // every external catalog that does not name it, so a connector reading it cannot tell a user
        // who asked for wall clocks from one who said nothing at all, and honouring it would force
        // DATETIMEV2 on every adbc catalog. This connector's default is TIMESTAMPTZ. Making the
        // property settable here needs fe-core to let a connector supply its own default first.
        boolean zoned = type.getTimezone() != null && !type.getTimezone().isEmpty();
        return ConnectorType.of(zoned ? "TIMESTAMPTZ" : "DATETIMEV2", scale, 0);
    }

    private static ConnectorType structType(String columnName, Field field) {
        List<Field> children = field.getChildren();
        if (children.isEmpty()) {
            throw new DorisConnectorException("Column '" + columnName
                    + "' is an Arrow struct with no fields, which Doris cannot represent");
        }
        List<String> names = new ArrayList<>(children.size());
        List<ConnectorType> types = new ArrayList<>(children.size());
        for (Field child : children) {
            // Lowercased level by level: BE indexes struct children by lowercase key, and a mixed-case
            // child name crashes it rather than failing the query.
            names.add(child.getName().toLowerCase(Locale.ROOT));
            types.add(toDorisType(columnName + "." + child.getName(), child, child.getType()));
        }
        return ConnectorType.structOf(names, types);
    }

    private static ConnectorType mapType(String columnName, Field field) {
        // Arrow models a map as list<struct<key, value>>, so the pair sits one level down.
        List<Field> children = field.getChildren();
        if (children.size() != 1 || children.get(0).getChildren().size() != 2) {
            throw new DorisConnectorException("Column '" + columnName
                    + "' is an Arrow map with an unexpected shape: " + field);
        }
        Field entries = children.get(0);
        return ConnectorType.mapOf(
                toDorisType(columnName + ".key", entries.getChildren().get(0),
                        entries.getChildren().get(0).getType()),
                toDorisType(columnName + ".value", entries.getChildren().get(1),
                        entries.getChildren().get(1).getType()));
    }

    private static ConnectorType childType(String columnName, Field field, int index) {
        List<Field> children = field.getChildren();
        if (children.isEmpty() || index < 0 || index >= children.size()) {
            throw new DorisConnectorException("Column '" + columnName
                    + "' has Arrow type " + field.getType() + " with no usable child type");
        }
        Field child = children.get(index);
        return toDorisType(columnName, child, child.getType());
    }

    private static DorisConnectorException unsupported(String columnName, ArrowType arrowType) {
        return new DorisConnectorException("Column '" + columnName + "' has Arrow type " + arrowType
                + ", which has no Doris equivalent. Cast or drop it on the remote side, or exclude the"
                + " column from the query.");
    }
}
