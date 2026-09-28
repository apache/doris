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

package org.apache.doris.arrow;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.thrift.TColumnDesc;

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * How FE describes a Doris column as an Arrow field.
 *
 * <p>This is the one place FE turns a Doris type into an Arrow type. Every Arrow schema FE hands a
 * client -- today the {@code table_schema} column of Flight SQL {@code GetTables}, next the result
 * and parameter schemas of prepared statements -- is built here, so that a client which types its
 * columns from one of them reads the batches BE emits as that type. The values mirror
 * {@code convert_to_arrow_type} in {@code be/src/format/arrow/arrow_row_batch.cpp}, which alone
 * decides the shape of the data on the wire; the cells known to disagree with it (a literal
 * {@code "UTC"} zone on TIMESTAMPTZ, {@code Null} for TIMEV2 / VARBINARY / AGG_STATE) are kept
 * deliberately until the BE Arrow type layer is reworked, and are then corrected here, once, against
 * a golden shared with BE (#67577). {@code DorisArrowTypeMappingTest} records every cell as it
 * stands, so a change to any of them is a change to that test as well.
 */
public final class DorisArrowTypeMapping {

    private DorisArrowTypeMapping() {
    }

    /**
     * The Arrow type of a Doris type; {@code precision} and {@code scale} are those of the column and
     * are null for a type that has none.
     */
    public static ArrowType toArrowType(PrimitiveType primitiveType, Integer precision, Integer scale) {
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
                    return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                } else if (scale > 0) {
                    return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
                } else {
                    return new ArrowType.Timestamp(TimeUnit.SECOND, null);
                }
            case TIMESTAMP_NS:
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
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

    /** The Arrow type of a column as {@code describeTables} reports it. */
    public static ArrowType toArrowType(TColumnDesc desc) {
        PrimitiveType primitiveType = PrimitiveType.fromThrift(desc.getColumnType());
        Integer precision = desc.isSetColumnPrecision() ? desc.getColumnPrecision() : null;
        Integer scale = desc.isSetColumnScale() ? desc.getColumnScale() : null;
        return toArrowType(primitiveType, precision, scale);
    }

    /**
     * One column of {@code dbName.tableName} as an Arrow field, with its nested types described down
     * to the leaves and the Flight SQL column metadata a client's {@code ResultSetMetaData} reads.
     */
    public static Field toField(String dbName, String tableName, TColumnDesc desc) {
        ArrowType arrowType = toArrowType(desc);
        return new Field(desc.getColumnName(),
                new FieldType(desc.isIsAllowNull(), arrowType, null,
                        flightSqlColumnMetadata(dbName, tableName, desc)),
                children(dbName, tableName, desc, arrowType));
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
    private static List<Field> children(String dbName, String tableName, TColumnDesc desc,
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
                return Collections.singletonList(toField(dbName, tableName, children.get(0)));
            case Map:
                // Arrow spells a map as list<entries: struct<key, value>>, with the entries struct and
                // the key both non-nullable -- the descriptor's key nullability is not carried over,
                // because an Arrow map with a nullable key is not a valid schema.
                if (children.size() != 2) {
                    return Collections.singletonList(
                            Field.notNullable(MapVector.DATA_VECTOR_NAME, new ArrowType.List()));
                }
                Field key = toField(dbName, tableName, children.get(0));
                Field value = toField(dbName, tableName, children.get(1));
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
                    structFields.add(toField(dbName, tableName, child));
                }
                return structFields;
            default:
                return null;
        }
    }

    private static Map<String, String> flightSqlColumnMetadata(final String dbName, final String tableName,
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
}
