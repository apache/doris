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

package org.apache.doris.arrowflight;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * What {@code CommandGetTables} says a column is, against what the query that follows actually
 * carries.
 *
 * <p><b>Why these assertions matter.</b> A Flight SQL client is entitled to type its columns from
 * the schema in {@code GetTables} and then read the batches without re-deriving anything -- that is
 * what the schema is for, and {@code getArrowType} is documented as mirroring
 * {@code convert_to_arrow_type} in the backend. When the two disagree the client does not get a
 * degraded answer, it gets a failed read: it decodes the batch as the type the metadata promised.
 * So each case below pins the Arrow type BE emits, not merely "some" type.
 *
 * <p>The descriptors are built the way {@code FrontendServiceImpl.getColumnDesc} builds them --
 * a complex column carries its element types as {@link TColumnDesc} children, named "item" for an
 * array and "key"/"value" for a map by {@code Column.createChildrenColumn}.
 */
public class FlightSqlSchemaHelperArrowTypeTest {

    private static final String DB = "test_db";
    private static final String TABLE = "test_tbl";

    private static TColumnDesc desc(String name, TPrimitiveType type) {
        TColumnDesc columnDesc = new TColumnDesc(name, type);
        // Nullable, so that a place where the mapping must force NOT NULL is proved to force it
        // rather than to inherit it.
        columnDesc.setIsAllowNull(true);
        return columnDesc;
    }

    private static TColumnDesc desc(String name, TPrimitiveType type, TColumnDesc... children) {
        TColumnDesc columnDesc = desc(name, type);
        columnDesc.setChildren(Arrays.asList(children));
        return columnDesc;
    }

    private static Field buildField(TColumnDesc columnDesc) {
        return Deencapsulation.invoke(FlightSqlSchemaHelper.class, "buildField", DB, TABLE, columnDesc);
    }

    private static ArrowType arrowType(PrimitiveType type, Integer precision, Integer scale) throws Exception {
        // Plain reflection rather than Deencapsulation: the descriptor of a column that has no precision
        // or scale hands the mapping null for both, and that null is part of what the table pins.
        Method getArrowType = FlightSqlSchemaHelper.class.getDeclaredMethod("getArrowType",
                PrimitiveType.class, Integer.class, Integer.class);
        getArrowType.setAccessible(true);
        return (ArrowType) getArrowType.invoke(null, type, precision, scale);
    }

    private static Arguments row(PrimitiveType type, ArrowType expected) {
        return Arguments.of(type, null, null, expected);
    }

    private static Arguments row(PrimitiveType type, int precision, int scale, ArrowType expected) {
        return Arguments.of(type, precision, scale, expected);
    }

    private static ArrowType timestamp(TimeUnit unit, String timezone) {
        return new ArrowType.Timestamp(unit, timezone);
    }

    /**
     * The mapping as it stands today, one row per {@link PrimitiveType} (plus one per precision / scale
     * band where the band picks the Arrow type). This is a record of the present, not of the ideal: the
     * rows marked "kept as is" are known to disagree with what BE emits and stay that way on purpose
     * until the BE Arrow type layer is reworked, after which the whole table is corrected in one step
     * against a golden shared with BE (tracked in #67577). Until then a change to any row is a
     * behaviour change that every {@code GetTables} client sees, and this test is what makes it
     * deliberate.
     */
    private static Stream<Arguments> mapping() {
        return Stream.of(
                row(PrimitiveType.BOOLEAN, new ArrowType.Bool()),
                row(PrimitiveType.TINYINT, new ArrowType.Int(8, true)),
                row(PrimitiveType.SMALLINT, new ArrowType.Int(16, true)),
                row(PrimitiveType.INT, new ArrowType.Int(32, true)),
                row(PrimitiveType.BIGINT, new ArrowType.Int(64, true)),
                row(PrimitiveType.FLOAT, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                row(PrimitiveType.DOUBLE, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                // BE writes a LARGEINT as its decimal text: it does not fit decimal128.
                row(PrimitiveType.LARGEINT, new ArrowType.Utf8()),
                row(PrimitiveType.CHAR, new ArrowType.Utf8()),
                row(PrimitiveType.VARCHAR, new ArrowType.Utf8()),
                row(PrimitiveType.STRING, new ArrowType.Utf8()),
                row(PrimitiveType.JSONB, new ArrowType.Utf8()),
                row(PrimitiveType.VARIANT, new ArrowType.Utf8()),
                // IPV4 rides in an int32 (parquet has no uint32); IPV6 is text.
                row(PrimitiveType.IPV4, new ArrowType.Int(32, true)),
                row(PrimitiveType.IPV6, new ArrowType.Utf8()),
                // The v1 date types stay text; DATEV2 is a day number, see dateV2IsDescribedAsDate32.
                row(PrimitiveType.DATE, new ArrowType.Utf8()),
                row(PrimitiveType.DATETIME, new ArrowType.Utf8()),
                row(PrimitiveType.DATEV2, new ArrowType.Date(DateUnit.DAY)),
                // DATETIMEV2 is a wall-clock value: a timezone-naive timestamp whose unit follows the scale,
                // with the bands' edges at scale 0 / 1 and 3 / 4.
                row(PrimitiveType.DATETIMEV2, 18, 0, timestamp(TimeUnit.SECOND, null)),
                row(PrimitiveType.DATETIMEV2, 19, 1, timestamp(TimeUnit.MILLISECOND, null)),
                row(PrimitiveType.DATETIMEV2, 21, 3, timestamp(TimeUnit.MILLISECOND, null)),
                row(PrimitiveType.DATETIMEV2, 22, 4, timestamp(TimeUnit.MICROSECOND, null)),
                row(PrimitiveType.DATETIMEV2, 24, 6, timestamp(TimeUnit.MICROSECOND, null)),
                row(PrimitiveType.TIMESTAMP_NS, 27, 9, timestamp(TimeUnit.NANOSECOND, null)),
                // The same bands as DATETIMEV2, but the timezone is the literal "UTC" where BE stamps
                // the session timezone. Kept as is (#67577).
                row(PrimitiveType.TIMESTAMPTZ, 18, 0, timestamp(TimeUnit.SECOND, "UTC")),
                row(PrimitiveType.TIMESTAMPTZ, 21, 3, timestamp(TimeUnit.MILLISECOND, "UTC")),
                row(PrimitiveType.TIMESTAMPTZ, 24, 6, timestamp(TimeUnit.MICROSECOND, "UTC")),
                // DECIMALV2 is always (27, 9) whatever the column declares; the v3 decimals carry theirs.
                row(PrimitiveType.DECIMALV2, 10, 2, new ArrowType.Decimal(27, 9, 128)),
                row(PrimitiveType.DECIMAL32, 9, 2, new ArrowType.Decimal(9, 2, 128)),
                row(PrimitiveType.DECIMAL64, 18, 4, new ArrowType.Decimal(18, 4, 128)),
                row(PrimitiveType.DECIMAL128, 38, 10, new ArrowType.Decimal(38, 10, 128)),
                row(PrimitiveType.DECIMAL256, 76, 20, new ArrowType.Decimal(76, 20, 256)),
                row(PrimitiveType.HLL, new ArrowType.Binary()),
                row(PrimitiveType.BITMAP, new ArrowType.Binary()),
                row(PrimitiveType.QUANTILE_STATE, new ArrowType.Binary()),
                // BE emits float64 for TIMEV2 and binary for VARBINARY and AGG_STATE; the schema says
                // Null for all three. Kept as is (#67577).
                row(PrimitiveType.TIMEV2, 18, 0, new ArrowType.Null()),
                row(PrimitiveType.VARBINARY, new ArrowType.Null()),
                row(PrimitiveType.AGG_STATE, new ArrowType.Null()),
                // The element types of a complex column live in the field's children, not in its type.
                row(PrimitiveType.ARRAY, new ArrowType.List()),
                row(PrimitiveType.MAP, new ArrowType.Map(false)),
                row(PrimitiveType.STRUCT, new ArrowType.Struct()),
                // Types that never name a stored column fall through to Null.
                row(PrimitiveType.INVALID_TYPE, new ArrowType.Null()),
                row(PrimitiveType.UNSUPPORTED, new ArrowType.Null()),
                row(PrimitiveType.NULL_TYPE, new ArrowType.Null()),
                row(PrimitiveType.LAMBDA_FUNCTION, new ArrowType.Null()),
                row(PrimitiveType.TEMPLATE, new ArrowType.Null()),
                row(PrimitiveType.BINARY, new ArrowType.Null()));
    }

    @ParameterizedTest(name = "{0}({1}, {2}) is described as {3}")
    @MethodSource("mapping")
    public void everyTypeIsDescribedAsToday(PrimitiveType type, Integer precision, Integer scale,
            ArrowType expected) throws Exception {
        Assertions.assertEquals(expected, arrowType(type, precision, scale));
    }

    /**
     * A type this table does not know is a type whose schema nobody has looked at: a new
     * {@link PrimitiveType} must get a row here, and the row must say what BE emits for it.
     */
    @Test
    public void everyPrimitiveTypeHasARow() {
        Set<PrimitiveType> covered = mapping().map(row -> (PrimitiveType) row.get()[0])
                .collect(Collectors.toSet());
        for (PrimitiveType type : PrimitiveType.values()) {
            Assertions.assertTrue(covered.contains(type), type + " has no row in the mapping table");
        }
    }

    /**
     * BE writes a DATEV2 column as {@code arrow::Date32Type} -- a day number. {@link DateUnit#MILLISECOND}
     * is date64, a different width and a different meaning, and a client that believes it renders and
     * compares the column as a datetime, then fails the read on the first batch with "not support convert
     * to datetimev2 from arrow type: 16".
     */
    @Test
    public void dateV2IsDescribedAsDate32() {
        Assertions.assertEquals(new ArrowType.Date(DateUnit.DAY),
                buildField(desc("d", TPrimitiveType.DATEV2)).getType());
    }

    /**
     * An Arrow list carries its element type in its child and nowhere else, so the placeholder child this
     * replaced ({@code ZeroVector}'s Null type) described every array in the catalog as an array OF
     * NOTHING while BE emitted {@code ListType(item)} in the data.
     */
    @Test
    public void arrayDescribesItsElementType() {
        Field array = buildField(desc("a", TPrimitiveType.ARRAY, desc("item", TPrimitiveType.INT)));

        Assertions.assertEquals(ArrowType.ArrowTypeID.List, array.getType().getTypeID());
        Assertions.assertEquals(1, array.getChildren().size());
        Field item = array.getChildren().get(0);
        Assertions.assertEquals("item", item.getName());
        Assertions.assertEquals(new ArrowType.Int(32, true), item.getType());
    }

    /**
     * Arrow spells a map as {@code list<entries: struct<key, value>>}. Both the entries struct and the key
     * are non-nullable in a valid Arrow schema, so the descriptor's nullability must not be carried over to
     * the key even though it is carried over everywhere else.
     */
    @Test
    public void mapDescribesKeyAndValue() {
        Field map = buildField(desc("m", TPrimitiveType.MAP,
                desc("key", TPrimitiveType.VARCHAR), desc("value", TPrimitiveType.INT)));

        Assertions.assertEquals(ArrowType.ArrowTypeID.Map, map.getType().getTypeID());
        Assertions.assertEquals(1, map.getChildren().size());

        Field entries = map.getChildren().get(0);
        Assertions.assertEquals(MapVector.DATA_VECTOR_NAME, entries.getName());
        Assertions.assertEquals(ArrowType.ArrowTypeID.Struct, entries.getType().getTypeID());
        Assertions.assertFalse(entries.isNullable(), "an arrow map's entries struct is never nullable");

        List<Field> pair = entries.getChildren();
        Assertions.assertEquals(2, pair.size());
        Assertions.assertEquals("key", pair.get(0).getName());
        Assertions.assertEquals(new ArrowType.Utf8(), pair.get(0).getType());
        Assertions.assertFalse(pair.get(0).isNullable(), "an arrow map with a nullable key is not a valid schema");
        Assertions.assertEquals("value", pair.get(1).getName());
        Assertions.assertEquals(new ArrowType.Int(32, true), pair.get(1).getType());
    }

    /** A struct with no fields is not "a struct", it is a column the client cannot read at all. */
    @Test
    public void structDescribesItsFields() {
        Field struct = buildField(desc("s", TPrimitiveType.STRUCT,
                desc("f1", TPrimitiveType.INT), desc("f2", TPrimitiveType.STRING)));

        Assertions.assertEquals(ArrowType.ArrowTypeID.Struct, struct.getType().getTypeID());
        Assertions.assertEquals(2, struct.getChildren().size());
        Assertions.assertEquals("f1", struct.getChildren().get(0).getName());
        Assertions.assertEquals(new ArrowType.Int(32, true), struct.getChildren().get(0).getType());
        Assertions.assertEquals("f2", struct.getChildren().get(1).getName());
        Assertions.assertEquals(new ArrowType.Utf8(), struct.getChildren().get(1).getType());
    }

    /** Nesting is where a per-column fix would have stopped: the descriptor's tree is walked to the leaves. */
    @Test
    public void nestedComplexTypesAreDescribedToTheLeaves() {
        Field outer = buildField(desc("a", TPrimitiveType.ARRAY,
                desc("item", TPrimitiveType.MAP,
                        desc("key", TPrimitiveType.VARCHAR),
                        desc("value", TPrimitiveType.ARRAY, desc("item", TPrimitiveType.BIGINT)))));

        Field innerMap = outer.getChildren().get(0);
        Assertions.assertEquals(ArrowType.ArrowTypeID.Map, innerMap.getType().getTypeID());
        List<Field> pair = innerMap.getChildren().get(0).getChildren();
        Assertions.assertEquals(new ArrowType.Utf8(), pair.get(0).getType());

        Field innerArray = pair.get(1);
        Assertions.assertEquals(ArrowType.ArrowTypeID.List, innerArray.getType().getTypeID());
        Assertions.assertEquals(new ArrowType.Int(64, true), innerArray.getChildren().get(0).getType());
    }

    /**
     * A descriptor that reports no children keeps the placeholders rather than an empty child list: a source
     * that cannot describe its nested types is no worse off than it was before this mapping existed.
     */
    @Test
    public void complexColumnWithoutChildrenKeepsThePlaceholder() {
        Field array = buildField(desc("a", TPrimitiveType.ARRAY));
        Assertions.assertEquals(1, array.getChildren().size());
        Assertions.assertEquals(BaseRepeatedValueVector.DATA_VECTOR_NAME, array.getChildren().get(0).getName());
        Assertions.assertEquals(ArrowType.ArrowTypeID.Null, array.getChildren().get(0).getType().getTypeID());

        Field map = buildField(desc("m", TPrimitiveType.MAP));
        Assertions.assertEquals(1, map.getChildren().size());
        Assertions.assertEquals(MapVector.DATA_VECTOR_NAME, map.getChildren().get(0).getName());
        Assertions.assertEquals(ArrowType.ArrowTypeID.List, map.getChildren().get(0).getType().getTypeID());

        Assertions.assertTrue(buildField(desc("s", TPrimitiveType.STRUCT)).getChildren().isEmpty());
    }

    /** A scalar column has no children to describe, and gaining one would change how it is read. */
    @Test
    public void scalarColumnHasNoChildren() {
        Assertions.assertTrue(buildField(desc("i", TPrimitiveType.INT)).getChildren().isEmpty());
    }

    /**
     * The client does not see the {@link Field} objects, it sees the serialized schema in the
     * {@code table_schema} column of {@code GetTables}. Asserting after a round trip through that encoding
     * is what proves the element types actually reach it.
     */
    @Test
    public void theSerializedSchemaCarriesTheChildren() throws IOException {
        byte[] serialized = FlightSqlSchemaHelper.getSerializedSchema(Collections.singletonList(
                buildField(desc("a", TPrimitiveType.ARRAY, desc("item", TPrimitiveType.INT)))));

        Schema schema = MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(serialized))));

        Field array = schema.getFields().get(0);
        Assertions.assertEquals(ArrowType.ArrowTypeID.List, array.getType().getTypeID());
        Assertions.assertEquals(new ArrowType.Int(32, true), array.getChildren().get(0).getType());
    }

    @Test
    public void serializedSchemaDescribesScalarAndNestedTimestampNs() throws IOException {
        byte[] serialized = FlightSqlSchemaHelper.getSerializedSchema(Arrays.asList(
                buildField(desc("ts", TPrimitiveType.TIMESTAMP_NS)),
                buildField(desc("items", TPrimitiveType.ARRAY,
                        desc("item", TPrimitiveType.TIMESTAMP_NS))),
                buildField(desc("by_name", TPrimitiveType.MAP,
                        desc("key", TPrimitiveType.VARCHAR),
                        desc("value", TPrimitiveType.TIMESTAMP_NS))),
                buildField(desc("record", TPrimitiveType.STRUCT,
                        desc("ts", TPrimitiveType.TIMESTAMP_NS)))));

        Schema schema = MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(serialized))));
        ArrowType.Timestamp timestampNs = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        Assertions.assertEquals(timestampNs, schema.getFields().get(0).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(1).getChildren().get(0).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(2).getChildren().get(0).getChildren().get(1).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(3).getChildren().get(0).getType());
    }
}
