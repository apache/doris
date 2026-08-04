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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
}
