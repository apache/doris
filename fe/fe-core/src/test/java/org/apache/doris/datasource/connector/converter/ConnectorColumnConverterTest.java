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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

class ConnectorColumnConverterTest {

    @Test
    void testScalarTypeRoundtrip() {
        // INT → ConnectorType → Doris Type should roundtrip
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(ScalarType.INT);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertEquals(ScalarType.INT, back);
    }

    @Test
    void testArrayTypeRoundtrip() {
        ArrayType arrayInt = ArrayType.create(ScalarType.INT, true);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(arrayInt);

        Assertions.assertEquals("ARRAY", ct.getTypeName());
        Assertions.assertEquals(1, ct.getChildren().size());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof ArrayType);
        Assertions.assertEquals(ScalarType.INT, ((ArrayType) back).getItemType());
    }

    @Test
    void testMapTypeRoundtrip() {
        MapType mapType = new MapType(ScalarType.createStringType(), ScalarType.INT);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(mapType);

        Assertions.assertEquals("MAP", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals("STRING", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("INT", ct.getChildren().get(1).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof MapType);
        Assertions.assertEquals(ScalarType.createStringType(), ((MapType) back).getKeyType());
        Assertions.assertEquals(ScalarType.INT, ((MapType) back).getValueType());
    }

    @Test
    void testStructTypeRoundtrip() {
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(new StructField("a", ScalarType.INT));
        fields.add(new StructField("b", ScalarType.createStringType()));
        StructType structType = new StructType(fields);

        ConnectorType ct = ConnectorColumnConverter.toConnectorType(structType);

        Assertions.assertEquals("STRUCT", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals(Arrays.asList("a", "b"), ct.getFieldNames());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("STRING", ct.getChildren().get(1).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof StructType);
        StructType backStruct = (StructType) back;
        Assertions.assertEquals(2, backStruct.getFields().size());
        Assertions.assertEquals("a", backStruct.getFields().get(0).getName());
        Assertions.assertEquals(ScalarType.INT, backStruct.getFields().get(0).getType());
    }

    @Test
    void testNestedComplexType() {
        // ARRAY<MAP<STRING, INT>>
        MapType innerMap = new MapType(ScalarType.createStringType(), ScalarType.INT);
        ArrayType nested = ArrayType.create(innerMap, true);

        ConnectorType ct = ConnectorColumnConverter.toConnectorType(nested);

        Assertions.assertEquals("ARRAY", ct.getTypeName());
        ConnectorType mapCt = ct.getChildren().get(0);
        Assertions.assertEquals("MAP", mapCt.getTypeName());
        Assertions.assertEquals("STRING", mapCt.getChildren().get(0).getTypeName());
        Assertions.assertEquals("INT", mapCt.getChildren().get(1).getTypeName());

        // Full roundtrip
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof ArrayType);
        Type backItem = ((ArrayType) back).getItemType();
        Assertions.assertTrue(backItem instanceof MapType);
    }

    @Test
    void testUnsupportedTypeConversion() {
        ConnectorType ct = ConnectorType.of("UNSUPPORTED", -1, -1);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back.isUnsupported());
    }

    @Test
    void testUnknownTypeDefaultsToUnsupported() {
        ConnectorType ct = ConnectorType.of("GEOMETRY", -1, -1);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back.isUnsupported());
    }

    @Test
    void testDecimalTypeRoundtrip() {
        ScalarType decimal = ScalarType.createDecimalV3Type(18, 6);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(decimal);

        // PrimitiveType.toString() returns the specific decimal width (DECIMAL64 for p<=18)
        Assertions.assertTrue(ct.getTypeName().startsWith("DECIMAL"));
        Assertions.assertEquals(18, ct.getPrecision());
        Assertions.assertEquals(6, ct.getScale());
    }

    @Test
    void testWithTimeZoneColumnSetsExtraInfo() {
        // A ConnectorColumn marked withTimeZone() must convert to a Doris Column carrying the
        // WITH_TIMEZONE extra info — the value DESC shows in its "Extra" column (IndexSchemaProcNode
        // reads Column.getExtraInfo()). This is the SPI transport for legacy
        // PaimonExternalTable/IcebergUtils setWithTZExtraInfo(). MUTATION: dropping the
        // setWithTZExtraInfo() call in convertColumn -> getExtraInfo()==null -> red.
        ConnectorColumn marked = new ConnectorColumn("ts_ltz",
                ConnectorType.of("TIMESTAMPTZ", 3, 0), null, true, null, true).withTimeZone();
        Column col = ConnectorColumnConverter.convertColumn(marked);
        Assertions.assertEquals("WITH_TIMEZONE", col.getExtraInfo());
    }

    @Test
    void testPlainColumnHasNoExtraInfo() {
        // Regression guard: an unmarked column must NOT receive the WITH_TIMEZONE extra info.
        ConnectorColumn plain = new ConnectorColumn("id",
                ConnectorType.of("INT", -1, -1), null, true, null, true);
        Column col = ConnectorColumnConverter.convertColumn(plain);
        Assertions.assertNull(col.getExtraInfo());
    }

    @Test
    void testCharVarcharLengthPreserved() {
        // Regression: CHAR/VARCHAR carry length in `len`, not `precision`; the
        // converter must encode the length into the ConnectorType precision field
        // so it survives the CREATE TABLE request path (previously emitted 0).
        ScalarType charType = ScalarType.createCharType(20);
        ConnectorType charCt = ConnectorColumnConverter.toConnectorType(charType);
        Assertions.assertEquals("CHAR", charCt.getTypeName());
        Assertions.assertEquals(20, charCt.getPrecision());
        Type charBack = ConnectorColumnConverter.convertType(charCt);
        Assertions.assertTrue(charBack instanceof ScalarType);
        Assertions.assertEquals(20, ((ScalarType) charBack).getLength());

        ScalarType varcharType = ScalarType.createVarcharType(255);
        ConnectorType varcharCt = ConnectorColumnConverter.toConnectorType(varcharType);
        Assertions.assertEquals("VARCHAR", varcharCt.getTypeName());
        Assertions.assertEquals(255, varcharCt.getPrecision());
        Type varcharBack = ConnectorColumnConverter.convertType(varcharCt);
        Assertions.assertTrue(varcharBack instanceof ScalarType);
        Assertions.assertEquals(255, ((ScalarType) varcharBack).getLength());
    }

    @Test
    void convertColumnDefaultsToVisible() {
        ConnectorType intType = ConnectorColumnConverter.toConnectorType(ScalarType.INT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("c", intType, null, true, null));
        Assertions.assertTrue(col.isVisible(),
                "a ConnectorColumn not marked invisible converts to a visible Doris column");
    }

    @Test
    void convertColumnPropagatesInvisibleMarker() {
        // WHY: the iceberg synthetic write columns (__DORIS_ICEBERG_ROWID_COL__ + the v3 row-lineage
        // columns) are hidden (Column.setIsVisible(false)). Post-flip they are declared through the
        // connector schema SPI, so the neutral ConnectorColumn must carry the invisible marker across the
        // boundary and the converter must re-apply it (mirroring the withTimeZone marker).
        // MUTATION: dropping the setIsVisible(false) re-apply leaves the column visible -> this turns red.
        ConnectorType intType = ConnectorColumnConverter.toConnectorType(ScalarType.INT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("rowid", intType, null, true, null).invisible());
        Assertions.assertFalse(col.isVisible(),
                "an invisible ConnectorColumn must convert to a hidden (isVisible=false) Doris column");
    }

    @Test
    void convertColumnDefaultsToUnsetUniqueId() {
        // Regression guard: a ConnectorColumn that does not carry a reserved field id must leave the Doris
        // Column at its default unset (-1) uniqueId — the converter must not stamp an id where none was
        // declared. Mirrors convertColumnDefaultsToVisible.
        ConnectorType bigintType = ConnectorColumnConverter.toConnectorType(ScalarType.BIGINT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("c", bigintType, null, true, null));
        Assertions.assertEquals(-1, col.getUniqueId(),
                "a ConnectorColumn without withUniqueId() converts to a Doris column with the default -1 uniqueId");
    }

    @Test
    void convertColumnPropagatesUniqueId() {
        // WHY: the iceberg v3 row-lineage columns must keep their reserved field ids across the SPI boundary
        // (_row_id=2147483540, _last_updated_sequence_number=2147483539), which BE matches by field id when
        // reading lineage from iceberg data files. Post-flip the connector declares them through the schema
        // SPI as invisible() + withUniqueId(reservedId), so both markers must survive the immutable-copy
        // chain AND the converter must re-apply Column.setUniqueId(). The .withUniqueId(...).invisible()
        // chaining order verifies invisible() preserves the carried uniqueId.
        // MUTATION: dropping the setUniqueId(cc.getUniqueId()) re-apply leaves the id at -1 -> this turns red.
        ConnectorType bigintType = ConnectorColumnConverter.toConnectorType(ScalarType.BIGINT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("_row_id", bigintType, null, true, null)
                        .withUniqueId(2147483540).invisible());
        Assertions.assertEquals(2147483540, col.getUniqueId(),
                "an invisible ConnectorColumn carrying a reserved uniqueId must convert to a Doris column with that id");
        Assertions.assertFalse(col.isVisible(),
                "the invisible marker must survive alongside the carried uniqueId");
    }

    @Test
    void convertColumnDefaultsToNotReservedPassthrough() {
        // Regression guard: a ConnectorColumn that does not carry the reserved-passthrough marker must leave
        // the Doris Column at its default false — the converter must not stamp it where none was declared.
        ConnectorType bigintType = ConnectorColumnConverter.toConnectorType(ScalarType.BIGINT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("c", bigintType, null, true, null));
        Assertions.assertFalse(col.isReservedPassthrough(),
                "a ConnectorColumn without reservedPassthrough() converts to a non-passthrough Doris column");
    }

    @Test
    void convertColumnPropagatesReservedPassthroughMarker() {
        // WHY: the iceberg v3 row-lineage columns (_row_id / _last_updated_sequence_number) are declared
        // through the connector schema SPI as invisible() + withUniqueId(reservedId) + reservedPassthrough().
        // The engine's MERGE/UPDATE / sink binding must recognize them generically via
        // Column.isReservedPassthrough() instead of string-matching iceberg column names, so the neutral marker
        // must survive the immutable-copy chain AND the converter must re-apply Column.setReservedPassthrough(true).
        // MUTATION: dropping the setReservedPassthrough(true) re-apply leaves it false -> this turns red.
        ConnectorType bigintType = ConnectorColumnConverter.toConnectorType(ScalarType.BIGINT);
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("_row_id", bigintType, null, true, null)
                        .withUniqueId(2147483540).invisible().reservedPassthrough());
        Assertions.assertTrue(col.isReservedPassthrough(),
                "a reservedPassthrough ConnectorColumn must convert to a reserved-passthrough Doris column");
        Assertions.assertFalse(col.isVisible(),
                "the invisible marker must survive alongside the reserved-passthrough marker");
        Assertions.assertEquals(2147483540, col.getUniqueId(),
                "the carried uniqueId must survive alongside the reserved-passthrough marker");
    }

    @Test
    void convertColumnReconstructsIcebergRowIdHiddenColumn() {
        // CONTRACT PIN (fe-core half): the iceberg connector declares the request-scoped row-id synthetic write
        // column (__DORIS_ICEBERG_ROWID_COL__) as an engine-neutral invisible STRUCT ConnectorColumn through
        // ConnectorWritePlanProvider.getSyntheticWriteColumns (pinned connector-side by
        // IcebergWritePlanProviderTest.getSyntheticWriteColumnsDeclaresRowIdStruct). The row-id column identity
        // is owned by the connector alone — fe-core no longer keeps a duplicate STRUCT definition — so this pin
        // asserts that converting that exact agreed shape yields the Doris hidden column fe-core's getFullSchema
        // appends: name / STRUCT field names+order+types / hidden / not-null. If the connector's declared shape
        // drifts, this pin and IcebergWritePlanProviderTest turn red together.
        ConnectorType rowIdStruct = ConnectorType.structOf(
                Arrays.asList("file_path", "row_position", "partition_spec_id", "partition_data"),
                Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT"),
                        ConnectorType.of("INT"), ConnectorType.of("STRING")));
        Column converted = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("__DORIS_ICEBERG_ROWID_COL__", rowIdStruct,
                        "Iceberg row position metadata", false, null, false).invisible());

        Assertions.assertEquals(Column.ICEBERG_ROWID_COL, converted.getName());
        Assertions.assertFalse(converted.isVisible(), "the row-id column must be hidden");
        Assertions.assertFalse(converted.isAllowNull(), "the row-id column must be not-null");
        // Pin the exact STRUCT type (field names + order + scalar types). Load-bearing: IcebergMergeCommand
        // types the not-matched INSERT-branch row-id NULL literal from this column's type, and BE resolves the
        // STRUCT fields by name — so STRING must stay ScalarType.createStringType(), not drift to another form.
        StructType expected = new StructType(new ArrayList<>(Arrays.asList(
                new StructField("file_path", ScalarType.createStringType()),
                new StructField("row_position", ScalarType.createType(PrimitiveType.BIGINT)),
                new StructField("partition_spec_id", ScalarType.createType(PrimitiveType.INT)),
                new StructField("partition_data", ScalarType.createStringType()))));
        Assertions.assertEquals(expected, converted.getType(),
                "the converted STRUCT must equal the row-id struct type (STRING/BIGINT/INT/STRING)");
    }

    @Test
    void testToConnectorColumnPreservesKeyAndAggregation() {
        Column key = new Column("k", Type.INT, true, null, true, null, "");
        ConnectorColumn ck = ConnectorColumnConverter.toConnectorColumn(key);
        Assertions.assertTrue(ck.isKey());
        Assertions.assertFalse(ck.isAggregated());

        // WHY (B2): the iceberg connector rejects aggregated columns in ALTER ADD/MODIFY COLUMN
        // (validateCommonColumnInfo); toConnectorColumn must carry isAggregated across the SPI or the
        // connector could not tell an aggregated column apart from a plain one. A mutation reverting to
        // the 5-arg ctor (dropping these flags) makes isAggregated default false -> this assert goes red.
        Column agg = new Column("s", Type.INT, false, AggregateType.SUM, true, null, "");
        ConnectorColumn ca = ConnectorColumnConverter.toConnectorColumn(agg);
        Assertions.assertTrue(ca.isAggregated());
        Assertions.assertFalse(ca.isKey());
        Assertions.assertEquals("s", ca.getName());
    }

    @Test
    void toConnectorTypeCarriesStructFieldNullabilityAndComment() {
        // WHY (B2b): a complex MODIFY COLUMN diffs field-by-field on the connector side, and CREATE TABLE
        // must preserve a NOT NULL / commented STRUCT field. toConnectorType must thread each field's
        // nullability + comment across the SPI; a mutation dropping them flips these asserts.
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(new StructField("a", ScalarType.INT, "ca", true));
        fields.add(new StructField("b", ScalarType.createStringType(), null, false));
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(new StructType(fields));
        Assertions.assertTrue(ct.isChildNullable(0));
        Assertions.assertEquals("ca", ct.getChildComment(0));
        Assertions.assertFalse(ct.isChildNullable(1));
    }

    @Test
    void toConnectorTypeThreadsArrayElementNullability() {
        // Doris ARRAY elements are always nullable (ArrayType.getContainsNull() is hard-coded true), so the
        // threaded value is always true; honoring a NOT NULL element from a non-Doris source happens in the
        // connector schema builder (IcebergSchemaBuilderTest.testNestedNullabilityAndCommentPreserved).
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(ArrayType.create(ScalarType.INT, true));
        Assertions.assertTrue(ct.isChildNullable(0));
    }

    @Test
    void toConnectorTypeCarriesMapValueNullability() {
        // child index 1 is the MAP value (keys are always required).
        MapType notNullValue = new MapType(ScalarType.createStringType(), ScalarType.INT, true, false);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(notNullValue);
        Assertions.assertFalse(ct.isChildNullable(1));
    }

    @Test
    void convertColumnStampsNestedFieldIdsOntoChildTree() {
        // WHY (H-10 L3): post-flip iceberg nested-column pruning is only correct if EVERY level of the Doris
        // Column tree carries uniqueId = iceberg field-id (legacy IcebergUtils.updateIcebergColumnUniqueId set
        // them recursively). The connector carries the top-level id on ConnectorColumn.withUniqueId and the
        // per-child ids on ConnectorType.withChildrenFieldIds; convertColumn must stamp the whole child tree so
        // SlotTypeReplacer can rewrite the nested access path to ids and BE matches the pruned leaf by id (a -1
        // leaf is skipped -> NULL). MUTATION: dropping the applyNestedFieldIds call leaves children at -1 -> red.
        // struct<a:int (field-id 4), b:string (field-id 5)>, top-level field-id 3
        ConnectorType structType = ConnectorType.structOf(
                Arrays.asList("a", "b"),
                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")))
                .withChildrenFieldIds(Arrays.asList(4, 5));
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("s", structType, null, true, null).withUniqueId(3));
        Assertions.assertEquals(3, col.getUniqueId(), "top-level struct column carries field-id 3");
        Assertions.assertEquals(4, col.getChildren().get(0).getUniqueId(), "struct field a carries field-id 4");
        Assertions.assertEquals(5, col.getChildren().get(1).getUniqueId(), "struct field b carries field-id 5");
    }

    @Test
    void convertColumnStampsDeeplyNestedAndArrayMapFieldIds() {
        // Verifies the recursion descends through an ARRAY element into a nested STRUCT, and that ARRAY/MAP
        // child ordering ([item] / [key,value]) aligns with ConnectorType.getChildFieldId (legacy parity:
        // updateIcebergColumnUniqueId recursed via ListType/MapType .fields()).
        // array<struct<c:int (id 7)>> with array element id 6, top-level id 5
        ConnectorType innerStruct = ConnectorType.structOf(
                Arrays.asList("c"), Arrays.asList(ConnectorType.of("INT")))
                .withChildrenFieldIds(Arrays.asList(7));
        ConnectorType arrayType = ConnectorType.arrayOf(innerStruct)
                .withChildrenFieldIds(Arrays.asList(6));
        Column arrCol = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("arr", arrayType, null, true, null).withUniqueId(5));
        Assertions.assertEquals(5, arrCol.getUniqueId());
        Column elem = arrCol.getChildren().get(0);                 // array element (the struct)
        Assertions.assertEquals(6, elem.getUniqueId(), "array element carries field-id 6");
        Assertions.assertEquals(7, elem.getChildren().get(0).getUniqueId(),
                "nested struct field c carries field-id 7");

        // map<string (id 9), int (id 10)>, top-level id 8
        ConnectorType mapType = ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT"))
                .withChildrenFieldIds(Arrays.asList(9, 10));
        Column mapCol = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("m", mapType, null, true, null).withUniqueId(8));
        Assertions.assertEquals(8, mapCol.getUniqueId());
        Assertions.assertEquals(9, mapCol.getChildren().get(0).getUniqueId(), "map key carries field-id 9");
        Assertions.assertEquals(10, mapCol.getChildren().get(1).getUniqueId(), "map value carries field-id 10");
    }

    @Test
    void convertColumnLeavesNestedUniqueIdsUnsetWithoutFieldIds() {
        // Regression guard: a connector that does NOT carry nested field ids (no withChildrenFieldIds, e.g.
        // paimon) must leave every child uniqueId at the default -1 — applyNestedFieldIds must be inert, so a
        // non-iceberg connector's nested columns are never accidentally stamped.
        ConnectorType structType = ConnectorType.structOf(
                Arrays.asList("a", "b"),
                Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING")));
        Column col = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("s", structType, null, true, null));
        Assertions.assertEquals(-1, col.getChildren().get(0).getUniqueId());
        Assertions.assertEquals(-1, col.getChildren().get(1).getUniqueId());
    }

    @Test
    void connectorTypeChildFieldIdDefaultsAndEqualityExclusion() {
        // getChildFieldId returns -1 for unset / out-of-range indices; withChildrenFieldIds is excluded from
        // equals/hashCode (type identity stays the structural shape), matching childrenNullable/childrenComments
        // so existing equality-based schema-change detection is unaffected.
        ConnectorType bare = ConnectorType.structOf(
                Arrays.asList("a"), Arrays.asList(ConnectorType.of("INT")));
        Assertions.assertEquals(-1, bare.getChildFieldId(0), "unset child field id defaults to -1");
        Assertions.assertEquals(-1, bare.getChildFieldId(5), "out-of-range child field id defaults to -1");
        ConnectorType withIds = bare.withChildrenFieldIds(Arrays.asList(4));
        Assertions.assertEquals(4, withIds.getChildFieldId(0));
        Assertions.assertEquals(bare, withIds, "field ids must be excluded from equals (structural identity)");
        Assertions.assertEquals(bare.hashCode(), withIds.hashCode(), "field ids must be excluded from hashCode");
    }

    @Test
    void testToConnectorColumnsConvertsList() {
        java.util.List<ConnectorColumn> cols = ConnectorColumnConverter.toConnectorColumns(
                Arrays.asList(new Column("a", Type.INT), new Column("b", Type.INT)));
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("a", cols.get(0).getName());
        Assertions.assertEquals("b", cols.get(1).getName());
    }
}
