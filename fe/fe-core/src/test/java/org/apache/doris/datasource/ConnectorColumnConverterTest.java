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

package org.apache.doris.datasource;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.datasource.iceberg.IcebergRowId;

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
    void convertColumnReconstructsIcebergRowIdHiddenColumn() {
        // CONTRACT PIN (③ C3b-core, fe-core half): the iceberg connector declares the request-scoped row-id
        // synthetic write column (__DORIS_ICEBERG_ROWID_COL__) as an engine-neutral invisible STRUCT
        // ConnectorColumn through ConnectorWritePlanProvider.getSyntheticWriteColumns (pinned connector-side by
        // IcebergWritePlanProviderTest.getSyntheticWriteColumnsDeclaresRowIdStruct). fe-core cannot import the
        // connector, so this two-sided pin asserts that converting that exact agreed shape yields the legacy
        // fe-core IcebergRowId.createHiddenColumn() byte-for-byte (name / STRUCT type / hidden / not-null) — the
        // column post-flip getFullSchema appends. If the connector's literal and the legacy IcebergRowId drift
        // apart, one of the two sides turns red.
        ConnectorType rowIdStruct = ConnectorType.structOf(
                Arrays.asList("file_path", "row_position", "partition_spec_id", "partition_data"),
                Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT"),
                        ConnectorType.of("INT"), ConnectorType.of("STRING")));
        Column converted = ConnectorColumnConverter.convertColumn(
                new ConnectorColumn("__DORIS_ICEBERG_ROWID_COL__", rowIdStruct,
                        "Iceberg row position metadata", false, null, false).invisible());

        Column legacy = IcebergRowId.createHiddenColumn();
        Assertions.assertEquals(Column.ICEBERG_ROWID_COL, converted.getName());
        Assertions.assertEquals(legacy.getName(), converted.getName());
        Assertions.assertEquals(legacy.getType(), converted.getType(),
                "the converted STRUCT must equal the legacy IcebergRowId struct type");
        Assertions.assertFalse(converted.isVisible(), "the row-id column must be hidden");
        Assertions.assertEquals(legacy.isVisible(), converted.isVisible());
        Assertions.assertEquals(legacy.isAllowNull(), converted.isAllowNull());
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
    void testToConnectorColumnsConvertsList() {
        java.util.List<ConnectorColumn> cols = ConnectorColumnConverter.toConnectorColumns(
                Arrays.asList(new Column("a", Type.INT), new Column("b", Type.INT)));
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("a", cols.get(0).getName());
        Assertions.assertEquals("b", cols.get(1).getName());
    }
}
