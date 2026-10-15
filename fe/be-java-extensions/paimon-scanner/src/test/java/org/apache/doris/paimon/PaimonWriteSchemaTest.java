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

package org.apache.doris.paimon;

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class PaimonWriteSchemaTest {

    @Test
    public void testChangelogOperationsSetPaimonRowKind() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "id", "name", "score", "region"},
                true);

        Assertions.assertEquals(RowKind.INSERT,
                changelogRow(schema, PaimonWriteSchema.INSERT_OPERATION).getRowKind());
        Assertions.assertEquals(RowKind.UPDATE_AFTER,
                changelogRow(schema, PaimonWriteSchema.UPDATE_OPERATION).getRowKind());
        Assertions.assertEquals(RowKind.DELETE,
                changelogRow(schema, PaimonWriteSchema.DELETE_OPERATION).getRowKind());
    }

    @Test
    public void testUnknownChangelogOperationRejected() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "id"},
                true);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> tableRow(schema, (byte) 9, 1));
    }

    @Test
    public void testReorderedInputProducesTableSchemaRow() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "score", "name", "id"});
        Object[] values = new Object[] {
                BinaryString.fromString("south"),
                86.5D,
                BinaryString.fromString("erin"),
                5
        };

        InternalRow tableRow = tableRow(schema, values);

        Assertions.assertEquals(5, tableRow.getInt(0));
        Assertions.assertEquals("erin", tableRow.getString(1).toString());
        Assertions.assertEquals(86.5D, tableRow.getDouble(2));
        Assertions.assertEquals("south", tableRow.getString(3).toString());
    }

    @Test
    public void testPartialInputLeavesMissingTableFieldsNull() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "id"});
        Object[] values = new Object[] {
                BinaryString.fromString("east"),
                6
        };

        InternalRow tableRow = tableRow(schema, values);

        Assertions.assertEquals(6, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
        Assertions.assertTrue(tableRow.isNullAt(2));
        Assertions.assertEquals("east", tableRow.getString(3).toString());
    }

    @Test
    public void testUnknownColumnRejectedDuringInitialization() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonWriteSchema.create(tableType(), new String[] {"unknown"}));

        Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }

    @Test
    public void testOmittedNotNullDefaultIsAppliedBeforeTableWriter() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType(false, VarCharType.MAX_LENGTH),
                        null, "unknown")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 7);

        Assertions.assertEquals(7, tableRow.getInt(0));
        Assertions.assertEquals("unknown", tableRow.getString(1).toString());
    }

    @Test
    public void testOmittedNotNullWithoutDefaultIsLeftForTableWriterValidation() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING().notNull())));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 7);

        Assertions.assertEquals(7, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testReorderedInputOverridesDefaultsWhileOmittedFieldUsesDefault() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING(), null, "unknown"),
                new DataField(2, "score", new DoubleType()),
                new DataField(3, "region", DataTypes.STRING(), null, "north")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"region", "score", "id"});

        InternalRow tableRow = tableRow(schema,
                BinaryString.fromString("south"),
                92.5D,
                8);

        Assertions.assertEquals(8, tableRow.getInt(0));
        Assertions.assertEquals("unknown", tableRow.getString(1).toString());
        Assertions.assertEquals(92.5D, tableRow.getDouble(2));
        Assertions.assertEquals("south", tableRow.getString(3).toString());
    }

    @Test
    public void testExplicitNullIsNotReplacedByOmittedFieldDefault() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING(), null, "unknown")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"name", "id"});

        InternalRow tableRow = tableRow(schema,
                null,
                9);

        Assertions.assertEquals(9, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testExplicitNullForRequiredFieldReachesWriterValidation() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "payload", DataTypes.VARIANT().notNull())));
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType, new String[] {"id", "payload"});

        Assertions.assertTrue(schema.inputType().getTypeAt(1).isNullable());
        InternalRow tableRow = tableRow(schema, 9, null);

        Assertions.assertEquals(9, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testPaimonWriterDefaultsExplicitNullRouteFields() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "bucket_key", new IntType(), null, "1"),
                new DataField(1, "partition_key", new VarCharType(VarCharType.MAX_LENGTH),
                        null, "default-partition")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType, new String[] {"bucket_key", "partition_key"});

        InternalRow tableRow = tableRow(schema, null, null);
        Assertions.assertTrue(tableRow.isNullAt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(tableType);
        Assertions.assertNotNull(defaultValueRow);
        InternalRow writerRow = defaultValueRow.replaceRow(tableRow);
        Assertions.assertEquals(1, writerRow.getInt(0));
        Assertions.assertEquals("default-partition", writerRow.getString(1).toString());
    }

    @Test
    public void testOmittedComplexDefaultsUsePaimonInternalValues() {
        RowType nestedType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "numbers", DataTypes.ARRAY(DataTypes.INT()), null, "[1, 2, 3]"),
                new DataField(2, "properties",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), null, "{one -> 1, two -> 2}"),
                new DataField(3, "nested", nestedType, null, "{42, default-value}")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 10);

        InternalArray numbers = tableRow.getArray(1);
        Assertions.assertEquals(3, numbers.size());
        Assertions.assertEquals(1, numbers.getInt(0));
        Assertions.assertEquals(3, numbers.getInt(2));

        InternalMap properties = tableRow.getMap(2);
        Assertions.assertEquals(2, properties.size());
        Map<String, Integer> actualProperties = new HashMap<>();
        for (int i = 0; i < properties.size(); i++) {
            actualProperties.put(
                    properties.keyArray().getString(i).toString(),
                    properties.valueArray().getInt(i));
        }
        Assertions.assertEquals(1, actualProperties.get("one"));
        Assertions.assertEquals(2, actualProperties.get("two"));

        InternalRow nested = tableRow.getRow(3, 2);
        Assertions.assertEquals(42, nested.getInt(0));
        Assertions.assertEquals("default-value", nested.getString(1).toString());
    }

    @Test
    public void testCharPaddingPadsCharValuesToTheColumnLengthInCodePoints() {
        // Spark and Flink pad a CHAR(n) value with spaces to n characters when they write it, and Paimon compares
        // primary keys and hashes buckets by bytes, so an unpadded 'a' would be another key than their 'a  '. A
        // longer value is kept: Doris has already decided whether to cut it, and VARCHAR is never padded.
        // MUTATION: not padding, or padding to the length in bytes -> red.
        PaimonWriteSchema schema = PaimonWriteSchema.create(charTableType(),
                new String[] {"id", "code", "note"}, false, true);

        Assertions.assertEquals("a  ", tableRow(schema, 1, string("a"), string("a")).getString(1).toString());
        Assertions.assertEquals("中  ", tableRow(schema, 1, string("中"), null).getString(1).toString());
        Assertions.assertEquals("abc", tableRow(schema, 1, string("abc"), null).getString(1).toString());
        Assertions.assertEquals("abcd", tableRow(schema, 1, string("abcd"), null).getString(1).toString());
        Assertions.assertTrue(tableRow(schema, 1, null, null).isNullAt(1));
        Assertions.assertEquals("a", tableRow(schema, 1, string("a"), string("a")).getString(2).toString());
    }

    @Test
    public void testCharPaddingPadsCharValuesNestedInArraysMapsAndRows() {
        // Spark and Flink pad nested CHAR values too. A map keeps its entries in their order.
        // MUTATION: padding only top-level values -> red.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "codes", DataTypes.ARRAY(DataTypes.CHAR(3))),
                new DataField(1, "labels", DataTypes.MAP(DataTypes.CHAR(2), DataTypes.CHAR(3))),
                new DataField(2, "item", DataTypes.ROW(DataTypes.FIELD(3, "code", DataTypes.CHAR(3)),
                        DataTypes.FIELD(4, "count", DataTypes.INT()))),
                new DataField(5, "rows", DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD(6, "code", DataTypes.CHAR(2))))),
                new DataField(7, "numbers", DataTypes.ARRAY(DataTypes.INT()))));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"codes", "labels", "item", "rows", "numbers"}, false, true);
        GenericArray numbers = new GenericArray(new Object[] {1, 2});

        InternalRow tableRow = tableRow(schema,
                new GenericArray(new Object[] {string("a"), null, string("abc")}),
                new GenericMap(Collections.singletonMap(string("k"), string("v"))),
                GenericRow.of(string("a"), 7),
                new GenericArray(new Object[] {GenericRow.of(string("x"))}),
                numbers);

        InternalArray codes = tableRow.getArray(0);
        Assertions.assertEquals("a  ", codes.getString(0).toString());
        Assertions.assertTrue(codes.isNullAt(1));
        Assertions.assertEquals("abc", codes.getString(2).toString());
        InternalMap labels = tableRow.getMap(1);
        Assertions.assertEquals(1, labels.size());
        Assertions.assertEquals("k ", labels.keyArray().getString(0).toString());
        Assertions.assertEquals("v  ", labels.valueArray().getString(0).toString());
        InternalRow item = tableRow.getRow(2, 2);
        Assertions.assertEquals("a  ", item.getString(0).toString());
        Assertions.assertEquals(7, item.getInt(1));
        Assertions.assertEquals("x ", tableRow.getArray(3).getRow(0, 1).getString(0).toString());
        Assertions.assertSame(numbers, tableRow.getArray(4));

        Map<BinaryString, BinaryString> keys = new LinkedHashMap<>();
        keys.put(string("k"), string("1"));
        keys.put(string("ab"), string("2"));
        InternalMap padded = tableRow(schema, null, new GenericMap(keys), null, null, null).getMap(1);
        Assertions.assertEquals(2, padded.size());
        Assertions.assertEquals("k ", padded.keyArray().getString(0).toString());
        Assertions.assertEquals("ab", padded.keyArray().getString(1).toString());
        Assertions.assertEquals("1  ", padded.valueArray().getString(0).toString());
        Assertions.assertEquals("2  ", padded.valueArray().getString(1).toString());
    }

    @Test
    public void testMapKeysThatPaddingMakesEqualFailTheRow() {
        // A map holds one entry per key, and Paimon's Avro reader keeps only one of the two 'k ' keys that padding
        // makes of the distinct keys 'k' and 'k ', so such a map fails the row. Keys that were equal before padding
        // are kept as they are, and keys that stay distinct are padded.
        // MUTATION: no check, or failing keys that were equal before padding too -> red.
        RowType tableType = new RowType(Collections.singletonList(
                new DataField(0, "labels", DataTypes.MAP(DataTypes.CHAR(2), DataTypes.INT()))));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"labels"}, false, true);

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> tableRow(schema, map(new Object[] {string("k"), string("k ")}, new Object[] {1, 2})));
        Assertions.assertTrue(e.getMessage().contains("column 'labels'"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("keys 'k' and 'k ' into the same key 'k '"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("enable_paimon_write_char_padding"), e.getMessage());

        InternalMap equalKeys = tableRow(schema, map(new Object[] {string("k"), string("k")}, new Object[] {1, 2}))
                .getMap(0);
        Assertions.assertEquals(2, equalKeys.size());
        Assertions.assertEquals("k ", equalKeys.keyArray().getString(0).toString());
        Assertions.assertEquals("k ", equalKeys.keyArray().getString(1).toString());
        InternalMap distinctKeys = tableRow(schema,
                map(new Object[] {string("k"), string("kk")}, new Object[] {1, 2})).getMap(0);
        Assertions.assertEquals("k ", distinctKeys.keyArray().getString(0).toString());
        Assertions.assertEquals("kk", distinctKeys.keyArray().getString(1).toString());
        Assertions.assertEquals(2, distinctKeys.valueArray().getInt(1));
    }

    @Test
    public void testCharValuesAreWrittenAsTheyAreWhenPaddingIsOff() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(charTableType(),
                new String[] {"id", "code", "note"}, false, false);

        Assertions.assertEquals("a", tableRow(schema, 1, string("a"), null).getString(1).toString());
    }

    @Test
    public void testCharPaddingLeavesUpdateAndDeleteRowsAlone() {
        // An UPDATE or DELETE row carries the key that Doris read back from the table, which must stay byte-equal
        // to the stored key for Paimon to match it: a key an earlier Doris wrote without padding would otherwise
        // never be updated or deleted. MUTATION: padding every row kind -> red.
        PaimonWriteSchema schema = PaimonWriteSchema.create(charTableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "id", "code"}, true, true);

        Assertions.assertEquals("a  ", tableRow(schema, PaimonWriteSchema.INSERT_OPERATION, 1, string("a"))
                .getString(1).toString());
        Assertions.assertEquals("a", tableRow(schema, PaimonWriteSchema.UPDATE_OPERATION, 1, string("a"))
                .getString(1).toString());
        Assertions.assertEquals("a", tableRow(schema, PaimonWriteSchema.DELETE_OPERATION, 1, string("a"))
                .getString(1).toString());
    }

    @Test
    public void testCharPaddingLeavesTheRowKindFieldAlone() {
        // Paimon takes the row kind of a row from the value of its rowkind.field as it is (RowKind.fromShortString),
        // so a CHAR(3) '+I' padded to '+I ' would fail the write. The other CHAR columns are padded.
        // MUTATION: padding the rowkind.field column -> red.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "kind", new CharType(3)),
                new DataField(2, "code", new CharType(3))));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"id", "kind", "code"}, false, true, "kind");

        InternalRow tableRow = tableRow(schema, 1, string("+I"), string("a"));
        Assertions.assertEquals("+I", tableRow.getString(1).toString());
        Assertions.assertEquals("a  ", tableRow.getString(2).toString());
    }

    @Test
    public void testRowKindFieldValueDecidesWhetherARowIsPadded() {
        // A table with a rowkind.field takes the kind of a row from that field's value when Paimon writes it, so a
        // '-D', '-U' or '+U' row carries a key to match, as an UPDATE or DELETE row does, and is not padded: a '-D'
        // row whose key Doris read back as 'k' has to delete the stored 'k', not 'k  '. Only a '+I' row is padded,
        // whether Doris writes the value or the field's default fills it in; a NULL is left for Paimon to reject.
        // MUTATION: padding by the row's RowKind alone, or reading the value from the input only -> red.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "code", new CharType(false, 3)),
                new DataField(1, "kind", new VarCharType(2)),
                new DataField(2, "label", new CharType(3))));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"code", "kind", "label"}, false, true, "kind", Collections.singletonList("code"));

        InternalRow inserted = tableRow(schema, string("k"), string("+I"), string("a"));
        Assertions.assertEquals("k  ", inserted.getString(0).toString());
        Assertions.assertEquals("a  ", inserted.getString(2).toString());
        for (String kind : Arrays.asList("-D", "-U", "+U")) {
            InternalRow row = tableRow(schema, string("k"), string(kind), string("a"));
            Assertions.assertEquals("k", row.getString(0).toString(), kind);
            Assertions.assertEquals("a", row.getString(2).toString(), kind);
        }
        Assertions.assertEquals("k", tableRow(schema, string("k"), null, string("a")).getString(0).toString());

        for (String kind : Arrays.asList("+I", "-D")) {
            RowType withDefault = new RowType(Arrays.asList(
                    new DataField(0, "code", new CharType(false, 3)),
                    new DataField(1, "kind", new VarCharType(2), null, kind)));
            PaimonWriteSchema defaulted = PaimonWriteSchema.create(withDefault, new String[] {"code"}, false, true,
                    "kind", Collections.singletonList("code"));
            Assertions.assertEquals("+I".equals(kind) ? "k  " : "k",
                    tableRow(defaulted, string("k")).getString(0).toString(), kind);
        }
    }

    @Test
    public void testOrcTablePadsOnlyItsPartitionValues() {
        // ORC pads every CHAR value it writes and strips the trailing spaces from one it reads, so padding changes
        // nothing that ORC stores, only the bucket of a CHAR key: an UPDATE or DELETE that reads 'k' back would
        // hash another bucket than the padded 'k  ' went to, and miss the row. A partition value is read back as
        // it is stored, so it is padded, and a MERGE that inserts a short one fails as for any padded key, while a
        // short key that is not padded does not. MUTATION: padding every column of an ORC table -> red.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "p", new CharType(false, 3)),
                new DataField(1, "code", new CharType(false, 3)),
                new DataField(2, "labels", DataTypes.MAP(DataTypes.CHAR(2), DataTypes.INT()))));
        PaimonWriteSchema insert = PaimonWriteSchema.create(tableType, new String[] {"p", "code", "labels"},
                false, true, null, Arrays.asList("p", "code"), Collections.singletonList("p"), true);

        InternalRow row = tableRow(insert, string("x"), string("k"),
                map(new Object[] {string("k"), string("k ")}, new Object[] {1, 2}));
        Assertions.assertEquals("x  ", row.getString(0).toString());
        Assertions.assertEquals("k", row.getString(1).toString());
        Assertions.assertEquals(2, row.getMap(2).size());
        Assertions.assertEquals("k", row.getMap(2).keyArray().getString(0).toString());

        PaimonWriteSchema merge = PaimonWriteSchema.create(tableType,
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "p", "code"}, true, true, null,
                Arrays.asList("p", "code"), Collections.singletonList("p"), true);
        Assertions.assertEquals("k", tableRow(merge, PaimonWriteSchema.INSERT_OPERATION, string("abc"),
                string("k")).getString(1).toString());
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> tableRow(merge, PaimonWriteSchema.INSERT_OPERATION, string("x"), string("k")));
        Assertions.assertTrue(e.getMessage().contains("CHAR(3) primary key column 'p'"), e.getMessage());
    }

    @Test
    public void testOrcFilesAreReadFromTheTableFormatOptions() {
        // MUTATION: ignoring file.format.per.level -> red.
        Assertions.assertTrue(PaimonWriteSchema.writesOrcFiles(Collections.singletonMap("file.format", "orc")));
        Assertions.assertTrue(PaimonWriteSchema.writesOrcFiles(Collections.singletonMap("file.format", "ORC")));
        Map<String, String> perLevel = new HashMap<>();
        perLevel.put("file.format", "parquet");
        perLevel.put("file.format.per.level", "0:avro,5:orc");
        Assertions.assertTrue(PaimonWriteSchema.writesOrcFiles(perLevel));
        Assertions.assertFalse(PaimonWriteSchema.writesOrcFiles(Collections.emptyMap()));
        Assertions.assertFalse(PaimonWriteSchema.writesOrcFiles(Collections.singletonMap("file.format", "avro")));
        Assertions.assertFalse(PaimonWriteSchema.writesOrcFiles(
                Collections.singletonMap("file.format.per.level", "0:avro")));
    }

    @Test
    public void testMergeInsertOfAShortCharKeyFails() {
        // MERGE matches its source rows to the table and checks the keys it inserts for duplicates by the key as
        // Doris holds it, unpadded: 'k' and 'k ' are two keys there, and neither matches a stored 'k  '. Padding
        // a shorter key afterwards would make them one key, or let a NOT MATCHED row replace the stored row it
        // did not match, so such a row fails. Code points are counted, as padding counts them.
        // MUTATION: no check, or counting bytes -> red; checking UPDATE / DELETE rows too -> red below.
        PaimonWriteSchema schema = PaimonWriteSchema.create(charKeyTableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "code", "label"}, true, true, null,
                Collections.singletonList("code"));

        for (String shortKey : Arrays.asList("k", "k ", "中中")) {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> tableRow(schema, PaimonWriteSchema.INSERT_OPERATION, string(shortKey), string("x")));
            Assertions.assertTrue(e.getMessage().contains("CHAR(3) primary key column 'code'"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("rpad(<key>, 3, ' ')"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("enable_paimon_write_char_padding"), e.getMessage());
        }
    }

    @Test
    public void testMergeInsertPadsTheRestOfAFullLengthKeyRow() {
        // Padding does not change a key as long as the column, or a longer one Doris kept, so that row is written
        // with the key as MERGE saw it, and its CHAR value columns are padded as in any inserted row.
        PaimonWriteSchema schema = PaimonWriteSchema.create(charKeyTableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "code", "label"}, true, true, null,
                Collections.singletonList("code"));

        InternalRow tableRow = tableRow(schema, PaimonWriteSchema.INSERT_OPERATION, string("abc"), string("x"));
        Assertions.assertEquals("abc", tableRow.getString(0).toString());
        Assertions.assertEquals("x  ", tableRow.getString(1).toString());
        Assertions.assertEquals("中中中", tableRow(schema, PaimonWriteSchema.INSERT_OPERATION,
                string("中中中"), null).getString(0).toString());
        Assertions.assertEquals("abcd", tableRow(schema, PaimonWriteSchema.INSERT_OPERATION,
                string("abcd"), null).getString(0).toString());
    }

    @Test
    public void testShortCharKeysThatMergeDoesNotInsertAreWrittenAsBefore() {
        // UPDATE and DELETE rows carry the stored key and are never padded; with padding off nothing is padded;
        // and an INSERT statement is no changelog write, so its keys are padded and nothing fails.
        PaimonWriteSchema merge = PaimonWriteSchema.create(charKeyTableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "code", "label"}, true, true, null,
                Collections.singletonList("code"));
        Assertions.assertEquals("k", tableRow(merge, PaimonWriteSchema.UPDATE_OPERATION, string("k"), string("x"))
                .getString(0).toString());
        Assertions.assertEquals("k", tableRow(merge, PaimonWriteSchema.DELETE_OPERATION, string("k"), string("x"))
                .getString(0).toString());

        PaimonWriteSchema unpadded = PaimonWriteSchema.create(charKeyTableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "code", "label"}, true, false, null,
                Collections.singletonList("code"));
        Assertions.assertEquals("k", tableRow(unpadded, PaimonWriteSchema.INSERT_OPERATION, string("k"),
                string("x")).getString(0).toString());

        PaimonWriteSchema insert = PaimonWriteSchema.create(charKeyTableType(),
                new String[] {"code", "label"}, false, true, null, Collections.singletonList("code"));
        Assertions.assertEquals("k  ", tableRow(insert, string("k"), string("x")).getString(0).toString());
    }

    @Test
    public void testCharPaddingIsReadFromTheTableOptionsTheFeSets() {
        // FE puts doris.write.char-padding on the table it serializes for the writer (PaimonWriteBinding). An FE
        // that predates the option leaves it out and does not pad its static partition values, so neither do we.
        Assertions.assertTrue(PaimonWriteSchema.charPadding(
                Collections.singletonMap("doris.write.char-padding", "true")));
        Assertions.assertFalse(PaimonWriteSchema.charPadding(
                Collections.singletonMap("doris.write.char-padding", "false")));
        Assertions.assertFalse(PaimonWriteSchema.charPadding(Collections.emptyMap()));
    }

    @Test
    public void testDuplicateColumnRejectedDuringInitialization() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonWriteSchema.create(tableType(), new String[] {"id", "id"}));

        Assertions.assertTrue(exception.getMessage().contains("Duplicate"));
    }

    private static RowType tableType() {
        return new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType()),
                new DataField(2, "score", new DoubleType()),
                new DataField(3, "region", new VarCharType())));
    }

    private static RowType charKeyTableType() {
        return new RowType(Arrays.asList(
                new DataField(0, "code", new CharType(false, 3)),
                new DataField(1, "label", new CharType(3))));
    }

    private static RowType charTableType() {
        return new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "code", new CharType(3)),
                new DataField(2, "note", new VarCharType(3))));
    }

    private static BinaryString string(String value) {
        return BinaryString.fromString(value);
    }

    /** A map over key and value arrays, which unlike GenericMap can hold one key twice. */
    private static InternalMap map(Object[] keys, Object[] values) {
        InternalArray keyArray = new GenericArray(keys);
        InternalArray valueArray = new GenericArray(values);
        return new InternalMap() {
            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public InternalArray keyArray() {
                return keyArray;
            }

            @Override
            public InternalArray valueArray() {
                return valueArray;
            }
        };
    }

    private static InternalRow changelogRow(PaimonWriteSchema schema, byte operation) {
        return tableRow(schema,
                operation,
                11,
                BinaryString.fromString("value"),
                1.0D,
                BinaryString.fromString("east"));
    }

    private static InternalRow tableRow(PaimonWriteSchema schema, Object... values) {
        return schema.tableRow(GenericRow.of(values));
    }
}
