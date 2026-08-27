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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit coverage for the section 2.4 static validation matrix in
 * {@link LanceIndexMutationValidator}. Every check is exercised positively and negatively;
 * the per-op typed rejections that follow a successful validation live in
 * AlterTableCommandLanceIndexTest.
 */
public class LanceIndexMutationValidatorTest {

    private static LanceExternalCatalog filesystemCatalog() {
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.isRestCatalogConfigured()).thenReturn(false);
        return catalog;
    }

    private static LanceExternalCatalog restCatalog() {
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.isRestCatalogConfigured()).thenReturn(true);
        return catalog;
    }

    /**
     * Builds a Lance table mock whose getColumn lookup is case-insensitive, mirroring
     * ExternalTable.getColumn.
     */
    private static LanceExternalTable tableWithColumns(Map<String, Column> columns) {
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        Mockito.when(table.getColumn(Mockito.anyString())).thenAnswer(invocation -> {
            String name = invocation.getArgument(0);
            for (Map.Entry<String, Column> entry : columns.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }
            return null;
        });
        return table;
    }

    private static Column notNullColumn(String name, Type type) {
        return new Column(name, type, false, null, false, null, "");
    }

    private static Column nullableColumn(String name, Type type) {
        return new Column(name, type, false, null, true, null, "");
    }

    private static IndexDefinition indexDef(String name, List<String> cols, String indexTypeName,
            Map<String, String> properties) {
        return new IndexDefinition(name, false, cols, indexTypeName, properties, "");
    }

    private static IndexDefinition annDef(Map<String, String> properties) {
        return indexDef("idx", Collections.singletonList("v"), "ANN", properties);
    }

    private static Map<String, String> validAnnProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("index_type", "IVF_PQ");
        properties.put("metric", "l2");
        properties.put("num_partitions", "256");
        properties.put("num_sub_vectors", "16");
        return properties;
    }

    private static LanceExternalTable annTable() {
        return tableWithColumns(Collections.singletonMap("v",
                notNullColumn("v", new ArrayType(Type.FLOAT))));
    }

    private static void assertRejected(String expectedMessage, IndexDefinition def,
            LanceExternalTable table) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), table, def));
        Assertions.assertEquals(expectedMessage, exception.getDetailMessage());
    }

    @Test
    public void testAnnHappyPath() {
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        annDef(validAnnProperties())));
        // num_bits = 8 is the one accepted optional value.
        Map<String, String> withNumBits = validAnnProperties();
        withNumBits.put("num_bits", "8");
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        annDef(withNumBits)));
        // metric is optional.
        Map<String, String> noMetric = validAnnProperties();
        noMetric.remove("metric");
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        annDef(noMetric)));
        // Column lookup is case-insensitive.
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        indexDef("idx", Collections.singletonList("V"), "ANN", validAnnProperties())));
    }

    @Test
    public void testAnnPropertyKeysAndValuesAreCaseInsensitive() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Index_Type", "ivf_pq");
        properties.put("METRIC", "COSINE");
        properties.put("Num_Partitions", "256");
        properties.put("NUM_SUB_VECTORS", "16");
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        annDef(properties)));
    }

    @Test
    public void testAnnCaseVariantDuplicatePropertyRejected() {
        // Case-variant duplicates must fail deterministically regardless of map iteration order.
        for (String badValue : new String[] {"garbage", "256"}) {
            Map<String, String> properties = validAnnProperties();
            properties.put("NUM_PARTITIONS", badValue);
            assertRejected("Duplicate property 'NUM_PARTITIONS' for Lance ANN index",
                    annDef(properties), annTable());
        }
    }

    @Test
    public void testAnnNullableColumnRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("v",
                nullableColumn("v", new ArrayType(Type.FLOAT))));
        assertRejected("ANN index must be built on a column that is not nullable",
                annDef(validAnnProperties()), table);
    }

    @Test
    public void testAnnNonArrayColumnRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("v",
                notNullColumn("v", Type.FLOAT)));
        assertRejected("ANN index column must be array type", annDef(validAnnProperties()), table);
    }

    @Test
    public void testAnnDoubleItemTypeRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("v",
                notNullColumn("v", new ArrayType(Type.DOUBLE))));
        assertRejected("ANN index column item type must be float type",
                annDef(validAnnProperties()), table);
    }

    @Test
    public void testAnnIndexTypePropertyRequired() {
        Map<String, String> missing = validAnnProperties();
        missing.remove("index_type");
        assertRejected("Lance ANN index requires property \"index_type\" = \"IVF_PQ\"",
                annDef(missing), annTable());

        Map<String, String> wrongValue = validAnnProperties();
        wrongValue.put("index_type", "IVF_FLAT");
        assertRejected("Lance ANN index requires property \"index_type\" = \"IVF_PQ\"",
                annDef(wrongValue), annTable());
    }

    @Test
    public void testAnnMetricValidated() {
        Map<String, String> badMetric = validAnnProperties();
        badMetric.put("metric", "l1");
        assertRejected("metric must be one of l2, cosine, dot", annDef(badMetric), annTable());

        for (String metric : new String[] {"l2", "cosine", "dot", "L2", "Cosine", "DOT"}) {
            Map<String, String> properties = validAnnProperties();
            properties.put("metric", metric);
            Assertions.assertDoesNotThrow(
                    () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                            annDef(properties)));
        }
    }

    @Test
    public void testAnnPositiveIntegerProperties() {
        for (String key : new String[] {"num_partitions", "num_sub_vectors"}) {
            Map<String, String> missing = validAnnProperties();
            missing.remove(key);
            assertRejected(key + " must be a positive integer", annDef(missing), annTable());

            for (String badValue : new String[] {"0", "-1", "abc", "1.5"}) {
                Map<String, String> bad = validAnnProperties();
                bad.put(key, badValue);
                assertRejected(key + " must be a positive integer", annDef(bad), annTable());
            }
        }
    }

    @Test
    public void testAnnNumBitsMustBeEight() {
        Map<String, String> nine = validAnnProperties();
        nine.put("num_bits", "9");
        assertRejected("num_bits must be 8", annDef(nine), annTable());

        Map<String, String> notNumeric = validAnnProperties();
        notNumeric.put("num_bits", "abc");
        assertRejected("num_bits must be 8", annDef(notNumeric), annTable());
    }

    @Test
    public void testAnnUnknownPropertyRejected() {
        Map<String, String> properties = validAnnProperties();
        properties.put("Foo", "1");
        assertRejected("Unknown property 'Foo' for Lance ANN index", annDef(properties), annTable());
    }

    @Test
    public void testBtreeAllowedColumnTypes() {
        for (Type type : new Type[] {Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT, Type.LARGEINT,
                Type.FLOAT, Type.DOUBLE, Type.DEFAULT_DECIMALV3, Type.STRING, Type.DATEV2,
                Type.DATETIMEV2, Type.TIMESTAMP_TZ}) {
            LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                    notNullColumn("c", type)));
            IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BTREE",
                    Collections.emptyMap());
            Assertions.assertDoesNotThrow(
                    () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), table, def),
                    "BTREE should accept column type " + type);
        }
    }

    @Test
    public void testBtreeRejectedColumnTypes() {
        for (Type type : new Type[] {Type.BOOLEAN, Type.TIMEV2, Type.VARBINARY,
                new ArrayType(Type.INT)}) {
            LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                    notNullColumn("c", type)));
            IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BTREE",
                    Collections.emptyMap());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), table, def));
            Assertions.assertTrue(
                    exception.getDetailMessage().startsWith("BTREE index does not support column type"),
                    "unexpected message for type " + type + ": " + exception.getDetailMessage());
        }
    }

    @Test
    public void testBtreePropertiesRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                notNullColumn("c", Type.INT)));
        IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BTREE",
                Collections.singletonMap("k", "v"));
        assertRejected("BTREE indexes do not support properties", def, table);
    }

    @Test
    public void testBtreeNullableColumnRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                nullableColumn("c", Type.INT)));
        IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BTREE",
                Collections.emptyMap());
        assertRejected("BTREE index must be built on a column that is not nullable", def, table);
    }

    @Test
    public void testBitmapAllowedColumnTypes() {
        for (Type type : new Type[] {Type.BOOLEAN, Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT,
                Type.LARGEINT, Type.STRING, Type.DATEV2}) {
            LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                    notNullColumn("c", type)));
            IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BITMAP",
                    Collections.emptyMap());
            Assertions.assertDoesNotThrow(
                    () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), table, def),
                    "BITMAP should accept column type " + type);
        }
    }

    @Test
    public void testBitmapRejectedColumnTypes() {
        // LARGEINT (Arrow uint64) is integral and accepted, exactly as in the BTREE matrix.
        for (Type type : new Type[] {Type.FLOAT, Type.DATETIMEV2}) {
            LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                    notNullColumn("c", type)));
            IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BITMAP",
                    Collections.emptyMap());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), table, def));
            Assertions.assertTrue(
                    exception.getDetailMessage().startsWith("BITMAP index does not support column type"),
                    "unexpected message for type " + type + ": " + exception.getDetailMessage());
        }
    }

    @Test
    public void testBitmapPropertiesRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("c",
                notNullColumn("c", Type.INT)));
        IndexDefinition def = indexDef("idx", Collections.singletonList("c"), "BITMAP",
                Collections.singletonMap("k", "v"));
        assertRejected("BITMAP indexes do not support properties", def, table);
    }

    @Test
    public void testMultiColumnRejected() {
        LanceExternalTable table = tableWithColumns(Collections.singletonMap("v",
                notNullColumn("v", new ArrayType(Type.FLOAT))));
        IndexDefinition def = indexDef("idx", Arrays.asList("v", "v"), "ANN", validAnnProperties());
        assertRejected("Lance index must be built on exactly one column", def, table);
    }

    @Test
    public void testMissingColumnRejected() {
        IndexDefinition def = indexDef("idx", Collections.singletonList("nope"), "ANN",
                validAnnProperties());
        assertRejected("Index column 'nope' does not exist", def, annTable());
    }

    @Test
    public void testIndexNameLengthBoundIsInUtf8Bytes() {
        char[] chars = new char[64];
        Arrays.fill(chars, 'a');
        IndexDefinition sixtyFour = indexDef(new String(chars), Collections.singletonList("v"),
                "ANN", validAnnProperties());
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateCreateIndex(filesystemCatalog(), annTable(),
                        sixtyFour));

        char[] tooLong = new char[65];
        Arrays.fill(tooLong, 'a');
        IndexDefinition sixtyFive = indexDef(new String(tooLong), Collections.singletonList("v"),
                "ANN", validAnnProperties());
        assertRejected("index name too long, the index name length at most is 64.", sixtyFive, annTable());

        // 33 two-byte characters are 66 UTF-8 bytes: the bound counts bytes, not characters.
        StringBuilder multibyte = new StringBuilder();
        for (int i = 0; i < 33; i++) {
            multibyte.append('é');
        }
        IndexDefinition multibyteName = indexDef(multibyte.toString(), Collections.singletonList("v"),
                "ANN", validAnnProperties());
        assertRejected("index name too long, the index name length at most is 64.", multibyteName, annTable());
    }

    @Test
    public void testUnsupportedIndexTypesRejected() {
        // A CREATE INDEX without USING defaults to INVERTED, which is not a Lance index type.
        IndexDefinition inverted = indexDef("idx", Collections.singletonList("v"), "INVERTED",
                Collections.emptyMap());
        assertRejected("Lance catalog tables only support USING ANN, BTREE, or BITMAP", inverted, annTable());

        IndexDefinition ngram = indexDef("idx", Collections.singletonList("v"), "NGRAM_BF",
                Collections.emptyMap());
        assertRejected("Lance catalog tables only support USING ANN, BTREE, or BITMAP", ngram, annTable());

        IndexDefinition noUsing = indexDef("idx", Collections.singletonList("v"), null,
                Collections.emptyMap());
        assertRejected("Lance catalog tables only support USING ANN, BTREE, or BITMAP", noUsing, annTable());
    }

    @Test
    public void testRestCatalogRejectedBeforeAnyOtherCheck() {
        IndexDefinition createDef = indexDef("idx", Collections.singletonList("missing"), "BTREE",
                Collections.singletonMap("k", "v"));
        AnalysisException createException = Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexMutationValidator.validateCreateIndex(restCatalog(), annTable(), createDef));
        Assertions.assertEquals("CREATE INDEX is not supported for Lance REST catalogs",
                createException.getDetailMessage());

        IndexDefinition orReplaceDef = new IndexDefinition("idx", false, Collections.singletonList("v"),
                "BTREE", Collections.emptyMap(), "", true);
        AnalysisException orReplaceException = Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexMutationValidator.validateCreateIndex(restCatalog(), annTable(), orReplaceDef));
        Assertions.assertEquals("CREATE OR REPLACE INDEX is not supported for Lance REST catalogs",
                orReplaceException.getDetailMessage());

        AnalysisException dropException = Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexMutationValidator.validateDropIndex(restCatalog()));
        Assertions.assertEquals("DROP INDEX is not supported for Lance REST catalogs",
                dropException.getDetailMessage());
    }

    @Test
    public void testDropIndexOnFilesystemCatalogPasses() {
        Assertions.assertDoesNotThrow(
                () -> LanceIndexMutationValidator.validateDropIndex(filesystemCatalog()));
    }
}
