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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IndexDefinitionTest {
    private IndexDefinition newBloomFilterIndexDefinition() {
        return new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER", null, "comment");
    }

    private Column newCatalogColumn(Type type, boolean isKey) {
        Column column = new Column("col1", type);
        column.setIsKey(isKey);
        return column;
    }

    @Test
    void testVariantIndexFormatV1() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("variant_index", false, Lists.newArrayList("col1"), "INVERTED",
                                                  null, "comment");
        try {
            def.checkColumn(new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE, true,
                                                 null, "comment"), KeysType.UNIQUE_KEYS, true, TInvertedIndexFileStorageFormat.V1);
            Assertions.fail("No exception throws.");
        } catch (AnalysisException e) {
            org.junit.jupiter.api.Assertions.assertInstanceOf(
                    org.apache.doris.nereids.exceptions.AnalysisException.class, e);
            Assertions.assertTrue(e.getMessage().contains("not supported in inverted index format V1"));
        }
    }

    void testArrayTypeSupport() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("array_index", false, Lists.newArrayList("col1"),
                "INVERTED", null, "array test");

        // Test array of supported types
        def.checkColumn(new ColumnDefinition("col1",
                ArrayType.of(StringType.INSTANCE), false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);

        def.checkColumn(new ColumnDefinition("col1",
                ArrayType.of(IntegerType.INSTANCE), false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);

        def.checkColumn(new ColumnDefinition("col1",
                    ArrayType.of(ArrayType.of(StringType.INSTANCE)), false,
                    AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);

        // Test array of unsupported types
        try {
            // Array<Float>
            def.checkColumn(new ColumnDefinition("col1",
                    ArrayType.of(FloatType.INSTANCE), false,
                    AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);
            Assertions.fail("No exception throws for unsupported array element type (Float).");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("is not supported in"));
        }

        try {
            // Array<Array<String>>
            def.checkColumn(new ColumnDefinition("col1",
                    ArrayType.of(ArrayType.of(StringType.INSTANCE)), false,
                    AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);
            Assertions.fail("No exception throws for array of array type.");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("is not supported in"));
        }

        try {
            // Array<Map<String, Int>>
            def.checkColumn(new ColumnDefinition("col1",
                    ArrayType.of(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE)), false,
                    AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);
            Assertions.fail("No exception throws for array of map type.");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("is not supported in"));
        }

        try {
            // Array<Struct<name:String, age:Int>>
            ArrayList<StructField> fields = new ArrayList<>();
            fields.add(new StructField("name", StringType.INSTANCE, true, null));
            fields.add(new StructField("age", IntegerType.INSTANCE, true, null));
            def.checkColumn(new ColumnDefinition("col1",
                    ArrayType.of(new StructType(fields)), false,
                    AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1);
            Assertions.fail("No exception throws for array of struct type.");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("is not supported in"));
        }
    }

    @Test
    void testNgramBFIndex() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put("gram_size", "3");
        properties.put("bf_size", "10000");

        IndexDefinition def = new IndexDefinition("ngram_bf_index", false, Lists.newArrayList("col1"), "NGRAM_BF",
                                                  properties, "comment");
        def.checkColumn(
                new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null);
    }

    @Test
    void testBloomFilterIndexTypeName() {
        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                null, "comment");
        Assertions.assertEquals(IndexType.BLOOMFILTER, def.getIndexType());
    }

    @Test
    void testBloomFilterIndexSupportsBloomFilterFppProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bloom_filter_fpp", "0.05");

        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                properties, "comment");
        Assertions.assertDoesNotThrow(def::validate);
        Assertions.assertEquals("0.05", def.getProperties().get("bloom_filter_fpp"));
    }

    @Test
    void testBloomFilterIndexRejectsNonBloomFilterFppProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("foo", "bar");

        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                properties, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, def::validate);
        Assertions.assertEquals("BLOOMFILTER index only supports property bloom_filter_fpp",
                exception.getMessage());
    }

    @Test
    void testBloomFilterIndexRejectsInvalidBloomFilterFppProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bloom_filter_fpp", "0.1");

        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                properties, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, def::validate);
        Assertions.assertTrue(exception.getMessage().contains("Bloom filter fpp should in [1.0E-4, 0.05]"));
    }

    @Test
    void testBloomFilterIndexSupportsVariantColumnDefinition() {
        // Baseline check for the new named BLOOMFILTER path on VARIANT columns.
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null));
    }

    @Test
    void testBloomFilterIndexSupportsVariantCatalogColumn() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                newCatalogColumn(Type.VARIANT, true), KeysType.DUP_KEYS, false, null));
    }

    @Test
    void testBloomFilterIndexSupportsVariantColumnDefinitionWithV1StorageFormat() {
        // Inverted-index-specific VARIANT + V1 rejection must not leak into BLOOMFILTER.
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1));
    }

    @Test
    void testBloomFilterIndexSupportsVariantColumnDefinitionWithDefaultStorageFormat() {
        // DEFAULT may resolve to the old V1 rules elsewhere, so this regression test keeps
        // BLOOMFILTER isolated from inverted-index-only validation.
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.DEFAULT));
    }

    @Test
    void testBloomFilterIndexSupportsVariantCatalogColumnWithDefaultStorageFormat() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                newCatalogColumn(Type.VARIANT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.DEFAULT));
    }

    @Test
    void testBloomFilterIndexSupportsVariantCatalogColumnWithV1StorageFormat() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                newCatalogColumn(Type.VARIANT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.V1));
    }

    @Test
    void testBloomFilterIndexRejectsUnsupportedColumnDefinitionType() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        // BOOLEAN is accepted by neither legacy nor named bloom filters.
        ColumnDefinition columnDef = new ColumnDefinition("col1", BooleanType.INSTANCE, false, AggregateType.NONE,
                true, null, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                columnDef, KeysType.DUP_KEYS, false, null));
        Assertions.assertEquals(columnDef.getType() + " is not supported in bloom filter index. invalid column: "
                + columnDef.getName(),
                exception.getMessage());
    }

    @Test
    void testBloomFilterIndexSupportsValueColumnOnDupKeys() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null));
    }

    @Test
    void testBloomFilterIndexRejectsValueColumnOnAggKeysForColumnDefinition() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        // BLOOMFILTER follows the same key-column restriction as the existing secondary indexes.
        ColumnDefinition columnDef = new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE,
                true, null, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                columnDef, KeysType.AGG_KEYS, false, null));
        Assertions.assertEquals("index should only be used in columns of DUP_KEYS/UNIQUE_KEYS table"
                + " or key columns of AGG_KEYS table. invalid index: " + def.getIndexName(),
                exception.getMessage());
    }

    @Test
    void testBloomFilterIndexSupportsValueColumnOnUniqueKeysForCatalogColumn() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertDoesNotThrow(() -> def.checkColumn(newCatalogColumn(Type.STRING, false),
                KeysType.UNIQUE_KEYS, false, null));
    }

    @Test
    void testBloomFilterIndexRejectsUnsupportedCatalogColumnType() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        // Cover the catalog Column overload so both checkColumn(...) entry points stay aligned.
        Column column = newCatalogColumn(Type.BOOLEAN, false);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> def.checkColumn(column, KeysType.DUP_KEYS, false, null));
        Assertions.assertEquals(column.getDataType() + " is not supported in bloom filter index. invalid column: "
                + column.getName(), exception.getMessage());
    }

    @Test
    void testBloomFilterIndexRejectsValueColumnOnAggKeysForCatalogColumn() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> def.checkColumn(newCatalogColumn(Type.STRING, false), KeysType.AGG_KEYS, false, null));
        Assertions.assertEquals("index should only be used in columns of DUP_KEYS/UNIQUE_KEYS table"
                + " or key columns of AGG_KEYS table. invalid index: " + def.getIndexName(),
                exception.getMessage());
    }

    @Test
    void testBloomFilterIndexOnlySingleColumn() {
        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1", "col2"),
                "BLOOMFILTER", null, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, def::validate);
        Assertions.assertEquals("BLOOMFILTER index can only apply to a single column.", exception.getMessage());
    }

    @Test
    void testBloomFilterIndexToSqlUsesBloomFilterKeyword() {
        IndexDefinition def = newBloomFilterIndexDefinition();
        Assertions.assertTrue(def.toSql().contains("USING BLOOMFILTER"));
    }

    @Test
    void testNgramBFIndexOnlySingleColumn() {
        IndexDefinition def = new IndexDefinition("ngram_bf_index", false, Lists.newArrayList("col1", "col2"),
                "NGRAM_BF", null, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, def::validate);
        Assertions.assertEquals("NGRAM_BF index can only apply to a single column.", exception.getMessage());
    }

    @Test
    void testNgramBFBuildIndexValidateWithoutColumns() {
        IndexDefinition def = new IndexDefinition("ngram_bf_index", null, IndexType.NGRAM_BF);
        Assertions.assertDoesNotThrow(def::validate);
    }

    @Test
    void testInvalidNgramBFIndexColumnType() {
        Map<String, String> properties = new HashMap<>();
        properties.put("gram_size", "3");
        properties.put("bf_size", "10000");

        IndexDefinition def = new IndexDefinition("ngram_bf_index", false, Lists.newArrayList("col1"), "NGRAM_BF",
                                                  properties, "comment");
        Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(
                        new ColumnDefinition("col1", IntegerType.INSTANCE, false, AggregateType.NONE, true, null,
                                             "comment"),
                        KeysType.DUP_KEYS, false, null));
    }

    @Test
    void testNgramBFIndexInvalidSize() {
        Map<String, String> properties = new HashMap<>();
        properties.put("gram_size", "256");
        properties.put("bf_size", "10000");

        IndexDefinition def = new IndexDefinition("ngram_bf_index", false, Lists.newArrayList("col1"), "NGRAM_BF",
                                                  properties, "comment");
        Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null,
                                                     "comment"),
                                KeysType.DUP_KEYS, false, null));
    }

    @Test
    void testNgramBFIndexInvalidSize2() {
        Map<String, String> properties = new HashMap<>();
        properties.put("gram_size", "3");
        properties.put("bf_size", "65536");

        IndexDefinition def = new IndexDefinition("ngram_bf_index", false, Lists.newArrayList("col1"), "NGRAM_BF",
                                                  properties, "comment");
        Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null,
                                                     "comment"),
                                KeysType.DUP_KEYS, false, null));
    }
}
