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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    @Test
    void testSniiInvertedIndexColumnTypes() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("snii_index", false, Lists.newArrayList("col1"),
                "INVERTED", null, "comment");

        def.checkColumn(new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true,
                        null, "comment"), KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new ColumnDefinition("col1", ArrayType.of(StringType.INSTANCE), false,
                        AggregateType.NONE, true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        // Scalar numerics are served by the SNII-native BKD.
        def.checkColumn(new ColumnDefinition("col1", IntegerType.INSTANCE, false, AggregateType.NONE,
                        true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new ColumnDefinition("col1", BigIntType.INSTANCE, false, AggregateType.NONE,
                        true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new ColumnDefinition("col1", DateTimeV2Type.SYSTEM_DEFAULT, false, AggregateType.NONE,
                        true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new ColumnDefinition("col1", DoubleType.INSTANCE, false, AggregateType.NONE,
                        true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        // A one-level ARRAY of numerics is served too: each element becomes its own
        // point under the row's id, so a row matches when any of its elements does.
        def.checkColumn(new ColumnDefinition("col1", ArrayType.of(IntegerType.INSTANCE), false,
                        AggregateType.NONE, true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        // A NESTED array has no flattening on the BE side and stays refused.
        AnalysisException nestedException = Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(new ColumnDefinition("col1",
                                ArrayType.of(ArrayType.of(IntegerType.INSTANCE)), false,
                                AggregateType.NONE, true, null, "comment"), KeysType.DUP_KEYS, false,
                        TInvertedIndexFileStorageFormat.SNII));
        Assertions.assertTrue(nestedException.getMessage().contains("not supported"));
    }

    @Test
    void testSniiInvertedIndexCatalogColumnTypes() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("snii_index", false, Lists.newArrayList("col1"),
                "INVERTED", null, "comment");

        def.checkColumn(new Column("col1", Type.STRING, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", org.apache.doris.catalog.ArrayType.create(Type.STRING), true),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII);

        def.checkColumn(new Column("col1", Type.INT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", Type.BIGINT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", Type.DATETIMEV2, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        def.checkColumn(new Column("col1", org.apache.doris.catalog.ArrayType.create(Type.INT), true),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII);
    }

    // A SNII index may be declared on a VARIANT column, exactly as on V2 and V3. The parent
    // column holds no postings of its own: each extracted sub-column is indexed under its own
    // suffix path, and it is the sub-column type that the BE writer factory routes on. The
    // parent type therefore cannot decide anything here, so it must not be refused here.
    @Test
    void testSniiAcceptsVariantColumn() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("snii_variant_index", false,
                Lists.newArrayList("col1"), "INVERTED", null, "comment");

        def.checkColumn(new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE,
                        true, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        def.checkColumn(new Column("col1", Type.VARIANT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
    }

    @Test
    void testValuesIndexProperties() {
        ColumnDefinition variant = new ColumnDefinition("col1", VariantType.INSTANCE, false,
                AggregateType.NONE, true, null, "");
        Map<Map<String, String>, String> rejected = Map.ofEntries(
                Map.entry(Map.of(), "requires a parser or analyzer"),
                Map.entry(Map.of("parser", "none"), "requires a parser or analyzer"),
                Map.entry(Map.of("analyzer", "none"), "requires a parser or analyzer"),
                Map.entry(Map.of("analyzer", "ValuesCustom"), "supports only built-in parsers and analyzers"),
                Map.entry(Map.of("analyzer", ""), "requires a parser or analyzer"),
                Map.entry(Map.of("normalizer", "lowercase"), "requires a parser or analyzer"),
                Map.entry(Map.of("parser", "english", "support_phrase", "true"), "support_phrase=true"),
                Map.entry(Map.of("parser", "english", "field_pattern", "a"), "field_pattern"),
                Map.entry(Map.of("parser", "english", "variant_index_scope", "paths"), "variant_index_scope"),
                Map.entry(Map.of("parser", "english", "variant_index_scope", "all_values"), "variant_index_scope"));
        IndexPolicyMgr policies = new IndexPolicyMgr();
        policies.replayCreateIndexPolicy(new IndexPolicy(99, "ValuesCustom", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "standard")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policies);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            rejected.forEach((properties, message) -> {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class, () -> valuesIndex(properties)
                        .checkColumn(variant, KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII));
                Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
            });
        }
        for (String key : List.of("variant_index_mode", "variant_root_format_version")) {
            IndexDefinition obsolete = new IndexDefinition("old", false, List.of("col1"), "INVERTED",
                    Map.of("parser", "english", key, "1"), "");
            Assertions.assertThrows(AnalysisException.class, () -> obsolete.checkColumn(variant,
                    KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII));
        }
        IndexDefinition values = valuesIndex(Map.of("parser", "english"));
        Map<String, String> persisted = values.translateToCatalogStyle().getProperties();
        Assertions.assertEquals(Map.of("variant_index_scope", "values", "parser", "english",
                "support_phrase", "false", "lower_case", "true"), persisted);
        Assertions.assertDoesNotThrow(() -> valuesIndex(persisted).checkColumn(variant,
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII));
        Assertions.assertThrows(AnalysisException.class, () -> values.checkColumn(variant,
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V2));
    }

    @Test
    void testValuesIndexSelectorsAndModels() {
        ColumnDefinition variant = new ColumnDefinition("col1", VariantType.INSTANCE, false,
                AggregateType.NONE, true, null, "");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(new IndexPolicyMgr());
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            for (String key : List.of("parser", "analyzer")) {
                List<String> selectors = key.equals("parser") ? List.of("english", "unicode", "chinese", "standard", "icu")
                        : List.of("standard", "unicode", "chinese", "icu", " UnIcOdE ");
                for (String selector : selectors) {
                    Assertions.assertDoesNotThrow(() -> valuesIndex(Map.of(key, selector)).checkColumn(variant,
                            KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII));
                }
                IndexDefinition values = valuesIndex(Map.of(key, "unicode"));
                Assertions.assertDoesNotThrow(() -> values.checkColumn(variant, KeysType.UNIQUE_KEYS,
                        true, TInvertedIndexFileStorageFormat.SNII));
                for (KeysType model : List.of(KeysType.UNIQUE_KEYS, KeysType.AGG_KEYS)) {
                    Assertions.assertThrows(AnalysisException.class, () -> values.checkColumn(variant,
                            model, false, TInvertedIndexFileStorageFormat.SNII));
                }
            }
        }
    }

    @Test
    void testValuesIndexUnsupportedTypes() {
        List<DataType> types = List.of(StringType.INSTANCE,
                new VariantType(List.of(), 0, false, 10000, 0, false, 0L, 64, true),
                new VariantType(List.of(new VariantField("wide", DecimalV3Type.createDecimalV3TypeNotCheck256(76, 2), ""))),
                new VariantType(List.of(new VariantField("wide",
                        ArrayType.of(DecimalV3Type.createDecimalV3TypeNotCheck256(76, 2)), ""))));
        List<String> messages = List.of("only be used on VARIANT", "does not support NestedGroup",
                "does not support declared Decimal256", "does not support declared Decimal256");
        for (int i = 0; i < types.size(); i++) {
            DataType type = types.get(i);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> valuesIndex(Map.of("parser", "english")).checkColumn(new ColumnDefinition("col1", type,
                            false, AggregateType.NONE, true, null, ""), KeysType.DUP_KEYS, false,
                            TInvertedIndexFileStorageFormat.SNII), type.toString());
            Assertions.assertTrue(error.getMessage().contains(messages.get(i)), error.getMessage());
        }
    }

    private IndexDefinition valuesIndex(Map<String, String> extra) {
        Map<String, String> properties = new HashMap<>();
        properties.put("variant_index_scope", "values");
        properties.putAll(extra);
        return new IndexDefinition("values_idx", false, List.of("col1"), "INVERTED", properties, "");
    }

    // SNII stores an ANN index as a blob logical index, the same mechanism the
    // native BKD uses, so it accepts ANN exactly as V2/V3 do. V1 still cannot:
    // it has no container to put one in.
    @Test
    void testSniiAcceptsAnnIndex() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("ann_index", false, Lists.newArrayList("col1"),
                "ANN", null, "comment");
        def.checkColumn(new ColumnDefinition("col1", ArrayType.of(FloatType.INSTANCE), false,
                        AggregateType.NONE, false, null, "comment"), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", org.apache.doris.catalog.ArrayType.create(Type.FLOAT), false),
                KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.SNII);
    }

    @Test
    void testV1RejectsAnnIndex() {
        IndexDefinition def = new IndexDefinition("ann_index", false, Lists.newArrayList("col1"),
                "ANN", null, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(new ColumnDefinition("col1", ArrayType.of(FloatType.INSTANCE), false,
                                AggregateType.NONE, false, null, "comment"), KeysType.DUP_KEYS, false,
                        TInvertedIndexFileStorageFormat.V1));
        Assertions.assertTrue(exception.getMessage().contains("ANN index is not supported in index format V1"));

        AnalysisException catalogException = Assertions.assertThrows(AnalysisException.class, () ->
                def.checkColumn(new Column("col1", org.apache.doris.catalog.ArrayType.create(Type.FLOAT), false),
                        KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V1));
        Assertions.assertTrue(catalogException.getMessage().contains(
                "ANN index is not supported in index format V1"));
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
        Assertions.assertDoesNotThrow(() -> def.checkColumn(
                new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null));
        Assertions.assertEquals("0.05", def.getProperties().get("bloom_filter_fpp"));
    }

    @Test
    void testBloomFilterIndexRejectsNonBloomFilterFppProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("foo", "bar");

        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                properties, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null));
        Assertions.assertEquals("BLOOMFILTER index only supports property bloom_filter_fpp",
                exception.getMessage());
    }

    @Test
    void testBloomFilterIndexRejectsInvalidBloomFilterFppProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bloom_filter_fpp", "0.1");

        IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                properties, "comment");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                KeysType.DUP_KEYS, false, null));
        Assertions.assertTrue(exception.getMessage().contains("Bloom filter fpp should in [1.0E-4, 0.05]"));
    }

    @Test
    void testBloomFilterIndexRejectsNonFiniteBloomFilterFppProperty() {
        for (String fpp : Lists.newArrayList("NaN", "Infinity", "-Infinity")) {
            Map<String, String> properties = new HashMap<>();
            properties.put("bloom_filter_fpp", fpp);

            IndexDefinition def = new IndexDefinition("bf_index", false, Lists.newArrayList("col1"), "BLOOMFILTER",
                    properties, "comment");
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                    new ColumnDefinition("col1", StringType.INSTANCE, false, AggregateType.NONE, true, null, "comment"),
                    KeysType.DUP_KEYS, false, null));
            Assertions.assertTrue(exception.getMessage().contains("Bloom filter fpp should in [1.0E-4, 0.05]"));
        }
    }

    @Test
    void testBloomFilterIndexSupportsVariantColumnDefinition() {
        // Baseline check for BfIndex on VARIANT columns.
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
        // BOOLEAN is accepted by neither BfColumns nor BfIndex.
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
