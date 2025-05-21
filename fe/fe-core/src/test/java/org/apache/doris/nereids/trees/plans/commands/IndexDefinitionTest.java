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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.ArrayType;
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
