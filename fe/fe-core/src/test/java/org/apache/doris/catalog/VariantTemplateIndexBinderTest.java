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

package org.apache.doris.catalog;

import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VariantTemplateIndexBinderTest {
    private static Column column(VariantField... fields) {
        return new Column("v", new VariantType(new ArrayList<>(List.of(fields))));
    }

    private static VariantField field(String name, Type type, PatternType mode) {
        return new VariantField(name, type, "", mode);
    }

    private static IndexDefinition definition(String name, Map<String, String> properties) {
        return new IndexDefinition(name, false, List.of("v"), "INVERTED", properties, "");
    }

    private static Index index(long id, String pattern) {
        return new Index(id, "idx" + id, List.of("v"), IndexType.INVERTED,
                pattern.isEmpty() ? Map.of() : Map.of("field_pattern", pattern), "");
    }

    private static org.apache.doris.nereids.types.VariantType createType(
            org.apache.doris.nereids.types.VariantField... fields) {
        return new org.apache.doris.nereids.types.VariantType(List.of(fields));
    }

    private static org.apache.doris.nereids.types.VariantField createField(
            String name, DataType type, PatternType mode) {
        return new org.apache.doris.nereids.types.VariantField(name, type, "", mode.name());
    }

    @Test
    public void testCreateBindingKeepsPatternCaseAndSupportsBothModes() {
        org.apache.doris.nereids.types.VariantType type = createType(
                createField("A", IntegerType.INSTANCE, PatternType.MATCH_NAME),
                createField("a.*", StringType.INSTANCE, PatternType.MATCH_NAME_GLOB));
        VariantTemplateIndexBinder.validate("v", type, List.of(definition("exact", Map.of("field_pattern", "A")),
                definition("glob", Map.of("field_pattern", "a.*", "parser", "english"))),
                TInvertedIndexFileStorageFormat.V2);
        Assertions.assertEquals("can not find field pattern: a in column: v",
                Assertions.assertThrows(AnalysisException.class, () -> VariantTemplateIndexBinder.validate("v", type,
                        List.of(definition("wrong_case", Map.of("field_pattern", "a"))),
                        TInvertedIndexFileStorageFormat.V2)).getMessage());
    }

    @Test
    public void testCreateTypeAndParserDiagnostics() {
        org.apache.doris.nereids.types.VariantType unsupported = createType(createField("map",
                org.apache.doris.nereids.types.MapType.of(StringType.INSTANCE, IntegerType.INSTANCE),
                PatternType.MATCH_NAME));
        Assertions.assertEquals("field pattern: map is not supported for inverted index of column: v",
                Assertions.assertThrows(AnalysisException.class, () -> VariantTemplateIndexBinder.validate(
                        "v", unsupported, List.of(definition("map_idx", Map.of("field_pattern", "map"))),
                        TInvertedIndexFileStorageFormat.V2)).getMessage());
        org.apache.doris.nereids.types.VariantType numeric = createType(
                createField("n", IntegerType.INSTANCE, PatternType.MATCH_NAME));
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> VariantTemplateIndexBinder.validate("v", numeric,
                        List.of(definition("parsed_number", Map.of("field_pattern", "n", "parser", "english"))),
                        TInvertedIndexFileStorageFormat.V2));
        Assertions.assertTrue(ex.getMessage().startsWith("invalid INVERTED index: field pattern: n, "));
        Assertions.assertTrue(ex.getMessage().contains("is not supported for column: v of type INT"));
    }

    @Test
    public void testMultipleIndexesRemainGroupedByPattern() {
        org.apache.doris.nereids.types.VariantType type = createType(
                createField("s", StringType.INSTANCE, PatternType.MATCH_NAME),
                createField("n", IntegerType.INSTANCE, PatternType.MATCH_NAME));
        VariantTemplateIndexBinder.validate("v", type, List.of(
                definition("s_plain", Map.of("field_pattern", "s")),
                definition("s_parsed", Map.of("field_pattern", "s", "parser", "english")),
                definition("n_plain", Map.of("field_pattern", "n"))), TInvertedIndexFileStorageFormat.V2);
        Assertions.assertEquals("column: v cannot have multiple inverted indexes of the same type"
                        + " with field pattern: n",
                Assertions.assertThrows(AnalysisException.class, () -> VariantTemplateIndexBinder.validate("v", type,
                        List.of(definition("n1", Map.of("field_pattern", "n")),
                                definition("n2", Map.of("field_pattern", "n"))),
                        TInvertedIndexFileStorageFormat.V2)).getMessage());
        Assertions.assertEquals("column: v cannot have multiple inverted indexes of the same type with field pattern: ",
                Assertions.assertThrows(AnalysisException.class, () -> VariantTemplateIndexBinder.validate("v", type,
                        List.of(definition("root1", Map.of()), definition("root2", Map.of())),
                        TInvertedIndexFileStorageFormat.V2)).getMessage());
    }

    @Test
    public void testCreateUsesOriginalNereidsTypesAndDoesNotConvertUnindexedFields() {
        DataType indexed = Mockito.spy(DecimalV3Type.createDecimalV3Type(18, 4));
        DataType unindexed = Mockito.spy(DecimalV3Type.createDecimalV3Type(18, 6));
        org.apache.doris.nereids.types.VariantType type = createType(
                createField("amount", indexed, PatternType.MATCH_NAME),
                createField("unindexed", unindexed, PatternType.MATCH_NAME));
        VariantTemplateIndexBinder.validate("v", type,
                List.of(definition("root", Map.of()), definition("amount", Map.of("field_pattern", "amount"))),
                TInvertedIndexFileStorageFormat.V2);
        // Type support uses the original object; only the indexed field's parser primitive is converted.
        Mockito.verify(indexed).isDecimalLikeType();
        Mockito.verify(indexed).toCatalogDataType();
        Mockito.verify(unindexed, Mockito.never()).toCatalogDataType();
    }

    @Test
    public void testQueryUsesFirstMatchingTemplateAndKeepsRootFallback() {
        VariantField glob = field("a.*", Type.STRING, PatternType.MATCH_NAME_GLOB);
        VariantField exact = field("a.b", Type.STRING, PatternType.MATCH_NAME);
        Index globIndex = index(1, "a.*");
        Index exactIndex = index(2, "a.b");
        Index rootIndex = index(3, "");
        List<Index> indexes = List.of(globIndex, exactIndex, rootIndex);
        Assertions.assertEquals(List.of(globIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(glob, exact), List.of("a", "b"), indexes));
        Assertions.assertEquals(List.of(exactIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact, glob), List.of("a", "b"), indexes));
        Assertions.assertEquals(List.of(rootIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact, glob), List.of("A", "b"), indexes));
        Assertions.assertEquals(List.of(), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact), List.of("a", "b"), List.of(rootIndex)));
        OlapTable table = new OlapTable();
        table.setIndexes(List.of(rootIndex));
        Assertions.assertSame(rootIndex, table.getInvertedIndex(column(exact), List.of("a", "b")));
    }

    @Test
    public void testQueryBindingUsesTheDeclaredPatternMode() {
        VariantField exact = field("a.*", Type.STRING, PatternType.MATCH_NAME);
        VariantField glob = field("a.*", Type.STRING, PatternType.MATCH_NAME_GLOB);
        Index fieldIndex = index(1, "a.*");
        Index rootIndex = index(2, "");
        List<Index> indexes = List.of(fieldIndex, rootIndex);
        Assertions.assertEquals(List.of(rootIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact), List.of("a", "b"), indexes));
        Assertions.assertEquals(List.of(fieldIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(glob), List.of("a", "b"), indexes));
        Assertions.assertEquals(List.of(fieldIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact), List.of("a", "*"), indexes));
        // Existing index metadata has no independent match-mode field. Preserve duplicate-text
        // metadata behavior: the first matching template controls, just as on the BE.
        Assertions.assertEquals(List.of(fieldIndex), VariantTemplateIndexBinder.getMatchingIndexes(
                column(exact, glob), List.of("a", "b"), indexes));
    }

    @Test
    public void testInvalidLegacyGlobAndMissingChildrenKeepExistingReadBehavior() {
        Column column = column(field("[", Type.STRING, PatternType.MATCH_NAME_GLOB),
                field("x", Type.STRING, PatternType.MATCH_NAME));
        Index index = index(1, "x");
        Index root = index(2, "");
        Assertions.assertEquals(List.of(index), VariantTemplateIndexBinder.getMatchingIndexes(
                column, List.of("x"), List.of(index, root)));
        Assertions.assertEquals(List.of(root), VariantTemplateIndexBinder.getMatchingIndexes(
                column(), List.of("x"), List.of(index, root)));
    }
}
