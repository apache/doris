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
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VariantTemplateIndexBinderTest {
    private static Column column(VariantField... fields) {
        return new Column("v", new VariantType(new ArrayList<>(List.of(fields)), Map.of()));
    }

    private static IndexDefinition index(String name, String pattern, String parser) {
        return new IndexDefinition(name, false, List.of("v"), "INVERTED",
                Map.of("field_pattern", pattern, "parser", parser), "");
    }

    private static void validate(Column column, IndexDefinition... indexes) {
        VariantTemplateIndexBinder.validate("v", column.getType(), List.of(indexes),
                TInvertedIndexFileStorageFormat.V2);
    }

    @Test
    public void testPatternCaseIsPreservedForValidationAndQueryBinding() {
        Column column = column(new VariantField("Event.Name", Type.STRING, "", PatternType.MATCH_NAME));
        validate(column, index("idx_name", "Event.Name", "english"));
        Assertions.assertThrows(AnalysisException.class,
                () -> validate(column, index("idx_wrong_case", "event.name", "english")));
        Assertions.assertEquals("Event.Name",
                VariantTemplateIndexBinder.firstMatchingPattern(column, List.of("Event", "Name")));
        Assertions.assertEquals("",
                VariantTemplateIndexBinder.firstMatchingPattern(column, List.of("event", "name")));
    }

    @Test
    public void testExactAndGlobMatchingAndFirstMatchRootFallback() {
        Column exact = column(new VariantField("a.*", Type.STRING, "", PatternType.MATCH_NAME));
        Assertions.assertEquals("", VariantTemplateIndexBinder.firstMatchingPattern(exact, List.of("a", "b")));
        Assertions.assertEquals("a.*", VariantTemplateIndexBinder.firstMatchingPattern(exact, List.of("a", "*")));
        Column globFirst = column(new VariantField("a.*", Type.STRING, ""),
                new VariantField("a.b", Type.INT, "", PatternType.MATCH_NAME));
        Assertions.assertEquals("a.*",
                VariantTemplateIndexBinder.firstMatchingPattern(globFirst, List.of("a", "b")));
        Column exactFirst = column(new VariantField("a.b", Type.INT, "", PatternType.MATCH_NAME),
                new VariantField("a.*", Type.STRING, ""));
        Assertions.assertEquals("a.b",
                VariantTemplateIndexBinder.firstMatchingPattern(exactFirst, List.of("a", "b")));
        Assertions.assertEquals("a.*",
                VariantTemplateIndexBinder.firstMatchingPattern(exactFirst, List.of("a", "c")));
        // Empty pattern is the binder's contract for selecting a root index.
        Assertions.assertEquals("", VariantTemplateIndexBinder.firstMatchingPattern(globFirst, List.of("other")));
    }

    @Test
    public void testMultipleIndexesAreGroupedByPattern() {
        Column text = column(new VariantField("a", Type.STRING, ""), new VariantField("b", Type.STRING, ""));
        validate(text, index("idx_a_en", "a", "english"), index("idx_a_none", "a", "none"),
                index("idx_b_en", "b", "english"));
        Assertions.assertThrows(AnalysisException.class,
                () -> validate(text, index("idx_a_en", "a", "english"), index("idx_a_en2", "a", "english")));
        Column numeric = column(new VariantField("a", Type.INT, ""), new VariantField("b", Type.INT, ""));
        validate(numeric, index("idx_a", "a", "none"), index("idx_b", "b", "none"));
        Assertions.assertThrows(AnalysisException.class,
                () -> validate(numeric, index("idx_a", "a", "none"), index("idx_a2", "a", "none")));
    }

    @Test
    public void testInvalidLegacyGlobIsSkippedAndLegacyNullMeansGlob() {
        Column legacy = column(new VariantField("[", Type.STRING, "", null),
                new VariantField("a.*", Type.STRING, "", null));
        Assertions.assertEquals("a.*",
                VariantTemplateIndexBinder.firstMatchingPattern(legacy, List.of("a", "b")));
        Assertions.assertEquals("", VariantTemplateIndexBinder.firstMatchingPattern(legacy, List.of("other")));
    }

    @Test
    public void testAppendedTemplatesKeepExistingIndexBindings() {
        VariantField existing = new VariantField("a.*", Type.STRING, "");
        Column before = column(existing);
        Column after = column(existing, new VariantField("a.b", Type.INT, "", PatternType.MATCH_NAME),
                new VariantField("z", Type.INT, "", PatternType.MATCH_NAME));
        List<Index> indexes = List.of(
                new Index(11, "idx_root", List.of("v"), IndexType.INVERTED, Map.of(), ""),
                new Index(12, "idx_a", List.of("v"), IndexType.INVERTED,
                        Map.of("field_pattern", "a.*", "parser", "english"), ""));
        VariantTemplateIndexBinder.validateIndexes(before, indexes, TInvertedIndexFileStorageFormat.V2);
        VariantTemplateIndexBinder.validateIndexes(after, indexes, TInvertedIndexFileStorageFormat.V2);
        for (List<String> path : List.of(List.of("a", "b"), List.of("a", "c"), List.of("other"))) {
            Assertions.assertEquals(VariantTemplateIndexBinder.firstMatchingPattern(before, path),
                    VariantTemplateIndexBinder.firstMatchingPattern(after, path));
        }
        Assertions.assertEquals("z", VariantTemplateIndexBinder.firstMatchingPattern(after, List.of("z")));
    }
}
