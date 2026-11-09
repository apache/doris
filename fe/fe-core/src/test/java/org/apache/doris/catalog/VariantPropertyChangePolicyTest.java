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

import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VariantPropertyChangePolicyTest {
    private static VariantField field(String pattern) {
        return new VariantField(pattern, Type.INT, "");
    }

    private static Column column(VariantField... fields) {
        return column(10, false, 10000, 0, false, 0L, 64, false, fields);
    }

    private static Column column(int count, boolean sparse, int stats, int shards, boolean doc,
            long minRows, int docShards, boolean nested, VariantField... fields) {
        return new Column("v", new VariantType(new ArrayList<>(Arrays.asList(fields)), count, sparse,
                stats, shards, doc, minRows, docShards, nested));
    }

    private static void rejected(Column source, Column target, String message) {
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> source.checkSchemaChangeAllowed(target));
        Assertions.assertTrue(ex.getMessage().contains(message), ex.getMessage());
    }

    @Test
    public void testAppendAndCountChange() throws Exception {
        Column source = column(field("a"));
        source.checkSchemaChangeAllowed(column(field("a"), field("b")));
        source.checkSchemaChangeAllowed(column(20, false, 10000, 0, false, 0L, 64, false, field("a")));
        source.checkSchemaChangeAllowed(column(0, false, 10000, 0, false, 0L, 64, false, field("a")));
        source.checkSchemaChangeAllowed(column(field("a")));
    }

    @Test
    public void testOnlyOrderedPrefixCanBeRetained() {
        Column source = column(field("a"), field("b"));
        for (Column target : List.of(column(field("a")), column(field("b"), field("a")),
                column(field("a"), field("c")),
                column(field("a"), new VariantField("b", Type.BIGINT, "")),
                column(field("a"), new VariantField("b", Type.INT, "", PatternType.MATCH_NAME)),
                column(field("a"), new VariantField("b", Type.INT, "comment")))) {
            rejected(source, target, "Can only append variant schema templates");
        }
        rejected(source, column(field("a"), field("b"), field("a")), "Duplicate variant schema template pattern");
        rejected(source, column(field("a"), field("b"), field("c"), field("c")),
                "Duplicate variant schema template pattern");
    }

    @Test
    public void testEntryComparisonIncludesNestedNullabilityAndLegacyPatternType() {
        Assertions.assertTrue(VariantPropertyChangePolicy.isAllowedEntryChange(
                new VariantField("a", Type.INT, "", null), field("a")));
        StructField nullable = new StructField("x", Type.INT, "", true);
        StructField nonNullable = new StructField("x", Type.INT, "", false);
        Assertions.assertFalse(VariantPropertyChangePolicy.isAllowedEntryChange(
                new VariantField("a", new MapType(Type.STRING, Type.INT, true, true), ""),
                new VariantField("a", new MapType(Type.STRING, Type.INT, true, false), "")));
        Assertions.assertFalse(VariantPropertyChangePolicy.isAllowedEntryChange(
                new VariantField("a", new ArrayType(new StructType(new ArrayList<>(List.of(nullable)))), ""),
                new VariantField("a", new ArrayType(new StructType(new ArrayList<>(List.of(nonNullable)))), "")));
    }

    @Test
    public void testEntryHookIsSingleWideningPointAndNotChangeDetection() throws Exception {
        VariantField before = field("a");
        VariantField widened = new VariantField("a", Type.BIGINT, "");
        Column source = column(before);
        Column target = column(widened);
        try (MockedStatic<VariantPropertyChangePolicy> policy = Mockito.mockStatic(
                VariantPropertyChangePolicy.class, Mockito.CALLS_REAL_METHODS)) {
            policy.when(() -> VariantPropertyChangePolicy.isAllowedEntryChange(before, widened))
                    .thenReturn(true);
            policy.clearInvocations();
            // A future hook allowing widening must not make equality-based change detection false.
            Assertions.assertTrue(VariantPropertyChangePolicy.templatesChanged(source, target));
            Assertions.assertDoesNotThrow(() -> VariantPropertyChangePolicy.checkColumnLevel(source, target));
            policy.verify(() -> VariantPropertyChangePolicy.isAllowedEntryChange(before, widened));
        }
        try (MockedStatic<VariantPropertyChangePolicy> policy = Mockito.mockStatic(
                VariantPropertyChangePolicy.class, Mockito.CALLS_REAL_METHODS)) {
            policy.when(() -> VariantPropertyChangePolicy.isAllowedEntryChange(before, before))
                    .thenReturn(false);
            policy.clearInvocations();
            // Retained prefix entries also go through the hook when a new entry is appended.
            rejected(source, column(before, field("b")), "Can only append variant schema templates");
            policy.verify(() -> VariantPropertyChangePolicy.isAllowedEntryChange(before, before));
        }
    }

    @Test
    public void testRestrictedModesAndDocMinRows() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            boolean sparse = mode == 0;
            boolean doc = mode == 1;
            boolean nested = mode == 2;
            Column source = column(10, sparse, 10000, 0, doc, 0L, 64, nested, field("a"));
            rejected(source, column(20, sparse, 10000, 0, doc, 0L, 64, nested, field("a")),
                    "with doc mode, nested group or typed paths to sparse enabled");
            rejected(source, column(10, sparse, 10000, 0, doc, 0L, 64, nested, field("a"), field("b")),
                    "with doc mode, nested group or typed paths to sparse enabled");
            source.checkSchemaChangeAllowed(column(10, sparse, 10000, 0, doc, 100L, 64, nested, field("a")));
        }
    }

    @Test
    public void testImmutablePropertyMessages() {
        Column source = column();
        rejected(source, column(10, true, 10000, 0, false, 0L, 64, false),
                "Can not change variant enable typed paths to sparse");
        rejected(source, column(10, false, 123, 0, false, 0L, 64, false),
                "Can not change variant max sparse column statistics size");
        rejected(source, column(10, false, 10000, 4, false, 0L, 64, false),
                "Can not change variant sparse bucket num");
        rejected(source, column(10, false, 10000, 0, true, 0L, 64, false),
                "Can not change variant enable doc snapshot mode");
        rejected(source, column(10, false, 10000, 0, false, 0L, 128, false),
                "Can not change variant doc snapshot shard count");
        rejected(source, column(10, false, 10000, 0, false, 0L, 64, true),
                "Can not change variant enable nested group");
    }

    @Test
    public void testPropertyChangeMustBeIsolated() {
        Column source = column();
        Column nullable = new Column(source);
        nullable.setIsAllowNull(!source.isAllowNull());
        Column comment = new Column(source);
        comment.setComment("changed");
        Column key = new Column(source);
        key.setIsKey(!source.isKey());
        Column aggregation = new Column(source);
        aggregation.setAggregationType(AggregateType.REPLACE, false);
        Column defaultValue = new Column("v", source.getType(), source.isKey(), source.getAggregationType(),
                source.isAllowNull(), "changed", source.getComment());
        for (Column target : List.of(nullable, comment, key, aggregation, defaultValue)) {
            Assertions.assertThrows(DdlException.class,
                    () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(source, target, false));
        }
        Assertions.assertThrows(DdlException.class,
                () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(source, source, true));
        // MOW value column: CREATE stores REPLACE as explicit, MODIFY COLUMN parses it as implicit.
        Column created = new Column(source);
        created.setAggregationType(AggregateType.REPLACE, false);
        Column modified = new Column(source);
        modified.setAggregationType(AggregateType.REPLACE, true);
        Assertions.assertDoesNotThrow(
                () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(created, modified, false));
    }
}
