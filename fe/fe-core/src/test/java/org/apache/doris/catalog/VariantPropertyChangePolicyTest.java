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
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VariantPropertyChangePolicyTest {
    private static Column column(Map<String, Object> changes) {
        return new Column("v", new VariantType(new ArrayList<>(),
                (int) changes.getOrDefault("count", 0),
                (boolean) changes.getOrDefault("typedToSparse", false),
                (int) changes.getOrDefault("stats", 10000),
                (int) changes.getOrDefault("sparseShards", 0),
                (boolean) changes.getOrDefault("doc", false),
                (long) changes.getOrDefault("threshold", 0L),
                (int) changes.getOrDefault("docShards", 64),
                (boolean) changes.getOrDefault("nested", false)));
    }

    @Test
    public void testImmutablePropertiesKeepExactMessages() {
        Map<String, Object> changes = Map.of("count", 1, "typedToSparse", true, "stats", 1,
                "sparseShards", 2, "doc", true, "docShards", 2, "nested", true);
        Map<String, String> messages = Map.of(
                "count", "Can not change variant max subcolumns count",
                "typedToSparse", "Can not change variant enable typed paths to sparse",
                "stats", "Can not change variant max sparse column statistics size",
                "sparseShards", "Can not change variant sparse bucket num",
                "doc", "Can not change variant enable doc snapshot mode",
                "docShards", "Can not change variant doc snapshot shard count",
                "nested", "Can not change variant enable nested group");
        Column original = column(Map.of());
        changes.forEach((name, value) -> {
            Column changed = column(Map.of(name, value));
            Assertions.assertEquals(messages.get(name), Assertions.assertThrows(DdlException.class,
                    () -> original.checkSchemaChangeAllowed(changed)).getDetailMessage());
            Assertions.assertEquals(messages.get(name), Assertions.assertThrows(DdlException.class,
                    () -> changed.checkSchemaChangeAllowed(original)).getDetailMessage());
        });
    }

    @Test
    public void testExistingDocThresholdAllowanceAndNoOp() throws Exception {
        Column original = column(Map.of());
        original.checkSchemaChangeAllowed(column(Map.of()));
        original.checkSchemaChangeAllowed(column(Map.of("threshold", 2L)));
        // Preserve the current column-level rule, including no new range checks or DOC rejection.
        original.checkSchemaChangeAllowed(column(Map.of("threshold", -1L)));
        column(Map.of("doc", true)).checkSchemaChangeAllowed(column(Map.of("doc", true, "threshold", 3L)));
        column(Map.of("nested", true)).checkSchemaChangeAllowed(column(Map.of("nested", true, "threshold", 3L)));
    }

    @Test
    public void testTemplatesRemainRejectedEvenWhenUnchanged() {
        Column templated = new Column("v", new VariantType(new ArrayList<>(List.of(
                new VariantField("a", Type.INT, "", PatternType.MATCH_NAME)))));
        Column plain = column(Map.of());
        for (Column source : List.of(plain, templated)) {
            Assertions.assertEquals("Can not change variant schema templates",
                    Assertions.assertThrows(DdlException.class,
                            () -> source.checkSchemaChangeAllowed(templated)).getDetailMessage());
        }
        Assertions.assertEquals("Can not change variant schema templates",
                Assertions.assertThrows(DdlException.class,
                        () -> templated.checkSchemaChangeAllowed(plain)).getDetailMessage());
    }

    @Test
    public void testDiagnosticPrecedence() {
        Assertions.assertEquals("Can not change variant max subcolumns count",
                Assertions.assertThrows(DdlException.class,
                        () -> column(Map.of()).checkSchemaChangeAllowed(
                                column(Map.of("count", 1, "typedToSparse", true, "doc", true)))).getDetailMessage());
    }

    @Test
    public void testRawPropertiesAreNotPolicyState() throws Exception {
        Column raw = new Column("v", new VariantType(Map.of("future_variant_property", "true")));
        column(Map.of()).checkSchemaChangeAllowed(raw);
        raw.checkSchemaChangeAllowed(column(Map.of()));
    }

    @Test
    public void testGsonImageRoundTripMatchesFreshEffectiveValues() throws Exception {
        for (Column fresh : List.of(column(Map.of("count", 7, "stats", 123, "sparseShards", 2)),
                column(Map.of("doc", true, "threshold", 23L, "docShards", 128)))) {
            VariantType type = (VariantType) fresh.getType();
            type.getProperties().put("unpersisted_test_property", "fresh");
            String json = GsonUtils.GSON.toJson(fresh);
            Assertions.assertFalse(json.contains("unpersisted_test_property"));
            Column restored = GsonUtils.GSON.fromJson(json, Column.class);
            Assertions.assertTrue(((VariantType) restored.getType()).getProperties().isEmpty());
            restored.checkSchemaChangeAllowed(fresh);
            fresh.checkSchemaChangeAllowed(restored);
        }
    }

    @Test
    public void testLegacyScalarVariantAndKnownRawPropertyNormalization() throws Exception {
        Column legacy = new Column("v", new ScalarType(PrimitiveType.VARIANT));
        legacy.checkSchemaChangeAllowed(new Column(legacy));
        Assertions.assertEquals("Can not change variant max sparse column statistics size",
                Assertions.assertThrows(DdlException.class,
                        () -> legacy.checkSchemaChangeAllowed(column(Map.of()))).getDetailMessage());
        // The legacy raw-properties constructor does not change the effective known getters.
        column(Map.of()).checkSchemaChangeAllowed(new Column("v",
                new VariantType(Map.of("variant_max_subcolumns_count", "9"))));
    }

    @Test
    public void testEveryPersistedVariantFieldIsClassified() {
        Set<String> persisted = Arrays.stream(VariantType.class.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SerializedName.class))
                .map(field -> field.getAnnotation(SerializedName.class).value()).collect(Collectors.toSet());
        Map<String, VariantPropertyChangePolicy.ChangeKind> classifications = Arrays.stream(
                VariantPropertyChangePolicy.FieldPolicy.values()).collect(Collectors.toMap(
                        field -> field.serializedName, field -> field.kind));
        Assertions.assertEquals(persisted, classifications.keySet(),
                "Every @SerializedName VariantType field requires an explicit ALTER classification");
        Assertions.assertEquals(Set.of("variantDocMaterializationMinRows"), classifiedAs(
                VariantPropertyChangePolicy.ChangeKind.LIGHT));
        Assertions.assertEquals(Set.of("fields", "fieldMap"), classifiedAs(
                VariantPropertyChangePolicy.ChangeKind.TEMPLATE));
        Assertions.assertEquals(Set.of("variantMaxSubcolumnsCount", "enableTypedPathsToSparse",
                "variantMaxSparseColumnStatisticsSize", "variantSparseHashShardCount", "enableVariantDocMode",
                "variantDocShardCount", "enableNestedGroup"), classifiedAs(
                        VariantPropertyChangePolicy.ChangeKind.IMMUTABLE));
    }

    private static Set<String> classifiedAs(VariantPropertyChangePolicy.ChangeKind kind) {
        return Arrays.stream(VariantPropertyChangePolicy.FieldPolicy.values())
                .filter(field -> field.kind == kind).map(field -> field.serializedName).collect(Collectors.toSet());
    }
}
