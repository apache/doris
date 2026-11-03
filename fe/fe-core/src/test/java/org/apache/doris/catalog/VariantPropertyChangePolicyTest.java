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
import org.apache.doris.persist.TableAddOrDropColumnsInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
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
        Map<String, Object> changes = Map.of("typedToSparse", true, "stats", 1,
                "sparseShards", 2, "doc", true, "docShards", 2, "nested", true);
        Map<String, String> messages = Map.of(
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
    public void testNonDocBudgetChangesAreLight() throws Exception {
        long seed = Long.getLong("variant.budget.seed", 20261103L);
        System.out.println("variant budget policy seed=" + seed);
        java.util.Random random = new java.util.Random(seed);
        int n = 1 + random.nextInt(1000);
        int[][] transitions = {{0, n}, {n, 0}, {n, n + 1 + random.nextInt(100)}, {n, 1}, {1, n}};
        for (int[] transition : transitions) {
            Column source = column(Map.of("count", transition[0]));
            Column target = column(Map.of("count", transition[1]));
            source.checkSchemaChangeAllowed(target);
            Assertions.assertTrue(VariantPropertyChangePolicy.hasBudgetChange(source, target),
                    "seed=" + seed + " transition=" + Arrays.toString(transition));
        }
        Assertions.assertFalse(VariantPropertyChangePolicy.hasBudgetChange(
                column(Map.of("count", n)), column(Map.of("count", n))));
        // The doc threshold keeps master's behavior: light without a position, heavy with one.
        Assertions.assertFalse(VariantPropertyChangePolicy.hasBudgetChange(
                column(Map.of()), column(Map.of("threshold", 2L))));
        // NestedGroup keeps its existing generic rules; only DOC budget changes are rejected.
        column(Map.of("nested", true)).checkSchemaChangeAllowed(column(Map.of("nested", true, "count", n)));
    }

    @Test
    public void testDocBudgetChangeRejected() {
        Column doc = column(Map.of("doc", true, "count", 5));
        Assertions.assertEquals("Can not change variant max subcolumns count on a DOC-mode column",
                Assertions.assertThrows(DdlException.class,
                        () -> doc.checkSchemaChangeAllowed(column(Map.of("doc", true)))).getDetailMessage());
        Assertions.assertEquals("Can not change variant max subcolumns count on a DOC-mode column",
                Assertions.assertThrows(DdlException.class, () -> column(Map.of("doc", true))
                        .checkSchemaChangeAllowed(doc)).getDetailMessage());
    }

    @Test
    public void testCountChangeOnTemplatedColumnKeepsTemplates() throws Exception {
        Column source = templatedWithCount(0, "a");
        // Count-only changes keep every template entry and stay light, in both directions.
        for (int count : new int[] {5, 1}) {
            source.checkSchemaChangeAllowed(templatedWithCount(count, "a"));
            templatedWithCount(count, "a").checkSchemaChangeAllowed(source);
        }
        Assertions.assertTrue(VariantPropertyChangePolicy.hasBudgetChange(source, templatedWithCount(5, "a")));
        // A count change that also edits templates is checked by the template ADD rules.
        Assertions.assertEquals("Can not change variant schema templates: removed entry: a",
                Assertions.assertThrows(DdlException.class,
                        () -> source.checkSchemaChangeAllowed(templatedWithCount(5, "b"))).getDetailMessage());
        // DOC mode still rejects the count change before the template check.
        Column doc = new Column("v", new VariantType(new ArrayList<>(List.of(
                new VariantField("a", Type.INT, "", PatternType.MATCH_NAME))), 0, false, 10000, 0, true, 0L, 64, false));
        Column docCounted = new Column("v", new VariantType(new ArrayList<>(List.of(
                new VariantField("a", Type.INT, "", PatternType.MATCH_NAME))), 5, false, 10000, 0, true, 0L, 64, false));
        Assertions.assertEquals("Can not change variant max subcolumns count on a DOC-mode column",
                Assertions.assertThrows(DdlException.class,
                        () -> doc.checkSchemaChangeAllowed(docCounted)).getDetailMessage());
    }

    private static Column templatedWithCount(int count, String pattern) {
        return new Column("v", new VariantType(new ArrayList<>(List.of(
                new VariantField(pattern, Type.INT, "", PatternType.MATCH_NAME))), count, false, 10000, 0, false, 0L, 64,
                false));
    }

    private static final VariantField A = entry("a", Type.INT, PatternType.MATCH_NAME);
    private static final VariantField GLOB = entry("a*", Type.STRING, PatternType.MATCH_NAME_GLOB);
    private static final VariantField B = entry("b", Type.BIGINT, PatternType.MATCH_NAME);

    private static VariantField entry(String pattern, Type type, PatternType mode) {
        return new VariantField(pattern, type, "", mode);
    }

    private static Column templated(Map<String, Object> changes, VariantField... fields) {
        return new Column("v", new VariantType(new ArrayList<>(List.of(fields)),
                (int) changes.getOrDefault("count", 0),
                (boolean) changes.getOrDefault("typedToSparse", false), 10000, 0,
                (boolean) changes.getOrDefault("doc", false), 0L, 64,
                (boolean) changes.getOrDefault("nested", false)));
    }

    private static String rejection(Column source, Column target) {
        return Assertions.assertThrows(DdlException.class,
                () -> source.checkSchemaChangeAllowed(target)).getDetailMessage();
    }

    @Test
    public void testWithoutTemplateDeltaMasterChildrenRuleRemains() throws Exception {
        Column templatedA = templated(Map.of(), A);
        Assertions.assertFalse(VariantPropertyChangePolicy.hasTemplateChange(templatedA, templated(Map.of(), A)));
        Assertions.assertEquals("Can not change variant schema templates",
                rejection(templatedA, templated(Map.of(), A)));
        // A count change on a column that already has templates is accepted, alone or with an ADD.
        templatedA.checkSchemaChangeAllowed(templated(Map.of("count", 5), A));
        templatedA.checkSchemaChangeAllowed(templated(Map.of("count", 5), A, B));
        Assertions.assertTrue(VariantPropertyChangePolicy.hasTemplateChange(templatedA,
                templated(Map.of("count", 5), A, B)));
    }

    @Test
    public void testAppendOnlyTemplateChangesAccepted() throws Exception {
        Column plain = column(Map.of());
        Column templatedA = templated(Map.of(), A);
        Assertions.assertTrue(VariantPropertyChangePolicy.hasTemplateChange(plain, templatedA));
        plain.checkSchemaChangeAllowed(templatedA);
        // Several appended entries, including a glob overlapping the retained exact entry.
        templatedA.checkSchemaChangeAllowed(templated(Map.of(), A, GLOB, B));
        // Count plus the first ADD on a template-less column passes both rules.
        plain.checkSchemaChangeAllowed(templated(Map.of("count", 5), A));
        Assertions.assertTrue(VariantPropertyChangePolicy.hasCountOrTemplateChange(plain, templatedA));
    }

    @Test
    public void testTemplateChangesRejectedWithDesignMessages() {
        Column templatedA = templated(Map.of(), A);
        Column twoEntries = templated(Map.of(), A, GLOB);
        Map<String, Column[]> cases = new java.util.LinkedHashMap<>();
        cases.put("Can not change variant schema templates: removed entry: a",
                new Column[] {twoEntries, templated(Map.of(), GLOB)});
        cases.put("Can not change variant schema templates: removed entry: a ",
                new Column[] {templatedA, column(Map.of())});
        cases.put("Can not change variant schema templates: removed entry: a  ",
                new Column[] {templatedA, templated(Map.of(), entry("A", Type.INT, PatternType.MATCH_NAME))});
        cases.put("Can not change variant schema templates: removed entry: a   ",
                new Column[] {templatedA, templated(Map.of(), B)});
        cases.put("Can not change type of existing variant schema template entry: a",
                new Column[] {templatedA, templated(Map.of(), entry("a", Type.BIGINT, PatternType.MATCH_NAME))});
        cases.put("Can not change type of existing variant schema template entry: d",
                new Column[] {templated(Map.of(), entry("d", ScalarType.createDecimalV3Type(10, 2),
                        PatternType.MATCH_NAME)), templated(Map.of(), entry("d",
                        ScalarType.createDecimalV3Type(12, 2), PatternType.MATCH_NAME))});
        // Catalog ARRAY elements are always nullable, so nested nullability is exercised through STRUCT.
        StructType nullableStruct = new StructType(new StructField("f", Type.INT, "", true));
        StructType notNullStruct = new StructType(new StructField("f", Type.INT, "", false));
        Column nullableS = templated(Map.of(), entry("s", nullableStruct, PatternType.MATCH_NAME));
        Column notNullS = templated(Map.of(), entry("s", notNullStruct, PatternType.MATCH_NAME));
        cases.put("Can not change nullability of existing variant schema template entry: s",
                new Column[] {nullableS, notNullS});
        cases.put("Can not change field_pattern_type of existing variant schema template entry: a",
                new Column[] {templatedA, templated(Map.of(), entry("a", Type.INT, PatternType.MATCH_NAME_GLOB))});
        cases.put("Can not change existing variant schema template entry: a",
                new Column[] {templatedA, templated(Map.of(), new VariantField("a", Type.INT, "changed",
                        PatternType.MATCH_NAME))});
        cases.put("Can not reorder existing variant schema template entries",
                new Column[] {twoEntries, templated(Map.of(), GLOB, A)});
        cases.put("Can not reorder existing variant schema template entries ",
                new Column[] {templatedA, templated(Map.of(), B, A)});
        cases.put("Can not change variant schema templates on a DOC-mode column",
                new Column[] {templated(Map.of("doc", true)), templated(Map.of("doc", true), A)});
        cases.put("Can not change variant schema templates on a NestedGroup column",
                new Column[] {templated(Map.of("nested", true)), templated(Map.of("nested", true), A)});
        cases.put("Can not change variant schema templates when typed_paths_to_sparse is enabled",
                new Column[] {templated(Map.of("typedToSparse", true)), templated(Map.of("typedToSparse", true), A)});
        cases.put("Duplicate variant schema template entry: a",
                new Column[] {templatedA, templated(Map.of(), A, entry("a", Type.INT, PatternType.MATCH_NAME))});
        cases.put("Duplicate variant schema template entry: a ",
                new Column[] {templatedA, templated(Map.of(), A, entry("a", Type.STRING,
                        PatternType.MATCH_NAME_GLOB))});
        cases.put("Can not add variant schema template entry with DECIMAL256 type: d",
                new Column[] {column(Map.of()), templated(Map.of(), entry("d",
                        ScalarType.createDecimalV3Type(76, 2), PatternType.MATCH_NAME))});
        cases.put("Can not add variant schema template entry with DECIMAL256 type: arr",
                new Column[] {templatedA, templated(Map.of(), A, entry("arr",
                        new ArrayType(ScalarType.createDecimalV3Type(76, 2), true), PatternType.MATCH_NAME))});
        // Keys carry trailing spaces only to keep repeated expectations distinct.
        for (Map.Entry<String, Column[]> c : cases.entrySet()) {
            Assertions.assertEquals(c.getKey().trim(), rejection(c.getValue()[0], c.getValue()[1]), c.getKey());
        }
    }

    @Test
    public void testTemplateAddMustBeIsolated() throws Exception {
        Column plain = column(Map.of());
        Column commented = templated(Map.of(), A);
        commented.setComment("changed");
        Assertions.assertEquals("Can not change variant schema templates together with other column attributes",
                Assertions.assertThrows(DdlException.class, () -> VariantPropertyChangePolicy
                        .checkPropertyChangeIsolated(plain, commented)).getMessage().replace("errCode = 2, "
                                + "detailMessage = ", ""));
        Column nullability = templated(Map.of(), A);
        nullability.setIsAllowNull(!plain.isAllowNull());
        Assertions.assertThrows(DdlException.class,
                () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(plain, nullability));
        VariantPropertyChangePolicy.checkPropertyChangeIsolated(plain, templated(Map.of(), A));
        // Combined count + ADD reports the count diagnostic first.
        Column both = templated(Map.of("count", 5), A);
        both.setComment("changed");
        Assertions.assertTrue(Assertions.assertThrows(DdlException.class,
                () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(plain, both)).getMessage()
                .contains("Can not change variant max subcolumns count together with other column attributes"));
    }

    @Test
    public void testAppendedTemplatesSurviveJournalAndImageRoundTrip() throws Exception {
        long seed = Long.getLong("variant.template.seed", 20261103L);
        java.util.Random random = new java.util.Random(seed);
        int added = 1 + random.nextInt(8);
        System.out.println("variant template journal seed=" + seed + " added=" + added);
        Type[] types = {Type.INT, Type.BIGINT, Type.STRING, Type.DOUBLE, new ArrayType(Type.INT, true)};
        List<VariantField> fields = new ArrayList<>(List.of(A));
        for (int i = 0; i < added; i++) {
            fields.add(entry("p" + i + (random.nextBoolean() ? "*" : ""), types[random.nextInt(types.length)],
                    random.nextBoolean() ? PatternType.MATCH_NAME : PatternType.MATCH_NAME_GLOB));
        }
        Column source = templated(Map.of(), A);
        Column column = templated(Map.of(), fields.toArray(new VariantField[0]));
        source.checkSchemaChangeAllowed(column);
        Column image = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class);
        Assertions.assertFalse(VariantPropertyChangePolicy.hasTemplateChange(column, image), "seed=" + seed);
        Assertions.assertEquals(column, image, "seed=" + seed);
        LinkedList<Column> schema = new LinkedList<>(List.of(column));
        Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();
        indexSchemaMap.put(2L, schema);
        Map<Long, List<Column>> oldSchemaMap = new HashMap<>();
        oldSchemaMap.put(2L, List.of(source));
        TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(
                "", 1L, 2L, 2L, indexSchemaMap, oldSchemaMap, new HashMap<>(), new ArrayList<>(), 3L);
        TableAddOrDropColumnsInfo replayed = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(info), TableAddOrDropColumnsInfo.class);
        Column replayedColumn = replayed.getIndexSchemaMap().get(2L).get(0);
        Assertions.assertFalse(VariantPropertyChangePolicy.hasTemplateChange(column, replayedColumn), "seed=" + seed);
        Assertions.assertEquals(column.getChildren(), replayedColumn.getChildren(), "seed=" + seed);
    }

    @Test
    public void testDiagnosticPrecedence() {
        Assertions.assertEquals("Can not change variant enable typed paths to sparse",
                Assertions.assertThrows(DdlException.class,
                        () -> column(Map.of()).checkSchemaChangeAllowed(
                                column(Map.of("count", 1, "typedToSparse", true, "doc", true)))).getDetailMessage());
        // An immutable DOC flip wins over the DOC budget diagnostic.
        Assertions.assertEquals("Can not change variant enable doc snapshot mode",
                Assertions.assertThrows(DdlException.class,
                        () -> column(Map.of("doc", true, "count", 3)).checkSchemaChangeAllowed(
                                column(Map.of()))).getDetailMessage());
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
        Assertions.assertEquals(Set.of("variantDocMaterializationMinRows", "variantMaxSubcolumnsCount"), classifiedAs(
                VariantPropertyChangePolicy.ChangeKind.LIGHT));
        Assertions.assertEquals(Set.of("fields", "fieldMap"), classifiedAs(
                VariantPropertyChangePolicy.ChangeKind.TEMPLATE));
        Assertions.assertEquals(Set.of("enableTypedPathsToSparse",
                "variantMaxSparseColumnStatisticsSize", "variantSparseHashShardCount", "enableVariantDocMode",
                "variantDocShardCount", "enableNestedGroup"), classifiedAs(
                        VariantPropertyChangePolicy.ChangeKind.IMMUTABLE));
    }

    private static Set<String> classifiedAs(VariantPropertyChangePolicy.ChangeKind kind) {
        return Arrays.stream(VariantPropertyChangePolicy.FieldPolicy.values())
                .filter(field -> field.kind == kind).map(field -> field.serializedName).collect(Collectors.toSet());
    }

    @Test
    public void testBudgetChangeMustBeIsolated() throws Exception {
        Column source = column(Map.of("count", 0));
        VariantPropertyChangePolicy.checkPropertyChangeIsolated(source, column(Map.of("count", 5)));
        List<Column> hitchhikers = new ArrayList<>();
        Column nullability = column(Map.of("count", 5));
        nullability.setIsAllowNull(!source.isAllowNull());
        hitchhikers.add(nullability);
        Column comment = column(Map.of("count", 5));
        comment.setComment("changed");
        hitchhikers.add(comment);
        Column key = column(Map.of("count", 5));
        key.setIsKey(!source.isKey());
        hitchhikers.add(key);
        Column aggregation = column(Map.of("count", 5));
        aggregation.setAggregationType(AggregateType.REPLACE, false);
        hitchhikers.add(aggregation);
        for (Column target : hitchhikers) {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> VariantPropertyChangePolicy.checkPropertyChangeIsolated(source, target));
            Assertions.assertTrue(e.getMessage().contains(
                    "Can not change variant max subcolumns count together with other column attributes"),
                    e.getMessage());
        }
        // Without a budget delta the isolation rule is not involved; master's checks still apply.
        Column commentOnly = column(Map.of());
        commentOnly.setComment("changed");
        VariantPropertyChangePolicy.checkPropertyChangeIsolated(source, commentOnly);
    }

    @Test
    public void testBudgetSurvivesJournalAndImageRoundTrip() throws Exception {
        long seed = Long.getLong("variant.budget.seed", 20261103L);
        int n = 1 + new java.util.Random(seed).nextInt(1000);
        System.out.println("variant budget journal seed=" + seed + " n=" + n);
        Column column = column(Map.of("count", n));
        // Image and edit-log entries serialize Column through GSON.
        Column image = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class);
        Assertions.assertEquals(n, image.getVariantMaxSubcolumnsCount());
        Assertions.assertEquals(column, image);
        LinkedList<Column> schema = new LinkedList<>(List.of(column));
        Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();
        indexSchemaMap.put(2L, schema);
        Map<Long, List<Column>> oldSchemaMap = new HashMap<>();
        oldSchemaMap.put(2L, List.of(column(Map.of())));
        TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(
                "", 1L, 2L, 2L, indexSchemaMap, oldSchemaMap, new HashMap<>(), new ArrayList<>(), 3L);
        TableAddOrDropColumnsInfo replayed = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(info), TableAddOrDropColumnsInfo.class);
        Assertions.assertEquals(n, replayed.getIndexSchemaMap().get(2L).get(0).getVariantMaxSubcolumnsCount());
    }
}
