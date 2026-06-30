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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link IcebergSchemaUtils} — the T06 field-id schema dictionary. The dictionary is the highest-risk
 * P6.2 carrier: a wrong/missing field-id entry makes BE either silently read NULL/garbage for renamed columns
 * or DCHECK-abort the whole BE on a missing column (CI #969249). These tests assert the decoded thrift dictionary
 * against the legacy {@code ExternalUtil.initSchemaInfoFor{All,Pruned}Column} expectation — not class names —
 * since the parity is otherwise UT-invisible (only P6.6 docker e2e exercises BE). No Mockito; real
 * {@link InMemoryCatalog}.
 */
public class IcebergSchemaUtilsTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));

    // --- helpers ---

    private static Table createTable(String name, Schema schema, Map<String, String> props) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", name), schema, null, null, props);
    }

    private static Table createTable(String name, Schema schema) {
        return createTable(name, schema, Collections.emptyMap());
    }

    /** Build the dictionary for the given requested column names and return the single (-1) entry. */
    private static TSchema dict(Table table, String... requestedLowerNames) {
        Map<Integer, List<String>> nameMapping = IcebergSchemaUtils.extractNameMapping(table);
        return IcebergSchemaUtils.buildCurrentSchema(table.schema(), Arrays.asList(requestedLowerNames),
                nameMapping);
    }

    /** Index the top-level fields of an entry by name (preserving order for ordering assertions). */
    private static Map<String, TField> topFields(TSchema schema) {
        Map<String, TField> byName = new LinkedHashMap<>();
        for (TFieldPtr ptr : schema.getRootField().getFields()) {
            byName.put(ptr.getFieldPtr().getName(), ptr.getFieldPtr());
        }
        return byName;
    }

    private static TField childByName(TField parent, String name) {
        for (TFieldPtr ptr : parent.getNestedField().getStructField().getFields()) {
            if (ptr.getFieldPtr().getName().equals(name)) {
                return ptr.getFieldPtr();
            }
        }
        throw new AssertionError("no nested field named " + name);
    }

    private static TFileScanRangeParams decode(String encoded) throws Exception {
        TFileScanRangeParams params = new TFileScanRangeParams();
        new TDeserializer(new TBinaryProtocol.Factory())
                .deserialize(params, Base64.getDecoder().decode(encoded));
        return params;
    }

    // --- the single-entry, -1-sentinel contract (legacy parity; the iceberg-vs-paimon divergence) ---

    @Test
    public void encodeProducesSingleMinusOneEntry() throws Exception {
        Table table = createTable("t1", SCHEMA);
        String encoded = IcebergSchemaUtils.encodeSchemaEvolutionProp(table, Arrays.asList("id", "name"));
        TFileScanRangeParams params = decode(encoded);

        // WHY: legacy iceberg sets current_schema_id = -1 and emits exactly ONE history_schema_info entry
        // (IcebergScanNode.createScanRangeLocations -> -1L); BE reads file field-ids from the file metadata and
        // matches them to this single table-side entry. MUTATION: emit per-committed-schema-id entries (the
        // paimon shape the HANDOFF planned) -> size != 1 -> red. MUTATION: a real schema id instead of -1 -> red.
        Assertions.assertTrue(params.isSetCurrentSchemaId());
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(1, params.getHistorySchemaInfoSize());
        Assertions.assertEquals(-1L, params.getHistorySchemaInfo().get(0).getSchemaId());
    }

    // --- top-level: iceberg field ids + lowercased names keyed off the requested columns ---

    @Test
    public void topLevelFieldsCarryIcebergFieldIdsAndLowercasedNames() {
        // A mixed-case iceberg schema: the dictionary's top-level names must be the LOWERCASED Doris slot names
        // (parseSchema lowercases with Locale.ROOT) so BE's StructNode keys match the scan slots, while the field
        // id is the iceberg field id (the rename-safe join key BE matches the file's embedded ids against). The
        // expected ids are read from table.schema() (iceberg reassigns field ids on table creation), proving the
        // dictionary carries the table's ACTUAL field ids, not a fabricated/positional value.
        Schema mixed = new Schema(
                Types.NestedField.required(7, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(9, "Name", Types.StringType.get()));
        Table table = createTable("mixed", mixed);
        Schema actual = table.schema();

        Map<String, TField> fields = topFields(dict(table, "id", "name"));

        Assertions.assertEquals(actual.caseInsensitiveFindField("id").fieldId(), fields.get("id").getId());
        Assertions.assertEquals(actual.caseInsensitiveFindField("name").fieldId(), fields.get("name").getId());
        // MUTATION: keep the iceberg case ("ID") -> the lowercase slot lookup misses -> red.
        Assertions.assertFalse(fields.containsKey("ID"));
        // Legacy parity (NOT the iceberg required/optional flag): ExternalUtil sets is_optional from the Doris
        // column's isAllowNull(), which parseSchema forces to true for EVERY iceberg column — so even the
        // REQUIRED "id" surfaces is_optional=true. MUTATION: leak field.isOptional() (required -> false) -> red.
        Assertions.assertTrue(fields.get("id").isIsOptional());
        Assertions.assertTrue(fields.get("name").isIsOptional());
    }

    @Test
    public void keyedOffRequestedColumnsOnlyIncludesRequested() {
        // CI #969249 invariant: the -1 entry's top-level names must equal the BE scan slots BY CONSTRUCTION.
        // Keying off the requested (pruned) columns guarantees that — requesting only "name" must NOT pull in
        // "id"/"extra". MUTATION: build from table.schema() (all columns) -> the entry over-covers -> red here,
        // and (the real bug) UNDER-covers when the FE slots lead the resolved schema -> BE DCHECK on the file.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table, "name"));

        Assertions.assertEquals(1, fields.size());
        Assertions.assertTrue(fields.containsKey("name"));
        Assertions.assertEquals(2, fields.get("name").getId());
    }

    @Test
    public void renamePreservesFieldIdAcrossEvolution() {
        // The crux of "one entry suffices": iceberg field ids are permanent. Rename name(id=2) -> full_name; the
        // dictionary keyed off the NEW name still carries the SAME field id 2, so BE matches an old file's
        // column (written as "name", field id 2) to the renamed table column by id. MUTATION: source the id from
        // the file/name rather than the stable iceberg field id -> id changes on rename -> red.
        Table table = createTable("t1", SCHEMA);
        table.updateSchema().renameColumn("name", "full_name").commit();

        Map<String, TField> fields = topFields(dict(table, "id", "full_name"));

        Assertions.assertEquals(2, fields.get("full_name").getId());
        Assertions.assertFalse(fields.containsKey("name"));
    }

    @Test
    public void emptyRequestedFallsBackToAllColumns() {
        // A count-only scan (no projected slots) / a table with no column handles yields an empty requested list;
        // the dictionary then carries all top-level columns (lowercased) so it is still a valid superset. MUTATION:
        // return an empty root struct -> BE has no table entry -> red.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table /* no requested names */));

        Assertions.assertEquals(3, fields.size());
        Assertions.assertEquals(1, fields.get("id").getId());
        Assertions.assertEquals(2, fields.get("name").getId());
        Assertions.assertEquals(3, fields.get("extra").getId());
    }

    @Test
    public void failsLoudWhenRequestedColumnAbsent() {
        // A requested column absent from the resolved schema is a genuine FE/connector inconsistency; fail loud
        // rather than silently drop it (a dropped column would make BE's StructNode DCHECK-abort the whole BE).
        Table table = createTable("t1", SCHEMA);
        Assertions.assertThrows(RuntimeException.class, () -> dict(table, "id", "does_not_exist"));
    }

    // --- scalar placeholder + nested struct/array/map carry field ids at every level ---

    @Test
    public void scalarFieldsUseStringPlaceholder() {
        // BE reads type.type only as a nested-vs-scalar discriminator on the field-id path, so every scalar is a
        // single STRING placeholder regardless of the real iceberg type (no full type conversion needed).
        // MUTATION: map INT -> TPrimitiveType.INT -> still passes BE but diverges from the verified placeholder;
        // the assertion pins the placeholder so the simplification is intentional, not accidental.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table, "id"));
        Assertions.assertEquals(TPrimitiveType.STRING, fields.get("id").getType().getType());
    }

    @Test
    public void nestedTypesCarryFieldIdsAtEveryLevel() {
        // Faithful to legacy ExternalUtil (NOT paimon, which omits ids on collection elements): every nested
        // field — struct child, array element, map key/value — carries its own iceberg field id, because the
        // iceberg BE reader field-id-matches nested fields too. MUTATION: drop the element/key/value ids -> red.
        Schema nested = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "info", Types.StructType.of(
                        Types.NestedField.required(3, "a", Types.IntegerType.get()),
                        Types.NestedField.optional(4, "b", Types.StringType.get()))),
                Types.NestedField.optional(5, "tags",
                        Types.ListType.ofRequired(6, Types.StringType.get())),
                Types.NestedField.optional(7, "props",
                        Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.IntegerType.get())));
        Table table = createTable("nested", nested);
        // iceberg reassigns field ids on table creation, so read the expected ids back from the table schema.
        Schema actual = table.schema();
        Types.StructType infoType = (Types.StructType) actual.findField("info").type();
        Types.ListType tagsType = (Types.ListType) actual.findField("tags").type();
        Types.MapType propsType = (Types.MapType) actual.findField("props").type();

        Map<String, TField> fields = topFields(dict(table, "info", "tags", "props"));

        // struct
        TField info = fields.get("info");
        Assertions.assertEquals(actual.findField("info").fieldId(), info.getId());
        Assertions.assertEquals(TPrimitiveType.STRUCT, info.getType().getType());
        Assertions.assertEquals(infoType.field("a").fieldId(), childByName(info, "a").getId());
        Assertions.assertEquals(infoType.field("b").fieldId(), childByName(info, "b").getId());
        Assertions.assertEquals(TPrimitiveType.STRING, childByName(info, "a").getType().getType());

        // array element
        TField tags = fields.get("tags");
        Assertions.assertEquals(TPrimitiveType.ARRAY, tags.getType().getType());
        TField element = tags.getNestedField().getArrayField().getItemField().getFieldPtr();
        Assertions.assertEquals(tagsType.elementId(), element.getId());
        Assertions.assertEquals(TPrimitiveType.STRING, element.getType().getType());

        // map key + value
        TField props = fields.get("props");
        Assertions.assertEquals(TPrimitiveType.MAP, props.getType().getType());
        Assertions.assertEquals(propsType.keyId(),
                props.getNestedField().getMapField().getKeyField().getFieldPtr().getId());
        Assertions.assertEquals(propsType.valueId(),
                props.getNestedField().getMapField().getValueField().getFieldPtr().getId());
    }

    @Test
    public void nestedStructChildNamesAreLowercased() {
        // The crash repro (test_iceberg_struct_schema_evolution / DROP_AND_ADD): a struct child whose iceberg
        // name has mixed case must be emitted LOWERCASED. The Doris slot's DataTypeStruct child names are
        // force-lowercased (StructField ctor -> this.name = name.toLowerCase()), and BE's StructNode looks the
        // child up by that lowercase name (children_column_exists/children.at). Keeping the iceberg case
        // ("DROP_AND_ADD") makes BE's children.at("drop_and_add") throw std::out_of_range -> SIGABRT on the whole
        // struct read (the DCHECK guard is compiled out in a Release BE). MUTATION (the bug): emit field.name()
        // verbatim for struct children -> the lowercase key is absent / the iceberg-cased key leaks -> red.
        Schema mixed = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a_struct", Types.StructType.of(
                        Types.NestedField.optional(3, "keep", Types.LongType.get()),
                        Types.NestedField.optional(4, "DROP_AND_ADD", Types.LongType.get()))));
        Table table = createTable("mixed_nested", mixed);
        // iceberg reassigns field ids on create, so read the expected id back from the table schema.
        Types.StructType structType = (Types.StructType) table.schema().findField("a_struct").type();

        TField aStruct = topFields(dict(table, "a_struct")).get("a_struct");
        Map<String, Integer> childIds = new LinkedHashMap<>();
        for (TFieldPtr ptr : aStruct.getNestedField().getStructField().getFields()) {
            childIds.put(ptr.getFieldPtr().getName(), ptr.getFieldPtr().getId());
        }

        // the mixed-case child must be addressable by its LOWERCASE name (the BE StructNode lookup key) and still
        // carry its (rename-safe) iceberg field id ...
        Assertions.assertTrue(childIds.containsKey("drop_and_add"));
        Assertions.assertEquals(structType.field("DROP_AND_ADD").fieldId(),
                childIds.get("drop_and_add").intValue());
        // ... and the iceberg-cased name must NOT leak through (that is exactly what aborts BE).
        Assertions.assertFalse(childIds.containsKey("DROP_AND_ADD"));
        Assertions.assertTrue(childIds.containsKey("keep"));
    }

    // --- name mapping (BE's fallback for old files lacking embedded field ids) ---

    @Test
    public void nameMappingCarriedWhenTablePropertyPresent() {
        // A table with schema.name-mapping.default makes each field carry TField.nameMapping so BE's
        // by_parquet_field_id_with_name_mapping can resolve old files written before field ids were embedded.
        // MUTATION: skip setting nameMapping -> isSetNameMapping false -> red.
        String json = NameMappingParser.toJson(MappingUtil.create(SCHEMA));
        Table table = createTable("t1", SCHEMA,
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING, json));

        Map<String, TField> fields = topFields(dict(table, "id", "name"));

        Assertions.assertTrue(fields.get("id").isSetNameMapping());
        Assertions.assertEquals(Collections.singletonList("id"), fields.get("id").getNameMapping());
        Assertions.assertTrue(fields.get("name").isSetNameMapping());
        Assertions.assertEquals(Collections.singletonList("name"), fields.get("name").getNameMapping());
    }

    @Test
    public void noNameMappingWhenTablePropertyAbsent() {
        // Without the property, no field carries a name mapping (BE then uses embedded field ids only). MUTATION:
        // unconditionally set nameMapping -> isSetNameMapping true -> red.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table, "id", "name"));
        Assertions.assertFalse(fields.get("id").isSetNameMapping());
        Assertions.assertFalse(fields.get("name").isSetNameMapping());
    }

    @Test
    public void extractNameMappingRecursesIntoNestedFields() {
        // extractNameMapping must capture NESTED field ids too (legacy extractMappingsFromNameMapping recurses),
        // so an old file's nested column can also fall back to name matching. MUTATION: stop recursing into
        // nestedMapping -> the nested ids are absent -> red.
        Schema nested = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "info", Types.StructType.of(
                        Types.NestedField.required(3, "a", Types.IntegerType.get()))));
        String json = NameMappingParser.toJson(MappingUtil.create(nested));
        Table table = createTable("nested", nested,
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING, json));

        Map<Integer, List<String>> mapping = IcebergSchemaUtils.extractNameMapping(table);

        Assertions.assertEquals(Collections.singletonList("id"), mapping.get(1));
        Assertions.assertEquals(Collections.singletonList("info"), mapping.get(2));
        Assertions.assertEquals(Collections.singletonList("a"), mapping.get(3));
    }

    @Test
    public void extractNameMappingFailsSoftOnMalformedProperty() {
        // A malformed name-mapping property must not break the scan (legacy catches + warns). MUTATION: let the
        // parse exception propagate -> the whole scan fails on a benign metadata quirk -> red.
        Table table = createTable("t1", SCHEMA,
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING, "{not valid json"));
        Map<Integer, List<String>> mapping = IcebergSchemaUtils.extractNameMapping(table);
        Assertions.assertTrue(mapping.isEmpty());
    }

    // --- round-trip through the prop transport (what the generic node does) ---

    @Test
    public void applyRoundTripsThroughEncodedProp() {
        // getScanNodeProperties encodes the dict, populateScanLevelParams applies it to the real params — the
        // exact path the generic PluginDrivenScanNode round-trips. MUTATION: drop one of the two copied fields in
        // applySchemaEvolution -> the params are missing it -> red.
        Table table = createTable("t1", SCHEMA);
        String encoded = IcebergSchemaUtils.encodeSchemaEvolutionProp(table, Arrays.asList("id", "name"));

        TFileScanRangeParams params = new TFileScanRangeParams();
        IcebergSchemaUtils.applySchemaEvolution(params, encoded);

        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(1, params.getHistorySchemaInfoSize());
        TStructField root = params.getHistorySchemaInfo().get(0).getRootField();
        Assertions.assertEquals(2, root.getFieldsSize());
    }

    @Test
    public void applyIsNoOpForNullOrEmpty() {
        // A null/empty prop (e.g. another connector's props map) must leave the params untouched, not throw.
        TFileScanRangeParams params = new TFileScanRangeParams();
        IcebergSchemaUtils.applySchemaEvolution(params, null);
        IcebergSchemaUtils.applySchemaEvolution(params, "");
        Assertions.assertFalse(params.isSetCurrentSchemaId());
        Assertions.assertFalse(params.isSetHistorySchemaInfo());
    }

    @Test
    public void applyFailsLoudOnCorruptProp() {
        // The prop is produced by us, so a decode failure is a real bug — fail loud rather than silently drop it
        // (a dropped dict re-introduces the silent wrong-rows BLOCKER on schema-evolved native reads).
        TFileScanRangeParams params = new TFileScanRangeParams();
        Assertions.assertThrows(RuntimeException.class,
                () -> IcebergSchemaUtils.applySchemaEvolution(params, "!!!not-base64-thrift!!!"));
    }
}
