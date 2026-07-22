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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
        // Mirror the production path (encodeSchemaEvolutionProp): thread the PRECISE isPresent() as
        // hasNameMapping (#65784), so a present-but-empty mapping is still authoritative.
        Optional<Map<Integer, List<String>>> nameMapping = IcebergSchemaUtils.extractNameMapping(table);
        return IcebergSchemaUtils.buildCurrentSchema(table.schema(), Arrays.asList(requestedLowerNames),
                nameMapping.orElse(Collections.emptyMap()), nameMapping.isPresent(), false);
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
        // buildCurrentSchema echoes the REQUESTED (pruned) column names VERBATIM as the dictionary's top-level
        // names so BE's StructNode keys match the scan slots; here lowercase requested names -> lowercase
        // top-level names. The field id is the iceberg field id (the rename-safe join key BE matches the file's
        // embedded ids against), read from table.schema() (iceberg reassigns ids on creation) proving the
        // dictionary carries ACTUAL field ids, not a fabricated/positional value. See
        // topLevelFieldsPreserveMixedCaseRequestedNames for the case-preserving (#65094) path.
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
    public void topLevelFieldsPreserveMixedCaseRequestedNames() {
        // #65094 read-path alignment: post-cutover getColumnHandles (IcebergConnectorMetadata:579) and
        // parseSchema (:1708) KEEP the iceberg top-level case, so the requested (pruned) names reaching the
        // dictionary are case-PRESERVED. buildCurrentSchema must echo them VERBATIM (no re-lowercasing) so the
        // -1 entry's top-level names byte-match the case-preserving Doris scan slots BE keys by; the field id
        // stays the rename-safe iceberg field id.
        Schema mixed = new Schema(
                Types.NestedField.required(7, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(9, "Name", Types.StringType.get()));
        Table table = createTable("mixed_preserve", mixed);
        Schema actual = table.schema();

        Map<String, TField> fields = topFields(dict(table, "ID", "Name"));

        // Case-preserved top-level names carry the ACTUAL iceberg field ids (resolved case-insensitively).
        Assertions.assertEquals(actual.caseInsensitiveFindField("id").fieldId(), fields.get("ID").getId());
        Assertions.assertEquals(actual.caseInsensitiveFindField("name").fieldId(), fields.get("Name").getId());
        // MUTATION: re-lowercase the top-level name (the pre-#65094 behavior) -> "ID"/"Name" absent, the
        // case-preserving Doris slot lookup misses -> red.
        Assertions.assertFalse(fields.containsKey("id"));
        Assertions.assertFalse(fields.containsKey("name"));
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

    // --- iceberg v3 row-lineage columns must be in the dict root (else BE ParquetReader SIGABRTs) ---

    @Test
    public void appendRowLineageAddsMetadataColumnsToDictRoot() throws Exception {
        // WHY: _row_id / _last_updated_sequence_number are GENERATED BE scan slots (they reach BE column_names)
        // but are NOT in schema.columns(), so a dict keyed off the requested columns omits them. BE's ParquetReader
        // iterates column_names and calls StructNode.children_column_exists(name) -> children.at(name), which
        // std::out_of_range-SIGABRTs the whole BE on a missing key (the exact crash on
        // "select _row_id from a v2->v3 upgraded table"). With appendRowLineage=true both columns must appear in
        // the dict root carrying their RESERVED iceberg field ids (BE matches them against the FILE field ids and
        // registers them not-in-file for a "null after upgrade" file). MUTATION: skip appendRowLineageFields ->
        // _row_id absent -> red.
        Table table = createTable("v3", SCHEMA);
        String encoded = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                table, table.schema(), Arrays.asList("id", "name"), true);
        Map<String, TField> fields = topFields(decode(encoded).getHistorySchemaInfo().get(0));

        Assertions.assertTrue(fields.containsKey("_row_id"));
        Assertions.assertTrue(fields.containsKey("_last_updated_sequence_number"));
        Assertions.assertEquals(2147483540, fields.get("_row_id").getId());
        Assertions.assertEquals(2147483539, fields.get("_last_updated_sequence_number").getId());
        // the requested data columns are still carried (row-lineage is APPENDED, not a replacement)
        Assertions.assertTrue(fields.containsKey("id"));
        Assertions.assertTrue(fields.containsKey("name"));
    }

    @Test
    public void withoutAppendRowLineageDictStaysPruned() throws Exception {
        // The format-version < 3 path (appendRowLineage=false, the 2-arg overload) must NOT inject row-lineage —
        // the pruned dict stays exactly the requested slots. MUTATION: always append -> _row_id present -> red.
        Table table = createTable("v2", SCHEMA);
        String encoded = IcebergSchemaUtils.encodeSchemaEvolutionProp(table, Arrays.asList("id", "name"));
        Map<String, TField> fields = topFields(decode(encoded).getHistorySchemaInfo().get(0));

        Assertions.assertFalse(fields.containsKey("_row_id"));
        Assertions.assertFalse(fields.containsKey("_last_updated_sequence_number"));
        Assertions.assertEquals(2, fields.size());
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

    // --- #65502: each field's iceberg initial default is carried onto the dict TField ---

    @Test
    public void initialDefaultsAreCarriedOntoDictFields() {
        // #65502: BE materializes a column (notably an equality-delete KEY) that is ABSENT from an OLD data
        // file with the field's iceberg initial default instead of NULL. So buildField must carry each field's
        // initial default: scalar values as the Doris string form (timestamp normalized to DATETIMEV2 spacing),
        // binary-like values (UUID/BINARY/FIXED) as a lossless Base64 carrier flagged is_base64 (their Doris
        // STRING/CHAR type can't tell BE to decode bytes). The expected values byte-match #65502's IcebergUtils
        // test. MUTATION: not setting initial_default_value -> BE backfills NULL -> mis-applied deletes -> red.
        Schema schema = new Schema(
                Types.NestedField.optional("added_int").withId(1).ofType(Types.IntegerType.get())
                        .withInitialDefault(7).build(),
                Types.NestedField.optional("added_ts").withId(2).ofType(Types.TimestampType.withoutZone())
                        .withInitialDefault(1_704_067_200_123_456L).build(),
                Types.NestedField.optional("added_uuid").withId(3).ofType(Types.UUIDType.get())
                        .withInitialDefault(UUID.fromString("00000000-0000-0000-0000-000000000000")).build(),
                Types.NestedField.optional("added_binary").withId(4).ofType(Types.BinaryType.get())
                        .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF})).build(),
                Types.NestedField.optional("added_fixed").withId(5).ofType(Types.FixedType.ofLength(4))
                        .withInitialDefault(ByteBuffer.wrap(new byte[] {3, 2, 1, 0})).build());

        Map<String, TField> fields = topFields(IcebergSchemaUtils.buildCurrentSchema(schema,
                Arrays.asList("added_int", "added_ts", "added_uuid", "added_binary", "added_fixed"),
                Collections.emptyMap()));

        // INT -> plain Doris string form, NOT flagged base64.
        Assertions.assertEquals("7", fields.get("added_int").getInitialDefaultValue());
        Assertions.assertFalse(fields.get("added_int").isSetInitialDefaultValueIsBase64());
        // TIMESTAMP without zone -> iceberg ISO "T" replaced by a space for DATETIMEV2 (matches #65502).
        Assertions.assertEquals("2024-01-01 00:00:00.123456", fields.get("added_ts").getInitialDefaultValue());
        Assertions.assertFalse(fields.get("added_ts").isSetInitialDefaultValueIsBase64());
        // UUID -> 16 raw bytes (MSB then LSB) Base64, flagged base64 so BE decodes rather than reads the text.
        Assertions.assertEquals("AAAAAAAAAAAAAAAAAAAAAA==", fields.get("added_uuid").getInitialDefaultValue());
        Assertions.assertTrue(fields.get("added_uuid").isInitialDefaultValueIsBase64());
        // BINARY / FIXED -> iceberg's identity human form is already Base64 of the raw bytes, flagged base64.
        Assertions.assertEquals("AAEC/w==", fields.get("added_binary").getInitialDefaultValue());
        Assertions.assertTrue(fields.get("added_binary").isInitialDefaultValueIsBase64());
        Assertions.assertEquals("AwIBAA==", fields.get("added_fixed").getInitialDefaultValue());
        Assertions.assertTrue(fields.get("added_fixed").isInitialDefaultValueIsBase64());
    }

    @Test
    public void fieldsWithoutInitialDefaultOmitTheCarrier() {
        // A field with no initial default must NOT set initial_default_value (BE then backfills NULL, the legacy
        // behaviour). MUTATION: unconditionally setting a default -> isSet true -> red.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table, "id", "name"));
        Assertions.assertFalse(fields.get("id").isSetInitialDefaultValue());
        Assertions.assertFalse(fields.get("name").isSetInitialDefaultValue());
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
        // #65784: a present table-level mapping is AUTHORITATIVE for every field. MUTATION: drop the flag -> red.
        Assertions.assertTrue(fields.get("id").isNameMappingIsAuthoritative());
        Assertions.assertTrue(fields.get("name").isNameMappingIsAuthoritative());
    }

    @Test
    public void noNameMappingWhenTablePropertyAbsent() {
        // Without the property, no field carries a name mapping (BE then uses embedded field ids only). MUTATION:
        // unconditionally set nameMapping -> isSetNameMapping true -> red.
        Table table = createTable("t1", SCHEMA);
        Map<String, TField> fields = topFields(dict(table, "id", "name"));
        Assertions.assertFalse(fields.get("id").isSetNameMapping());
        Assertions.assertFalse(fields.get("name").isSetNameMapping());
        // #65784: no property -> the authoritative flag stays unset, so BE keeps the legacy name fallback (an
        // old-FE plan's behavior on a new BE). MUTATION: unconditionally set the flag -> red.
        Assertions.assertFalse(fields.get("id").isSetNameMappingIsAuthoritative());
        Assertions.assertFalse(fields.get("name").isSetNameMappingIsAuthoritative());
    }

    @Test
    public void partialNameMappingPreservedAsAuthoritativeEmptyList() {
        // #65784 CORE FIX. With a PARTIAL table-level mapping (only field 1 "a" is named; field 2 "b" is
        // deliberately absent), the unmapped field "b" must still carry an EXPLICIT EMPTY mapping + the
        // authoritative flag — so BE materializes it as its default/NULL for a legacy file lacking field ids,
        // instead of silently matching a physical column named "b" and reading unrelated data. Exercised at the
        // builder with a hand-built partial map to isolate the fix from iceberg createTable id-reassignment (the
        // property->extractNameMapping->dict flow is covered by nameMappingCarriedWhenTablePropertyPresent).
        // MUTATION: re-gate on nameMapping.containsKey(fieldId) -> "b" carries no mapping -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "a", Types.IntegerType.get()),
                Types.NestedField.required(2, "b", Types.IntegerType.get()));
        Map<Integer, List<String>> partial = Collections.singletonMap(1, Collections.singletonList("a"));

        // hasNameMapping=true: the table carries a (partial) mapping, so it is authoritative for EVERY field.
        Map<String, TField> fields = topFields(IcebergSchemaUtils.buildCurrentSchema(schema,
                Arrays.asList("a", "b"), partial, true, false));

        // mapped field -> its alias, authoritative
        Assertions.assertTrue(fields.get("a").isSetNameMapping());
        Assertions.assertEquals(Collections.singletonList("a"), fields.get("a").getNameMapping());
        Assertions.assertTrue(fields.get("a").isNameMappingIsAuthoritative());
        // unmapped field -> EMPTY list SET (NOT omitted), authoritative
        Assertions.assertTrue(fields.get("b").isSetNameMapping());
        Assertions.assertTrue(fields.get("b").getNameMapping().isEmpty());
        Assertions.assertTrue(fields.get("b").isNameMappingIsAuthoritative());
    }

    @Test
    public void hasNameMappingDistinguishesAbsentFromPresentEmpty() {
        // #65784 distinction (mirrors ExternalUtilTest.testInitSchemaInfoForAllColumnDistinguishesAbsentAndEmpty
        // NameMapping): hasNameMapping=true with an EMPTY map (a table mapping that is PRESENT but maps nothing)
        // is still authoritative — every field gets an empty list + the flag; hasNameMapping=false (property
        // absent) sets NEITHER. Exercised at the builder to isolate the flag from iceberg JSON parsing.
        Schema schema = new Schema(Types.NestedField.required(1, "a", Types.IntegerType.get()));

        Map<String, TField> present = topFields(IcebergSchemaUtils.buildCurrentSchema(schema,
                Collections.singletonList("a"), Collections.emptyMap(), true, false));
        Assertions.assertTrue(present.get("a").isSetNameMapping());
        Assertions.assertTrue(present.get("a").getNameMapping().isEmpty());
        Assertions.assertTrue(present.get("a").isNameMappingIsAuthoritative());

        Map<String, TField> absent = topFields(IcebergSchemaUtils.buildCurrentSchema(schema,
                Collections.singletonList("a"), Collections.emptyMap(), false, false));
        Assertions.assertFalse(absent.get("a").isSetNameMapping());
        Assertions.assertFalse(absent.get("a").isSetNameMappingIsAuthoritative());
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

        Map<Integer, List<String>> mapping = IcebergSchemaUtils.extractNameMapping(table).orElseThrow();

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
        // A malformed property yields Optional.empty() (absent, not a present-empty mapping) -> hasNameMapping
        // false -> the scan proceeds on the legacy name fallback instead of breaking.
        Assertions.assertFalse(IcebergSchemaUtils.extractNameMapping(table).isPresent());
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
