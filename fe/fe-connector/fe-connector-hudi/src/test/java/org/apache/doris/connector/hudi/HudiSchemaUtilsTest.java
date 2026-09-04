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

package org.apache.doris.connector.hudi;

import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Same-loader unit tests for {@link HudiSchemaUtils} — the HD-C4a {@code InternalSchema -> thrift} converter that
 * later steps (C4c/C4d) turn into the native-reader schema dictionary. A wrong/missing entry makes BE either
 * silently read NULL for a renamed column or SIGABRT the whole process on a mixed-case nested name, so these
 * assertions pin the two deliberate deviations from legacy {@code HudiUtils.getSchemaInfo} (every-level
 * lowercasing + STRING-placeholder scalar) plus the id/optional/nested structure legacy already carried. No
 * Mockito, no live metaClient: the {@link InternalSchema} is hand-built via {@link Types}.
 *
 * <p>End-to-end BE field-id matching is only observable on the flip-time docker e2e (native
 * {@code by_table_field_id}); these tests cover the FE-side thrift shape the dictionary is built from.</p>
 */
public class HudiSchemaUtilsTest {

    // A hand-built hudi InternalSchema exercising the casing boundary + every nested container:
    //   Id     INT      (required -> is_optional=false)      field id 1
    //   Name   STRING   (optional)                           field id 2
    //   Addr   STRUCT<Street:STRING>                         field id 3 (child Street id 4)  <- mixed-case nested
    //   Tags   ARRAY<STRING>                                 field id 5 (element id 6)
    //   Props  MAP<STRING,INT>                               field id 7 (key id 8, value id 9)
    // Mixed-case top-level names (Id/Name/Addr/Tags/Props) and a mixed-case nested struct child (Street)
    // exercise the every-level lowercasing crux. schemaId 42 proves the InternalSchema-keyed path uses the
    // schema's OWN committed id (a per-version history entry), not the -1 sentinel.
    private static final long SCHEMA_ID = 42L;

    private static InternalSchema buildInternalSchema() {
        Types.Field street = Types.Field.get(4, true, "Street", Types.StringType.get());
        Types.RecordType addrType = Types.RecordType.get(Collections.singletonList(street));
        List<Types.Field> fields = Arrays.asList(
                Types.Field.get(1, false, "Id", Types.IntType.get()),
                Types.Field.get(2, true, "Name", Types.StringType.get()),
                Types.Field.get(3, true, "Addr", addrType),
                Types.Field.get(5, true, "Tags", Types.ArrayType.get(6, true, Types.StringType.get())),
                Types.Field.get(7, true, "Props",
                        Types.MapType.get(8, 9, Types.StringType.get(), Types.IntType.get())));
        return new InternalSchema(SCHEMA_ID, Types.RecordType.get(fields));
    }

    /** Index a struct's top-level {@link TField} children by name (preserving order for ordering assertions). */
    private static Map<String, TField> topFields(TSchema schema) {
        Map<String, TField> byName = new LinkedHashMap<>();
        for (TFieldPtr ptr : schema.getRootField().getFields()) {
            byName.put(ptr.getFieldPtr().getName(), ptr.getFieldPtr());
        }
        return byName;
    }

    private static TField structChild(TField parent, String name) {
        for (TFieldPtr ptr : parent.getNestedField().getStructField().getFields()) {
            if (ptr.getFieldPtr().getName().equals(name)) {
                return ptr.getFieldPtr();
            }
        }
        throw new AssertionError("no nested field named " + name);
    }

    // --- schema id: InternalSchema-keyed uses the schema's own id; explicit-id keys off the sentinel ---

    @Test
    public void schemaKeyedOffInternalSchemaCommittedId() {
        // A per-version history entry must carry the InternalSchema's OWN committed id so BE can resolve a native
        // file's schema_id against it. MUTATION: hard-code -1 in buildSchemaInfo(InternalSchema) -> the history
        // entry no longer matches any file's committed schema_id -> BE "miss table/file schema info" -> red.
        TSchema schema = HudiSchemaUtils.buildSchemaInfo(buildInternalSchema());
        Assertions.assertEquals(SCHEMA_ID, schema.getSchemaId());
    }

    @Test
    public void explicitSchemaIdIsCarried() {
        // The later -1 target entry is built via buildSchemaInfo(CURRENT_SCHEMA_ID, fields). MUTATION: ignore the
        // schemaId arg -> the target entry loses the -1 sentinel BE selects as the table-side overlay -> red.
        List<Types.Field> fields = buildInternalSchema().getRecord().fields();
        TSchema schema = HudiSchemaUtils.buildSchemaInfo(HudiSchemaUtils.CURRENT_SCHEMA_ID, fields);
        Assertions.assertEquals(-1L, schema.getSchemaId());
    }

    // --- top-level: field ids + lowercased names + is_optional (legacy parity) ---

    @Test
    public void topLevelFieldsCarryHudiFieldIdsAndLowercasedNames() {
        // The dictionary's top-level names must be LOWERCASED (Locale.ROOT) so BE's table-side StructNode keys
        // match the lowercase Doris scan slots, while the id is the hudi InternalSchema field id (the rename-safe
        // join key). MUTATION: keep the hudi case ("Id") -> the lowercase slot lookup misses -> red here, silent
        // NULL on BE.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));

        Assertions.assertEquals(Arrays.asList("id", "name", "addr", "tags", "props"),
                Arrays.asList(fields.keySet().toArray()));
        Assertions.assertFalse(fields.containsKey("Id"));
        Assertions.assertFalse(fields.containsKey("Name"));

        Assertions.assertEquals(1, fields.get("id").getId());
        Assertions.assertEquals(2, fields.get("name").getId());
        Assertions.assertEquals(3, fields.get("addr").getId());
        Assertions.assertEquals(5, fields.get("tags").getId());
        Assertions.assertEquals(7, fields.get("props").getId());
    }

    @Test
    public void isOptionalMirrorsHudiNullability() {
        // Legacy carries hudi's own optional flag at every level (getSchemaInfo sets is_optional from
        // field.isOptional()). A required column stays is_optional=false; an optional one true. MUTATION:
        // hard-code true (leak iceberg's force-nullable habit) -> the required "id" flips -> red.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));
        Assertions.assertFalse(fields.get("id").isIsOptional());
        Assertions.assertTrue(fields.get("name").isIsOptional());
    }

    // --- scalar placeholder + nested struct/array/map carry field ids at every level ---

    @Test
    public void scalarFieldsUseStringPlaceholder() {
        // BE reads type.type only as a nested-vs-scalar discriminator on the field-id path, so every scalar is a
        // single STRING placeholder regardless of the real hudi type — the deliberate deviation from legacy's full
        // fromAvroHudiTypeToDorisType map. MUTATION: port the real type (INT for "id") -> not STRING -> red; the
        // assertion pins the simplification as intentional.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));
        Assertions.assertEquals(TPrimitiveType.STRING, fields.get("id").getType().getType());
        Assertions.assertEquals(TPrimitiveType.STRING, fields.get("name").getType().getType());
    }

    @Test
    public void nestedStructChildIsLowercasedAndCarriesId() {
        // THE SIGABRT crux (mirror of the iceberg DROP_AND_ADD fix): a mixed-case nested struct child ("Street")
        // must be emitted LOWERCASED. The same history_schema_info thrift feeds the v1 format/table hudi reader,
        // whose StructNode is looked up by the lowercase Doris slot name; keeping the hudi case makes BE's
        // children.at("street") throw std::out_of_range -> whole-process SIGABRT. MUTATION (the bug): emit
        // field.name() verbatim for struct children -> the lowercase key is absent / the mixed-case key leaks -> red.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));
        TField addr = fields.get("addr");
        Assertions.assertEquals(TPrimitiveType.STRUCT, addr.getType().getType());

        TField street = structChild(addr, "street");
        Assertions.assertEquals(4, street.getId());
        Assertions.assertEquals(TPrimitiveType.STRING, street.getType().getType());
        // ... and the hudi-cased name must NOT leak through (that is exactly what aborts BE).
        Assertions.assertThrows(AssertionError.class, () -> structChild(addr, "Street"));
    }

    @Test
    public void arrayElementCarriesIdAndType() {
        // Faithful to legacy: the array element is a nested TField carrying its own hudi field id (BE field-id
        // matches nested fields too). MUTATION: drop the element field id -> BE cannot map the element -> red.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));
        TField tags = fields.get("tags");
        Assertions.assertEquals(TPrimitiveType.ARRAY, tags.getType().getType());

        TField element = tags.getNestedField().getArrayField().getItemField().getFieldPtr();
        Assertions.assertEquals(6, element.getId());
        Assertions.assertEquals(TPrimitiveType.STRING, element.getType().getType());
    }

    @Test
    public void mapKeyAndValueCarryIdsAndTypes() {
        // Faithful to legacy: map key (index 0) + value (index 1) each carry their own hudi field id. MUTATION:
        // swap or drop the key/value ids -> BE mis-maps the map entries -> red.
        Map<String, TField> fields = topFields(HudiSchemaUtils.buildSchemaInfo(buildInternalSchema()));
        TField props = fields.get("props");
        Assertions.assertEquals(TPrimitiveType.MAP, props.getType().getType());

        TField key = props.getNestedField().getMapField().getKeyField().getFieldPtr();
        TField value = props.getNestedField().getMapField().getValueField().getFieldPtr();
        Assertions.assertEquals(8, key.getId());
        Assertions.assertEquals(9, value.getId());
        Assertions.assertEquals(TPrimitiveType.STRING, key.getType().getType());
        Assertions.assertEquals(TPrimitiveType.STRING, value.getType().getType());
    }

    // --- round-trip through the base64 prop transport (what C4d's getScanNodeProperties/apply will do) ---

    @Test
    public void encodeApplyRoundTripsThroughBase64() {
        // encode() serializes the dict; applySchemaEvolution() copies current_schema_id + history_schema_info back
        // onto the real params — the exact transport C4d round-trips through the scan-node props. MUTATION: drop
        // either copied field in applySchemaEvolution -> the params are missing it -> red. The nested "street"
        // stays lowercased after the round-trip (proves the whole tree survives serialization).
        TSchema history = HudiSchemaUtils.buildSchemaInfo(buildInternalSchema());
        String encoded = HudiSchemaUtils.encode(HudiSchemaUtils.CURRENT_SCHEMA_ID,
                Collections.singletonList(history));

        TFileScanRangeParams params = new TFileScanRangeParams();
        HudiSchemaUtils.applySchemaEvolution(params, encoded);

        Assertions.assertTrue(params.isSetCurrentSchemaId());
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(1, params.getHistorySchemaInfoSize());
        TSchema decoded = params.getHistorySchemaInfo().get(0);
        Assertions.assertEquals(SCHEMA_ID, decoded.getSchemaId());
        Map<String, TField> decodedTop = topFields(decoded);
        Assertions.assertEquals(5, decodedTop.size());
        // The nested struct child survives serialization with its lowercased name AND its field id (structChild's
        // case-sensitive lookup fails if the round-trip dropped it or left it mixed-case; the id assertion proves
        // the whole nested TField — not just the name — round-trips).
        Assertions.assertEquals(4, structChild(decodedTop.get("addr"), "street").getId());
    }

    @Test
    public void applyIsNoOpForNullOrEmpty() {
        // A null/empty prop (e.g. another connector's props map, or a handle that emits no dict) must leave the
        // params untouched, not throw. MUTATION: drop the guard -> Base64.decode(null) NPEs -> red.
        TFileScanRangeParams params = new TFileScanRangeParams();
        HudiSchemaUtils.applySchemaEvolution(params, null);
        HudiSchemaUtils.applySchemaEvolution(params, "");
        Assertions.assertFalse(params.isSetCurrentSchemaId());
        Assertions.assertFalse(params.isSetHistorySchemaInfo());
    }

    @Test
    public void applyFailsLoudOnCorruptProp() {
        // The prop is produced by us, so a decode failure is a real bug — fail loud rather than silently drop it
        // (a dropped dict re-introduces the silent wrong-rows risk on schema-evolved native reads). MUTATION:
        // swallow the exception -> no throw -> red.
        TFileScanRangeParams params = new TFileScanRangeParams();
        Assertions.assertThrows(RuntimeException.class,
                () -> HudiSchemaUtils.applySchemaEvolution(params, "!!!not-base64-thrift!!!"));
    }

    // ========== C4d: scan-level dictionary (-1 target entry + history entries) ==========

    private static HudiColumnHandle handle(String name, int fieldId) {
        return new HudiColumnHandle(name, "string", false, fieldId);
    }

    private static InternalSchema internalSchemaWithId(long schemaId) {
        return new InternalSchema(schemaId, Types.RecordType.get(Collections.singletonList(
                Types.Field.get(1, false, "id", Types.IntType.get()))));
    }

    @Test
    public void targetEntryKeyedOffRequestedColumnsWithSchemaIds() {
        // The -1 target entry's top-level names must be EXACTLY the requested (scan-slot) columns, in order, each
        // carrying the stable field id + full nested structure looked up BY NAME in the base InternalSchema. This
        // is the overlay BE stamps each table column's id from. MUTATION: emit all base columns instead of the
        // requested subset -> extra "name"/"tags"/"props" appear -> red.
        InternalSchema base = buildInternalSchema();
        TSchema target = HudiSchemaUtils.buildTargetSchema(
                Arrays.asList(handle("id", 1), handle("Addr", 3)), base);

        Assertions.assertEquals(-1L, target.getSchemaId());
        Map<String, TField> top = topFields(target);
        Assertions.assertEquals(Arrays.asList("id", "addr"), Arrays.asList(top.keySet().toArray()));
        // "id" carries the base InternalSchema field id (1) + scalar placeholder type ...
        Assertions.assertEquals(1, top.get("id").getId());
        Assertions.assertEquals(TPrimitiveType.STRING, top.get("id").getType().getType());
        // ... "Addr" (mixed-case request) is matched case-insensitively, lowercased, id 3, with its nested struct
        // child "street" carried (id 4) — proves the target entry keeps nested field ids for BE nested matching.
        TField addr = top.get("addr");
        Assertions.assertEquals(3, addr.getId());
        Assertions.assertEquals(TPrimitiveType.STRUCT, addr.getType().getType());
        Assertions.assertEquals(4, structChild(addr, "street").getId());
    }

    @Test
    public void targetEntryFallsBackToScalarForColumnAbsentFromSchema() {
        // A requested column not in the base InternalSchema (e.g. a _hoodie_* meta column on a schema-evolved
        // table whose commit-metadata schema omits meta fields) must still appear in the -1 entry (it IS a BE scan
        // slot) as a scalar placeholder carrying the handle's field id. MUTATION: drop it -> the -1 entry is
        // missing a scan slot -> BE StructNode lookup miss -> red.
        InternalSchema base = buildInternalSchema();
        TSchema target = HudiSchemaUtils.buildTargetSchema(
                Arrays.asList(handle("id", 1), handle("_hoodie_commit_time", 77)), base);

        Map<String, TField> top = topFields(target);
        Assertions.assertTrue(top.containsKey("_hoodie_commit_time"));
        Assertions.assertEquals(77, top.get("_hoodie_commit_time").getId());
        Assertions.assertEquals(TPrimitiveType.STRING, top.get("_hoodie_commit_time").getType().getType());
    }

    @Test
    public void targetEntryEmptyRequestedFallsBackToAllBaseFields() {
        // A count-only scan (no projected columns) yields an empty requested list; the -1 entry then carries all
        // base top-level fields (a valid superset). MUTATION: emit an empty root -> BE has no target overlay -> red.
        TSchema target = HudiSchemaUtils.buildTargetSchema(Collections.emptyList(), buildInternalSchema());
        Assertions.assertEquals(5, target.getRootField().getFieldsSize());
    }

    @Test
    public void dictNonEvolutionEmitsTargetPlusBaseVersion() {
        // Non-evolution table: no schema history file, so the dict is the -1 target + the single base
        // (convert()) version. MUTATION: skip the base entry -> a native file's schema_id (0) has no history
        // entry -> BE "miss schema info" fail-loud -> red.
        InternalSchema base = internalSchemaWithId(0L);
        String encoded = HudiSchemaUtils.buildSchemaEvolutionDict(
                Collections.singletonList(handle("id", 1)), base, false, Collections.emptyList());

        TFileScanRangeParams params = new TFileScanRangeParams();
        HudiSchemaUtils.applySchemaEvolution(params, encoded);
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        Assertions.assertEquals(2, params.getHistorySchemaInfoSize());
        Assertions.assertEquals(-1L, params.getHistorySchemaInfo().get(0).getSchemaId());
        Assertions.assertEquals(0L, params.getHistorySchemaInfo().get(1).getSchemaId());
    }

    @Test
    public void dictGatedOffWhenAnyProjectedColumnUnresolved() {
        // BE field-id matching is per-FILE, not per-column: a projected column with no field id (e.g. a _hoodie_*
        // meta column on a schema-evolved table whose commit-metadata schema omits meta fields) cannot be BY_NAME'd
        // on its own, so the WHOLE dict must be suppressed -> BE stays on BY_NAME for the scan (no silent
        // const-NULL). The gate returns empty BEFORE touching the metaClient/schemaResolver, so the nulls here are
        // never dereferenced. MUTATION: drop the gate -> the dict is built (NPEs on the null resolver here, and at
        // runtime would emit a -1 target field with id=-1 that BE reads as const-NULL) -> not empty -> red.
        Assertions.assertFalse(HudiSchemaUtils.buildSchemaEvolutionProp(
                null, null, null, Arrays.asList(handle("id", 1), handle("_hoodie_commit_time", -1))).isPresent());
    }

    @Test
    public void resolveFileSchemaNonEvolutionReturnsBaseWithoutMetaClient() {
        // The COMMON (schema.on.read off) per-file schema_id source: the non-evolution branch returns the base
        // schema (its schemaId, 0 for convert()) WITHOUT reading the file path or metaClient -> it must equal the
        // version-0 history entry the dict emits (self-consistency, else BE "miss schema info"). Passing a null
        // metaClient proves the branch never dereferences it. MUTATION: return null / a different schema -> red.
        InternalSchema base = internalSchemaWithId(0L);
        InternalSchema resolved = HudiSchemaUtils.resolveFileInternalSchema(
                "s3://b/t/anyfile.parquet", false, base, null);
        Assertions.assertSame(base, resolved);
        Assertions.assertEquals(0L, resolved.schemaId());
    }

    @Test
    public void dictEvolutionEmitsTargetPlusAllHistoricalVersions() {
        // Evolution table: the dict is the -1 target + ONE entry per committed schema version (all-historical =
        // robust superset of the referenced-file versions). MUTATION: emit only the base version -> a native file
        // written under an older version has no history entry -> BE "miss schema info" -> red.
        InternalSchema base = internalSchemaWithId(2L);
        String encoded = HudiSchemaUtils.buildSchemaEvolutionDict(
                Collections.singletonList(handle("id", 1)), base, true,
                Arrays.asList(internalSchemaWithId(0L), internalSchemaWithId(1L), internalSchemaWithId(2L)));

        TFileScanRangeParams params = new TFileScanRangeParams();
        HudiSchemaUtils.applySchemaEvolution(params, encoded);
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        // -1 target + versions 0/1/2
        Assertions.assertEquals(4, params.getHistorySchemaInfoSize());
        Assertions.assertEquals(-1L, params.getHistorySchemaInfo().get(0).getSchemaId());
        Assertions.assertEquals(0L, params.getHistorySchemaInfo().get(1).getSchemaId());
        Assertions.assertEquals(1L, params.getHistorySchemaInfo().get(2).getSchemaId());
        Assertions.assertEquals(2L, params.getHistorySchemaInfo().get(3).getSchemaId());
    }

    // ========== HD-C5b: scan-side schema AT the pinned instant (FOR TIME AS OF over an evolution table) ==========

    @Test
    public void jniSchemaNoPinReturnsLatestWithoutResolving() {
        // A plain read (queryInstant == null) must NOT reload the timeline / resolve at-instant -> byte-identical
        // to before HD-C5b, and the JNI column list stays the latest schema. Passing a null schemaResolver +
        // metaClient proves the pin branch is never entered. MUTATION: drop the null-instant short-circuit ->
        // resolveInternalSchemaAtInstant NPEs on the null metaClient -> red.
        Schema latest = AvroInternalSchemaConverter.convert(buildInternalSchema(), "hudi_table");
        Assertions.assertSame(latest,
                HudiSchemaUtils.resolveJniColumnSchema(null, null, latest, null));
    }

    @Test
    public void jniSchemaUsesPinnedInstantSchemaNames() {
        // FOR TIME AS OF over a schema-on-read table: the JNI (MOR-realtime) reader's column list must be the
        // schema AT the pin, so a column renamed AFTER the pin shows its HISTORICAL (pinned) name to the merge
        // reader (legacy HudiScanNode:222-224). MUTATION: return latestAvro instead of converting the pinned
        // InternalSchema -> the pinned field name is absent -> red.
        InternalSchema pinned = new InternalSchema(3L, Types.RecordType.get(Collections.singletonList(
                Types.Field.get(1, false, "oldname", Types.IntType.get()))));
        // The SAME field is "newname" at latest — the JNI list must NOT use it under the pin.
        Schema latest = AvroInternalSchemaConverter.convert(new InternalSchema(5L, Types.RecordType.get(
                Collections.singletonList(Types.Field.get(1, false, "newname", Types.IntType.get())))),
                "hudi_table");

        Schema jni = HudiSchemaUtils.chooseJniSchema(latest, Optional.of(pinned));
        List<String> names = new ArrayList<>();
        jni.getFields().forEach(f -> names.add(f.name()));
        Assertions.assertEquals(Collections.singletonList("oldname"), names);
    }

    @Test
    public void atInstantOverlayCarriesFullPinnedSchemaWithHistoricalNames() {
        // HD-C5b: the at-instant -1 overlay is built from the FULL pinned schema (the empty-requested path over
        // the pinned InternalSchema), so every pinned field carries its PINNED (historical) name + stable id and
        // the overlay is a SUPERSET of the scan slots — BE matches old files BY FIELD ID and looks each scan slot
        // up BY NAME (a scan slot MISSING from the overlay would std::out_of_range/SIGABRT; extra fields are
        // ignored). This test pins that PURE assembly with a rename-shaped fixture (field "oldname"@pin), asserting
        // the overlay keys off the PINNED names, not latest. MUTATION: buildSchemaEvolutionDict dropping a pinned
        // field from the empty-requested overlay, or emitting only the base version instead of all versions -> red.
        // NOT covered here (e2e-owed, design §5 HD-C5b): the PROVIDER ROUTING that selects this full-pinned path
        // when a FOR TIME AS OF pin is present (getScanNodeProperties `if (pinnedSchema.isPresent())` +
        // resolveInternalSchemaAtInstant / buildSchemaEvolutionDictAtInstant) needs a LIVE metaClient; a mutation
        // deleting that branch (a pinned read falling back to the latest-keyed steady-state dict) is only
        // observable at flip-time e2e — no same-loader test can catch it.
        InternalSchema pinned = new InternalSchema(3L, Types.RecordType.get(Arrays.asList(
                Types.Field.get(1, false, "oldname", Types.IntType.get()),
                Types.Field.get(2, true, "other", Types.StringType.get()))));
        String encoded = HudiSchemaUtils.buildSchemaEvolutionDict(
                Collections.emptyList(), pinned, true,
                Arrays.asList(internalSchemaWithId(0L), internalSchemaWithId(3L)));

        TFileScanRangeParams params = new TFileScanRangeParams();
        HudiSchemaUtils.applySchemaEvolution(params, encoded);
        Assertions.assertEquals(-1L, params.getCurrentSchemaId());
        TSchema overlay = params.getHistorySchemaInfo().get(0);
        Assertions.assertEquals(-1L, overlay.getSchemaId());
        Map<String, TField> top = topFields(overlay);
        // BOTH pinned fields present with their pinned names + stable ids (full-pinned superset).
        Assertions.assertEquals(Arrays.asList("oldname", "other"), new ArrayList<>(top.keySet()));
        Assertions.assertEquals(1, top.get("oldname").getId());
        Assertions.assertEquals(2, top.get("other").getId());
        // history = -1 overlay + the two committed versions (all-versions, Option B).
        Assertions.assertEquals(3, params.getHistorySchemaInfoSize());
    }
}
