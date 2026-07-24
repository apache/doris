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

import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Builds the native-reader schema dictionary ({@code current_schema_id} + {@code history_schema_info}) so BE
 * matches file&harr;table columns BY FIELD ID across schema evolution (rename/reorder), instead of falling
 * back to NAME matching (which silently reads NULL/garbage for renamed columns) or DCHECK-aborting the whole
 * BE on a missing column. Self-contained iceberg&rarr;thrift port of legacy {@code IcebergScanNode.create
 * ScanRangeLocations} + {@code ExternalUtil.initSchemaInfoFor{All,Pruned}Column} + {@code extractNameMapping}
 * (mirrors {@code IcebergPartitionUtils}/{@code IcebergPredicateConverter}; zero fe-core import).
 *
 * <p><b>Iceberg vs paimon (the load-bearing divergence)</b>: iceberg emits exactly ONE schema entry
 * ({@code current_schema_id = -1}). BE reads the FILE field ids straight from the parquet/orc file metadata
 * ({@code iceberg_reader.cpp by_parquet_field_id} / {@code by_orc_field_id}) and matches them by equality to
 * this single table-side entry. Because iceberg field ids are permanent invariants, NO per-file
 * {@code schema_id} is looked up (legacy emits only the {@code -1} entry too) — unlike paimon/hudi
 * ({@code by_table_field_id}), which match the FE-supplied file schema and therefore need a per-committed-id
 * history. See {@code designs/P6-T06-iceberg-scan-fieldid-design.md} §0/§1.</p>
 *
 * <p>The {@code -1} entry is keyed off the REQUESTED columns (= the authoritative Doris scan slots), so its
 * top-level names == the BE scan-slot names BY CONSTRUCTION — the invariant BE's {@code StructNode}
 * {@code children_column_exists} DCHECK relies on (CI #969249). Per-field {@code name_mapping} (from the
 * table's {@code schema.name-mapping.default}) is carried for BE's old-file fallback
 * ({@code by_parquet_field_id_with_name_mapping}). Each {@code TField} carries only what BE's field-id path
 * consumes — {@code id} / {@code name} / a nested-vs-scalar {@code type.type} tag (a {@code STRING}
 * placeholder for every scalar; BE never inspects the scalar tag) / {@code name_mapping} — and, faithful to
 * legacy {@code ExternalUtil}, an {@code id}/{@code name} at EVERY nesting level (array element, map
 * key/value, struct child), unlike paimon which omits them on collection elements.</p>
 */
public final class IcebergSchemaUtils {

    private static final Logger LOG = LogManager.getLogger(IcebergSchemaUtils.class);

    // Legacy parity: current_schema_id is the -1 sentinel ("latest"); the current/target schema is also
    // pushed into history_schema_info under this id (IcebergScanNode.createScanRangeLocations -> -1L).
    static final long CURRENT_SCHEMA_ID = -1L;

    // Iceberg v3 row-lineage metadata columns (_row_id / _last_updated_sequence_number). Names + reserved field
    // ids MIRROR IcebergConnectorMetadata's constants (a fe-core contract test pins those to
    // IcebergUtils.ICEBERG_ROW_ID_COL / ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL + the reserved ids). They are
    // never in schema.columns(), yet are projected as GENERATED BE scan slots -> they must be appended to the
    // dict for a format-version >= 3 table so BE's StructNode children map carries them (else the ParquetReader,
    // which iterates column_names unconditionally, does children.at("_row_id") -> std::out_of_range and SIGABRTs
    // the whole BE). See appendRowLineageFields.
    static final String ICEBERG_ROW_ID_COL = "_row_id";
    static final String ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL = "_last_updated_sequence_number";
    static final int ICEBERG_ROW_ID_FIELD_ID = 2147483540;
    static final int ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = 2147483539;

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private IcebergSchemaUtils() {
    }

    /**
     * Orchestrator: build the schema dictionary for {@code table} keyed off the requested (lowercased) column
     * names and serialize it for transport via the scan-node props. {@code requestedLowerNames} is the pruned
     * scan-slot list ({@code PluginDrivenScanNode} hands the provider the requested columns); an empty list
     * (count-only scan / no column handles) falls back to all top-level schema columns.
     */
    static String encodeSchemaEvolutionProp(Table table, List<String> requestedLowerNames) {
        return encodeSchemaEvolutionProp(table, table.schema(), requestedLowerNames, false, false);
    }

    /**
     * Like {@link #encodeSchemaEvolutionProp(Table, List)} but builds the dictionary from an explicit
     * {@code dictSchema} (the latest schema for a normal read, or a historical schema for a time-travel read —
     * T07 Option A passes the PINNED schema with an empty {@code requestedLowerNames} so the dict covers every
     * BE scan slot). The name mapping is still read from {@code table} (it is table-level, not schema-versioned).
     *
     * <p>{@code appendRowLineage} (set by the caller when the table format-version &gt;= 3) appends the iceberg
     * v3 row-lineage columns ({@code _row_id} / {@code _last_updated_sequence_number}) to the dict root. They are
     * GENERATED BE scan slots (so they reach BE {@code column_names}) but are NOT in {@code schema.columns()}, so
     * without this the BE {@code StructNode} children map misses them and the ParquetReader's unconditional
     * {@code children.at("_row_id")} {@code std::out_of_range}-SIGABRTs the whole BE. See
     * {@link #appendRowLineageFields}.</p>
     */
    static String encodeSchemaEvolutionProp(Table table, Schema dictSchema, List<String> requestedLowerNames,
            boolean appendRowLineage) {
        // Thin overload: default enableTimestampTz=false (the callers that do not thread the catalog's
        // enable.mapping.timestamp_tz flag keep the pre-#65502 UTC-wall-time behaviour).
        return encodeSchemaEvolutionProp(table, dictSchema, requestedLowerNames, appendRowLineage, false);
    }

    /**
     * The real builder. {@code enableTimestampTz} (#65502) mirrors the catalog's
     * {@code enable.mapping.timestamp_tz} flag: it is threaded into {@link #buildCurrentSchema} so a
     * TIMESTAMPTZ column's iceberg initial default is serialized consistently with how BE will read the
     * column (keep the trailing offset when tz-mapping is on, drop it — DATETIMEV2 UTC wall time — when off).
     */
    static String encodeSchemaEvolutionProp(Table table, Schema dictSchema, List<String> requestedLowerNames,
            boolean appendRowLineage, boolean enableTimestampTz) {
        Optional<Map<Integer, List<String>>> nameMapping = extractNameMapping(table);
        // #65784: it is the PRESENCE of the table-level mapping (not its non-emptiness) that makes it
        // authoritative for BE, so thread isPresent() through as hasNameMapping.
        TSchema current = buildCurrentSchema(dictSchema, requestedLowerNames,
                nameMapping.orElse(Collections.emptyMap()), nameMapping.isPresent(), enableTimestampTz);
        if (appendRowLineage) {
            appendRowLineageFields(current.getRootField());
        }
        return encode(CURRENT_SCHEMA_ID, Collections.singletonList(current));
    }

    /**
     * Decode the schema-evolution prop produced by {@link #encodeSchemaEvolutionProp} and copy
     * {@code current_schema_id} + {@code history_schema_info} onto the real scan params. Fail loud on a decode
     * error — this prop is produced by us, so a failure is a real bug, and silently dropping it would
     * re-introduce the silent wrong-rows BLOCKER on schema-evolved native reads.
     */
    static void applySchemaEvolution(TFileScanRangeParams params, String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            TFileScanRangeParams carrier = new TFileScanRangeParams();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(carrier, bytes);
            if (carrier.isSetCurrentSchemaId()) {
                params.setCurrentSchemaId(carrier.getCurrentSchemaId());
            }
            if (carrier.isSetHistorySchemaInfo()) {
                params.setHistorySchemaInfo(carrier.getHistorySchemaInfo());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply iceberg schema-evolution info to scan params", e);
        }
    }

    /**
     * Extract the iceberg name mapping ({@code schema.name-mapping.default}) as field-id &rarr; alternate
     * names, recursing into nested mappings. Returns {@link Optional#empty()} when the table has NO
     * name-mapping property, and a present (possibly empty) map when it does — the distinction #65784 relies on
     * to make a table-level mapping AUTHORITATIVE (an unmapped field then materializes its default/NULL instead
     * of silently matching a physical column by its current name; see {@link #buildField}). Port of legacy
     * {@code IcebergScanNode.extractNameMapping} + {@code IcebergUtils.getNameMapping} (#65784); fail-soft (a
     * parse error logs + yields {@code Optional.empty()}, so a malformed property never breaks the scan).
     */
    static Optional<Map<Integer, List<String>>> extractNameMapping(Table table) {
        String nameMappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        if (nameMappingJson == null || nameMappingJson.isEmpty()) {
            return Optional.empty();
        }
        try {
            NameMapping mapping = NameMappingParser.fromJson(nameMappingJson);
            if (mapping == null) {
                return Optional.empty();
            }
            Map<Integer, List<String>> result = new HashMap<>();
            collectNameMappings(mapping.asMappedFields(), result);
            return Optional.of(result);
        } catch (Exception e) {
            // If name mapping parsing fails, continue without it (legacy parity).
            LOG.warn("Failed to parse name mapping from Iceberg table properties", e);
            return Optional.empty();
        }
    }

    private static void collectNameMappings(MappedFields fields, Map<Integer, List<String>> result) {
        if (fields == null) {
            return;
        }
        for (MappedField field : fields.fields()) {
            // Iceberg permits id-less wrapper entries; only their nested ID-bearing aliases can participate in
            // Doris field-id lookup (matches #65784 getNameMapping — a null id must not become a map key).
            if (field.id() != null) {
                result.put(field.id(), new ArrayList<>(field.names()));
            }
            collectNameMappings(field.nestedMapping(), result);
        }
    }

    /**
     * Build the single {@code TSchema} (schema_id = -1) keyed off the requested column names (the CI #969249
     * fix: the top-level names == the BE scan slots so the {@code StructNode} DCHECK can never miss). Each
     * requested name is matched case-insensitively to the iceberg schema; its top-level {@code TField} name is
     * the requested name VERBATIM (byte-matching the Doris slot name; case-preserved post-#65094). An
     * empty/{@code null} {@code requestedLowerNames} falls back to all top-level columns (lowercased). Fail
     * loud if a requested column is absent from the schema (a genuine FE/connector inconsistency — not a
     * silent drop).
     */
    static TSchema buildCurrentSchema(Schema schema, List<String> requestedLowerNames,
            Map<Integer, List<String>> nameMapping) {
        // Thin overload: default enableTimestampTz=false (pre-#65502 timestamp-default behaviour) and derive
        // hasNameMapping from the map (a non-empty map ⇒ the table carried a mapping — the #65784 default;
        // the production path threads the precise isPresent() instead).
        return buildCurrentSchema(schema, requestedLowerNames, nameMapping,
                nameMapping != null && !nameMapping.isEmpty(), false);
    }

    /**
     * The real builder; {@code enableTimestampTz} (#65502) is threaded into every {@link #buildField} call so a
     * TIMESTAMPTZ column's iceberg initial default is serialized to match BE's read of the column (see
     * {@link #serializeInitialDefault}). {@code hasNameMapping} (#65784) marks the table-level name mapping
     * AUTHORITATIVE, so every field carries an explicit (possibly empty) per-field mapping (see
     * {@link #buildField}).
     */
    static TSchema buildCurrentSchema(Schema schema, List<String> requestedLowerNames,
            Map<Integer, List<String>> nameMapping, boolean hasNameMapping, boolean enableTimestampTz) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(CURRENT_SCHEMA_ID);
        TStructField root = new TStructField();
        if (requestedLowerNames == null || requestedLowerNames.isEmpty()) {
            for (Types.NestedField field : schema.columns()) {
                addField(root, buildField(field, field.name().toLowerCase(Locale.ROOT), nameMapping,
                        hasNameMapping, enableTimestampTz));
            }
        } else {
            for (String name : requestedLowerNames) {
                Types.NestedField field = schema.caseInsensitiveFindField(name);
                if (field == null) {
                    throw new RuntimeException("iceberg schema-evolution: requested column '" + name
                            + "' not found in the table schema");
                }
                addField(root, buildField(field, name, nameMapping, hasNameMapping, enableTimestampTz));
            }
        }
        tSchema.setRootField(root);
        return tSchema;
    }

    /**
     * Recursively build a {@link TField} from an iceberg {@link Types.NestedField}. {@code nameOverride}
     * replaces the field name at the top level (the Doris slot name, case-preserved post-#65094);
     * {@code null} (every nested field) falls back to the iceberg field name LOWERCASED. Lowercasing is
     * load-bearing for nested struct
     * children: the Doris slot's {@code DataTypeStruct} child names are force-lowercased ({@code StructField}
     * ctor, via {@code ConnectorColumnConverter}), and BE's {@code StructNode} looks the child up by that
     * lowercase name — keeping the iceberg case (e.g. {@code DROP_AND_ADD}) makes BE's
     * {@code children.at("drop_and_add")} throw {@code std::out_of_range} and SIGABRT the whole struct read.
     * For array {@code element} / map {@code key}/{@code value} the lowercasing is a no-op (iceberg's canonical
     * names are already lowercase; BE matches collection nodes positionally anyway). Carries the iceberg field
     * id + name + nullability +
     * name-mapping at EVERY level (legacy {@code ExternalUtil} parity), and a nested-vs-scalar {@code type.type}
     * (a {@code STRING} placeholder for scalars — BE uses it only as a discriminator).
     */
    private static TField buildField(Types.NestedField field, String nameOverride,
            Map<Integer, List<String>> nameMapping, boolean hasNameMapping, boolean enableTimestampTz) {
        TField tField = new TField();
        tField.setId(field.fieldId());
        tField.setName(nameOverride != null ? nameOverride : field.name().toLowerCase(Locale.ROOT));
        // is_optional is byte-matched to legacy: ExternalUtil sets it from the Doris column's isAllowNull(),
        // which IcebergConnectorMetadata.parseSchema forces to true for EVERY iceberg column (a required iceberg
        // field still surfaces nullable). BE does NOT read is_optional on the iceberg field-id path
        // (table_schema_change_helper / iceberg_reader never reference it), so this is inert there, but we keep
        // legacy parity rather than leak iceberg's required/optional flag into the dictionary.
        tField.setIsOptional(true);
        if (hasNameMapping) {
            // #65784: a table-level name mapping is AUTHORITATIVE. Emit an explicit — possibly EMPTY — per-field
            // list (every field, not just the mapped ones) and flag it, so BE materializes an unmapped legacy
            // field (a file without embedded field ids) as its default/NULL instead of silently matching a
            // physical column by its current name. Absence of the flag keeps the legacy name fallback, which
            // preserves an old-FE plan's behavior on a new BE during a rolling upgrade.
            tField.setNameMapping(new ArrayList<>(
                    nameMapping.getOrDefault(field.fieldId(), Collections.emptyList())));
            tField.setNameMappingIsAuthoritative(true);
        }

        // #65502: carry each field's iceberg initial default so BE can materialize an equality-delete key
        // (or any column) that is absent from an old data file with its typed default instead of NULL.
        // Binary-like values (UUID/BINARY/FIXED) go through a lossless Base64 carrier flagged for BE, because
        // their Doris type (STRING/CHAR when varbinary-mapping is off) can't tell BE to decode bytes; other
        // values use the Doris FE string form (timestamp normalized to DATETIMEV2 spacing).
        if (field.initialDefault() != null) {
            if (isBinaryLike(field.type())) {
                tField.setInitialDefaultValue(serializeBinaryInitialDefault(field.type(), field.initialDefault()));
                tField.setInitialDefaultValueIsBase64(true);
            } else {
                tField.setInitialDefaultValue(
                        serializeInitialDefault(field.type(), field.initialDefault(), enableTimestampTz));
            }
        }

        Type type = field.type();
        TColumnType columnType = new TColumnType();
        if (type.isPrimitiveType()) {
            // Scalar: BE reads type.type only as a nested-vs-scalar discriminator (it never inspects the
            // specific scalar tag in the field-id path), so a single placeholder is sufficient.
            columnType.setType(TPrimitiveType.STRING);
            tField.setType(columnType);
            return tField;
        }

        TNestedField nestedField = new TNestedField();
        switch (type.typeId()) {
            case LIST: {
                columnType.setType(TPrimitiveType.ARRAY);
                Types.ListType listType = (Types.ListType) type;
                TArrayField arrayField = new TArrayField();
                arrayField.setItemField(fieldPtr(
                        buildField(listType.fields().get(0), null, nameMapping, hasNameMapping, enableTimestampTz)));
                nestedField.setArrayField(arrayField);
                break;
            }
            case MAP: {
                columnType.setType(TPrimitiveType.MAP);
                Types.MapType mapType = (Types.MapType) type;
                List<Types.NestedField> kv = mapType.fields();
                TMapField mapField = new TMapField();
                mapField.setKeyField(fieldPtr(
                        buildField(kv.get(0), null, nameMapping, hasNameMapping, enableTimestampTz)));
                mapField.setValueField(fieldPtr(
                        buildField(kv.get(1), null, nameMapping, hasNameMapping, enableTimestampTz)));
                nestedField.setMapField(mapField);
                break;
            }
            case STRUCT: {
                columnType.setType(TPrimitiveType.STRUCT);
                Types.StructType structType = (Types.StructType) type;
                TStructField structField = new TStructField();
                for (Types.NestedField child : structType.fields()) {
                    addField(structField, buildField(child, null, nameMapping, hasNameMapping, enableTimestampTz));
                }
                nestedField.setStructField(structField);
                break;
            }
            default:
                // Defensive: a non-primitive type id we don't model (e.g. a future iceberg nested type). Emit a
                // scalar placeholder so BE treats it as a leaf rather than descending into an unset nested field.
                columnType.setType(TPrimitiveType.STRING);
                tField.setType(columnType);
                return tField;
        }
        tField.setType(columnType);
        tField.setNestedField(nestedField);
        return tField;
    }

    private static String serializeInitialDefault(Type type, Object value, boolean enableTimestampTz) {
        String humanValue = Transforms.identity(type).toHumanString(type, value);
        if (type.typeId() == TypeID.TIMESTAMP) {
            // Iceberg prints ISO-8601 (2024-01-01T00:00:00); Doris DATETIMEV2 needs a space separator.
            String dorisValue = humanValue.replace('T', ' ');
            if (((Types.TimestampType) type).shouldAdjustToUTC() && !enableTimestampTz) {
                // timestamptz human form carries a trailing offset; DATETIMEV2 has no offset carrier, so keep
                // the displayed UTC wall time and drop the suffix (only when tz-mapping is off).
                return dorisValue.replaceFirst("(Z|[+-]\\d{2}:\\d{2})$", "");
            }
            return dorisValue;
        }
        return humanValue;
    }

    private static boolean isBinaryLike(Type type) {
        return type.typeId() == TypeID.UUID || type.typeId() == TypeID.BINARY || type.typeId() == TypeID.FIXED;
    }

    /**
     * The Doris FE default-value string for a field's WRITE default (iceberg {@code writeDefault}, applied to
     * new rows), used to fill an INSERT-omitted column and to render the column default in DESCRIBE — or
     * {@code null} when there is nothing to surface. Only flat scalar defaults map to a Doris {@code Column}
     * default string, so complex types (STRUCT/LIST/MAP) and binary-like types (UUID/BINARY/FIXED) — whose
     * value can't be carried as a plain unquoted literal that DESCRIBE / INSERT re-parse — return null.
     * Non-binary scalars reuse the same human-string form as the read-side initial default (timestamp
     * normalized to DATETIMEV2 spacing, timestamptz offset handling honored) so a write default displays
     * exactly like a read default. This ONLY populates the FE {@link org.apache.doris.connector.api.ConnectorColumn}
     * metadata; it is orthogonal to the initialDefault BE-dictionary path in {@link #buildField} (#65502).
     */
    static String writeDefaultToDorisString(Type type, Object writeDefault, boolean enableTimestampTz) {
        if (writeDefault == null || !type.isPrimitiveType() || isBinaryLike(type)) {
            return null;
        }
        return serializeInitialDefault(type, writeDefault, enableTimestampTz);
    }

    private static String serializeBinaryInitialDefault(Type type, Object value) {
        if (type.typeId() != TypeID.UUID) {
            // BINARY/FIXED: iceberg's identity human form is already Base64 of the raw bytes.
            return Transforms.identity(type).toHumanString(type, value);
        }
        UUID uuid = (UUID) value;
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.putLong(uuid.getMostSignificantBits());
        bytes.putLong(uuid.getLeastSignificantBits());
        return Base64.getEncoder().encodeToString(bytes.array());
    }

    private static void addField(TStructField structField, TField child) {
        structField.addToFields(fieldPtr(child));
    }

    /**
     * Append the iceberg v3 row-lineage scalar fields ({@code _row_id} / {@code _last_updated_sequence_number})
     * to the dict root so BE's {@code StructNode} children map contains them. Idempotent (skips a name already
     * present — defensive against a data column literally named {@code _row_id}). Each field carries its reserved
     * iceberg field id: BE matches it against the FILE field ids ({@code by_parquet_field_id_with_name_mapping}),
     * registering it not-in-file for a v2 "null after upgrade" file (then backfilled by the iceberg
     * generated-column handler) or reading it when a v3 file materialized it — exactly the legacy slot-driven
     * behavior. A superset root (row-lineage appended even when a query does not project it) is harmless: BE only
     * looks up its own {@code column_names}, mirroring the full-schema dict the snapshot-pin / top-N branches
     * already emit.
     */
    private static void appendRowLineageFields(TStructField root) {
        appendScalarFieldIfAbsent(root, ICEBERG_ROW_ID_FIELD_ID, ICEBERG_ROW_ID_COL);
        appendScalarFieldIfAbsent(root, ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID,
                ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL);
    }

    private static void appendScalarFieldIfAbsent(TStructField root, int id, String lowerName) {
        if (root.isSetFields()) {
            for (TFieldPtr existing : root.getFields()) {
                if (existing.isSetFieldPtr() && lowerName.equals(existing.getFieldPtr().getName())) {
                    return;
                }
            }
        }
        TField tField = new TField();
        tField.setId(id);
        tField.setName(lowerName);
        // Byte-match buildField's scalar leaf: is_optional true (inert on BE's field-id path) + a STRING
        // placeholder type tag (BE reads type.type only as a nested-vs-scalar discriminator).
        tField.setIsOptional(true);
        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);
        tField.setType(columnType);
        addField(root, tField);
    }

    private static TFieldPtr fieldPtr(TField field) {
        TFieldPtr ptr = new TFieldPtr();
        ptr.setFieldPtr(field);
        return ptr;
    }

    private static String encode(long currentSchemaId, List<TSchema> history) {
        TFileScanRangeParams carrier = new TFileScanRangeParams();
        carrier.setCurrentSchemaId(currentSchemaId);
        carrier.setHistorySchemaInfo(history);
        try {
            byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(carrier);
            return BASE64_ENCODER.encodeToString(bytes);
        } catch (Exception | LinkageError e) {
            // Catch LinkageError (e.g. IncompatibleClassChangeError from a thrift classloader split) too:
            // wrapped as a RuntimeException it surfaces as a clean per-query failure instead of escaping the
            // connection handler as an uncaught Error and killing the whole mysql session (mirrors paimon).
            throw new RuntimeException("Failed to serialize iceberg schema-evolution info", e);
        }
    }
}
