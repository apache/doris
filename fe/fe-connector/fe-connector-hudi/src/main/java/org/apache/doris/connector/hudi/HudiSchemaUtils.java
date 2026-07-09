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

import org.apache.avro.Schema;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Builds the native-reader schema dictionary ({@code current_schema_id} + {@code history_schema_info}) so BE
 * matches file&harr;table columns BY FIELD ID across schema evolution (rename/reorder), instead of falling back
 * to NAME matching (which silently reads NULL for a renamed column on its old files). Self-contained
 * hudi&rarr;thrift port of legacy {@code HudiUtils.getSchemaInfo(InternalSchema)} + {@code getSchemaInfo(
 * List&lt;Types.Field&gt;)} + {@code getSchemaInfo(Types.Field)} (zero fe-core import).
 *
 * <p><b>Template = paimon, lowercase = iceberg.</b> Like paimon ({@code by_table_field_id}) hudi supplies the
 * per-file schema to BE, so the dictionary needs a per-committed-version history ({@code -1} target entry + one
 * entry per referenced split {@code schema_id}) — unlike iceberg's single {@code -1} entry. Field ids come from
 * hudi's {@link InternalSchema} (stable across renames). This class is only the pure {@code InternalSchema ->
 * thrift} converter + the base64 transport round-trip; the dictionary orchestration (the {@code -1} entry from
 * the requested columns, the per-referenced-version entries, the per-split {@code schema_id}) lands in later
 * steps that reuse {@link #buildSchemaInfo} + {@link #encode}.</p>
 *
 * <p><b>Two deliberate deviations from legacy</b> ({@code HudiUtils.getSchemaInfo}):
 * <ul>
 *   <li><b>Lowercase EVERY nesting level</b> ({@code Locale.ROOT}, mirroring
 *       {@code IcebergSchemaUtils.buildField}) — legacy lowercased nothing. The same {@code history_schema_info}
 *       thrift is also consumed by the v1 {@code format/table} hudi reader, whose {@code StructNode} is keyed by
 *       the RAW history {@code TField.name} then looked up by the LOWERCASED Doris slot name; a mixed-case
 *       nested field name there throws {@code std::out_of_range} and SIGABRTs the whole BE (mirror of the iceberg
 *       {@code DROP_AND_ADD} fix).</li>
 *   <li><b>Scalar {@code TColumnType} = {@code STRING} placeholder</b> — legacy emitted the full Doris type
 *       ({@code fromAvroHudiTypeToDorisType(...).toColumnTypeThrift()}); BE uses {@code type.type} only as a
 *       nested-vs-scalar discriminator on the field-id path, so a single placeholder suffices and avoids porting
 *       the avro&rarr;Doris type map.</li>
 * </ul>
 * {@code id} + {@code name} + {@code is_optional} are carried at EVERY nesting level (legacy parity — a missing
 * {@code id} at any level makes BE's field-id matcher silently drop the column, worse than {@code BY_NAME}).</p>
 */
public final class HudiSchemaUtils {

    // Legacy parity: current_schema_id is the -1 sentinel ("latest"/target); the current/target schema is also
    // pushed into history_schema_info under this id. BE's find_external_root_field selects the -1 entry as the
    // table-side overlay; a real id equal to any per-split id would drive the banned v1 case-sensitive path.
    static final long CURRENT_SCHEMA_ID = -1L;

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private HudiSchemaUtils() {
    }

    /**
     * Build one {@link TSchema} from a hudi {@link InternalSchema}, keyed by the schema's own committed id
     * (a per-version history entry). Port of legacy {@code HudiUtils.getSchemaInfo(InternalSchema)}.
     */
    static TSchema buildSchemaInfo(InternalSchema internalSchema) {
        return buildSchemaInfo(internalSchema.schemaId(), internalSchema.getRecord().fields());
    }

    /**
     * Build one {@link TSchema} from an explicit schema id + top-level fields. The later {@code -1} target entry
     * (built from the requested columns) reuses this with {@link #CURRENT_SCHEMA_ID}.
     */
    static TSchema buildSchemaInfo(long schemaId, List<Types.Field> fields) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(buildStructField(fields));
        return tSchema;
    }

    private static TStructField buildStructField(List<Types.Field> fields) {
        TStructField structField = new TStructField();
        for (Types.Field field : fields) {
            structField.addToFields(fieldPtr(buildField(field)));
        }
        return structField;
    }

    /**
     * Recursively convert a hudi {@link Types.Field} to a {@link TField}, carrying {@code id} / {@code name}
     * (lowercased with {@code Locale.ROOT} at every level) / {@code is_optional}, a nested-vs-scalar
     * {@code type.type} tag ({@code STRING} placeholder for scalars), and the nested {@code array}/{@code map}/
     * {@code struct} structure. Port of legacy {@code HudiUtils.getSchemaInfo(Types.Field)}.
     */
    static TField buildField(Types.Field field) {
        TField tField = new TField();
        // Lowercase every level (Locale.ROOT): BE's table-side StructNode is looked up by the lowercase Doris
        // slot name; a mixed-case nested name would SIGABRT the v1 reader (see class javadoc).
        tField.setName(field.name().toLowerCase(Locale.ROOT));
        tField.setId(field.fieldId());
        tField.setIsOptional(field.isOptional());

        TColumnType columnType = new TColumnType();
        TNestedField nestedField = new TNestedField();
        switch (field.type().typeId()) {
            case ARRAY: {
                columnType.setType(TPrimitiveType.ARRAY);
                Types.ArrayType arrayType = (Types.ArrayType) field.type();
                TArrayField arrayField = new TArrayField();
                arrayField.setItemField(fieldPtr(buildField(arrayType.fields().get(0))));
                nestedField.setArrayField(arrayField);
                tField.setNestedField(nestedField);
                break;
            }
            case MAP: {
                columnType.setType(TPrimitiveType.MAP);
                Types.MapType mapType = (Types.MapType) field.type();
                TMapField mapField = new TMapField();
                mapField.setKeyField(fieldPtr(buildField(mapType.fields().get(0))));
                mapField.setValueField(fieldPtr(buildField(mapType.fields().get(1))));
                nestedField.setMapField(mapField);
                tField.setNestedField(nestedField);
                break;
            }
            case RECORD: {
                columnType.setType(TPrimitiveType.STRUCT);
                Types.RecordType recordType = (Types.RecordType) field.type();
                nestedField.setStructField(buildStructField(recordType.fields()));
                tField.setNestedField(nestedField);
                break;
            }
            default:
                // Scalar: BE reads type.type only as a nested-vs-scalar discriminator on the field-id path (it
                // never inspects the specific scalar tag), so a single placeholder is sufficient and avoids
                // replicating the full hudi->Doris primitive mapping (drops legacy fromAvroHudiTypeToDorisType).
                columnType.setType(TPrimitiveType.STRING);
                break;
        }
        tField.setType(columnType);
        return tField;
    }

    /**
     * Serialize the schema dictionary into a base64 {@code TBinaryProtocol} blob, carried by a throwaway
     * {@link TFileScanRangeParams} (the exact thrift target so {@link #applySchemaEvolution} only copies the two
     * fields back). Mirrors iceberg/paimon.
     */
    static String encode(long currentSchemaId, List<TSchema> history) {
        TFileScanRangeParams carrier = new TFileScanRangeParams();
        carrier.setCurrentSchemaId(currentSchemaId);
        carrier.setHistorySchemaInfo(history);
        try {
            byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(carrier);
            return BASE64_ENCODER.encodeToString(bytes);
        } catch (Exception | LinkageError e) {
            // Catch LinkageError (e.g. IncompatibleClassChangeError from a thrift classloader split) too: wrapped
            // as a RuntimeException it surfaces as a clean per-query failure instead of escaping the connection
            // handler as an uncaught Error and killing the whole mysql session (mirrors iceberg/paimon).
            throw new RuntimeException("Failed to serialize hudi schema-evolution info", e);
        }
    }

    /**
     * Decode the prop produced by {@link #encode} and copy {@code current_schema_id} + {@code history_schema_info}
     * onto the real scan params. Fail loud on a decode error — the prop is produced by us, so a failure is a real
     * bug, and silently dropping it would re-introduce the silent wrong-rows risk on schema-evolved native reads.
     * A {@code null}/empty prop (e.g. another connector's props map) is a no-op.
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
            throw new RuntimeException("Failed to apply hudi schema-evolution info to scan params", e);
        }
    }

    private static TFieldPtr fieldPtr(TField field) {
        TFieldPtr ptr = new TFieldPtr();
        ptr.setFieldPtr(field);
        return ptr;
    }

    // ========== mode-aware InternalSchema resolvers (shared by field-id / schema-id / dict steps) ==========

    /** The mode-aware table {@link InternalSchema} for the latest commit + whether schema-on-read evolution is on. */
    static final class ResolvedInternalSchema {
        final InternalSchema internalSchema;
        final boolean enableSchemaEvolution;

        ResolvedInternalSchema(InternalSchema internalSchema, boolean enableSchemaEvolution) {
            this.internalSchema = internalSchema;
            this.enableSchemaEvolution = enableSchemaEvolution;
        }
    }

    /**
     * Resolve the mode-aware table {@link InternalSchema} for the LATEST commit. Mirror of legacy
     * {@code HiveMetaStoreClientHelper.getHudiTableSchema}: when {@code hoodie.schema.on.read.enable} is on, the
     * ids come from the commit-metadata {@link InternalSchema} (STABLE across renames) and evolution is flagged
     * true; otherwise from {@code AvroInternalSchemaConverter.convert(latestAvro)} (positional ids, version 0)
     * with evolution false. The no-arg {@code getTableInternalSchemaFromCommitMetadata()} pins the latest commit
     * (steady-state / no time-travel pin). Shared by the field-id ({@code HudiConnectorMetadata}) and the
     * per-file schema-id / dict scan paths so both agree on the id source.
     */
    static ResolvedInternalSchema resolveTableInternalSchema(TableSchemaResolver schemaResolver, Schema latestAvro) {
        Option<InternalSchema> fromCommit = schemaResolver.getTableInternalSchemaFromCommitMetadata();
        if (fromCommit.isPresent()) {
            return new ResolvedInternalSchema(fromCommit.get(), true);
        }
        return new ResolvedInternalSchema(AvroInternalSchemaConverter.convert(latestAvro), false);
    }

    /**
     * Resolve the {@link InternalSchema} of the commit that WROTE a given base file — its {@code schemaId()} is
     * the per-split {@code THudiFileDesc.schema_id} BE stamps native files with on the field-id path. Port of
     * legacy {@code HudiScanNode.setHudiParams}: evolution on &rarr; {@code FSUtils.getCommitTime(fileName)}
     * &rarr; {@code InternalSchemaCache.searchSchemaAndCache} (the real committed versionId); off &rarr; the base
     * (non-evolution) InternalSchema ({@code convert(latestAvro)}, version {@code 0}). The base schema is passed
     * in (resolved once per scan via {@link #resolveTableInternalSchema}) so a non-evolution scan pays no
     * per-file metaClient touch. Runs on the TCCL-pinned scan thread; shared with the dict step (which reuses the
     * returned InternalSchema to build the history entry for that version).
     */
    static InternalSchema resolveFileInternalSchema(String filePath, boolean enableSchemaEvolution,
            InternalSchema baseInternalSchema, HoodieTableMetaClient metaClient) {
        if (!enableSchemaEvolution) {
            return baseInternalSchema;
        }
        long commitTime = Long.parseLong(FSUtils.getCommitTime(new File(filePath).getName()));
        return InternalSchemaCache.searchSchemaAndCache(commitTime, metaClient);
    }

    // ========== HD-C5b: table schema AT the pinned instant (FOR TIME AS OF over a schema-on-read table) ==========

    /**
     * Resolve the table {@link InternalSchema} AS OF {@code queryInstant} (a {@code FOR TIME AS OF} pin), or
     * {@link Optional#empty()} when the table is not schema-on-read (no commit-metadata {@link InternalSchema} at
     * the instant) — the caller then uses the LATEST schema, which for a non-evolution table is byte-equivalent
     * (its schema never changed; design decision D3). Mirror of legacy {@code
     * HiveMetaStoreClientHelper.getHudiTableSchema} at-instant path + HD-C5a's {@code getSchemaFromMetaClient}:
     * reload the active timeline first (so a just-committed instant's schema file is readable, not a stale cached
     * one) then {@code getTableInternalSchemaFromCommitMetadata(instant)}. Runs on the caller's TCCL-pinned scan
     * thread ({@code planScan} / {@code getScanNodeProperties}); the off-scan-thread {@code getTableSchema}
     * at-instant read is wrapped by HD-C5a's {@code HudiMetaClientExecutor.execute} separately.
     */
    static Optional<InternalSchema> resolveInternalSchemaAtInstant(TableSchemaResolver schemaResolver,
            HoodieTableMetaClient metaClient, String queryInstant) {
        metaClient.reloadActiveTimeline();
        Option<InternalSchema> atInstant = schemaResolver.getTableInternalSchemaFromCommitMetadata(queryInstant);
        return atInstant.isPresent() ? Optional.of(atInstant.get()) : Optional.empty();
    }

    /**
     * The Avro schema whose fields drive the JNI (MOR-realtime) reader's {@code column_names}/{@code column_types}:
     * the schema AT {@code queryInstant} for a {@code FOR TIME AS OF} read over a schema-on-read table (legacy
     * {@code HudiScanNode:222-224}, kept in lockstep with the native {@code -1} overlay), else {@code latestAvro}
     * (a plain read {@code queryInstant == null}, or a non-evolution table whose schema never changed — D3). A
     * {@code null} instant short-circuits BEFORE {@link #resolveInternalSchemaAtInstant}, so a plain read does not
     * reload the timeline (byte-identical to before this step); the pure choice is {@link #chooseJniSchema}.
     */
    static Schema resolveJniColumnSchema(TableSchemaResolver schemaResolver, HoodieTableMetaClient metaClient,
            Schema latestAvro, String queryInstant) {
        Optional<InternalSchema> pinned = queryInstant == null
                ? Optional.empty()
                : resolveInternalSchemaAtInstant(schemaResolver, metaClient, queryInstant);
        return chooseJniSchema(latestAvro, pinned);
    }

    /**
     * Pure JNI-schema choice (package-private for same-loader testing): the at-instant schema converted back to
     * Avro when a pinned {@link InternalSchema} is present, else {@code latestAvro} unchanged. Uses the same
     * {@code AvroInternalSchemaConverter.convert(InternalSchema, name)} as HD-C5a's {@code
     * columnsFromInternalSchema} (the record name is cosmetic — only the root record is named).
     */
    static Schema chooseJniSchema(Schema latestAvro, Optional<InternalSchema> pinnedSchema) {
        return pinnedSchema.isPresent()
                ? AvroInternalSchemaConverter.convert(pinnedSchema.get(), "hudi_table")
                : latestAvro;
    }

    // ========== scan-level schema-evolution dictionary (current_schema_id + history_schema_info) ==========

    /**
     * Build + serialize the native-reader schema dictionary for the scan-node prop. The {@code -1} target entry
     * is keyed off the {@code requestedColumns} (its top-level names == BE scan slots by construction); the
     * history entries cover every schema version any native file could carry. Touches the metaClient to resolve
     * the mode-aware base schema and the historical versions; the pure dict assembly is
     * {@link #buildSchemaEvolutionDict}. Runs on the TCCL-pinned scan thread.
     */
    static Optional<String> buildSchemaEvolutionProp(HoodieTableMetaClient metaClient,
            TableSchemaResolver schemaResolver, Schema latestAvro, List<HudiColumnHandle> requestedColumns) {
        // Field-id matching is PER-FILE in BE, NOT per-column: once a native file carries a schema_id, EVERY
        // projected column of that file is matched by id, with NO per-column BY_NAME fallback. So if ANY projected
        // column has no resolved field id, emitting the dict would silently read that column as const-NULL. The
        // case that hits this is a projected _hoodie_* meta column on a schema-on-read (evolution) table: the
        // connector exposes the meta columns (getTableAvroSchema(true)) but the mode-aware commit-metadata
        // InternalSchema omits them, so their handle field id stays UNSET (-1). Skip the dict entirely then -> BE
        // stays on BY_NAME for the WHOLE scan (the safe baseline: only renamed columns read wrong, exactly as
        // before this feature; meta columns read correctly by name). A data-only projection (all ids resolved)
        // still gets full field-id / rename matching. FLIP-TIME RESIDUAL: full rename-correctness for SELECT* over
        // an evolution table needs reserved meta-column field ids injected into every entry (iceberg row-lineage
        // pattern) or dropping meta exposure to mirror legacy -- deferred, owed the flip e2e.
        for (HudiColumnHandle handle : requestedColumns) {
            if (handle.getFieldId() < 0) {
                return Optional.empty();
            }
        }
        ResolvedInternalSchema resolved = resolveTableInternalSchema(schemaResolver, latestAvro);
        Collection<InternalSchema> historical = resolved.enableSchemaEvolution
                ? allHistoricalSchemas(metaClient)
                : Collections.emptyList();
        return Optional.of(buildSchemaEvolutionDict(requestedColumns, resolved.internalSchema,
                resolved.enableSchemaEvolution, historical));
    }

    /**
     * The native-reader schema dictionary for a {@code FOR TIME AS OF} read over a schema-on-read table (HD-C5b):
     * the {@code -1} target overlay is built from the FULL {@code pinnedSchema} (a SUPERSET of the pinned scan
     * slots), NOT from the requested column handles. The handles are LATEST-keyed ({@code getColumnHandles} runs
     * before the MVCC pin), so a column renamed after the pinned instant is absent from them under its pinned
     * (historical) name; building the overlay from them would emit a SUBSET missing that scan slot, and BE's
     * field-id reader ({@code by_table_field_id} StructNode) requires EVERY scan slot to be present in the {@code
     * -1} overlay (a missing slot std::out_of_range / SIGABRTs; extra fields are looked up by name only, so a
     * superset is safe). The history entries are ALL committed versions (Option B, identical to the steady-state
     * dict). {@code pinnedSchema} being present already implies schema-on-read is on (its commit-metadata
     * InternalSchema resolved), so evolution is {@code true}. Touches the metaClient to read the schema history;
     * runs on the TCCL-pinned scan thread.
     */
    static String buildSchemaEvolutionDictAtInstant(HoodieTableMetaClient metaClient, InternalSchema pinnedSchema) {
        return buildSchemaEvolutionDict(Collections.emptyList(), pinnedSchema, true,
                allHistoricalSchemas(metaClient));
    }

    /**
     * Pure assembly of the schema dictionary (package-private for same-loader testing): one {@code -1} target
     * entry (from the requested columns + base schema) + the history entries. HISTORY-SET = ALL committed
     * schema versions (a robust SUPERSET of the referenced-file versions, mirroring the paimon connector's
     * all-{@code listAllIds} emission): self-consistent by construction (every native file's {@code schema_id}
     * is present, so BE never fails "miss schema info"), with no coupling between the dict and which files a
     * given scan happens to select. For a non-evolution table there is no history file, so the single
     * non-evolution InternalSchema (version {@code 0}) is emitted as the only history entry.
     */
    static String buildSchemaEvolutionDict(List<HudiColumnHandle> requestedColumns, InternalSchema baseSchema,
            boolean enableSchemaEvolution, Collection<InternalSchema> historicalVersions) {
        List<TSchema> history = new ArrayList<>();
        history.add(buildTargetSchema(requestedColumns, baseSchema));
        if (enableSchemaEvolution) {
            for (InternalSchema version : historicalVersions) {
                history.add(buildSchemaInfo(version));
            }
        } else {
            // Non-evolution: no history file; the base convert()-schema (version 0) is the only file version.
            history.add(buildSchemaInfo(baseSchema));
        }
        return encode(CURRENT_SCHEMA_ID, history);
    }

    /**
     * The {@code -1} target/current overlay, keyed off the requested (pruned) columns so its top-level names
     * equal the BE scan slots (the CI-969249 StructNode invariant). Each requested column's full nested
     * structure + stable field id is looked up BY NAME in {@code baseSchema}. Empty {@code requestedColumns}
     * (count-only scan) falls back to all base top-level fields.
     *
     * <p>The scalar-placeholder fallback (a requested column absent from {@code baseSchema}) is DEFENSIVE only:
     * {@link #buildSchemaEvolutionProp} already gates the whole dict OFF when any projected column has an unset
     * field id, so in production every column reaching here resolves in {@code baseSchema}. The fallback is kept
     * so a direct/test call still produces a complete overlay rather than dropping a scan slot.</p>
     */
    static TSchema buildTargetSchema(List<HudiColumnHandle> requestedColumns, InternalSchema baseSchema) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(CURRENT_SCHEMA_ID);
        TStructField root = new TStructField();
        if (requestedColumns == null || requestedColumns.isEmpty()) {
            for (Types.Field field : baseSchema.getRecord().fields()) {
                root.addToFields(fieldPtr(buildField(field)));
            }
        } else {
            Map<String, Types.Field> byName = new HashMap<>();
            for (Types.Field field : baseSchema.getRecord().fields()) {
                byName.put(field.name().toLowerCase(Locale.ROOT), field);
            }
            for (HudiColumnHandle handle : requestedColumns) {
                Types.Field field = byName.get(handle.getName().toLowerCase(Locale.ROOT));
                root.addToFields(fieldPtr(field != null
                        ? buildField(field)
                        : scalarField(handle.getName().toLowerCase(Locale.ROOT), handle.getFieldId())));
            }
        }
        tSchema.setRootField(root);
        return tSchema;
    }

    /** All committed InternalSchema versions of the table (empty for a non-evolution table). */
    private static Collection<InternalSchema> allHistoricalSchemas(HoodieTableMetaClient metaClient) {
        String historyStr = new FileBasedInternalSchemaStorageManager(metaClient).getHistorySchemaStr();
        if (historyStr == null || historyStr.isEmpty()) {
            return Collections.emptyList();
        }
        return SerDeHelper.parseSchemas(historyStr).values();
    }

    /** A scalar-leaf {@link TField} (STRING placeholder) carrying a name + field id (for the target-entry fallback). */
    private static TField scalarField(String name, int id) {
        TField tField = new TField();
        tField.setName(name);
        tField.setId(id);
        tField.setIsOptional(true);
        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);
        tField.setType(columnType);
        return tField;
    }
}
