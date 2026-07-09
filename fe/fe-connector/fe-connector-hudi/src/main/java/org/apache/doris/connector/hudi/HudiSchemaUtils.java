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

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Base64;
import java.util.List;
import java.util.Locale;

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
}
