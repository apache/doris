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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.index.LanceIndexInspection;
import org.apache.doris.datasource.lance.index.LanceIndexMutationExecutor.PreflightResult;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.thrift.TLanceIndexMutationType;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.lance.schema.LanceField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Lance index admission (design sections 2.2 and 4.1): the single place where a statically
 * validated top-level CREATE [OR REPLACE]/DROP INDEX statement against a Lance catalog table is
 * checked against authoritative metadata. The whole flow runs against one pinned admission
 * snapshot — the IF preflight never takes a second metadata read, and no catalog/db/table
 * metadata lock is held while the snapshot loader does its JNI work (design section 5.1).
 *
 * <p>The step order is the correctness contract: name normalization and reserved prefix come
 * first, before target capture and the snapshot read (fail cheap-first — a reserved name
 * rejected at admission depth costs no remote read), then case-only collision analysis, IF
 * preflight (including the two-stage {@code matches}: requested-algorithm equality plus
 * physical-family corroboration), column-lookup collision analysis, schema contract from
 * the stored column name, and deterministic properties JSON. A mutation that passes the whole
 * preflight completes the capture phase by returning the {@link LanceIndexMutationPlan} the
 * statement executes outside the admission critical section; the plan's entry-internal final
 * preflight re-decides the authoritative verdict with these exact semantics right before the
 * single dispatch. The IF no-op cases complete as genuine no-ops (nothing is created or
 * dropped) and return no plan. Every rejection leaves no durable state behind.
 */
public final class LanceIndexAdmission {

    /**
     * The snapshot read seam. Tests inject a prepared snapshot here so admission runs without
     * FE startup or JNI; the production default delegates to the catalog's merged-snapshot read.
     */
    public interface SnapshotLoader {
        LanceIndexAdmissionSnapshot load(LanceExternalCatalog catalog, String dbName, String tableName)
                throws Exception;
    }

    private static final SnapshotLoader DEFAULT_LOADER = new SnapshotLoader() {
        @Override
        public LanceIndexAdmissionSnapshot load(LanceExternalCatalog catalog, String dbName,
                String tableName) throws Exception {
            return catalog.loadTableIndexAdmissionSnapshot(dbName, tableName);
        }
    };

    private LanceIndexAdmission() {
    }

    /**
     * Admits a top-level CREATE [OR REPLACE] INDEX. Static validation
     * ({@link LanceIndexMutationValidator#validateCreateIndex}) must already have passed for
     * {@code def}.
     *
     * @return the execution plan for a mutation that would be admitted, or {@code null} when
     *         the IF preflight resolved the statement to a genuine no-op (nothing is created;
     *         the statement completes with the default OK packet)
     */
    @Nullable
    public static LanceIndexMutationPlan admitCreate(LanceExternalCatalog catalog,
            LanceExternalDatabase db, LanceExternalTable table, IndexDefinition def,
            boolean ifNotExists) throws Exception {
        return admitCreate(DEFAULT_LOADER, catalog, db, table, def, ifNotExists);
    }

    @Nullable
    static LanceIndexMutationPlan admitCreate(SnapshotLoader loader, LanceExternalCatalog catalog,
            LanceExternalDatabase db, LanceExternalTable table, IndexDefinition def, boolean ifNotExists)
            throws Exception {
        // 1. Display/normalized names and the reserved system prefix, checked before any metadata
        // read so a reserved name rejected at admission depth costs no remote snapshot read (fail
        // cheap-first). The prefix is rejected for CREATE and REPLACE exactly as for DROP; the
        // static layer rejects it first and this is the defense-in-depth copy at admission depth.
        String displayName = def.getIndexName();
        String normalizedName = LanceIndexNameNormalizer.normalize(displayName);
        LanceIndexMutationValidator.rejectIfReservedIndexName(displayName);
        // 2. One pinned snapshot for every authoritative decision below.
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        CatalogMgr.LanceIndexTarget target = catalogMgr.captureLanceIndexTarget(catalog);
        LanceIndexAdmissionSnapshot snapshot = loader.load(catalog, db.getRemoteName(), table.getRemoteName());
        // 3.+4. Case-only analysis (design section 4.1) and IF preflight (design section 2.2),
        // decided by the same authoritative determination the entry-internal final preflight
        // re-runs before the dispatch.
        if (decideCreateAgainstSnapshot(snapshot, def, ifNotExists, displayName, normalizedName)
                == PreflightResult.NO_OP) {
            // The empty action is deliberate: the no-op decision needed no local work inside
            // the fence, but entering the critical section still revalidates the captured
            // target - a DROP CATALOG or identity ALTER that landed between the snapshot
            // read and here must fail this no-op instead of OK-ing against a stale target.
            catalogMgr.withLanceIndexAdmission(catalog, target, () -> null);
            return null;
        }
        // 5. Fail closed when the table lookup relation cannot resolve the request column
        // uniquely: a dataset can hold top-level fields that differ only by case (V versus v),
        // and ExternalTable.getColumn returns the first hit, so admitting would journal an
        // arbitrary one of them. The pinned snapshot decides, never the cached table schema.
        rejectIfAmbiguousLookupColumn(snapshot, def.getCols().get(0));
        // 6. Schema contract v1 from the stored column name (never the raw user spelling). The
        // build itself is the admission-depth type validation against the pinned snapshot.
        String storedColumn = storedColumnName(table, def.getCols().get(0));
        LanceIndexSchemaContract schemaContract = LanceSchemaContractBuilder.build(
                snapshot.getTopLevelFields(), storedColumn);
        // 7. The capture-fenced handoff: produce the execution plan by local work only. The
        // statement executes it outside this critical section; the plan pins the admitted
        // dataset version and carries the entry-internal final preflight that re-decides the
        // authoritative verdict against current metadata before the single dispatch.
        return catalogMgr.withLanceIndexAdmission(catalog, target, () -> new LanceIndexMutationPlan(
                catalog,
                def.isOrReplace() ? TLanceIndexMutationType.REPLACE : TLanceIndexMutationType.CREATE,
                snapshot.getDatasetUri(), snapshot.getDatasetVersion(), normalizedName,
                storedColumn, requestedAlgorithm(def), def.getProperties(), ifNotExists, false,
                schemaContract,
                () -> decideCreateAgainstSnapshot(
                        loader.load(catalog, db.getRemoteName(), table.getRemoteName()),
                        def, ifNotExists, displayName, normalizedName)));
    }

    /**
     * Admits a top-level DROP INDEX. The static name bounds
     * ({@link LanceIndexMutationValidator#validateDropIndex}) must already have passed.
     *
     * @return the execution plan for a drop that would be admitted, or {@code null} when
     *         {@code IF EXISTS} resolved the statement to a genuine no-op (nothing is dropped;
     *         the statement completes with the default OK packet)
     */
    @Nullable
    public static LanceIndexMutationPlan admitDrop(LanceExternalCatalog catalog,
            LanceExternalDatabase db, LanceExternalTable table, String indexName, boolean ifExists)
            throws Exception {
        return admitDrop(DEFAULT_LOADER, catalog, db, table, indexName, ifExists);
    }

    @Nullable
    static LanceIndexMutationPlan admitDrop(SnapshotLoader loader, LanceExternalCatalog catalog,
            LanceExternalDatabase db, LanceExternalTable table, String indexName, boolean ifExists)
            throws Exception {
        // Fail cheap-first: the reserved prefix is rejected before target capture and the
        // snapshot read, so it costs no remote read.
        String normalizedName = LanceIndexNameNormalizer.normalize(indexName);
        LanceIndexMutationValidator.rejectIfReservedIndexName(indexName);
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        CatalogMgr.LanceIndexTarget target = catalogMgr.captureLanceIndexTarget(catalog);
        LanceIndexAdmissionSnapshot snapshot = loader.load(catalog, db.getRemoteName(), table.getRemoteName());
        if (decideDropAgainstSnapshot(snapshot, indexName, normalizedName, ifExists)
                == PreflightResult.NO_OP) {
            // Same deliberate empty action as the CREATE no-op above: no local work to run
            // inside the fence, but the target revalidation still applies.
            catalogMgr.withLanceIndexAdmission(catalog, target, () -> null);
            return null;
        }
        // The capture-fenced handoff, exactly as on the CREATE side; a DROP carries no column,
        // no algorithm and no schema contract.
        return catalogMgr.withLanceIndexAdmission(catalog, target, () -> new LanceIndexMutationPlan(
                catalog, TLanceIndexMutationType.DROP, snapshot.getDatasetUri(),
                snapshot.getDatasetVersion(), normalizedName, "", "", null, false, ifExists,
                null, () -> decideDropAgainstSnapshot(
                        loader.load(catalog, db.getRemoteName(), table.getRemoteName()),
                        indexName, normalizedName, ifExists)));
    }

    /**
     * The authoritative CREATE determination against one snapshot (design sections 2.2 and 4.1),
     * shared by admission and the entry-internal final preflight so the two decisions have
     * exactly the same semantics: case-only ambiguous collisions fail closed first; a unique
     * stored match on a non-REPLACE request is the IF preflight — no IF flag rejects as
     * "already exists", an IF flag with a matching definition is a no-op, a mismatching
     * definition rejects as such; CREATE OR REPLACE and an absent name both proceed.
     */
    private static PreflightResult decideCreateAgainstSnapshot(LanceIndexAdmissionSnapshot snapshot,
            IndexDefinition def, boolean ifNotExists, String displayName, String normalizedName)
            throws AnalysisException {
        List<String> storedNames = logicalIndexNames(snapshot);
        if (LanceIndexFamilies.isAmbiguousCaseCollision(storedNames, normalizedName)) {
            rejectInvalid("index name '" + displayName
                    + "' is ambiguous: multiple Lance indexes differ only by case");
        }
        String storedName = LanceIndexFamilies.uniqueMatch(storedNames, normalizedName);
        if (!def.isOrReplace() && storedName != null) {
            if (!ifNotExists) {
                rejectInvalid("index '" + displayName + "' already exists");
            }
            if (!matchesExistingDefinition(snapshot, storedName, def)) {
                rejectInvalid("index '" + displayName + "' already exists with a different definition");
            }
            return PreflightResult.NO_OP;
        }
        return PreflightResult.PROCEED;
    }

    /**
     * The authoritative DROP determination against one snapshot, shared by admission and the
     * entry-internal final preflight: case-only ambiguous collisions fail closed, an absent
     * name is an {@code IF EXISTS} no-op or a "not found" rejection, a unique match proceeds.
     */
    private static PreflightResult decideDropAgainstSnapshot(LanceIndexAdmissionSnapshot snapshot,
            String indexName, String normalizedName, boolean ifExists) throws AnalysisException {
        List<String> storedNames = logicalIndexNames(snapshot);
        if (LanceIndexFamilies.isAmbiguousCaseCollision(storedNames, normalizedName)) {
            rejectInvalid("index name '" + indexName
                    + "' is ambiguous: multiple Lance indexes differ only by case");
        }
        String storedName = LanceIndexFamilies.uniqueMatch(storedNames, normalizedName);
        if (storedName == null) {
            if (ifExists) {
                return PreflightResult.NO_OP;
            }
            rejectInvalid("index '" + indexName + "' not found");
        }
        return PreflightResult.PROCEED;
    }

    /**
     * The section 2.2 definition match, two stages: (a) the requested algorithm must equal the
     * stored logical algorithm under family normalization — a same-name different-algorithm
     * request is a mismatch, never a no-op; (b) the physical entry of the same name must exist
     * and back the logical algorithm (snapshot self-consistency, failing closed); (c) the single
     * normalized column, with the request side in the same loader path-segment representation as
     * the logical side, must be equal; (d) whitelist properties are compared per property — a
     * value the request sets and the snapshot exposes must be equal, an unexposed snapshot value
     * is skipped, and a property the request omits is never compared except num_bits, whose
     * omitted request value defaults to the always-persisted 8.
     */
    private static boolean matchesExistingDefinition(LanceIndexAdmissionSnapshot snapshot,
            String storedName, IndexDefinition def) {
        LanceShowIndexInfo logical = null;
        for (LanceShowIndexInfo index : snapshot.getLogicalIndexes()) {
            if (index.getName().equals(storedName)) {
                logical = index;
                break;
            }
        }
        if (logical == null) {
            return false;
        }
        String requestAlgorithm = requestedAlgorithm(def);
        if (requestAlgorithm == null || !LanceIndexFamilies.normalize(logical.getIndexType())
                .equals(LanceIndexFamilies.normalize(requestAlgorithm))) {
            return false;
        }
        LanceIndexAdmissionSnapshot.PhysicalIndexInfo physical = null;
        for (LanceIndexAdmissionSnapshot.PhysicalIndexInfo entry : snapshot.getPhysicalIndexes()) {
            if (entry.getName().equals(storedName)) {
                physical = entry;
                break;
            }
        }
        if (physical == null
                || !LanceIndexFamilies.isCompatible(logical.getIndexType(), physical.getIndexTypeName())) {
            return false;
        }
        if (logical.getColumns().size() != 1) {
            return false;
        }
        // The logical column is a loader path segment: field names containing characters outside
        // [A-Za-z0-9_] are backtick-escaped (embedded backticks doubled). The parser hands over the
        // raw spelling, so the request column is formatted with the same rule before both sides
        // pass name normalization — otherwise such columns are falsely rejected as a mismatch.
        String requestColumn = LanceIndexNameNormalizer.normalize(
                LanceIndexInspection.formatFieldPathSegment(def.getCols().get(0)));
        if (!LanceIndexNameNormalizer.normalize(logical.getColumns().get(0)).equals(requestColumn)) {
            return false;
        }
        return whitelistPropertiesMatch(logical, def);
    }

    /**
     * Per-property whitelist comparison (metric ↔ metric_type, num_sub_vectors ↔
     * compression.num_sub_vectors, num_bits ↔ compression.num_bits; an omitted request num_bits
     * compares as the always-persisted 8). num_partitions is never compared (section 2.2).
     * BTREE/BITMAP carry no user build properties, so the comparison is vacuous for them.
     */
    private static boolean whitelistPropertiesMatch(LanceShowIndexInfo logical, IndexDefinition def) {
        if (def.getLanceIndexType() != null) {
            return true;
        }
        Map<String, String> request = normalizedAnnProperties(def.getProperties());
        JsonObject exposed = parseSnapshotProperties(logical.getProperties());
        if (exposed == null && logical.getProperties() != null && !logical.getProperties().isEmpty()) {
            // A malformed provider payload is not "nothing exposed": fail the comparison closed
            // rather than guess at a match (design section 3.4).
            return false;
        }
        String metric = request.get("metric");
        if (metric != null) {
            JsonElement exposedMetric = exposed == null ? null : exposed.get("metric_type");
            // Lance stores the metric uppercased ("L2") while the validated request vocabulary is
            // lowercase ("l2"): both sides fold under the root locale before comparison. An
            // exposed but non-primitive metric is malformed provider data and fails closed
            // (design section 3.4), like an unparsable numeric property below.
            if (exposedMetric != null && (!exposedMetric.isJsonPrimitive()
                    || !exposedMetric.getAsString().toLowerCase(Locale.ROOT)
                            .equals(metric.toLowerCase(Locale.ROOT)))) {
                return false;
            }
        }
        // A compression block that is present but not an object is malformed provider data:
        // fail closed (design section 3.4) rather than treat every numeric property as
        // unexposed. ANN requests always carry num_sub_vectors, so there is always at least
        // one numeric property to corroborate.
        if (exposed != null && exposed.has("compression") && !exposed.get("compression").isJsonObject()) {
            return false;
        }
        JsonObject compression = exposed == null || !exposed.has("compression")
                ? null : exposed.getAsJsonObject("compression");
        // The request side of num_bits is never "unset": the validator accepts an omitted value
        // and the effective default is 8 (section 2.4), so the preflight compares an
        // effective 8 against any exposed compression.num_bits instead of skipping.
        String requestNumBits = request.get("num_bits") == null ? "8" : request.get("num_bits");
        return numericPropertyMatches(request.get("num_sub_vectors"), compression, "num_sub_vectors")
                && numericPropertyMatches(requestNumBits, compression, "num_bits");
    }

    /**
     * True when the request leaves the property unset (never compared) or the snapshot exposes
     * no value for it (skipped); otherwise both values must parse as equal longs. An exposed
     * non-primitive, fractional, overflowing or otherwise unparseable value is malformed
     * provider data and fails closed (design section 3.4), matching the metric comparison above.
     */
    private static boolean numericPropertyMatches(String requestValue, JsonObject compression,
            String exposedKey) {
        if (requestValue == null) {
            return true;
        }
        JsonElement exposed = compression == null ? null : compression.get(exposedKey);
        if (exposed == null) {
            return true;
        }
        if (!exposed.isJsonPrimitive()) {
            return false;
        }
        try {
            // getAsLong silently truncates fractions and wraps overflowing JSON numbers.
            return Long.parseLong(exposed.getAsString()) == Long.parseLong(requestValue.trim());
        } catch (RuntimeException e) {
            // An exposed but non-numeric value cannot corroborate equality: fail closed.
            return false;
        }
    }

    /**
     * Parses the bounded properties JSON the loader produced for the logical index. Returns null
     * only when the payload is absent (nothing exposed, every property comparison is skipped);
     * malformed content also returns null and the caller fails the comparison closed.
     */
    private static JsonObject parseSnapshotProperties(String propertiesJson) {
        if (propertiesJson == null || propertiesJson.isEmpty()) {
            return null;
        }
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(propertiesJson);
        } catch (RuntimeException e) {
            return null;
        }
        return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
    }

    /** The requested algorithm: the BTREE/BITMAP literal, or the validated ANN index_type. */
    @Nullable
    private static String requestedAlgorithm(IndexDefinition def) {
        if (def.getLanceIndexType() != null) {
            return def.getLanceIndexType();
        }
        return normalizedAnnProperties(def.getProperties()).get("index_type");
    }

    /**
     * The request properties with case-folded keys. Static validation has already rejected
     * unknown and duplicate (case-insensitively) keys, so a plain last-wins fold is exact here.
     */
    private static Map<String, String> normalizedAnnProperties(Map<String, String> properties) {
        Map<String, String> normalized = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            normalized.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        return normalized;
    }

    /**
     * Fails closed when more than one top-level field of the pinned snapshot matches the request
     * column under the table lookup relation {@code String.equalsIgnoreCase} — deliberately not a
     * ROOT-lowercase fold, which decides some Unicode pairs differently and would diverge from
     * the relation {@link org.apache.doris.datasource.ExternalTable#getColumn} actually applies.
     * That lookup returns the first hit, so an ambiguous request could journal the wrong stored
     * column (design section 4.1 fail-closed rule, applied to columns).
     */
    private static void rejectIfAmbiguousLookupColumn(LanceIndexAdmissionSnapshot snapshot,
            String requestColumn) throws AnalysisException {
        int matches = 0;
        for (LanceField field : snapshot.getTopLevelFields()) {
            if (requestColumn.equalsIgnoreCase(field.getName())) {
                ++matches;
            }
        }
        if (matches > 1) {
            rejectInvalid("index column '" + requestColumn
                    + "' is ambiguous: multiple Lance fields differ only by case");
        }
    }

    private static String storedColumnName(LanceExternalTable table, String requestColumn)
            throws AnalysisException {
        Column column = table.getColumn(requestColumn);
        if (column == null) {
            // Unreachable after static validation; kept fail-closed because the contract build
            // keys on the stored name.
            rejectInvalid("Index column '" + requestColumn + "' does not exist");
        }
        return column.getName();
    }

    private static List<String> logicalIndexNames(LanceIndexAdmissionSnapshot snapshot) {
        List<String> names = new ArrayList<>(snapshot.getLogicalIndexes().size());
        for (LanceShowIndexInfo index : snapshot.getLogicalIndexes()) {
            names.add(index.getName());
        }
        return names;
    }

    private static void rejectInvalid(String detail) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID, detail);
    }
}
