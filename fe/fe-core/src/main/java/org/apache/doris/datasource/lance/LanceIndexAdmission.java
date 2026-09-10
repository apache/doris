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
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.job.LanceIndexDatasetLocator;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Lance index admission (design sections 2.2 and 4.1): the single place where a statically
 * validated top-level CREATE [OR REPLACE]/DROP INDEX statement against a Lance catalog table is
 * turned into a durable job. The whole flow runs against one pinned admission snapshot — the
 * IF preflight never takes a second metadata read, and no catalog/db/table metadata lock is held
 * while the snapshot loader does its JNI work (design section 5.1).
 *
 * <p>The step order is the correctness contract: name normalization and reserved prefix come
 * first, before target capture and the snapshot read (fail cheap-first — a reserved name
 * rejected at admission depth costs no remote read), then case-only collision analysis, IF
 * preflight (including the two-stage {@code matches}: requested-algorithm equality plus
 * physical-family corroboration), schema contract from the stored column name, locator
 * normalization, deterministic properties JSON, positive-quota assertion, and only then exactly
 * one id allocation and the durable {@code createJob} transfer.
 * Every rejection before {@code createJob} leaves no job, no fence, no quota charge, no journal
 * record, and no id allocation; the manager's own fence/quota rejections pass through verbatim
 * (an id burned by them is accepted — ids are never required to be contiguous).
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

    /** The admission result: the durable job id, or null for an IF no-op (no job created). */
    public static final class Outcome {
        private final Long jobId;

        private Outcome(Long jobId) {
            this.jobId = jobId;
        }

        /**
         * The admitted job id, or null when the IF preflight made the statement an immediate
         * no-op (design section 2.2: "returns an immediate no-op, without creating a job").
         */
        @Nullable
        public Long getJobId() {
            return jobId;
        }
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
     */
    public static Outcome admitCreate(LanceExternalCatalog catalog, LanceExternalDatabase db,
            LanceExternalTable table, IndexDefinition def, boolean ifNotExists) throws Exception {
        return admitCreate(DEFAULT_LOADER, catalog, db, table, def, ifNotExists);
    }

    static Outcome admitCreate(SnapshotLoader loader, LanceExternalCatalog catalog,
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
        // 3. Case-only analysis (design section 4.1): ambiguous external collisions fail closed;
        // a unique match resolves to the stored display name.
        List<String> storedNames = logicalIndexNames(snapshot);
        if (LanceIndexFamilies.isAmbiguousCaseCollision(storedNames, normalizedName)) {
            rejectInvalid("index name '" + displayName
                    + "' is ambiguous: multiple Lance indexes differ only by case");
        }
        String storedName = LanceIndexFamilies.uniqueMatch(storedNames, normalizedName);
        // 4. IF preflight (design section 2.2).
        boolean orReplace = def.isOrReplace();
        if (!orReplace && storedName != null) {
            if (!ifNotExists) {
                rejectInvalid("index '" + displayName + "' already exists");
            }
            if (!matchesExistingDefinition(snapshot, storedName, def)) {
                rejectInvalid("index '" + displayName + "' already exists with a different definition");
            }
            return catalogMgr.withLanceIndexAdmission(catalog, target, () -> new Outcome(null));
        }
        // 5. Schema contract v1 from the stored column name (never the raw user spelling).
        String storedColumnName = storedColumnName(table, def.getCols().get(0));
        LanceIndexSchemaContract contract =
                LanceSchemaContractBuilder.build(snapshot.getTopLevelFields(), storedColumnName);
        // 6. The fence locator is the normalized dataset uri of the same pinned snapshot.
        String locator = normalizeLocator(snapshot);
        // 7. Deterministic normalized properties JSON for ANN; scalar families persist null.
        boolean ann = def.getLanceIndexType() == null;
        String indexType = ann ? annIndexType(def) : def.getLanceIndexType();
        String propertiesJson = ann ? buildAnnPropertiesJson(def) : null;
        // D7 backstop: quota values from fe.conf bypass the ADMIN SET callback, so admission
        // re-asserts positivity before any id allocation or durable transfer.
        assertPositiveQuotas();
        return catalogMgr.withLanceIndexAdmission(catalog, target, () -> {
            // 8. Exactly one id allocation, after every preflight above has passed.
            long jobId = Env.getCurrentEnv().getNextId();
            String creator = ConnectContext.get().getQualifiedUser();
            // 9. REPLACE on an existing name persists the stored display name (section 4.1) so the
            // worker locates the case-sensitive target; a fresh REPLACE keeps the user's spelling.
            String persistedDisplayName = (orReplace && storedName != null) ? storedName : displayName;
            LanceIndexJob job;
            try {
                job = new LanceIndexJob(jobId, creator, catalog.getId(), db.getFullName(), table.getName(),
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, locator, persistedDisplayName, normalizedName,
                        orReplace ? LanceIndexJobMutationType.REPLACE : LanceIndexJobMutationType.CREATE,
                        ifNotExists, false, indexType, storedColumnName, propertiesJson,
                        snapshot.getDatasetVersion(), contract);
            } catch (IllegalArgumentException e) {
                throw invalidAdmission(e.getMessage());
            }
            Env.getCurrentEnv().getLanceIndexJobManager().createJob(job,
                    Config.lance_index_job_max_unresolved_per_table,
                    Config.lance_index_job_max_unresolved_per_catalog,
                    Config.lance_index_job_max_unresolved_global);
            // 10. The job and its fence are durable once createJob returns.
            return new Outcome(jobId);
        });
    }

    /**
     * Admits a top-level DROP INDEX. The static name bounds
     * ({@link LanceIndexMutationValidator#validateDropIndex}) must already have passed.
     */
    public static Outcome admitDrop(LanceExternalCatalog catalog, LanceExternalDatabase db,
            LanceExternalTable table, String indexName, boolean ifExists) throws Exception {
        return admitDrop(DEFAULT_LOADER, catalog, db, table, indexName, ifExists);
    }

    static Outcome admitDrop(SnapshotLoader loader, LanceExternalCatalog catalog,
            LanceExternalDatabase db, LanceExternalTable table, String indexName, boolean ifExists)
            throws Exception {
        // Fail cheap-first: the reserved prefix is rejected before target capture and the
        // snapshot read, so it costs no remote read.
        String normalizedName = LanceIndexNameNormalizer.normalize(indexName);
        LanceIndexMutationValidator.rejectIfReservedIndexName(indexName);
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        CatalogMgr.LanceIndexTarget target = catalogMgr.captureLanceIndexTarget(catalog);
        LanceIndexAdmissionSnapshot snapshot = loader.load(catalog, db.getRemoteName(), table.getRemoteName());
        List<String> storedNames = logicalIndexNames(snapshot);
        if (LanceIndexFamilies.isAmbiguousCaseCollision(storedNames, normalizedName)) {
            rejectInvalid("index name '" + indexName
                    + "' is ambiguous: multiple Lance indexes differ only by case");
        }
        String storedName = LanceIndexFamilies.uniqueMatch(storedNames, normalizedName);
        if (storedName == null) {
            if (ifExists) {
                return catalogMgr.withLanceIndexAdmission(catalog, target, () -> new Outcome(null));
            }
            rejectInvalid("index '" + indexName + "' not found");
        }
        String locator = normalizeLocator(snapshot);
        assertPositiveQuotas();
        return catalogMgr.withLanceIndexAdmission(catalog, target, () -> {
            long jobId = Env.getCurrentEnv().getNextId();
            String creator = ConnectContext.get().getQualifiedUser();
            // DROP only runs past the preflight with a unique match, so the stored display name is
            // always persisted (section 4.1); definition fields stay null on a DROP job record.
            LanceIndexJob job;
            try {
                job = new LanceIndexJob(jobId, creator, catalog.getId(), db.getFullName(), table.getName(),
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, locator, storedName, normalizedName,
                        LanceIndexJobMutationType.DROP, false, ifExists, null, null, null,
                        snapshot.getDatasetVersion(), null);
            } catch (IllegalArgumentException e) {
                throw invalidAdmission(e.getMessage());
            }
            Env.getCurrentEnv().getLanceIndexJobManager().createJob(job,
                    Config.lance_index_job_max_unresolved_per_table,
                    Config.lance_index_job_max_unresolved_per_catalog,
                    Config.lance_index_job_max_unresolved_global);
            return new Outcome(jobId);
        });
    }

    /**
     * The section 2.2 definition match, two stages: (a) the requested algorithm must equal the
     * stored logical algorithm under family normalization — a same-name different-algorithm
     * request is a mismatch, never a no-op; (b) the physical entry of the same name must exist
     * and back the logical algorithm (snapshot self-consistency, failing closed); (c) the single
     * normalized column must be equal; (d) whitelist properties are compared per property — a
     * value the request sets and the snapshot exposes must be equal, an unexposed snapshot value
     * is skipped, and a property the request omits is never compared.
     */
    private static boolean matchesExistingDefinition(LanceIndexAdmissionSnapshot snapshot,
            String storedName, IndexDefinition def) {
        LanceLogicalIndex logical = null;
        for (LanceLogicalIndex index : snapshot.getLogicalIndexes()) {
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
        String requestColumn = LanceIndexNameNormalizer.normalize(def.getCols().get(0));
        if (!LanceIndexNameNormalizer.normalize(logical.getColumns().get(0)).equals(requestColumn)) {
            return false;
        }
        return whitelistPropertiesMatch(logical, def);
    }

    /**
     * Per-property whitelist comparison (metric ↔ metric_type, num_sub_vectors ↔
     * compression.num_sub_vectors, num_bits ↔ compression.num_bits). num_partitions is never
     * compared (section 2.2). BTREE/BITMAP carry no user build properties, so the comparison is
     * vacuous for them.
     */
    private static boolean whitelistPropertiesMatch(LanceLogicalIndex logical, IndexDefinition def) {
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
        return numericPropertyMatches(request.get("num_sub_vectors"), compression, "num_sub_vectors")
                && numericPropertyMatches(request.get("num_bits"), compression, "num_bits");
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

    /** The persisted job index type: the uppercased validated ANN index_type, else the literal. */
    private static String annIndexType(IndexDefinition def) {
        String indexType = normalizedAnnProperties(def.getProperties()).get("index_type");
        return indexType == null ? null : indexType.toUpperCase(Locale.ROOT);
    }

    /**
     * The deterministic params JSON for an admitted ANN job: lowercased keys in TreeMap order,
     * index_type as the uppercased original value, metric lowercased, numeric values as their
     * original strings, and num_bits pinned to 8 (design section 2.4). Bounded well under
     * {@link LanceIndexJob#MAX_PROPERTIES_JSON_BYTES} by the five-key whitelist.
     */
    private static String buildAnnPropertiesJson(IndexDefinition def) throws AnalysisException {
        Map<String, String> request = normalizedAnnProperties(def.getProperties());
        TreeMap<String, String> persisted = new TreeMap<>();
        String indexType = request.get("index_type");
        if (indexType != null) {
            persisted.put("index_type", indexType.toUpperCase(Locale.ROOT));
        }
        String metric = request.get("metric");
        if (metric != null) {
            persisted.put("metric", metric.toLowerCase(Locale.ROOT));
        }
        putIfPresent(persisted, request, "num_partitions");
        putIfPresent(persisted, request, "num_sub_vectors");
        persisted.put("num_bits", "8");
        try {
            return GsonUtils.GSON.toJson(persisted);
        } catch (IllegalArgumentException e) {
            throw invalidAdmission(e.getMessage());
        }
    }

    private static void putIfPresent(TreeMap<String, String> target, Map<String, String> source,
            String key) {
        String value = source.get(key);
        if (value != null) {
            target.put(key, value);
        }
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

    private static String normalizeLocator(LanceIndexAdmissionSnapshot snapshot)
            throws AnalysisException {
        try {
            return LanceIndexDatasetLocator.normalize(snapshot.getDatasetUri());
        } catch (IllegalArgumentException e) {
            throw invalidAdmission(e.getMessage());
        }
    }

    private static List<String> logicalIndexNames(LanceIndexAdmissionSnapshot snapshot) {
        List<String> names = new ArrayList<>(snapshot.getLogicalIndexes().size());
        for (LanceLogicalIndex index : snapshot.getLogicalIndexes()) {
            names.add(index.getName());
        }
        return names;
    }

    /**
     * D7: the unresolved-job quotas are a section 9.7 enablement precondition. The ADMIN SET
     * callback validates them, but fe.conf loading bypasses callbacks, so admission asserts them
     * again before allocating an id. The manager independently rejects non-positive limits
     * at the durable-transfer boundary.
     */
    private static void assertPositiveQuotas() throws AnalysisException {
        if (Config.lance_index_job_max_unresolved_per_table <= 0) {
            rejectNonPositiveQuota("lance_index_job_max_unresolved_per_table",
                    Config.lance_index_job_max_unresolved_per_table);
        }
        if (Config.lance_index_job_max_unresolved_per_catalog <= 0) {
            rejectNonPositiveQuota("lance_index_job_max_unresolved_per_catalog",
                    Config.lance_index_job_max_unresolved_per_catalog);
        }
        if (Config.lance_index_job_max_unresolved_global <= 0) {
            rejectNonPositiveQuota("lance_index_job_max_unresolved_global",
                    Config.lance_index_job_max_unresolved_global);
        }
    }

    private static void rejectNonPositiveQuota(String configItem, long value) throws AnalysisException {
        ErrorReport.reportAnalysisException("%s", ErrorCode.ERR_LANCE_INDEX_MUTATION_DISABLED,
                "Lance index admission requires positive unresolved-job quotas, but " + configItem
                        + " = " + value + "; fix the FE configuration before enabling "
                        + "enable_lance_index_mutation");
    }

    private static void rejectInvalid(String detail) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID, detail);
    }

    private static AnalysisException invalidAdmission(String detail) {
        return new AnalysisException(detail, ErrorCode.ERR_LANCE_INDEX_INVALID);
    }
}
