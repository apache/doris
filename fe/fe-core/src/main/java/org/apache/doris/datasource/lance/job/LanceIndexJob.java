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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The minimal durable Lance index job record. It contains only what the
 * lifecycle invariants require: job identity/creator/revision and bounded
 * timestamps; the persisted target identity and normalized same-name fence
 * key; the mutation intent; the admitted dataset version and schema-contract
 * representation; the mutation outcome, independent refresh state, typed
 * result, and a bounded sanitized message; the dispatch identity (selected BE,
 * BE process epoch, immutable invocation ID, deadline), possible-live
 * ownership, and any matching termination proof; and the FORCE audit fields
 * (populated only by the FORCE_RELEASE slice; here they are carried for
 * durability and replay).
 *
 * <p>The record never carries secrets, raw/opaque provider data, or unbounded
 * values: name/message/note/properties fields are length-checked at
 * construction/mutation time. Credentials are resolved at execution time from
 * the current catalog and are never persisted here.
 *
 * <p>Serialization is the standard Gson stream: {@code Text.writeString} of
 * the JSON form. Every durable field carries a short {@link SerializedName};
 * fields missing in old data keep the safe in-class defaults, so replay stays
 * tolerant of additive evolution. An instance is effectively immutable while
 * published in the manager: every durable transition is applied to a private
 * copy that is logged and then swapped in, so the object written to the edit
 * log is never mutated afterwards.
 */
public class LanceIndexJob implements Writable {
    /** Bound on the user/build properties JSON snapshot. */
    public static final int MAX_PROPERTIES_JSON_BYTES = 4096;
    /** Bound on the FORCE_RELEASE operator note and the late-commit warning text. */
    public static final int MAX_FORCE_TEXT_BYTES = 1024;
    /** Bound on persisted creator, target-name, mutation, and FORCE-actor text. */
    public static final int MAX_DURABLE_TEXT_BYTES = 1024;
    /** Dispatch identities are UUID-like tokens, not arbitrary worker output. */
    public static final int MAX_INVOCATION_ID_BYTES = 256;

    // ------------------------------------------------------------------
    // Identity
    // ------------------------------------------------------------------
    @SerializedName(value = "jid")
    private long jobId;

    @SerializedName(value = "cr")
    private String creator;

    /** Bumped by +1 on every durable transition; callbacks and replay are revision-checked. */
    @SerializedName(value = "rev")
    private long revision;

    @SerializedName(value = "ctm")
    private long createTimeMs;

    @SerializedName(value = "utm")
    private long updateTimeMs;

    // ------------------------------------------------------------------
    // Target / fence identity
    // ------------------------------------------------------------------
    @SerializedName(value = "cid")
    private long catalogId;

    /** Local database name, kept for privilege and FORCE resolution when the catalog still resolves. */
    @SerializedName(value = "dbn")
    private String dbName;

    /** Local table name, kept for privilege and FORCE resolution when the catalog still resolves. */
    @SerializedName(value = "tbn")
    private String tableName;

    @SerializedName(value = "prv")
    private String provider = LanceIndexFenceKey.PROVIDER_DIRECTORY;

    @SerializedName(value = "loc")
    private String normalizedLocator;

    @SerializedName(value = "din")
    private String displayIndexName;

    @SerializedName(value = "nin")
    private String normalizedIndexName;

    // ------------------------------------------------------------------
    // Mutation intent
    // ------------------------------------------------------------------
    @SerializedName(value = "mt")
    private LanceIndexJobMutationType mutationType = LanceIndexJobMutationType.CREATE;

    @SerializedName(value = "ine")
    private boolean ifNotExists;

    @SerializedName(value = "ie")
    private boolean ifExists;

    /** Logical Lance algorithm (IVF_PQ / BTREE / BITMAP); required for CREATE/REPLACE, nullable for DROP. */
    @SerializedName(value = "it")
    private String indexType;

    @SerializedName(value = "cn")
    private String columnName;

    @SerializedName(value = "pj")
    private String propertiesJson;

    // ------------------------------------------------------------------
    // Admission snapshot
    // ------------------------------------------------------------------
    @SerializedName(value = "adv")
    private long admittedDatasetVersion;

    /** Nullable only for DROP, which does not revalidate an indexed-field contract. */
    @SerializedName(value = "sc")
    private LanceIndexSchemaContract schemaContract;

    // ------------------------------------------------------------------
    // Dual state
    // ------------------------------------------------------------------
    @SerializedName(value = "ms")
    private LanceIndexJobMutationState mutationState = LanceIndexJobMutationState.UNKNOWN;

    /**
     * Safe default for a corrupt record missing the key: REQUIRED holds the fence on a
     * terminal record, mirroring the null fallback in {@link #isUnresolved()}. A legal
     * record always carries the state explicitly (admission sets NOT_REQUIRED).
     */
    @SerializedName(value = "rs")
    private LanceIndexJobRefreshState refreshState = LanceIndexJobRefreshState.REQUIRED;

    // ------------------------------------------------------------------
    // Result
    // ------------------------------------------------------------------
    @SerializedName(value = "res")
    private LanceIndexJobResult result;

    // ------------------------------------------------------------------
    // Dispatch
    // ------------------------------------------------------------------
    @SerializedName(value = "bid")
    private Long backendId;

    @SerializedName(value = "bpe")
    private Long beProcessEpoch;

    /**
     * Immutable revision established by PENDING -> RUNNING. Result and
     * termination-proof callbacks use this dispatch identity rather than racing
     * on the record's global revision. Null only before dispatch or in old data.
     */
    @SerializedName(value = "drv")
    private Long dispatchRevision;

    /** Immutable per-dispatch UUID; callbacks must present the matching identity. */
    @SerializedName(value = "iid")
    private String invocationId;

    /** Bounds wait/runtime only; never proves termination and never releases a possible-live slot. */
    @SerializedName(value = "dlm")
    private Long deadlineMs;

    @SerializedName(value = "plo")
    private boolean possibleLiveOwned;

    @SerializedName(value = "tp")
    private LanceIndexTerminationProof terminationProof = LanceIndexTerminationProof.NONE;

    // ------------------------------------------------------------------
    // FORCE_RELEASE audit (populated only by the FORCE slice; durable + replayable here)
    // ------------------------------------------------------------------
    @SerializedName(value = "fr")
    private boolean forceReleased;

    @SerializedName(value = "fa")
    private String forceActor;

    @SerializedName(value = "ftm")
    private Long forceTimeMs;

    @SerializedName(value = "fn")
    private String forceNote;

    @SerializedName(value = "fw")
    private String forceWarning;

    /**
     * No-arg constructor for Gson replay only; missing fields keep the safe
     * defaults declared above (UNKNOWN mutation state holds the fence, never
     * the redispatchable PENDING; REQUIRED refresh state owes a refresh rather
     * than silently releasing the fence).
     */
    public LanceIndexJob() {
    }

    /**
     * Admission constructor: identity, intent, and the admission snapshot. The
     * manager initializes the lifecycle fields when the job is admitted.
     */
    public LanceIndexJob(long jobId, String creator, long catalogId, String dbName, String tableName,
            String provider, String normalizedLocator, String displayIndexName, String normalizedIndexName,
            LanceIndexJobMutationType mutationType, boolean ifNotExists, boolean ifExists, String indexType,
            String columnName, String propertiesJson, long admittedDatasetVersion,
            LanceIndexSchemaContract schemaContract) {
        this.jobId = jobId;
        this.creator = checkRequiredBytes(creator, MAX_DURABLE_TEXT_BYTES, "creator");
        this.catalogId = catalogId;
        this.dbName = checkRequiredBytes(dbName, MAX_DURABLE_TEXT_BYTES, "dbName");
        this.tableName = checkRequiredBytes(tableName, MAX_DURABLE_TEXT_BYTES, "tableName");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.normalizedLocator = Objects.requireNonNull(normalizedLocator, "normalizedLocator");
        setDisplayIndexName(displayIndexName);
        setNormalizedIndexName(normalizedIndexName);
        this.mutationType = Objects.requireNonNull(mutationType, "mutationType");
        this.ifNotExists = ifNotExists;
        this.ifExists = ifExists;
        setIndexType(indexType);
        setColumnName(columnName);
        setPropertiesJson(propertiesJson);
        this.admittedDatasetVersion = admittedDatasetVersion;
        this.schemaContract = schemaContract;
        validateForAdmission();
    }

    /**
     * Copy used by the manager to stage a durable transition: the copy is
     * mutated, written to the edit log, and then swapped in verbatim, so a
     * published instance is never mutated after being logged. Every field is
     * carried over; the {@code result} and {@code schemaContract} references
     * are shared, which is safe because both are immutable values that are
     * only ever replaced wholesale, never mutated in place.
     */
    public LanceIndexJob(LanceIndexJob other) {
        this.jobId = other.jobId;
        this.creator = other.creator;
        this.revision = other.revision;
        this.createTimeMs = other.createTimeMs;
        this.updateTimeMs = other.updateTimeMs;
        this.catalogId = other.catalogId;
        this.dbName = other.dbName;
        this.tableName = other.tableName;
        this.provider = other.provider;
        this.normalizedLocator = other.normalizedLocator;
        this.displayIndexName = other.displayIndexName;
        this.normalizedIndexName = other.normalizedIndexName;
        this.mutationType = other.mutationType;
        this.ifNotExists = other.ifNotExists;
        this.ifExists = other.ifExists;
        this.indexType = other.indexType;
        this.columnName = other.columnName;
        this.propertiesJson = other.propertiesJson;
        this.admittedDatasetVersion = other.admittedDatasetVersion;
        this.schemaContract = other.schemaContract;
        this.mutationState = other.mutationState;
        this.refreshState = other.refreshState;
        this.result = other.result;
        this.backendId = other.backendId;
        this.beProcessEpoch = other.beProcessEpoch;
        this.dispatchRevision = other.dispatchRevision;
        this.invocationId = other.invocationId;
        this.deadlineMs = other.deadlineMs;
        this.possibleLiveOwned = other.possibleLiveOwned;
        this.terminationProof = other.terminationProof;
        this.forceReleased = other.forceReleased;
        this.forceActor = other.forceActor;
        this.forceTimeMs = other.forceTimeMs;
        this.forceNote = other.forceNote;
        this.forceWarning = other.forceWarning;
    }

    // ------------------------------------------------------------------
    // Derived helpers
    // ------------------------------------------------------------------

    /**
     * The durable same-name fence key of this job.
     */
    public LanceIndexFenceKey fenceKey() {
        return new LanceIndexFenceKey(catalogId, provider, normalizedLocator, normalizedIndexName);
    }

    /**
     * The per persisted table/locator quota identity.
     */
    public LanceIndexJobQuota.TableQuotaKey getTableQuotaKey() {
        return new LanceIndexJobQuota.TableQuotaKey(catalogId, normalizedLocator);
    }

    /**
     * Whether this job still holds its same-name fence and unresolved quota.
     * Fence and quota are released together: PENDING/RUNNING always hold; a
     * known terminal job holds until its required refresh is DONE (FAILED
     * still holds, refresh may retry); UNKNOWN holds until a durable
     * FORCE_RELEASE. A null state from a corrupt record is treated as UNKNOWN,
     * the safe direction (fence retained, never redispatched).
     */
    public boolean isUnresolved() {
        LanceIndexJobMutationState ms = mutationState == null ? LanceIndexJobMutationState.UNKNOWN : mutationState;
        switch (ms) {
            case PENDING:
            case RUNNING:
                return true;
            case UNKNOWN:
                return !forceReleased;
            case COMMITTED:
            case NOT_COMMITTED:
            default:
                LanceIndexJobRefreshState rs =
                        refreshState == null ? LanceIndexJobRefreshState.REQUIRED : refreshState;
                return rs == LanceIndexJobRefreshState.REQUIRED
                        || rs == LanceIndexJobRefreshState.RUNNING
                        || rs == LanceIndexJobRefreshState.FAILED;
        }
    }

    /**
     * Whether this job still owns a possible-live worker slot: released only
     * by a matching termination proof or a durable FORCE_RELEASE, never by a
     * deadline.
     */
    public boolean holdsPossibleLiveSlot() {
        return possibleLiveOwned
                && (terminationProof == null || terminationProof == LanceIndexTerminationProof.NONE)
                && !forceReleased;
    }

    /**
     * Validate the invariant-bearing and bounded fields before this record is
     * admitted. This is intentionally separate from Gson replay, which must
     * remain tolerant of old or corrupt records in the safe direction.
     */
    public void validateForAdmission() {
        checkRequiredBytes(creator, MAX_DURABLE_TEXT_BYTES, "creator");
        checkRequiredBytes(dbName, MAX_DURABLE_TEXT_BYTES, "dbName");
        checkRequiredBytes(tableName, MAX_DURABLE_TEXT_BYTES, "tableName");
        if (provider == null) {
            throw new IllegalArgumentException("lance index job provider must not be null");
        }
        if (!LanceIndexFenceKey.PROVIDER_DIRECTORY.equals(provider)) {
            throw new IllegalArgumentException("lance index job provider must be DIRECTORY");
        }
        if (normalizedLocator == null) {
            throw new IllegalArgumentException("normalized dataset locator must not be null");
        }
        String canonicalLocator = LanceIndexDatasetLocator.normalize(normalizedLocator);
        if (!canonicalLocator.equals(normalizedLocator)) {
            throw new IllegalArgumentException("dataset locator is not in canonical identity form");
        }
        LanceIndexNameNormalizer.validateDisplayName(displayIndexName);
        validateNormalizedIndexName(normalizedIndexName);
        String expectedNormalizedName = LanceIndexNameNormalizer.normalize(displayIndexName);
        if (!expectedNormalizedName.equals(normalizedIndexName)) {
            throw new IllegalArgumentException("normalized index name does not match the display name");
        }
        if (mutationType == null) {
            throw new IllegalArgumentException("mutation type must not be null");
        }
        checkBytes(indexType, MAX_DURABLE_TEXT_BYTES, "indexType");
        checkBytes(columnName, MAX_DURABLE_TEXT_BYTES, "columnName");
        checkBytes(propertiesJson, MAX_PROPERTIES_JSON_BYTES, "propertiesJson");
        checkBytes(invocationId, MAX_INVOCATION_ID_BYTES, "invocationId");
        checkBytes(forceActor, MAX_DURABLE_TEXT_BYTES, "forceActor");
        checkBytes(forceNote, MAX_FORCE_TEXT_BYTES, "forceNote");
        checkBytes(forceWarning, MAX_FORCE_TEXT_BYTES, "forceWarning");
        if (result != null) {
            checkBytes(result.getSanitizedMessage(), LanceIndexJobResult.MAX_MESSAGE_BYTES, "sanitizedMessage");
        }
        if (schemaContract != null) {
            schemaContract.validateForAdmission();
        }
    }

    // ------------------------------------------------------------------
    // Accessors. Setters for bounded text fields re-validate the bound.
    // ------------------------------------------------------------------

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = checkBytes(creator, MAX_DURABLE_TEXT_BYTES, "creator");
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getUpdateTimeMs() {
        return updateTimeMs;
    }

    public void setUpdateTimeMs(long updateTimeMs) {
        this.updateTimeMs = updateTimeMs;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getProvider() {
        return provider;
    }

    public String getNormalizedLocator() {
        return normalizedLocator;
    }

    public String getDisplayIndexName() {
        return displayIndexName;
    }

    public final void setDisplayIndexName(String displayIndexName) {
        LanceIndexNameNormalizer.validateDisplayName(displayIndexName);
        this.displayIndexName = displayIndexName;
    }

    public String getNormalizedIndexName() {
        return normalizedIndexName;
    }

    public final void setNormalizedIndexName(String normalizedIndexName) {
        validateNormalizedIndexName(normalizedIndexName);
        this.normalizedIndexName = normalizedIndexName;
    }

    private static void validateNormalizedIndexName(String normalizedIndexName) {
        if (normalizedIndexName == null || normalizedIndexName.isEmpty()) {
            throw new IllegalArgumentException("normalized index name must not be null or empty");
        }
        if (normalizedIndexName.getBytes(StandardCharsets.UTF_8).length
                > LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES) {
            throw new IllegalArgumentException(
                    "normalized index name exceeds " + LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES + " UTF-8 bytes");
        }
    }

    public LanceIndexJobMutationType getMutationType() {
        return mutationType;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = checkBytes(indexType, MAX_DURABLE_TEXT_BYTES, "indexType");
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = checkBytes(columnName, MAX_DURABLE_TEXT_BYTES, "columnName");
    }

    public String getPropertiesJson() {
        return propertiesJson;
    }

    public final void setPropertiesJson(String propertiesJson) {
        this.propertiesJson = checkBytes(propertiesJson, MAX_PROPERTIES_JSON_BYTES, "propertiesJson");
    }

    public long getAdmittedDatasetVersion() {
        return admittedDatasetVersion;
    }

    public LanceIndexSchemaContract getSchemaContract() {
        return schemaContract;
    }

    public LanceIndexJobMutationState getMutationState() {
        return mutationState;
    }

    public void setMutationState(LanceIndexJobMutationState mutationState) {
        this.mutationState = Objects.requireNonNull(mutationState, "mutationState");
    }

    public LanceIndexJobRefreshState getRefreshState() {
        return refreshState;
    }

    public void setRefreshState(LanceIndexJobRefreshState refreshState) {
        this.refreshState = Objects.requireNonNull(refreshState, "refreshState");
    }

    public LanceIndexJobResult getResult() {
        return result;
    }

    public void setResult(LanceIndexJobResult result) {
        this.result = result;
    }

    public Long getBackendId() {
        return backendId;
    }

    public void setBackendId(Long backendId) {
        this.backendId = backendId;
    }

    public Long getBeProcessEpoch() {
        return beProcessEpoch;
    }

    public void setBeProcessEpoch(Long beProcessEpoch) {
        this.beProcessEpoch = beProcessEpoch;
    }

    public Long getDispatchRevision() {
        return dispatchRevision;
    }

    public void setDispatchRevision(Long dispatchRevision) {
        this.dispatchRevision = dispatchRevision;
    }

    public String getInvocationId() {
        return invocationId;
    }

    public void setInvocationId(String invocationId) {
        this.invocationId = checkBytes(invocationId, MAX_INVOCATION_ID_BYTES, "invocationId");
    }

    public Long getDeadlineMs() {
        return deadlineMs;
    }

    public void setDeadlineMs(Long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    public boolean isPossibleLiveOwned() {
        return possibleLiveOwned;
    }

    public void setPossibleLiveOwned(boolean possibleLiveOwned) {
        this.possibleLiveOwned = possibleLiveOwned;
    }

    public LanceIndexTerminationProof getTerminationProof() {
        return terminationProof;
    }

    public void setTerminationProof(LanceIndexTerminationProof terminationProof) {
        this.terminationProof = Objects.requireNonNull(terminationProof, "terminationProof");
    }

    public boolean isForceReleased() {
        return forceReleased;
    }

    public void setForceReleased(boolean forceReleased) {
        this.forceReleased = forceReleased;
    }

    public String getForceActor() {
        return forceActor;
    }

    public void setForceActor(String forceActor) {
        this.forceActor = checkBytes(forceActor, MAX_DURABLE_TEXT_BYTES, "forceActor");
    }

    public Long getForceTimeMs() {
        return forceTimeMs;
    }

    public void setForceTimeMs(Long forceTimeMs) {
        this.forceTimeMs = forceTimeMs;
    }

    public String getForceNote() {
        return forceNote;
    }

    public void setForceNote(String forceNote) {
        this.forceNote = checkBytes(forceNote, MAX_FORCE_TEXT_BYTES, "forceNote");
    }

    public String getForceWarning() {
        return forceWarning;
    }

    public void setForceWarning(String forceWarning) {
        this.forceWarning = checkBytes(forceWarning, MAX_FORCE_TEXT_BYTES, "forceWarning");
    }

    private static String checkBytes(String value, int maxBytes, String fieldName) {
        if (value != null && value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(fieldName + " exceeds " + maxBytes + " UTF-8 bytes");
        }
        return value;
    }

    private static String checkRequiredBytes(String value, int maxBytes, String fieldName) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be null or empty");
        }
        return checkBytes(value, maxBytes, fieldName);
    }

    // ------------------------------------------------------------------
    // Serialization (Gson stream style, see DropIndexPolicyLog)
    // ------------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static LanceIndexJob read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), LanceIndexJob.class);
    }

    /**
     * Deliberately omits the locator: job lookups must not disclose target
     * details to callers without privilege, and log lines reuse this form.
     */
    @Override
    public String toString() {
        return "LanceIndexJob{jobId=" + jobId + ", revision=" + revision + ", catalogId=" + catalogId
                + ", db=" + dbName + ", table=" + tableName + ", index=" + displayIndexName
                + ", mutationType=" + mutationType + ", mutationState=" + mutationState
                + ", refreshState=" + refreshState + ", possibleLiveSlot=" + holdsPossibleLiveSlot()
                + ", forceReleased=" + forceReleased + '}';
    }
}
