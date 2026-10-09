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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceIndexMutationOutcome;
import org.apache.doris.datasource.lance.LanceIndexSchemaContract;
import org.apache.doris.datasource.lance.storage.LanceStorageOptions;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TApplicationException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Executes one admitted Lance index mutation synchronously inside the statement (design v6
 * sections 2.2 and 6): master-only, single-send, budget-bounded, classifying the answer into
 * the four-way {@link org.apache.doris.datasource.lance.LanceIndexMutationOutcome}.
 *
 * <p>The synchronous semantics this class owns, end to end:
 *
 * <ul>
 * <li><b>Single send.</b> Exactly zero or one dispatch happens per statement: the request is
 *     built and sent once inside the entry; no path - forwarding, timeout, cancellation, or a
 *     rejected backend answer - produces a second send of the same invocation id.
 * <li><b>Four-way outcome.</b> The classifier trusts only a complete answer: an OK status with
 *     lance result code 0 is a success; a complete non-OK status (including the
 *     UNKNOWN_METHOD answer an old backend gives during a rolling upgrade) proves the
 *     invocation never executed and is a confirmed failure; the exact typed commit-conflict
 *     code is the only post-invocation typed code that proves a non-commit (confirmed
 *     failure); everything else after the send started - any other typed code, any transport
 *     failure, a partial answer - is indeterminate. Message text never participates.
 * <li><b>Active-name entry.</b> A per (catalog, normalized dataset uri, normalized index name)
 *     compare-and-set entry is held from acquisition until classification and cleanup finish -
 *     not until the statement returns - so a same-name statement arriving while the entry is
 *     held fails fast with the typed busy error instead of queueing behind the running one.
 * <li><b>Bounded admission.</b> The entry phases run on a dedicated bounded pool with a finite
 *     queue (independent of the read pool); a full pool or queue is another typed busy
 *     rejection, never an unbounded wait.
 * <li><b>Statement budget.</b> The caller passes one absolute deadline; waiting for pool
 *     admission, the send, and the wait for the answer all share the remaining budget. A
 *     budget exhausted before the send is a confirmed failure that explicitly says nothing
 *     was sent; a budget exhausted after the send is indeterminate.
 * <li><b>Best-effort cancellation.</b> The cancel signal is checked at phase boundaries before
 *     the send; once the send started, the blocking read cannot be interrupted, the socket
 *     timeout (the remaining budget) closes the call, and a cancellation never rolls back a
 *     possible commit.
 * <li><b>Refresh obligations</b> (design v6 section 6.4): a trusted commit owes the local
 *     table caches an invalidation, whose failure downgrades the outcome to
 *     committed-refresh-incomplete - never a build failure; a no-op owes the same
 *     invalidation, whose failure is the typed refresh error of the confirmed family; a
 *     commit-conflict confirmed failure owes a best-effort invalidation of the externally
 *     advanced metadata, whose failure only attaches a diagnostic and never changes the
 *     classification. See {@link LanceIndexMutationRefresher} for what is actually stale.
 * </ul>
 *
 * <p>Master-only is the caller's guarantee (the statement forwards to the master before
 * reaching this class) plus the assertion at entry. All busy state is memory-only and never
 * persisted: a restart or master switch clears it, and a crashed holder cannot wedge a name.
 */
public final class LanceIndexMutationExecutor {

    /**
     * The native lance-c {@code LANCE_ERR_COMMIT_CONFLICT} value (the vocabulary behind the
     * wire's {@code lance_error_code}): the one typed post-invocation code that proves another
     * writer won the optimistic race, hence a non-commit. Every other non-zero code after the
     * mutating invocation began cannot prove a non-commit and stays indeterminate.
     */
    public static final int LANCE_ERR_COMMIT_CONFLICT = 8;

    private static final ConcurrentHashMap<String, MutationEntry> IN_FLIGHT = new ConcurrentHashMap<>();

    /**
     * Test seam for the master assertion: the entry task's phases run on pool threads, where a
     * thread-local test mock of {@link Env} cannot reach, so the production default reads Env
     * through a swappable supplier. Production behavior is exactly "current Env is master".
     */
    @VisibleForTesting
    static volatile BooleanSupplier masterCheck = () -> {
        Env env = Env.getCurrentEnv();
        return env != null && env.isMaster();
    };

    /**
     * Test seam for backend inventory, same reasoning as {@link #masterCheck}: production reads
     * the current system info service.
     */
    @VisibleForTesting
    static volatile Supplier<SystemInfoService> backendInventory = Env::getCurrentSystemInfo;

    /**
     * The bounded admission pool, independent of the Lance metadata read pool by design: a
     * mutation may block a worker for the whole statement budget and must never starve SHOW
     * INDEX or query metadata reads. Sized once from the config defaults at class load - a
     * live pool cannot be resized - so config changes take effect after a restart; values
     * loaded from fe.conf bypass the config callback, so the positive invariant is re-asserted
     * here (same discipline as the read pool's fixed bounds).
     */
    private static final ThreadPoolExecutor EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            Math.max(1, Config.lance_index_mutation_max_concurrency),
            Math.max(1, Config.lance_index_mutation_max_queued),
            "lance-index-mutation",
            false,
            new ThreadPoolExecutor.AbortPolicy());

    private LanceIndexMutationExecutor() {
    }

    /** The entry-internal final preflight verdict: proceed to the dispatch, or complete as no-op. */
    public enum PreflightResult {
        PROCEED,
        NO_OP
    }

    /**
     * The entry-internal final preflight as a throwing seam: the caller's preflight speaks its
     * own typed rejection by throwing (an {@link org.apache.doris.common.AnalysisException}
     * keeps its error code and propagates to the statement unchanged), or decides the no-op
     * verdict by returning it.
     */
    @FunctionalInterface
    public interface FinalPreflight {
        PreflightResult run() throws Exception;
    }

    /**
     * One mutation statement's inputs, as produced by admission plus the statement budget.
     * Immutable; the builder validates nothing because admission already has - every field is
     * trusted as admitted.
     */
    public static final class MutationRequest {
        private final LanceExternalCatalog catalog;
        private final TLanceIndexMutationType mutationType;
        private final String normalizedIndexName;
        private final String columnName;
        private final String indexType;
        private final Map<String, String> properties;
        private final boolean ifNotExists;
        private final boolean ifExists;
        private final String datasetUri;
        private final long admittedDatasetVersion;
        private final LanceIndexSchemaContract schemaContract;
        private final long deadlineMs;
        private final FinalPreflight finalPreflight;
        private final BooleanSupplier cancelSignal;
        private final LanceIndexMutationRefresher refresher;

        private MutationRequest(Builder builder) {
            this.catalog = builder.catalog;
            this.mutationType = builder.mutationType;
            this.normalizedIndexName = builder.normalizedIndexName;
            this.columnName = builder.columnName;
            this.indexType = builder.indexType;
            this.properties = builder.properties;
            this.ifNotExists = builder.ifNotExists;
            this.ifExists = builder.ifExists;
            this.datasetUri = builder.datasetUri;
            this.admittedDatasetVersion = builder.admittedDatasetVersion;
            this.schemaContract = builder.schemaContract;
            this.deadlineMs = builder.deadlineMs;
            this.finalPreflight = builder.finalPreflight;
            this.cancelSignal = builder.cancelSignal;
            // Production wiring sets the table and gets the production refresher without
            // saying so; an explicit refresher wins; a request with neither (tests, or a
            // caller with no obligation) gets the no-op.
            this.refresher = builder.refresher != null ? builder.refresher
                    : builder.table != null ? LanceIndexMutationRefresher.forTable(builder.table)
                    : LanceIndexMutationRefresher.NOOP;
        }

        /** Builder entry; the preflight supplier and cancel signal have safe defaults. */
        public static Builder newBuilder(LanceExternalCatalog catalog, TLanceIndexMutationType mutationType,
                String datasetUri, long admittedDatasetVersion, String normalizedIndexName, long deadlineMs) {
            return new Builder(catalog, mutationType, datasetUri, admittedDatasetVersion,
                    normalizedIndexName, deadlineMs);
        }

        /** Builder of {@link MutationRequest}. */
        public static final class Builder {
            private final LanceExternalCatalog catalog;
            private final TLanceIndexMutationType mutationType;
            private final String datasetUri;
            private final long admittedDatasetVersion;
            private final String normalizedIndexName;
            private final long deadlineMs;
            private String columnName = "";
            private String indexType = "";
            private Map<String, String> properties = null;
            private boolean ifNotExists = false;
            private boolean ifExists = false;
            private LanceIndexSchemaContract schemaContract = null;
            private FinalPreflight finalPreflight = () -> PreflightResult.PROCEED;
            private BooleanSupplier cancelSignal = () -> false;
            private LanceExternalTable table = null;
            private LanceIndexMutationRefresher refresher = null;

            private Builder(LanceExternalCatalog catalog, TLanceIndexMutationType mutationType,
                    String datasetUri, long admittedDatasetVersion, String normalizedIndexName, long deadlineMs) {
                this.catalog = catalog;
                this.mutationType = mutationType;
                this.datasetUri = datasetUri;
                this.admittedDatasetVersion = admittedDatasetVersion;
                this.normalizedIndexName = normalizedIndexName;
                this.deadlineMs = deadlineMs;
            }

            public Builder setColumnName(String columnName) {
                this.columnName = columnName;
                return this;
            }

            public Builder setIndexType(String indexType) {
                this.indexType = indexType;
                return this;
            }

            public Builder setProperties(Map<String, String> properties) {
                this.properties = properties;
                return this;
            }

            public Builder setIfNotExists(boolean ifNotExists) {
                this.ifNotExists = ifNotExists;
                return this;
            }

            public Builder setIfExists(boolean ifExists) {
                this.ifExists = ifExists;
                return this;
            }

            public Builder setSchemaContract(LanceIndexSchemaContract schemaContract) {
                this.schemaContract = schemaContract;
                return this;
            }

            /**
             * The entry-internal final preflight, run once inside the held entry before any
             * budget is spent: it re-decides the authoritative no-op verdict against current
             * metadata. Returning {@link PreflightResult#NO_OP} completes the statement without
             * a dispatch; throwing propagates the thrower's own typed rejection unchanged.
             */
            public Builder setFinalPreflight(FinalPreflight finalPreflight) {
                this.finalPreflight = finalPreflight;
                return this;
            }

            /** The cancel flag of the statement, checked at phase boundaries before the send. */
            public Builder setCancelSignal(BooleanSupplier cancelSignal) {
                this.cancelSignal = cancelSignal;
                return this;
            }

            /**
             * The mutated table. Setting it derives the production refresh obligation
             * ({@link LanceIndexMutationRefresher#forTable}) unless an explicit refresher is
             * also set, so the statement wiring cannot forget the obligation.
             */
            public Builder setTable(LanceExternalTable table) {
                this.table = table;
                return this;
            }

            /** Overrides the derived refresher; tests inject fakes and the no-op here. */
            public Builder setRefresher(LanceIndexMutationRefresher refresher) {
                this.refresher = refresher;
                return this;
            }

            public MutationRequest build() {
                return new MutationRequest(this);
            }
        }

        /** The effective refresher (explicit override, or derived from the table, or no-op). */
        @VisibleForTesting
        LanceIndexMutationRefresher getRefresher() {
            return refresher;
        }
    }

    /**
     * Runs one mutation synchronously and returns its classified outcome. Throws the typed
     * busy {@link AnalysisException} when the same name is in flight or admission capacity is
     * exhausted (both fail fast, nothing is queued), and rethrows the final preflight's own
     * typed rejection unchanged. Any non-success outcome is the caller's to surface via
     * {@code LanceIndexMutationOutcome.toUserException()}.
     */
    public static LanceIndexMutationOutcome execute(MutationRequest request) throws AnalysisException {
        return execute(request, ThriftLanceIndexMutationDispatcher.INSTANCE, EXECUTOR);
    }

    @VisibleForTesting
    static LanceIndexMutationOutcome execute(MutationRequest request,
            LanceIndexMutationDispatcher dispatcher, ExecutorService executor) throws AnalysisException {
        assertMaster();
        // Fast-fail busy guard: a same-name statement never queues behind the running one, so
        // this check happens before any pool admission.
        String key = guardKey(request);
        MutationEntry entry = new MutationEntry();
        if (IN_FLIGHT.putIfAbsent(key, entry) != null) {
            throw busy("an active mutation still holds index '" + request.normalizedIndexName
                    + "' on this dataset; this statement was not queued");
        }
        Future<LanceIndexMutationOutcome> future;
        try {
            future = executor.submit(() -> runEntryTask(request, dispatcher, key, entry));
        } catch (RejectedExecutionException e) {
            // Admission capacity is full: release the entry ourselves - no task ever took
            // ownership - and fail fast with the typed busy error.
            releaseEntry(key, entry);
            throw busy("the mutation execution capacity (running plus queued) is exhausted;"
                    + " this statement was not queued");
        }
        return waitForOutcome(request, future);
    }

    /**
     * The statement side of the wait: bounded by the remaining budget, never cancelling the
     * entry task. A timeout or interruption does not know whether the send already happened,
     * so both surface indeterminate - the underlying task still converges on its own (an
     * un-dispatched task is stopped by its own budget re-check) and releases the entry.
     */
    private static LanceIndexMutationOutcome waitForOutcome(MutationRequest request,
            Future<LanceIndexMutationOutcome> future) throws AnalysisException {
        long waitMillis = Math.max(1L, request.deadlineMs - System.currentTimeMillis());
        try {
            return future.get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return LanceIndexMutationOutcome.indeterminate(0,
                    "the statement budget elapsed before a complete trusted result arrived;"
                    + " the mutation may or may not have been dispatched");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return LanceIndexMutationOutcome.indeterminate(0,
                    "the statement was interrupted while waiting for the mutation result;"
                    + " the mutation may or may not have committed");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof AnalysisException) {
                // The final preflight (and only it) speaks its own typed error; keep it.
                throw (AnalysisException) cause;
            }
            throw new IllegalStateException("lance index mutation execution failed unexpectedly", cause);
        }
    }

    /**
     * The entry task: owns the entry from submit to classification-and-cleanup, whatever the
     * caller thread does in between (the entry must outlive an early statement return).
     */
    private static LanceIndexMutationOutcome runEntryTask(MutationRequest request,
            LanceIndexMutationDispatcher dispatcher, String key, MutationEntry entry)
            throws AnalysisException {
        try {
            return runEntryPhases(request, dispatcher);
        } finally {
            releaseEntry(key, entry);
        }
    }

    /**
     * The entry phases, in order: final preflight, budget check, backend selection, request
     * build, single dispatch, classification. Every pre-dispatch failure is a confirmed
     * failure whose message states that nothing was sent; everything after the send started
     * that is not a complete trusted answer is indeterminate.
     */
    private static LanceIndexMutationOutcome runEntryPhases(MutationRequest request,
            LanceIndexMutationDispatcher dispatcher) throws AnalysisException {
        if (request.cancelSignal.getAsBoolean()) {
            return cancelledBeforeDispatch("before the entry task began");
        }
        // 1. Entry-internal final preflight: the authoritative no-op verdict. A no-op never
        // spends budget and never dispatches - there is nothing to send. A typed rejection
        // keeps its own error code; anything else the preflight throws is a wiring bug.
        PreflightResult preflight;
        try {
            preflight = request.finalPreflight.run();
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("the entry-internal final preflight failed unexpectedly", e);
        }
        if (preflight == PreflightResult.NO_OP) {
            return completeNoOp(request);
        }
        // 2. Budget check before anything is spent on the send.
        long remainingMs = request.deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
            return LanceIndexMutationOutcome.confirmedFailure(0,
                    "the statement budget was exhausted before the dispatch; nothing was sent");
        }
        if (request.cancelSignal.getAsBoolean()) {
            return cancelledBeforeDispatch("after the budget check, before the dispatch");
        }
        // 3. One alive backend; none is a pre-dispatch rejection, not an ambiguity.
        Backend backend = selectBackend();
        if (backend == null) {
            return LanceIndexMutationOutcome.confirmedFailure(0,
                    "no alive backend is available to execute the mutation; nothing was sent");
        }
        // 4. Build the wire request (single dispatch identity, storage options resolved from
        // the catalog now, at send time). A build failure predates the send: confirmed failure.
        TLanceIndexMutationRequest wireRequest;
        try {
            wireRequest = buildRequest(request, backend);
        } catch (Exception e) {
            return LanceIndexMutationOutcome.confirmedFailure(0,
                    "building the dispatch request failed before anything was sent: "
                            + boundedDescription(e));
        }
        // 5. The single dispatch. The call itself is the wait, bounded by the remaining budget.
        TLanceIndexMutationResult result;
        try {
            result = dispatcher.dispatch(wireRequest, backend, remainingMs);
        } catch (TApplicationException e) {
            if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                // An old backend in a rolling upgrade provably never served the call - the
                // same clean-rejection semantics as a complete non-OK status.
                return LanceIndexMutationOutcome.confirmedFailure(0,
                        "the backend does not serve lance_index_mutate (rolling upgrade);"
                                + " nothing was executed");
            }
            return indeterminateAfterDispatch(e);
        } catch (Exception e) {
            return indeterminateAfterDispatch(e);
        }
        // 6. Classification (the refresh obligations hang off the trusted classes).
        return classify(request, result);
    }

    /**
     * Completes a no-op (design v6 section 6.4): nothing changed remotely, but the statement
     * still owes the local table caches an invalidation - a defensive fence against table-
     * scoped caches populated against an older dataset version during this statement's
     * lifetime. A residual failure is the typed refresh error of the confirmed family: the
     * statement made no change, so the failure is neither a build failure nor an ambiguity.
     */
    private static LanceIndexMutationOutcome completeNoOp(MutationRequest request)
            throws AnalysisException {
        String refreshFailure = request.refresher.invalidateTableMetadata();
        if (refreshFailure != null) {
            throw refreshFailed("the no-op completed, but " + refreshFailure);
        }
        return LanceIndexMutationOutcome.success();
    }

    /**
     * Completes a trusted native success (design v6 section 6.4): the commit is a fact, then
     * the local table caches are invalidated. A refresh failure downgrades the outcome to
     * committed-refresh-incomplete - the commit is never rewritten as a build failure, never
     * denied, and never retried by this statement.
     */
    private static LanceIndexMutationOutcome completeWithCommitRefresh(MutationRequest request) {
        String refreshFailure = request.refresher.invalidateTableMetadata();
        if (refreshFailure == null) {
            return LanceIndexMutationOutcome.success();
        }
        return LanceIndexMutationOutcome.committedRefreshIncomplete(
                LanceIndexMutationOutcome.LANCE_RESULT_OK,
                "the commit stands and is not affected; " + refreshFailure);
    }

    /**
     * Classification (design v6 section 6.5): trusts only complete answers, reads the status
     * first and the typed code second, and never the message text. The shipped backend stub
     * answers NOT_IMPLEMENTED_ERROR, so a dispatched statement against it classifies here as
     * a confirmed failure.
     */
    private static LanceIndexMutationOutcome classify(MutationRequest request,
            TLanceIndexMutationResult result) {
        if (result == null || !result.isSetStatus() || result.getStatus().getStatusCode() == null) {
            return LanceIndexMutationOutcome.indeterminate(0,
                    "the dispatch returned no complete status");
        }
        TStatus status = result.getStatus();
        if (status.getStatusCode() != TStatusCode.OK) {
            return LanceIndexMutationOutcome.confirmedFailure(0,
                    "the backend refused the invocation before executing it: "
                            + status.getStatusCode() + backendMessages(status));
        }
        if (!result.isSetLanceErrorCode()) {
            return LanceIndexMutationOutcome.indeterminate(0,
                    "an OK status carried no typed lance result code (partial answer)");
        }
        int lanceResultCode = result.getLanceErrorCode();
        if (lanceResultCode == LanceIndexMutationOutcome.LANCE_RESULT_OK) {
            return completeWithCommitRefresh(request);
        }
        if (lanceResultCode == LANCE_ERR_COMMIT_CONFLICT) {
            // The exact typed commit conflict proves this invocation committed nothing:
            // confirmed failure. Another writer advanced the dataset metadata, so the local
            // caches get a best-effort refresh whose failure only attaches a diagnostic -
            // the classification never changes because a refresh failed.
            String refreshFailure = request.refresher.invalidateTableMetadata();
            String message = "typed lance commit conflict: another writer advanced the dataset"
                    + " metadata first";
            if (refreshFailure != null) {
                message += "; the best-effort refresh of the externally advanced metadata"
                        + " also failed: " + refreshFailure;
            }
            return LanceIndexMutationOutcome.confirmedFailure(lanceResultCode, message);
        }
        return LanceIndexMutationOutcome.indeterminate(lanceResultCode,
                "typed lance result code " + lanceResultCode + " arrived after the mutating"
                        + " invocation began; it cannot prove the mutation did not commit");
    }

    private static LanceIndexMutationOutcome indeterminateAfterDispatch(Exception e) {
        return LanceIndexMutationOutcome.indeterminate(0,
                "the dispatch failed after the send may have started (" + boundedDescription(e)
                        + "); the mutation may or may not have committed");
    }

    private static LanceIndexMutationOutcome cancelledBeforeDispatch(String phase) {
        return LanceIndexMutationOutcome.confirmedFailure(0,
                "the statement was cancelled " + phase + "; the dispatch never happened, so"
                        + " nothing was committed. Cancellation after the dispatch is"
                        + " best-effort and never rolls back a possible commit");
    }

    private static Backend selectBackend() {
        SystemInfoService systemInfo = backendInventory.get();
        if (systemInfo == null) {
            return null;
        }
        List<Long> backendIds = systemInfo.selectBackendIdsByPolicy(
                new BeSelectionPolicy.Builder().needScheduleAvailable().allowOnSameHost()
                        .preferComputeNode(true)
                        .assignExpectBeNum(systemInfo.getAllBackendIds(false).size()).build(), -1);
        for (Long backendId : backendIds) {
            Backend backend = systemInfo.getBackend(backendId);
            if (backend != null && backend.getBePort() > 0) {
                return backend;
            }
        }
        return null;
    }

    /**
     * Builds the wire request. The invocation id is minted here, once, for the single send;
     * the process epoch is read once and the same value travels on the wire; storage options
     * are resolved from the current catalog properties at send time (never persisted, logged,
     * or echoed); the schema contract travels for CREATE/REPLACE only.
     */
    private static TLanceIndexMutationRequest buildRequest(MutationRequest request, Backend backend) {
        TLanceIndexMutationRequest wireRequest = new TLanceIndexMutationRequest();
        wireRequest.setInvocationId(UUID.randomUUID().toString());
        wireRequest.setBeProcessEpoch(backend.getProcessEpoch());
        wireRequest.setDeadlineMs(request.deadlineMs);
        wireRequest.setMutationType(request.mutationType);
        wireRequest.setIndexName(request.normalizedIndexName);
        wireRequest.setColumnName(request.columnName == null ? "" : request.columnName);
        wireRequest.setIndexType(request.indexType == null ? "" : request.indexType);
        if (request.properties != null && !request.properties.isEmpty()) {
            wireRequest.setPropertiesJson(GsonUtils.GSON.toJson(request.properties));
        }
        if (request.ifNotExists) {
            wireRequest.setIfNotExists(true);
        }
        if (request.ifExists) {
            wireRequest.setIfExists(true);
        }
        wireRequest.setDatasetUri(request.datasetUri);
        wireRequest.setAdmittedDatasetVersion(request.admittedDatasetVersion);
        if (request.schemaContract != null) {
            wireRequest.setSchemaContractJson(GsonUtils.GSON.toJson(request.schemaContract));
        }
        wireRequest.setStorageOptions(LanceStorageOptions.fromDorisStorageProperties(
                request.datasetUri, request.catalog.getCatalogProperty()
                        .getOrderedStoragePropertiesList()));
        return wireRequest;
    }

    /**
     * The statement's total budget deadline: the statement budget starts at statement entry
     * and is capped by {@link Config#lance_index_mutation_budget_cap_seconds}; with no
     * statement context the cap alone is the budget.
     */
    public static long budgetDeadlineMs(long statementStartMs) {
        int capSeconds = Math.max(1, Config.lance_index_mutation_budget_cap_seconds);
        ConnectContext context = ConnectContext.get();
        int queryTimeoutSeconds = context == null ? 0 : context.getQueryTimeoutS();
        int budgetSeconds = queryTimeoutSeconds > 0 ? Math.min(queryTimeoutSeconds, capSeconds)
                : capSeconds;
        long deadline = statementStartMs + budgetSeconds * 1000L;
        return deadline < 0 ? Long.MAX_VALUE : deadline;
    }

    private static void assertMaster() {
        if (!masterCheck.getAsBoolean()) {
            throw new IllegalStateException("the synchronous lance index mutation executor runs"
                    + " only on the master FE; the statement layer must forward first");
        }
    }

    private static String guardKey(MutationRequest request) {
        return request.catalog.getId() + "|" + normalizeDatasetUri(request.datasetUri)
                + "|" + request.normalizedIndexName;
    }

    /**
     * Normalizes a dataset uri for the busy guard: surrounding whitespace and trailing slashes
     * are insignificant, while case is significant (object-store buckets and keys are), so the
     * uri is deliberately not case-folded.
     */
    static String normalizeDatasetUri(String datasetUri) {
        String normalized = datasetUri.trim();
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static void releaseEntry(String key, MutationEntry entry) {
        if (entry.release()) {
            // Conditional remove: a later statement's fresh entry for the same key is never
            // removed by an older releaser.
            IN_FLIGHT.remove(key, entry);
        }
    }

    private static AnalysisException busy(String detail) {
        return new AnalysisException(ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY.formatErrorMsg(detail),
                ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY);
    }

    /** The no-op family's typed refresh error (confirmed family: nothing was mutated). */
    private static AnalysisException refreshFailed(String detail) {
        return new AnalysisException(
                ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED.formatErrorMsg(detail),
                ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED);
    }

    private static String backendMessages(TStatus status) {
        if (status.getErrorMsgs() == null || status.getErrorMsgs().isEmpty()) {
            return "";
        }
        // Diagnostics only; the outcome model bounds and sanitizes whatever lands here.
        return ", message: " + String.join("; ", status.getErrorMsgs());
    }

    private static String boundedDescription(Throwable t) {
        String message = t.getMessage();
        return message == null ? t.getClass().getSimpleName() : t.getClass().getSimpleName()
                + ": " + message;
    }

    @VisibleForTesting
    static int inFlightCount() {
        return IN_FLIGHT.size();
    }

    /**
     * The in-flight busy guard entry: an {@link AtomicReference} status used as a lock in the
     * Dictionary LOADING style - ACQUIRED on the compare-and-set of map insertion, released
     * exactly once by the owner (the entry task, or the submitter when no task ever took
     * ownership), never persisted, so a restart or master switch self-heals.
     */
    private static final class MutationEntry {
        /** Lifecycle of one guard entry: active while the mutation owns the name. */
        private enum Status {
            ACTIVE,
            RELEASED
        }

        private final AtomicReference<Status> status = new AtomicReference<>(Status.ACTIVE);

        /** Releases the entry exactly once; a second release (a bug) is a no-op. */
        boolean release() {
            return status.compareAndSet(Status.ACTIVE, Status.RELEASED);
        }
    }
}
