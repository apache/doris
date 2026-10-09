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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;

/**
 * The four-way client-facing outcome of one synchronous Lance index mutation statement (design
 * sections 6.2-6.5).
 *
 * <p>Classification trusts only a complete, identity-matching provider result; message text
 * never participates in the decision (design section 6.5). The four kinds are the whole
 * client-facing contract:
 *
 * <ul>
 * <li>{@link Kind#SUCCESS SUCCESS} — a complete trusted native success. The commit is a fact,
 *     and the required metadata refresh finished inside the statement.
 * <li>{@link Kind#CONFIRMED_FAILURE CONFIRMED_FAILURE} — nothing was committed, proven either
 *     before the send (nothing was sent: budget exhaustion, admission rejection, a backend
 *     connection that could not be established) or by a complete trusted result: a clean
 *     rejection before the mutating native invocation (for example NOT_IMPLEMENTED from a BE
 *     that has not learned the dispatch RPC) or the typed commit-conflict code.
 * <li>{@link Kind#INDETERMINATE INDETERMINATE} — dispatched, but no complete trusted result
 *     arrived (timeout, disconnect, worker loss, protocol ambiguity). The mutation may or may
 *     not have committed.
 * <li>{@link Kind#COMMITTED_REFRESH_INCOMPLETE COMMITTED_REFRESH_INCOMPLETE} — a complete
 *     trusted success was received, so the commit stands, but the metadata refresh did not
 *     complete. Reported as its own class so a refresh failure is never rewritten as a build
 *     failure (design section 6.4).
 * </ul>
 *
 * <p>Why INDETERMINATE exists as a distinct class instead of a generic timeout error: the
 * pinned provider ABI loses information by design — an aggregate error after the mutating
 * invocation began cannot prove that nothing committed. A blind retry of a possibly-committed
 * mutation could collide with the first attempt's own commit ("already exists") or replace an
 * index the first attempt created, so nothing in Doris retries this class automatically and the
 * client-facing text directs the user to the authoritative metadata
 * ({@code SHOW INDEX FROM} and {@code lance_index_entries()}) instead.
 */
public final class LanceIndexMutationOutcome {

    /** The classification of one mutation statement; exactly one kind per outcome. */
    public enum Kind {
        SUCCESS,
        CONFIRMED_FAILURE,
        INDETERMINATE,
        COMMITTED_REFRESH_INCOMPLETE
    }

    /** The lance result code of a native success; every other value is provider-defined. */
    public static final int LANCE_RESULT_OK = 0;

    /**
     * Bound of the carried message, in UTF-8 bytes. Matches the provider-message bound of
     * {@link LanceErrorMessages}: the message travels inside client-facing error text, so an
     * unbounded provider payload must never reach it.
     */
    public static final int MAX_MESSAGE_BYTES = 1024;

    private final Kind kind;
    private final int lanceResultCode;
    private final String message;

    private LanceIndexMutationOutcome(Kind kind, int lanceResultCode, String message) {
        this.kind = kind;
        this.lanceResultCode = lanceResultCode;
        // Defense in depth: the dispatch layer is expected to pass already-sanitized provider
        // text (LanceErrorMessages), but the model enforces its own bound so no future caller
        // can smuggle an unbounded raw provider payload or control characters into the carried
        // message. The raw text is never stored.
        this.message = LanceErrorMessages.truncateUtf8(
                LanceErrorMessages.removeControlCharacters(message == null ? "" : message),
                MAX_MESSAGE_BYTES);
    }

    /**
     * A complete trusted native success: lance result code 0, no residual diagnostic. The
     * statement returns normally; this outcome carries no client-facing exception.
     */
    public static LanceIndexMutationOutcome success() {
        return new LanceIndexMutationOutcome(Kind.SUCCESS, LANCE_RESULT_OK, "");
    }

    /**
     * A confirmed non-commit. {@code lanceResultCode} is the typed code of the trusted
     * rejecting result, or {@link #LANCE_RESULT_OK} when the failure predates dispatch and no
     * provider result exists (budget exhaustion, no backend, a backend connection that could
     * not be established, admission rejection).
     */
    public static LanceIndexMutationOutcome confirmedFailure(int lanceResultCode, String message) {
        return new LanceIndexMutationOutcome(Kind.CONFIRMED_FAILURE, lanceResultCode, message);
    }

    /**
     * Dispatched but unresolved: no complete trusted result was received. {@code
     * lanceResultCode} is whatever partial or mismatched code was last seen (0 when none was),
     * carried for diagnostics only — it never feeds the classification.
     */
    public static LanceIndexMutationOutcome indeterminate(int lanceResultCode, String message) {
        return new LanceIndexMutationOutcome(Kind.INDETERMINATE, lanceResultCode, message);
    }

    /**
     * The commit is a fact — a complete trusted native success was received — but the metadata
     * refresh did not complete inside the statement.
     */
    public static LanceIndexMutationOutcome committedRefreshIncomplete(int lanceResultCode,
            String message) {
        return new LanceIndexMutationOutcome(Kind.COMMITTED_REFRESH_INCOMPLETE, lanceResultCode,
                message);
    }

    public Kind getKind() {
        return kind;
    }

    /**
     * The typed lance result code of the trusted result that drove the classification: 0 for a
     * native success, 0 when the outcome was classified before any provider result existed,
     * otherwise the provider-defined code.
     */
    public int getLanceResultCode() {
        return lanceResultCode;
    }

    /**
     * The bounded sanitized diagnostic (control characters stripped, UTF-8 truncated to
     * {@link #MAX_MESSAGE_BYTES}); never the raw provider payload.
     */
    public String getMessage() {
        return message;
    }

    /**
     * The client-facing exception for this outcome, each kind on its own stable error code.
     * SUCCESS has no exception — the statement returns normally — so reaching this method on a
     * success outcome is a caller wiring bug and throws instead of inventing an error.
     */
    public AnalysisException toUserException() {
        switch (kind) {
            case CONFIRMED_FAILURE:
                return userException(ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED);
            case INDETERMINATE:
                return userException(ErrorCode.ERR_LANCE_INDEX_MUTATION_INDETERMINATE);
            case COMMITTED_REFRESH_INCOMPLETE:
                return userException(ErrorCode.ERR_LANCE_INDEX_MUTATION_COMMITTED_REFRESH_INCOMPLETE);
            default:
                throw new IllegalStateException(
                        "A successful mutation outcome has no client-facing exception");
        }
    }

    /**
     * Builds the exception directly instead of going through ErrorReport:
     * reportAnalysisException throws the exception from inside the helper, while this model
     * returns the exception for the caller to throw at the statement boundary.
     */
    private AnalysisException userException(ErrorCode errorCode) {
        return new AnalysisException(errorCode.formatErrorMsg(detail()), errorCode);
    }

    private String detail() {
        return message.isEmpty()
                ? "lance result code " + lanceResultCode
                : "lance result code " + lanceResultCode + ", detail: " + message;
    }
}
