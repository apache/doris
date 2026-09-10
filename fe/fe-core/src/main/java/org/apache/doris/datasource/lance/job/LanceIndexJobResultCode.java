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

/**
 * Typed result codes of the one-shot Lance index invocation. Only saved typed
 * codes are ever classified; message text is never inspected to infer an
 * outcome. Codes fall into three groups:
 *
 * <ul>
 *   <li>PRE_INVOCATION_*: a complete trusted rejection before the native call
 *   (dataset version / schema contract / credential / resource revalidation).
 *   These always prove NOT_COMMITTED.</li>
 *   <li>NATIVE_*: the saved LanceErrorCode of the single native invocation,
 *   read before the consuming error message.</li>
 *   <li>NO_TRUSTED_RESULT: any ambiguity after the send may have occurred
 *   (EOF, signal, OOM, panic, timeout, BE loss, malformed/partial protocol,
 *   identity mismatch after acceptance, or a master-transfer sweep that found
 *   a durable RUNNING without a complete matching terminal result).</li>
 * </ul>
 */
public enum LanceIndexJobResultCode {
    /** Pre-invocation: admitted dataset version / schema contract no longer matches. */
    PRE_INVOCATION_STALE_ADMISSION,
    /** Pre-invocation: the recomputed contract is not a supported contract v1. */
    PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
    /** Pre-invocation: credentials are known to be expired. */
    PRE_INVOCATION_CREDENTIAL_EXPIRED,
    /** Pre-invocation: trusted busy / pre-FFI resource rejection. */
    PRE_INVOCATION_RESOURCE_REJECTED,
    /** LANCE_OK from the one native invocation. */
    NATIVE_OK,
    /** LANCE_ERR_COMMIT_CONFLICT: the commit lost a race; the external dataset advanced. */
    NATIVE_COMMIT_CONFLICT,
    /** LANCE_ERR_NOT_FOUND after invocation. */
    NATIVE_NOT_FOUND,
    /** LANCE_ERR_INVALID_ARGUMENT after invocation. */
    NATIVE_INVALID_ARGUMENT,
    /** LANCE_ERR_NOT_SUPPORTED after invocation. */
    NATIVE_NOT_SUPPORTED,
    /** LANCE_ERR_INDEX after invocation (also the coarse duplicate-CREATE error). */
    NATIVE_INDEX,
    /** LANCE_ERR_IO after invocation. */
    NATIVE_IO,
    /** LANCE_ERR_INTERNAL after invocation. */
    NATIVE_INTERNAL,
    /** No complete trusted result exists; commitment cannot be proven either way. */
    NO_TRUSTED_RESULT;

    public boolean isPreInvocation() {
        return this == PRE_INVOCATION_STALE_ADMISSION
                || this == PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT
                || this == PRE_INVOCATION_CREDENTIAL_EXPIRED
                || this == PRE_INVOCATION_RESOURCE_REJECTED;
    }

    /**
     * Classify a complete saved result into the durable (mutationState, refreshState,
     * completionReason) triple, following the provider-result classification table:
     *
     * <ul>
     *   <li>PRE_INVOCATION_* -&gt; NOT_COMMITTED; refresh REQUIRED only when the trusted
     *   pre-invocation revalidation observed relevant external metadata advancement.</li>
     *   <li>NATIVE_OK -&gt; COMMITTED + REQUIRED.</li>
     *   <li>NATIVE_COMMIT_CONFLICT -&gt; NOT_COMMITTED + REQUIRED (the external dataset
     *   advanced).</li>
     *   <li>NATIVE_NOT_FOUND -&gt; CREATE/REPLACE: UNKNOWN + NOT_REQUIRED (no attribution);
     *   DROP: NOT_COMMITTED + REQUIRED, with IF_CONDITION_NOOP for DROP IF EXISTS.</li>
     *   <li>NATIVE_INVALID_ARGUMENT / NOT_SUPPORTED / INDEX / IO / INTERNAL -&gt;
     *   UNKNOWN + NOT_REQUIRED (no outcome inference).</li>
     *   <li>NO_TRUSTED_RESULT -&gt; UNKNOWN + NOT_REQUIRED.</li>
     * </ul>
     */
    public static Classification classify(LanceIndexJobMutationType mutationType, LanceIndexJobResultCode resultCode,
            boolean ifExists, boolean externalMetadataAdvanced) {
        if (resultCode == null) {
            resultCode = NO_TRUSTED_RESULT;
        }
        switch (resultCode) {
            case PRE_INVOCATION_STALE_ADMISSION:
            case PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT:
            case PRE_INVOCATION_CREDENTIAL_EXPIRED:
            case PRE_INVOCATION_RESOURCE_REJECTED:
                return new Classification(LanceIndexJobMutationState.NOT_COMMITTED,
                        externalMetadataAdvanced
                                ? LanceIndexJobRefreshState.REQUIRED : LanceIndexJobRefreshState.NOT_REQUIRED,
                        LanceIndexJobCompletionReason.NONE);
            case NATIVE_OK:
                return new Classification(LanceIndexJobMutationState.COMMITTED, LanceIndexJobRefreshState.REQUIRED,
                        LanceIndexJobCompletionReason.NONE);
            case NATIVE_COMMIT_CONFLICT:
                return new Classification(LanceIndexJobMutationState.NOT_COMMITTED, LanceIndexJobRefreshState.REQUIRED,
                        LanceIndexJobCompletionReason.NONE);
            case NATIVE_NOT_FOUND:
                if (mutationType == LanceIndexJobMutationType.DROP) {
                    return new Classification(LanceIndexJobMutationState.NOT_COMMITTED,
                            LanceIndexJobRefreshState.REQUIRED,
                            ifExists ? LanceIndexJobCompletionReason.IF_CONDITION_NOOP
                                    : LanceIndexJobCompletionReason.NONE);
                }
                return new Classification(LanceIndexJobMutationState.UNKNOWN, LanceIndexJobRefreshState.NOT_REQUIRED,
                        LanceIndexJobCompletionReason.NONE);
            case NATIVE_INVALID_ARGUMENT:
            case NATIVE_NOT_SUPPORTED:
            case NATIVE_INDEX:
            case NATIVE_IO:
            case NATIVE_INTERNAL:
            case NO_TRUSTED_RESULT:
            default:
                return new Classification(LanceIndexJobMutationState.UNKNOWN, LanceIndexJobRefreshState.NOT_REQUIRED,
                        LanceIndexJobCompletionReason.NONE);
        }
    }

    /**
     * The immutable outcome of {@link #classify}: the durable mutation state, the
     * independent refresh obligation, and the completion reason.
     */
    public static final class Classification {
        private final LanceIndexJobMutationState mutationState;
        private final LanceIndexJobRefreshState refreshState;
        private final LanceIndexJobCompletionReason completionReason;

        private Classification(LanceIndexJobMutationState mutationState, LanceIndexJobRefreshState refreshState,
                LanceIndexJobCompletionReason completionReason) {
            this.mutationState = mutationState;
            this.refreshState = refreshState;
            this.completionReason = completionReason;
        }

        public LanceIndexJobMutationState getMutationState() {
            return mutationState;
        }

        public LanceIndexJobRefreshState getRefreshState() {
            return refreshState;
        }

        public LanceIndexJobCompletionReason getCompletionReason() {
            return completionReason;
        }
    }
}
