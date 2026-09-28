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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Exhaustive coverage of the provider-result classification table: every saved typed
 * result code is classified into the durable (mutationState, refreshState,
 * completionReason) triple for the full cross product of 13 codes x 3 mutation types
 * x ifExists x externalMetadataAdvanced (156 combinations). The expected values are an
 * independent restatement of the design table, so a row flipped on either side fails.
 *
 * <p>Key invariants pinned here: PRE_INVOCATION_* always prove NOT_COMMITTED and owe a
 * refresh only when trusted revalidation observed external metadata advancement;
 * NATIVE_NOT_FOUND has no attribution for CREATE/REPLACE (UNKNOWN) but is a clean
 * NOT_COMMITTED for DROP; IF_CONDITION_NOOP exists only for DROP IF EXISTS + NOT_FOUND;
 * every other ambiguous post-invocation outcome is UNKNOWN + NOT_REQUIRED.
 */
public class LanceIndexJobResultClassifyTest {

    @Test
    public void fullClassificationProductMatchesDesignTable() {
        int combos = 0;
        for (LanceIndexJobResultCode code : LanceIndexJobResultCode.values()) {
            for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
                for (boolean ifExists : new boolean[]{false, true}) {
                    for (boolean advanced : new boolean[]{false, true}) {
                        LanceIndexJobResultCode.Classification classification =
                                LanceIndexJobResultCode.classify(type, code, ifExists, advanced);
                        String context = "code=" + code + ", type=" + type
                                + ", ifExists=" + ifExists + ", advanced=" + advanced;
                        Assertions.assertEquals(expectedMutationState(type, code),
                                classification.getMutationState(), context);
                        Assertions.assertEquals(expectedRefreshState(type, code, advanced),
                                classification.getRefreshState(), context);
                        Assertions.assertEquals(expectedCompletionReason(type, code, ifExists),
                                classification.getCompletionReason(), context);
                        combos++;
                    }
                }
            }
        }
        Assertions.assertEquals(13 * 3 * 2 * 2, combos);
    }

    @Test
    public void ifConditionNoopExistsOnlyForDropIfExistsNotFound() {
        int noopCombos = 0;
        for (LanceIndexJobResultCode code : LanceIndexJobResultCode.values()) {
            for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
                for (boolean ifExists : new boolean[]{false, true}) {
                    for (boolean advanced : new boolean[]{false, true}) {
                        LanceIndexJobResultCode.Classification classification =
                                LanceIndexJobResultCode.classify(type, code, ifExists, advanced);
                        if (classification.getCompletionReason() == LanceIndexJobCompletionReason.IF_CONDITION_NOOP) {
                            noopCombos++;
                            Assertions.assertEquals(LanceIndexJobMutationType.DROP, type);
                            Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_NOT_FOUND, code);
                            Assertions.assertTrue(ifExists);
                        }
                    }
                }
            }
        }
        // Exactly the two advanced-flag variants of (DROP, NATIVE_NOT_FOUND, ifExists).
        Assertions.assertEquals(2, noopCombos);
    }

    @Test
    public void preInvocationRefreshOwesOnlyWhenExternalMetadataAdvanced() {
        for (LanceIndexJobResultCode code : LanceIndexJobResultCode.values()) {
            if (!code.isPreInvocation()) {
                continue;
            }
            for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
                Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED,
                        LanceIndexJobResultCode.classify(type, code, false, false).getRefreshState());
                Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED,
                        LanceIndexJobResultCode.classify(type, code, false, true).getRefreshState());
                Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED,
                        LanceIndexJobResultCode.classify(type, code, true, true).getMutationState());
            }
        }
    }

    @Test
    public void nativeOkCommitsAndOwesRefreshForEveryMutationType() {
        for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
            LanceIndexJobResultCode.Classification classification =
                    LanceIndexJobResultCode.classify(type, LanceIndexJobResultCode.NATIVE_OK, false, false);
            Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, classification.getMutationState());
            Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, classification.getRefreshState());
            Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, classification.getCompletionReason());
        }
    }

    @Test
    public void commitConflictIsNotCommittedWithRefreshOwed() {
        for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
            LanceIndexJobResultCode.Classification classification = LanceIndexJobResultCode.classify(
                    type, LanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT, false, false);
            Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, classification.getMutationState());
            Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, classification.getRefreshState());
            Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, classification.getCompletionReason());
        }
    }

    @Test
    public void nativeNotFoundAttributionDependsOnMutationType() {
        for (LanceIndexJobMutationType type : new LanceIndexJobMutationType[]{
                LanceIndexJobMutationType.CREATE, LanceIndexJobMutationType.REPLACE}) {
            LanceIndexJobResultCode.Classification classification = LanceIndexJobResultCode.classify(
                    type, LanceIndexJobResultCode.NATIVE_NOT_FOUND, false, false);
            Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, classification.getMutationState());
            Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, classification.getRefreshState());
            Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, classification.getCompletionReason());
        }
        LanceIndexJobResultCode.Classification dropClassification = LanceIndexJobResultCode.classify(
                LanceIndexJobMutationType.DROP, LanceIndexJobResultCode.NATIVE_NOT_FOUND, false, false);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, dropClassification.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, dropClassification.getRefreshState());
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, dropClassification.getCompletionReason());
    }

    @Test
    public void ambiguousPostInvocationOutcomesAreUnknownWithoutRefresh() {
        for (LanceIndexJobResultCode code : new LanceIndexJobResultCode[]{
                LanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT, LanceIndexJobResultCode.NATIVE_NOT_SUPPORTED,
                LanceIndexJobResultCode.NATIVE_INDEX, LanceIndexJobResultCode.NATIVE_IO,
                LanceIndexJobResultCode.NATIVE_INTERNAL, LanceIndexJobResultCode.NO_TRUSTED_RESULT}) {
            for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
                LanceIndexJobResultCode.Classification classification =
                        LanceIndexJobResultCode.classify(type, code, true, true);
                Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, classification.getMutationState(),
                        "code=" + code + ", type=" + type);
                Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, classification.getRefreshState(),
                        "code=" + code + ", type=" + type);
                Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, classification.getCompletionReason(),
                        "code=" + code + ", type=" + type);
            }
        }
    }

    @Test
    public void nullResultCodeFallsBackToNoTrustedResult() {
        LanceIndexJobResultCode.Classification classification =
                LanceIndexJobResultCode.classify(LanceIndexJobMutationType.CREATE, null, false, false);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, classification.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, classification.getRefreshState());
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, classification.getCompletionReason());
    }

    @Test
    public void isPreInvocationCoversExactlyTheFourTrustedRejections() {
        int preInvocationCount = 0;
        for (LanceIndexJobResultCode code : LanceIndexJobResultCode.values()) {
            if (code.isPreInvocation()) {
                preInvocationCount++;
            }
        }
        Assertions.assertEquals(4, preInvocationCount);
        Assertions.assertEquals(13, LanceIndexJobResultCode.values().length);
    }

    /**
     * Independent restatement of the design table, mutation-state column.
     */
    private static LanceIndexJobMutationState expectedMutationState(
            LanceIndexJobMutationType type, LanceIndexJobResultCode code) {
        if (isPreInvocationSpec(code)) {
            return LanceIndexJobMutationState.NOT_COMMITTED;
        }
        switch (code) {
            case NATIVE_OK:
                return LanceIndexJobMutationState.COMMITTED;
            case NATIVE_COMMIT_CONFLICT:
                return LanceIndexJobMutationState.NOT_COMMITTED;
            case NATIVE_NOT_FOUND:
                return type == LanceIndexJobMutationType.DROP
                        ? LanceIndexJobMutationState.NOT_COMMITTED : LanceIndexJobMutationState.UNKNOWN;
            default:
                return LanceIndexJobMutationState.UNKNOWN;
        }
    }

    /**
     * Independent restatement of the design table, refresh-obligation column.
     */
    private static LanceIndexJobRefreshState expectedRefreshState(
            LanceIndexJobMutationType type, LanceIndexJobResultCode code, boolean externalMetadataAdvanced) {
        if (isPreInvocationSpec(code)) {
            return externalMetadataAdvanced
                    ? LanceIndexJobRefreshState.REQUIRED : LanceIndexJobRefreshState.NOT_REQUIRED;
        }
        switch (code) {
            case NATIVE_OK:
            case NATIVE_COMMIT_CONFLICT:
                return LanceIndexJobRefreshState.REQUIRED;
            case NATIVE_NOT_FOUND:
                return type == LanceIndexJobMutationType.DROP
                        ? LanceIndexJobRefreshState.REQUIRED : LanceIndexJobRefreshState.NOT_REQUIRED;
            default:
                return LanceIndexJobRefreshState.NOT_REQUIRED;
        }
    }

    /**
     * Independent restatement of the design table, completion-reason column.
     */
    private static LanceIndexJobCompletionReason expectedCompletionReason(
            LanceIndexJobMutationType type, LanceIndexJobResultCode code, boolean ifExists) {
        return type == LanceIndexJobMutationType.DROP && code == LanceIndexJobResultCode.NATIVE_NOT_FOUND && ifExists
                ? LanceIndexJobCompletionReason.IF_CONDITION_NOOP : LanceIndexJobCompletionReason.NONE;
    }

    private static boolean isPreInvocationSpec(LanceIndexJobResultCode code) {
        return code == LanceIndexJobResultCode.PRE_INVOCATION_STALE_ADMISSION
                || code == LanceIndexJobResultCode.PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT
                || code == LanceIndexJobResultCode.PRE_INVOCATION_CREDENTIAL_EXPIRED
                || code == LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED;
    }
}
