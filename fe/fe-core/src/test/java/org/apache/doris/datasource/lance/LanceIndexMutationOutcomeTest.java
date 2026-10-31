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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * Unit coverage for {@link LanceIndexMutationOutcome}: factory fields, the bounded sanitized
 * message contract, and the per-kind mapping onto the 5104+ error-code family asserted with
 * {@code getMysqlErrorCode()} (the AlterTableCommandLanceAdmissionTest assertion pattern).
 */
public class LanceIndexMutationOutcomeTest {

    @Test
    public void testSuccessFactoryFields() {
        LanceIndexMutationOutcome outcome = LanceIndexMutationOutcome.success();
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS, outcome.getKind());
        Assertions.assertEquals(LanceIndexMutationOutcome.LANCE_RESULT_OK, outcome.getLanceResultCode());
        Assertions.assertEquals(0, outcome.getLanceResultCode());
        Assertions.assertEquals("", outcome.getMessage());
    }

    @Test
    public void testConfirmedFailureFactoryFields() {
        LanceIndexMutationOutcome outcome =
                LanceIndexMutationOutcome.confirmedFailure(7, "index quota exceeded");
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertEquals(7, outcome.getLanceResultCode());
        Assertions.assertEquals("index quota exceeded", outcome.getMessage());
    }

    @Test
    public void testIndeterminateFactoryFields() {
        LanceIndexMutationOutcome outcome = LanceIndexMutationOutcome.indeterminate(3, "request timeout");
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
        Assertions.assertEquals(3, outcome.getLanceResultCode());
        Assertions.assertEquals("request timeout", outcome.getMessage());
    }

    @Test
    public void testCommittedRefreshIncompleteFactoryFields() {
        LanceIndexMutationOutcome outcome =
                LanceIndexMutationOutcome.committedRefreshIncomplete(0, "cache invalidation timed out");
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.COMMITTED_REFRESH_INCOMPLETE,
                outcome.getKind());
        Assertions.assertEquals(0, outcome.getLanceResultCode());
        Assertions.assertEquals("cache invalidation timed out", outcome.getMessage());
    }

    @Test
    public void testNullMessageIsNormalizedToEmpty() {
        // Factories accept a null diagnostic (a pre-dispatch classification may have none) but
        // never expose one: the carried message is always non-null bounded text.
        Assertions.assertEquals("",
                LanceIndexMutationOutcome.confirmedFailure(7, null).getMessage());
        Assertions.assertEquals("",
                LanceIndexMutationOutcome.indeterminate(3, null).getMessage());
        Assertions.assertEquals("",
                LanceIndexMutationOutcome.committedRefreshIncomplete(0, null).getMessage());
    }

    @Test
    public void testEachFailureKindMapsToItsOwnErrorCode() {
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED,
                LanceIndexMutationOutcome.confirmedFailure(7, "rejected")
                        .toUserException().getMysqlErrorCode());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_INDETERMINATE,
                LanceIndexMutationOutcome.indeterminate(3, "timeout")
                        .toUserException().getMysqlErrorCode());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_COMMITTED_REFRESH_INCOMPLETE,
                LanceIndexMutationOutcome.committedRefreshIncomplete(0, "refresh")
                        .toUserException().getMysqlErrorCode());
    }

    @Test
    public void testSuccessOutcomeHasNoUserException() {
        // Success returns normally to the client; asking it for an error text is a wiring bug.
        Assertions.assertThrows(IllegalStateException.class,
                () -> LanceIndexMutationOutcome.success().toUserException());
    }

    @Test
    public void testErrorCodesNumberedFrom5104() {
        // 5103 stays retired (ErrorCode.java comment pins it); the mutation family starts at 5104.
        Assertions.assertEquals(5104, ErrorCode.ERR_LANCE_INDEX_MUTATION_INDETERMINATE.getCode());
        Assertions.assertEquals(5105,
                ErrorCode.ERR_LANCE_INDEX_MUTATION_COMMITTED_REFRESH_INCOMPLETE.getCode());
        Assertions.assertEquals(5106, ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY.getCode());
        Assertions.assertEquals(5107, ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED.getCode());
        Assertions.assertEquals(5108, ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED.getCode());
    }

    @Test
    public void testIndeterminateMessageStatesUnknownCommitAndPointsToAuthoritativeMetadata() {
        AnalysisException exception =
                LanceIndexMutationOutcome.indeterminate(3, "request timeout").toUserException();
        String message = exception.getDetailMessage();
        // Design section 6.2: state that the mutation may or may not have committed, and direct
        // the user to the authoritative metadata instead of a blind retry.
        Assertions.assertTrue(message.contains("may or may not have committed"), message);
        Assertions.assertTrue(message.contains("SHOW INDEX FROM"), message);
        Assertions.assertTrue(message.contains("lance_index_entries()"), message);
        // It must never read as proof of non-commit.
        Assertions.assertFalse(message.contains("not committed"), message);
        // The typed code and sanitized detail ride along for diagnostics.
        Assertions.assertTrue(message.contains("lance result code 3"), message);
        Assertions.assertTrue(message.contains("request timeout"), message);
    }

    @Test
    public void testCommittedRefreshIncompleteMessageKeepsTheCommitFact() {
        AnalysisException exception = LanceIndexMutationOutcome
                .committedRefreshIncomplete(0, "metadata cache eviction failed").toUserException();
        String message = exception.getDetailMessage();
        // Design section 6.4: the commit is a fact and only the refresh is missing; a refresh
        // failure must never be rewritten as a build failure.
        Assertions.assertTrue(message.contains("committed successfully"), message);
        Assertions.assertTrue(message.contains("not a build failure"), message);
        Assertions.assertTrue(message.contains("SHOW INDEX FROM"), message);
        Assertions.assertFalse(message.contains("nothing was committed"), message);
    }

    @Test
    public void testConfirmedFailureMessageStatesTheNonCommit() {
        AnalysisException exception =
                LanceIndexMutationOutcome.confirmedFailure(7, "not implemented").toUserException();
        String message = exception.getDetailMessage();
        Assertions.assertTrue(message.contains("nothing was committed"), message);
        Assertions.assertTrue(message.contains("lance result code 7"), message);
        Assertions.assertTrue(message.contains("not implemented"), message);
    }

    @Test
    public void testMessageTruncatedToBoundOnEveryFactory() {
        String oversized = repeat('x', 64 * 1024);
        LanceIndexMutationOutcome[] outcomes = {
            LanceIndexMutationOutcome.confirmedFailure(7, oversized),
            LanceIndexMutationOutcome.indeterminate(3, oversized),
            LanceIndexMutationOutcome.committedRefreshIncomplete(0, oversized)};
        for (LanceIndexMutationOutcome outcome : outcomes) {
            Assertions.assertEquals(LanceIndexMutationOutcome.MAX_MESSAGE_BYTES,
                    outcome.getMessage().getBytes(StandardCharsets.UTF_8).length);
            // Truncation keeps a prefix, never a re-encoding of the payload.
            Assertions.assertTrue(oversized.startsWith(outcome.getMessage()));
        }
    }

    @Test
    public void testMessageTruncationRespectsUtf8CodePointBoundary() {
        String emoji = new String(Character.toChars(0x1F600));
        // 1022 single-byte chars + one 4-byte code point exceeds the 1024-byte bound by two
        // bytes: the whole code point is dropped, never split into an orphan surrogate.
        String doesNotFit = repeat('a', 1022) + emoji;
        LanceIndexMutationOutcome outcome = LanceIndexMutationOutcome.indeterminate(3, doesNotFit);
        Assertions.assertEquals(repeat('a', 1022), outcome.getMessage());
        // A code point that fits exactly is kept whole.
        String fitsExactly = repeat('a', 1020) + emoji;
        Assertions.assertEquals(fitsExactly,
                LanceIndexMutationOutcome.indeterminate(3, fitsExactly).getMessage());
        Assertions.assertEquals(LanceIndexMutationOutcome.MAX_MESSAGE_BYTES,
                LanceIndexMutationOutcome.indeterminate(3, fitsExactly).getMessage()
                        .getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void testControlCharactersStrippedFromMessage() {
        String withControls = "line1\nline2\tmark\u0001end";
        LanceIndexMutationOutcome outcome = LanceIndexMutationOutcome.indeterminate(3, withControls);
        Assertions.assertEquals("line1line2markend", outcome.getMessage());
    }

    @Test
    public void testUserExceptionTextStaysBounded() {
        // The client-facing text is the template plus the bounded detail, so an oversized
        // provider payload cannot blow up the error message either.
        LanceIndexMutationOutcome outcome =
                LanceIndexMutationOutcome.indeterminate(3, repeat('y', 64 * 1024));
        String message = outcome.toUserException().getDetailMessage();
        Assertions.assertTrue(message.getBytes(StandardCharsets.UTF_8).length
                <= LanceIndexMutationOutcome.MAX_MESSAGE_BYTES + 512, message);
        Assertions.assertTrue(message.contains("lance result code 3"), message);
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; ++i) {
            builder.append(value);
        }
        return builder.toString();
    }
}
