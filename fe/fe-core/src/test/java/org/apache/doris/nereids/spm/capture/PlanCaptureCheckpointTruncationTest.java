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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The durable checkpoint must never step over retries it cannot
 * carry.
 *
 * One default audit page holds up to 500 rows and a transient catalog / store outage
 * can queue all of them, but the checkpoint persists only the most recent
 * MAX_PERSISTED_RETRIES (64) candidates while it used to persist the cursor AFTER the
 * whole page: after a restart / leader handoff the omitted older candidates were
 * neither in the retry queue nor reachable by keyset pagination (and could age outside
 * the five-minute overlap). The writer now falls back to the PRE-PAGE state whenever the
 * retry state is truncated, so the next leader re-scans the page.
 */
public class PlanCaptureCheckpointTruncationTest {

    private static Map<String, String> persist(PlanCaptureManager manager) {
        Map<String, String> captured = new HashMap<>();
        manager.setCheckpointWriterForTest((sql, params) -> captured.putAll(params));
        manager.persistCheckpointForTest();
        return captured;
    }

    @Test
    public void testTruncatedRetryStatePersistsThePrePageCursor() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // > MAX_PERSISTED_RETRIES (64) failures in this page: the queue holds 70
            // entries, the checkpoint can persist only the last 64
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-a", "qid-a", 70,
                    50L, 999L, "cursor-z", "qid-z", "tail-a", "tail-z");
            Map<String, String> params = persist(manager);

            Assertions.assertEquals("49", params.get("lastScan"),
                    "the durable watermark must stay BEFORE the page (a truncation must not"
                            + " advance it past omitted retries)");
            Assertions.assertEquals("100", params.get("pendingStart"),
                    "the scanned window must stay pending so the page is re-scanned");
            Assertions.assertEquals("200", params.get("pendingEnd"));
            Assertions.assertEquals("-1", params.get("cursorQueryTime"),
                    "the durable cursor must be the PRE-PAGE cursor");
            Assertions.assertEquals("cursor-a", params.get("cursorTime"));
            Assertions.assertEquals("qid-a", params.get("cursorQueryId"));
            Assertions.assertEquals("tail-a", params.get("cursorTail"),
                    "the durable cursor TAIL must fall back to the pre-page tail as well");

            Map<String, CapturedQuery> persistedQueue =
                    PlanCaptureManager.decodeRetryQueue(params.get("retryQueue"));
            Assertions.assertTrue(persistedQueue.size() <= 64,
                    "the persisted queue stays bounded: " + persistedQueue.size());
        } finally {
            manager.resetForTest();
        }
    }

    @Test
    public void testCompleteRetryStateRewindsBeforeQueuedRetries() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-a", "qid-a", 5,
                    50L, 999L, "cursor-z", "qid-z", "tail-a", "tail-z");
            Map<String, String> params = persist(manager);

            // #7: the JSON budget is NOT the trigger - the restore assigns the
            // checkpoint cursor as the anchor of EVERY queued entry, so the durable cursor
            // must sit before the oldest queued row even when all of them fit the payload
            Assertions.assertEquals("49", params.get("lastScan"),
                    "a queued retry must rewind the durable watermark before its row");
            Assertions.assertEquals("-1", params.get("cursorQueryTime"));
            Assertions.assertEquals("cursor-a", params.get("cursorTime"));
            Assertions.assertEquals("qid-a", params.get("cursorQueryId"));
            Assertions.assertEquals("tail-a", params.get("cursorTail"),
                    "the durable cursor tail must be the pre-page tail of the queued entry");
            Assertions.assertEquals("100", params.get("pendingStart"));
            Assertions.assertEquals("200", params.get("pendingEnd"));
            Assertions.assertEquals(5,
                    PlanCaptureManager.decodeRetryQueue(params.get("retryQueue")).size());
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * #7: a 64-to-65 transition across TWO handoffs. A checkpoint whose retry
     * state FITS the budget (64 entries) used to persist the LIVE cursor - past all
     * queued rows. The restored leader takes that cursor as the anchor of every restored
     * entry, so when its 65th failure truncates the JSON, the rewind lands AFTER the
     * dropped oldest row: the next leader can neither reload it from the queue nor
     * re-scan it (keyset pagination is past it), losing its remaining attempt.
     */
    @Test
    public void testCheckpointStaysBeforeQueuedRowsAcrossTwoHandoffs() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // leader A: 64 queued retries - exactly the JSON budget, NOT truncated
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-page", "qid-page",
                    64, 50L, 999L, "cursor-live", "qid-live", "tail-page", "tail-live");
            Map<String, String> params = persist(manager);
            Assertions.assertEquals("49", params.get("lastScan"));
            Assertions.assertEquals("cursor-page", params.get("cursorTime"));
            Assertions.assertEquals("-1", params.get("cursorQueryTime"));
            Assertions.assertEquals("tail-page", params.get("cursorTail"));
            Assertions.assertEquals(64,
                    PlanCaptureManager.decodeRetryQueue(params.get("retryQueue")).size());

            // HANDOFF 1: leader B restores that checkpoint (its cursor IS the anchor of
            // every restored retry)
            manager.resetForTest();
            manager.applyCheckpointRow(new ResultRow(List.of("49", "100", "200", "-1",
                    "cursor-page", "qid-page", params.get("failedAttempts"),
                    params.get("retryQueue"), "tail-page")));
            Assertions.assertTrue(manager.isQueuedForTest("seed-failed-0"),
                    "the oldest retry survives the handoff");

            // leader B queues a 65th failure -> the persisted JSON truncates and drops
            // the OLDEST retry
            manager.handleCandidateForTest(failingCandidate(999));
            Assertions.assertTrue(manager.isQueuedForTest("qid-999"));

            Map<String, String> second = persist(manager);
            Assertions.assertEquals("49", second.get("lastScan"),
                    "the truncating checkpoint must rewind to the anchor the restored"
                            + " retries carry, which sits BEFORE their rows");
            Assertions.assertEquals("-1", second.get("cursorQueryTime"));
            Assertions.assertEquals("cursor-page", second.get("cursorTime"));
            Assertions.assertEquals("qid-page", second.get("cursorQueryId"));
            Assertions.assertEquals("tail-page", second.get("cursorTail"));
            Map<String, CapturedQuery> persistedQueue =
                    PlanCaptureManager.decodeRetryQueue(second.get("retryQueue"));
            Assertions.assertFalse(persistedQueue.containsKey("seed-failed-0"),
                    "the oldest retry is beyond the JSON budget: " + persistedQueue.keySet());
            Assertions.assertTrue(persistedQueue.size() <= 64);
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * Two cycles plus handoff: page 1 queues 70 failures (its checkpoint rewinds before
     * page 1); a LATER cycle whose page already starts after page 1 must NOT move the
     * durable cursor forward - the oldest entries are omitted from the persisted JSON, so
     * on handoff they could be reached neither from the queue nor by re-scanning (the old
     * fallback re-saved the CURRENT page start, already past them).
     */
    @Test
    public void testLaterPageKeepsDurableCursorBeforeOmittedRetries() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // cycle 1: 70 failures queued from page 1 (pre-page cursor "cursor-page1")
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-page1", "qid-page1",
                    70, 50L, 999L, "cursor-page2", "qid-page2", "tail-page1", "tail-page2");
            // cycle 2: the SAME queue still exceeds the budget, but the current page has
            // already advanced past page 1 (seed with 0 new entries: the queue and its
            // first-wins anchors stay, only the page state moves)
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-page2", "qid-page2",
                    0, 90L, 999L, "cursor-page3", "qid-page3", "tail-page2", "tail-page3");

            Map<String, String> params = persist(manager);
            Assertions.assertEquals("49", params.get("lastScan"),
                    "the durable watermark must stay before PAGE 1 (the oldest omitted"
                            + " retry), not move to the later page");
            Assertions.assertEquals("-1", params.get("cursorQueryTime"));
            Assertions.assertEquals("cursor-page1", params.get("cursorTime"),
                    "the durable cursor must be the OLDEST retry's page anchor: "
                            + params.get("cursorTime"));
            Assertions.assertEquals("qid-page1", params.get("cursorQueryId"));
            Assertions.assertEquals("tail-page1", params.get("cursorTail"));

            // HANDOFF: the next process restores the persisted row
            manager.resetForTest();
            manager.applyCheckpointRow(new ResultRow(List.of("49", "100", "200", "-1",
                    "cursor-page1", "qid-page1", params.get("failedAttempts"),
                    params.get("retryQueue"), "tail-page1")));
            Object[] restored = manager.checkpointFieldsForTest();
            Assertions.assertEquals("cursor-page1", restored[4],
                    "the handed-off cursor must still sit before the omitted retries");
            Assertions.assertEquals("tail-page1", restored[6]);
        } finally {
            manager.resetForTest();
        }
    }

    /**\n     * The retry replay must NOT reorder the queue: the old remove-before-retry moved a
     * replayed page-1 failure BEHIND the entries a later page queued, so the "first
     * entry = earliest anchor" assumption broke - persistCheckpoint then saved the LATER
     * page's cursor while encodeRetryQueue dropped the older page-1 entries whose rows
     * sat before it (unrecoverable on handoff).
     */
    @Test
    public void testReplayKeepsFirstSeenOrderForAnchors() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // cycle 1: page 1 state; 70 failures queue with PAGE 1 anchors
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-page1", "qid-page1",
                    0, 50L, 999L, "cursor-z1", "qid-z1", "tail-page1", "tail-z1");
            List<String> page1Keys = new ArrayList<>();
            for (int i = 0; i < 70; i++) {
                CapturedQuery candidate = failingCandidate(i);
                page1Keys.add(PlanCaptureManager.retryKeyOf(candidate));
                manager.handleCandidateForTest(candidate);
            }
            Assertions.assertTrue(manager.isQueuedForTest(page1Keys.get(0)));

            // cycle 2: a LATER page queues 70 NEW failures (its state is past page 1)
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-page2", "qid-page2",
                    0, 90L, 999L, "cursor-z2", "qid-z2", "tail-page2", "tail-z2");
            Set<String> page2Keys = new HashSet<>();
            for (int i = 70; i < 140; i++) {
                CapturedQuery candidate = failingCandidate(i);
                page2Keys.add(PlanCaptureManager.retryKeyOf(candidate));
                manager.handleCandidateForTest(candidate);
            }

            // the page did not contain the page-1 rows: they are replayed now (attempt 2)
            manager.replayQueuedFailuresForTest(page2Keys);
            Assertions.assertEquals(2, manager.failedAttemptsForTest(page1Keys.get(0)),
                    "a replayed failure must still get its bounded attempt");
            Assertions.assertTrue(manager.isQueuedForTest(page1Keys.get(0)));

            Map<String, String> params = persist(manager);
            Assertions.assertEquals("cursor-page1", params.get("cursorTime"),
                    "the durable cursor must stay before PAGE 1 (the oldest queued retry),"
                            + " not move to the later page: " + params.get("cursorTime"));
            Assertions.assertEquals("tail-page1", params.get("cursorTail"));
            Assertions.assertEquals("49", params.get("lastScan"));
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * A failure cap must never drop entries before their bounded retries are spent: a
     * page-1 failure evicted here has its audit row behind the LIVE cursor (and may be
     * older than the overlap window), so the running leader could never retry it even
     * without handoff / checkpoint truncation.
     */
    @Test
    public void testFailuresAreNotEvictedBeforeTheirRetriesAreSpent() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // more than the old 10k cap, e.g. two 6000-row failing pages
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-a", "qid-a",
                    10_050, 50L, 999L, "cursor-z", "qid-z", "tail-a", "tail-z");
            manager.handleCandidateForTest(failingCandidate(1));
            Assertions.assertTrue(manager.isQueuedForTest("seed-failed-0"),
                    "the oldest failure must not be evicted before its bounded retries"
                            + " are spent (its row is unreachable from the live cursor)");
            Assertions.assertEquals(1, manager.failedAttemptsForTest("seed-failed-0"));
        } finally {
            manager.resetForTest();
        }
    }

    /** One candidate that FAILS to capture (retryable) in the unit-test environment. */
    private static CapturedQuery failingCandidate(int i) {
        return new CapturedQuery(
                "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = " + i,
                5000, 100000 + i, 0, "digest-" + i, "hash", "db", "internal",
                "qid-" + i);
    }
}
