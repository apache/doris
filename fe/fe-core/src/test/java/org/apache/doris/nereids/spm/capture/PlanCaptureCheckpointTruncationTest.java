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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tenth review round: the durable checkpoint must never step over retries it cannot
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
                    50L, 999L, "cursor-z", "qid-z");
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

            Map<String, CapturedQuery> persistedQueue =
                    PlanCaptureManager.decodeRetryQueue(params.get("retryQueue"));
            Assertions.assertTrue(persistedQueue.size() <= 64,
                    "the persisted queue stays bounded: " + persistedQueue.size());
        } finally {
            manager.resetForTest();
        }
    }

    @Test
    public void testCompleteRetryStateAdvancesTheCursor() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            manager.seedCheckpointStateForTest(100L, 200L, -1L, "cursor-a", "qid-a", 5,
                    50L, 999L, "cursor-z", "qid-z");
            Map<String, String> params = persist(manager);

            Assertions.assertEquals("50", params.get("lastScan"),
                    "with a fully persisted retry state the watermark advances normally");
            Assertions.assertEquals("999", params.get("cursorQueryTime"));
            Assertions.assertEquals("cursor-z", params.get("cursorTime"));
            Assertions.assertEquals("qid-z", params.get("cursorQueryId"));
            Assertions.assertEquals("100", params.get("pendingStart"));
            Assertions.assertEquals("200", params.get("pendingEnd"));
            Assertions.assertEquals(5,
                    PlanCaptureManager.decodeRetryQueue(params.get("retryQueue")).size());
        } finally {
            manager.resetForTest();
        }
    }
}
