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

import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Full capture-cycle checkpoint handoff tests.
 *
 * - A FAILED first checkpoint read must abort the WHOLE cycle (scan + persist): deriving
 *   a fresh window and UPSERTing it would overwrite the previous leader's pending cursor,
 *   and even a failed write would leave nonzero local state that marks the checkpoint
 *   "loaded" without ever retrying the promised read (the pending tail then becomes
 *   unreachable).
 * - A truncated window must resume from the FULL cursor (including its tail) and persist
 *   it, so a later leader continues exactly after the last consumed row.
 */
public class PlanCaptureCycleHandoffTest {

    /** A scanner stub that records the resume state it was called with. */
    private static final class RecordingScanner extends AuditLogScanner {
        private final AtomicInteger calls = new AtomicInteger();
        private final List<long[]> windows = new ArrayList<>();
        private final List<Object[]> cursors = new ArrayList<>();
        private final List<String> tails = new ArrayList<>();
        private final List<CapturedQuery> candidates;
        private final boolean exhausted;
        private final long returnCursorQueryTime;
        private final String returnCursorTime;
        private final String returnCursorQueryId;
        private final String returnCursorTail;

        RecordingScanner(List<CapturedQuery> candidates, boolean exhausted,
                long returnCursorQueryTime, String returnCursorTime,
                String returnCursorQueryId, String returnCursorTail) {
            this.candidates = candidates;
            this.exhausted = exhausted;
            this.returnCursorQueryTime = returnCursorQueryTime;
            this.returnCursorTime = returnCursorTime;
            this.returnCursorQueryId = returnCursorQueryId;
            this.returnCursorTail = returnCursorTail;
        }

        @Override
        public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
                long cursorQueryTime, String cursorTime, String cursorQueryId,
                String cursorTail) {
            calls.incrementAndGet();
            windows.add(new long[] {startTimeMs, endTimeMs});
            cursors.add(new Object[] {cursorQueryTime, cursorTime, cursorQueryId});
            tails.add(cursorTail);
            return new ScanBatch(candidates, exhausted, returnCursorQueryTime,
                    returnCursorTime, returnCursorQueryId, returnCursorTail);
        }
    }

    @Test
    public void testCycleAbortsUntilCheckpointReadSucceeds() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicInteger reads = new AtomicInteger();
            String tail = "[\"10.0.0.1\",\"h1\",\"100\",\"10\",\"m1\"]";
            manager.setCheckpointReaderForTest(() -> {
                if (reads.incrementAndGet() == 1) {
                    throw new RuntimeException("internal table not ready");
                }
                // the previous leader's TRUNCATED window + full cursor
                return List.of(new ResultRow(List.of(
                        "123456", "100", "200", "7", "2026-01-01 00:00:00", "qid-cursor",
                        "{}", "{}", tail)));
            });

            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<String> statements = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> statements.add(sql));

            // cycle 1: the checkpoint read FAILS. Deriving a fresh window / persisting it
            // here would overwrite the previous leader's pending cursor (or, if the write
            // also fails, leave local progress that marks the checkpoint loaded without
            // ever retrying the promised read) - NOTHING may be scanned or written.
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "a failed checkpoint read must skip the scan");
            Assertions.assertEquals(0, statements.size(),
                    "a failed checkpoint read must skip the persist");
            Assertions.assertEquals(0L, manager.checkpointFieldsForTest()[0],
                    "no local progress may be derived while the checkpoint is unconfirmed");
            Assertions.assertFalse(manager.isCheckpointLoadedForTest(),
                    "the failed read must stay retryable");

            // cycle 2: the read succeeds. The PREVIOUS LEADER's pending window is scanned
            // FROM its cursor (tail included), then persisted.
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(2, reads.get(),
                    "the cycle must RETRY the promised read instead of skipping it");
            Assertions.assertEquals(1, scanner.calls.get(), "the resumed cycle scans");
            Assertions.assertArrayEquals(new long[] {100L, 200L}, scanner.windows.get(0),
                    "the previous leader's pending window bounds are reused verbatim");
            Assertions.assertEquals(7L, scanner.cursors.get(0)[0],
                    "the previous leader's cursor is resumed");
            Assertions.assertEquals("2026-01-01 00:00:00", scanner.cursors.get(0)[1]);
            Assertions.assertEquals("qid-cursor", scanner.cursors.get(0)[2]);
            Assertions.assertEquals(tail, scanner.tails.get(0),
                    "the previous leader's cursor tail is resumed as well");
            Assertions.assertEquals(1, statements.size(),
                    "the exhausted window persists its advanced state");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(200L, fields[0],
                    "an exhausted window advances the watermark to the consumed end");
            Assertions.assertEquals(AuditLogScanner.CURSOR_ABSENT, fields[3],
                    "the resume cursor is dropped once the window is exhausted");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * A TRUNCATED batch (limit reached) must persist the FULL cursor of its last raw row
     * - including the tail - and the next cycle must resume with exactly that tail.
     */
    @Test
    public void testTruncatedCycleKeepsFullCursorInCheckpoint() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String tail = "[\"10.0.0.2\",\"h2\",\"200\",\"20\",\"m2\"]";
            RecordingScanner scanner = new RecordingScanner(List.of(), false,
                    42L, "2026-01-02 00:00:00", "qid-t", tail);
            manager.setScannerForTest(scanner);
            // the internal table exists but has no row yet (a successful empty read)
            manager.setCheckpointReaderForTest(List::of);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) ->
                    persisted.add(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(42L, fields[3],
                    "a truncated window keeps the cursor of its last consumed row");
            Assertions.assertEquals("2026-01-02 00:00:00", fields[4]);
            Assertions.assertEquals("qid-t", fields[5]);
            Assertions.assertEquals(tail, fields[6],
                    "the FULL cursor tail must be kept in memory");
            Assertions.assertEquals(2, persisted.size(),
                    "the initial reservation AND the consumed page both persist");
            Assertions.assertEquals(tail, persisted.get(1).get("cursorTail"),
                    "the tail must be persisted for the next leader");

            // the next cycle resumes inside the SAME pending window with the SAME tail
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(2, scanner.calls.get());
            Assertions.assertEquals(tail, scanner.tails.get(1),
                    "the resumed scan must use the persisted tail: " + scanner.tails);
            Assertions.assertArrayEquals(scanner.windows.get(0), scanner.windows.get(1),
                    "a truncated window keeps both bounds across cycles");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * The internal statements of the capture daemon must carry a SHORT explicit timeout:
     * the default StatisticsUtil overloads inherit the analyze timeout (43,200 s), so a
     * stalled audit read / checkpoint read / checkpoint write could hold the single
     * capture cycle for hours and delay every later capture and retry. A timeout is
     * handled like any other internal-I/O failure: the cycle aborts (read) or completes
     * best-effort (write) and the NEXT cycle succeeds.
     */
    @Test
    public void testInternalStatementTimeoutsRecoverNextCycle() {
        Assertions.assertTrue(AuditLogScanner.AUDIT_SCAN_TIMEOUT_SECONDS > 0
                        && AuditLogScanner.AUDIT_SCAN_TIMEOUT_SECONDS <= 60,
                "the audit read must use a SHORT explicit timeout, got "
                        + AuditLogScanner.AUDIT_SCAN_TIMEOUT_SECONDS);
        Assertions.assertTrue(PlanCaptureManager.CHECKPOINT_IO_TIMEOUT_SECONDS > 0
                        && PlanCaptureManager.CHECKPOINT_IO_TIMEOUT_SECONDS <= 60,
                "the checkpoint read / write must use a SHORT explicit timeout, got "
                        + PlanCaptureManager.CHECKPOINT_IO_TIMEOUT_SECONDS);

        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicInteger reads = new AtomicInteger();
            String tail = "[\"10.0.0.1\",\"h1\",\"100\",\"10\",\"m1\"]";
            manager.setCheckpointReaderForTest(() -> {
                if (reads.incrementAndGet() == 1) {
                    throw new RuntimeException("internal statement timed out after 10s");
                }
                return List.of(new ResultRow(List.of("123456", "100", "200", "7",
                        "2026-01-01 00:00:00", "qid-cursor", "{}", "{}", tail)));
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicInteger writes = new AtomicInteger();
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> {
                if (writes.incrementAndGet() == 1) {
                    throw new RuntimeException("internal statement timed out after 10s");
                }
                persisted.add(new HashMap<>(params));
            });

            // cycle 1: the READ times out -> nothing is scanned or written, the cycle
            // stays retryable
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "a timed-out checkpoint read must skip the scan");
            Assertions.assertEquals(0, writes.get(),
                    "a timed-out checkpoint read must skip the persist");
            Assertions.assertFalse(manager.isCheckpointLoadedForTest(),
                    "the timed-out read must stay retryable");

            // cycle 2: the read succeeds, the WRITE times out -> the cycle still finishes
            // (the checkpoint is best effort) and the scan resumed the pending window
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertEquals(1, writes.get());
            Assertions.assertArrayEquals(new long[] {100L, 200L}, scanner.windows.get(0),
                    "the pending window is reused after the read recovered");
            Assertions.assertEquals(7L, scanner.cursors.get(0)[0]);
            Assertions.assertEquals("2026-01-01 00:00:00", scanner.cursors.get(0)[1]);

            // cycle 3: both internal statements succeed -> the checkpoint is persisted
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, persisted.size(),
                    "the first cycle whose write succeeds persists the checkpoint");
            Assertions.assertEquals(String.valueOf(manager.checkpointFieldsForTest()[0]),
                    persisted.get(0).get("lastScan"),
                    "the persisted watermark must match the advanced local state");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * The FIRST window after a successful-but-EMPTY read exists only in memory: if the
     * first checkpoint write fails and the process dies / hands over before any later
     * write succeeds, the takeover knows nothing about the window and derives a NEW one -
     * permanently skipping this page's unconsumed tail (the later overlap only reaches
     * rows younger than the new watermark). The cycle must record the window it is about
     * to consume BEFORE consuming it, and must not scan at all while even that write
     * fails (nothing durable could resume what it consumed).
     */
    @Test
    public void testFirstWindowIsReservedBeforeScan() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            manager.setCheckpointReaderForTest(List::of); // successful EMPTY read
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicInteger writes = new AtomicInteger();
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> {
                if (writes.incrementAndGet() == 1) {
                    throw new RuntimeException("internal statement timed out after 10s");
                }
                persisted.add(new HashMap<>(params));
            });

            // cycle 1: the RESERVATION fails -> the window is NOT consumed, so a
            // takeover still finds every row of it in the audit log
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "a window that could not be made durable must not be scanned");
            Assertions.assertEquals(1, writes.get(),
                    "the failed reservation is the only attempted write");
            Assertions.assertFalse(manager.isDurableCheckpointObservedForTest(),
                    "the failed reservation leaves no durable state");

            // cycle 2: the write succeeds -> the window is durable BEFORE the scan ...
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get(), "the retried cycle scans");
            Assertions.assertFalse(persisted.isEmpty(), "the reservation must persist");
            // ... describing the window the scan actually consumed, so a takeover in
            // between resumes from THAT window instead of deriving a fresh one
            Assertions.assertEquals(String.valueOf(scanner.windows.get(0)[0]),
                    persisted.get(0).get("pendingStart"),
                    "the reserved row must point at the window's start: " + persisted);
            Assertions.assertEquals(String.valueOf(scanner.windows.get(0)[1]),
                    persisted.get(0).get("pendingEnd"),
                    "the reserved row must point at the window's end: " + persisted);
            // the exhausted window then overwrites the reservation with its advanced state
            Assertions.assertEquals(2, persisted.size(),
                    "the exhausted cycle persists its advanced watermark: " + persisted);
            Assertions.assertTrue(manager.isDurableCheckpointObservedForTest(),
                    "once a row is durable the reservation is skipped");
        } finally {
            manager.resetForTest();
        }
    }
}
