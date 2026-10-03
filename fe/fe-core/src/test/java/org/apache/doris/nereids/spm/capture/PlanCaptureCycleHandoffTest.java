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
import java.util.concurrent.atomic.AtomicReference;

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
        /** Thresholds of the filter each page was scanned with, in call order. */
        private final List<long[]> thresholds = new ArrayList<>();
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
                PlanCaptureFilter filter, long cursorQueryTime, String cursorTime,
                String cursorQueryId, String cursorTail) {
            calls.incrementAndGet();
            windows.add(new long[] {startTimeMs, endTimeMs});
            cursors.add(new Object[] {cursorQueryTime, cursorTime, cursorQueryId});
            tails.add(cursorTail);
            thresholds.add(new long[] {filter.getMinQueryTimeMs(), filter.getMinScanRows()});
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
     * The reader seam of a checkpoint store: a row is readable only AFTER the writer
     * made it visible (see {@link #checkpointRow}), so the reservation confirmation
     * exercises the same read path the load does.
     */
    private static ResultRow checkpointRow(Map<String, String> params) {
        return new ResultRow(List.of(
                params.get("lastScan"), params.get("pendingStart"), params.get("pendingEnd"),
                params.get("cursorQueryTime"),
                params.getOrDefault("cursorTime", ""),
                params.getOrDefault("cursorQueryId", ""),
                params.getOrDefault("failedAttempts", "{}"),
                params.getOrDefault("retryQueue", "{}"),
                params.getOrDefault("cursorTail", ""),
                params.getOrDefault("minQueryTimeMs", "-1"),
                params.getOrDefault("minScanRows", "-1"),
                params.getOrDefault("includePattern", ""),
                params.getOrDefault("excludePattern", "")));
    }

    /**
     * A TRUNCATED window is drained page by page WITHIN one cycle (a single page per
     * wakeup would make the backlog grow by one interval per page), and every consumed
     * page is persisted with the FULL cursor of its last raw row - including the tail - so
     * the next cycle resumes with exactly that tail. The cycle stops at the page budget
     * and asks for a PROMPT resume instead of waiting another full interval.
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
            // the internal table exists but has no row yet (a successful empty read); the
            // writer makes every persisted row READABLE, so the reservation confirmation
            // (persistCheckpointAndConfirm) sees it
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> {
                persisted.add(new HashMap<>(params));
                visible.set(new HashMap<>(params));
            });

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(PlanCaptureManager.maxPagesPerCycleForTest(), scanner.calls.get(),
                    "one cycle drains up to the page budget of the same window");
            Assertions.assertEquals(42L, fields[3],
                    "a truncated window keeps the cursor of its last consumed row");
            Assertions.assertEquals("2026-01-02 00:00:00", fields[4]);
            Assertions.assertEquals("qid-t", fields[5]);
            Assertions.assertEquals(tail, fields[6],
                    "the FULL cursor tail must be kept in memory");
            Assertions.assertEquals(2 + PlanCaptureManager.maxPagesPerCycleForTest(), persisted.size(),
                    "the initial reservation, every drained page AND the final state persist");
            Assertions.assertEquals(tail, persisted.get(persisted.size() - 1).get("cursorTail"),
                    "the tail must be persisted for the next leader");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "a window cut off by the page budget must not wait a full interval");

            // the next cycle resumes inside the SAME pending window with the SAME tail
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(2 * PlanCaptureManager.maxPagesPerCycleForTest(),
                    scanner.calls.get());
            Assertions.assertEquals(tail,
                    scanner.tails.get(PlanCaptureManager.maxPagesPerCycleForTest()),
                    "the resumed scan must use the persisted tail: " + scanner.tails);
            Assertions.assertArrayEquals(scanner.windows.get(0),
                    scanner.windows.get(PlanCaptureManager.maxPagesPerCycleForTest()),
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
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get()))); // successful EMPTY read
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
                // the write only becomes readable once it succeeded: the reservation is
                // consumed after the visibility confirmation (persistCheckpointAndConfirm)
                visible.set(new HashMap<>(params));
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

    /**
     * A reservation whose internal INSERT returned OK with transaction status COMMITTED
     * (publication timed out) is NOT readable yet. Consuming the window anyway relies on
     * a reservation no takeover can read: a leadership change before publication makes
     * the next FE derive a LATER window - the overlap only reaches younger rows - and
     * permanently skips this page's unconsumed tail. The cycle must wait for a READABLE
     * row (bounded) and, when it never becomes visible, skip without consuming.
     */
    @Test
    public void testReservationRequiresAReadableRow() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // the write reports OK but the store never publishes: reader stays empty
            manager.setCheckpointReaderForTest(List::of);
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicInteger writes = new AtomicInteger();
            manager.setCheckpointWriterForTest((sql, params) -> writes.incrementAndGet());

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, writes.get(), "the reservation attempts the write");
            Assertions.assertEquals(0, scanner.calls.get(),
                    "an unreadable reservation must not consume the window");
            Assertions.assertFalse(manager.isDurableCheckpointObservedForTest(),
                    "the unconfirmed reservation must stay retryable");

            // the publication lands: the next cycle re-persists (idempotent UPSERT) and
            // only then consumes the window
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> {
                writes.incrementAndGet();
                visible.set(new HashMap<>(params));
            });
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get(),
                    "the confirmed reservation lets the cycle consume the window");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-22 #5: a NON-EMPTY read is not proof the reservation became visible - the
     * visible row may be the OLD master's reservation (published after its demotion).
     * Confirming "some row exists" would consume this window, and the final UPSERT would
     * replace the old master's still-unconsumed pending window. Only a row carrying OUR
     * pending bounds may confirm the reservation.
     */
    @Test
    public void testReservationMustMatchOurOwnWindow() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // the successful-but-empty read of the first cycle, then the store keeps
            // showing the OLD master's reservation (a different window)
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> reads.getAndIncrement() == 0
                    ? List.of()
                    : List.of(new ResultRow(List.of(
                            "123456", "500", "600", "7", "2026-01-01 00:00:00", "qid-old",
                            "{}", "{}", ""))));
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "a foreign (old master's) row must not confirm OUR reservation");
            Assertions.assertFalse(manager.isDurableCheckpointObservedForTest(),
                    "the reservation must stay retryable");

            // the store publishes OUR reservation: the next cycle confirms and consumes
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get(),
                    "the same-window row confirms the reservation");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-25 #5: the FINAL progress UPSERT is fenced by leadership. A demoted FE's local
     * cursor / retry queue is OBSOLETE - the new master may have advanced or REWOUND the
     * durable checkpoint (it can queue a retry for a late audit row the old cursor had not
     * reached yet) - and a forwarded UPSERT would replace that queue and cursor with ours.
     * The row behind the revived cursor is then neither replayed from the queue nor
     * reachable by keyset pagination, so it is never retried.
     */
    @Test
    public void testFinalCheckpointWriteIsFencedAfterADemotion() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            AtomicInteger writes = new AtomicInteger();
            manager.setCheckpointWriterForTest((sql, params) -> {
                writes.incrementAndGet();
                visible.set(new HashMap<>(params));
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertEquals(2, writes.get(),
                    "the initial reservation AND the consumed page both persist");

            // the mastership is lost while this FE is mid-cycle: the page may be finished
            // (the candidates were already captured), but the durable checkpoint now
            // belongs to the new leader
            manager.checkpointLeadershipProbeForTest = () -> false;
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(2, scanner.calls.get(),
                    "a demoted FE may still finish the page it started");
            Assertions.assertEquals(2, writes.get(),
                    "the obsolete cursor must not be UPSERTed over the new leader's checkpoint");
        } finally {
            manager.checkpointLeadershipProbeForTest = null;
            manager.resetForTest();
        }
    }

    /**
     * round-25 #5: a RE-PROMOTED FE must drop its obsolete in-memory progress and reload
     * the durable checkpoint before capturing again. The other leader may have advanced or
     * (for queued retries) rewound it, so resuming from the stale local cursor would skip
     * exactly the rows that leader queued (or re-consume rows it already handled).
     */
    @Test
    public void testPromotionReloadsTheDurableCheckpoint() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String tail = "[\"10.0.0.3\",\"h3\",\"300\",\"30\",\"m3\"]";
            // the new master's checkpoint: a DIFFERENT cursor, rewound for a queued retry
            ResultRow newMasters = new ResultRow(List.of(
                    "999", "500", "600", "42", "2026-01-03 00:00:00", "qid-new",
                    "{\"k\":1}", "{\"k\":1}", tail));
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> {
                reads.incrementAndGet();
                return List.of(newMasters);
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<String> statements = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> statements.add(sql));

            manager.reloadCheckpointOnPromotion();
            Assertions.assertFalse(manager.isCheckpointLoadedForTest(),
                    "promotion must drop the stale local progress");

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, reads.get(), "the re-promoted FE re-reads the checkpoint");
            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertEquals(42L, scanner.cursors.get(0)[0],
                    "the scan must resume from the NEW master's cursor, not from the"
                            + " obsolete local progress");
            Assertions.assertEquals("2026-01-03 00:00:00", scanner.cursors.get(0)[1]);
            Assertions.assertEquals("qid-new", scanner.cursors.get(0)[2]);
            Assertions.assertEquals(tail, scanner.tails.get(0),
                    "the queued-retry cursor tail must survive the reload");
            Assertions.assertTrue(manager.isCheckpointLoadedForTest(),
                    "the reloaded checkpoint stays loaded for the next cycle");
        } finally {
            manager.checkpointLeadershipProbeForTest = null;
            manager.resetForTest();
        }
    }

    // ==================== round-29 #3 / #4: drain, prompt resume, pinned thresholds ====================

    /**
     * A scanner stub with a REPEATING (truncated) page and an exhaust page it switches to
     * after a scripted number of truncated pages - the keyset cursor advances in
     * production, so the drain's page sequence is what a test has to script.
     */
    private static final class DrainingScanner extends AuditLogScanner {
        private final AtomicInteger calls = new AtomicInteger();
        private final List<long[]> windows = new ArrayList<>();
        private final List<Object[]> cursors = new ArrayList<>();
        private final List<String> tails = new ArrayList<>();
        private final List<long[]> thresholds = new ArrayList<>();
        private final ScanBatch truncatedPage;
        private final ScanBatch exhaustedPage;
        private volatile int truncatedPages;

        DrainingScanner(ScanBatch truncatedPage, ScanBatch exhaustedPage, int truncatedPages) {
            this.truncatedPage = truncatedPage;
            this.exhaustedPage = exhaustedPage;
            this.truncatedPages = truncatedPages;
        }

        void truncatePages(int pages) {
            this.truncatedPages = pages;
        }

        /** Truncates the NEXT {@code pages} calls (the counter is cumulative). */
        void truncateNextPages(int pages) {
            this.truncatedPages = calls.get() + pages;
        }

        @Override
        public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
                PlanCaptureFilter filter, long cursorQueryTime, String cursorTime,
                String cursorQueryId, String cursorTail) {
            int call = calls.incrementAndGet();
            windows.add(new long[] {startTimeMs, endTimeMs});
            cursors.add(new Object[] {cursorQueryTime, cursorTime, cursorQueryId});
            tails.add(cursorTail);
            thresholds.add(new long[] {filter.getMinQueryTimeMs(), filter.getMinScanRows()});
            return call <= truncatedPages ? truncatedPage : exhaustedPage;
        }
    }

    private static AuditLogScanner.ScanBatch truncatedPage(long cursorQueryTime, String cursorTime,
            String cursorQueryId, String cursorTail) {
        return new AuditLogScanner.ScanBatch(List.of(), false, cursorQueryTime, cursorTime,
                cursorQueryId, cursorTail);
    }

    private static AuditLogScanner.ScanBatch candidatePage(CapturedQuery candidate,
            long cursorQueryTime, String cursorTime, String cursorQueryId, String cursorTail) {
        return new AuditLogScanner.ScanBatch(List.of(candidate), false, cursorQueryTime,
                cursorTime, cursorQueryId, cursorTail);
    }

    private static AuditLogScanner.ScanBatch exhaustedPage() {
        return new AuditLogScanner.ScanBatch(List.of(), true, AuditLogScanner.CURSOR_ABSENT,
                "", "", "");
    }

    /**
     * #4: ONE cycle consumes the whole truncated window page by page - the second page
     * resumes from the first page's FULL cursor - and only then advances the watermark. A
     * single page per wakeup would make the backlog grow by one interval per page.
     */
    @Test
    public void testOneCycleDrainsTheWindowPageByPage() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String tail = "[\"10.0.0.3\",\"h3\",\"300\",\"30\",\"m3\"]";
            DrainingScanner scanner = new DrainingScanner(
                    truncatedPage(42L, "2026-01-02 00:00:00", "qid-page", tail),
                    exhaustedPage(), 3);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(4, scanner.calls.get(),
                    "3 truncated pages plus the page that exhausts the window: "
                            + scanner.calls.get());
            Assertions.assertEquals(42L, scanner.cursors.get(1)[0],
                    "page 2 resumes from page 1's cursor");
            Assertions.assertEquals("2026-01-02 00:00:00", scanner.cursors.get(1)[1]);
            Assertions.assertEquals("qid-page", scanner.cursors.get(1)[2]);
            Assertions.assertEquals(tail, scanner.tails.get(1),
                    "every page resumes with the FULL cursor tail");

            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(scanner.windows.get(0)[1], ((Number) fields[0]).longValue(),
                    "an exhausted window advances the watermark to the consumed end");
            Assertions.assertEquals(0L, ((Number) fields[1]).longValue(),
                    "the drained window leaves no pending window");
            Assertions.assertEquals(-1L, ((Number) fields[7]).longValue(),
                    "no threshold snapshot remains for a drained window");
            Assertions.assertFalse(manager.isPendingWindowResumePromptForTest(),
                    "a drained window keeps the configured interval");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * #4: a window that STILL has rows when the page budget is reached stays pending with
     * its cursor AND asks for a prompt resume; the next cycle continues the same window and
     * finishes it.
     */
    @Test
    public void testPageBudgetLeavesTheWindowPendingAndReschedulesPromptly() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String tail = "[\"10.0.0.4\",\"h4\",\"400\",\"40\",\"m4\"]";
            DrainingScanner scanner = new DrainingScanner(
                    truncatedPage(43L, "2026-01-02 00:00:00", "qid-keep", tail),
                    exhaustedPage(), Integer.MAX_VALUE);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(PlanCaptureManager.maxPagesPerCycleForTest(), scanner.calls.get(),
                    "the drain is BOUNDED by the page budget");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(43L, ((Number) fields[3]).longValue(),
                    "the unfinished window keeps the cursor of its last consumed row");
            Assertions.assertEquals(tail, fields[6]);
            Assertions.assertEquals(scanner.windows.get(0)[0], ((Number) fields[1]).longValue(),
                    "the pending window keeps its bounds");
            Assertions.assertEquals(scanner.windows.get(0)[1], ((Number) fields[2]).longValue());
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "a window cut off by the page budget resumes after the short delay");

            // the next cycle continues the SAME window and finishes it
            scanner.truncateNextPages(1);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(PlanCaptureManager.maxPagesPerCycleForTest() + 2,
                    scanner.calls.get());
            Assertions.assertEquals(43L,
                    ((Number) scanner.cursors.get(PlanCaptureManager.maxPagesPerCycleForTest())[0])
                            .longValue(),
                    "the resumed cycle starts from the persisted cursor");
            Object[] drained = manager.checkpointFieldsForTest();
            Assertions.assertEquals(0L, ((Number) drained[1]).longValue(),
                    "the finished window drops its bounds");
            Assertions.assertFalse(manager.isPendingWindowResumePromptForTest(),
                    "a finished window keeps the configured interval again");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * #3: a window is scanned with the threshold snapshot it was OPENED with. The audit SQL
     * and the in-memory filter must agree: a `SET GLOBAL
     * plan_capture_min_query_time_ms` between two pages of one window otherwise returned
     * rows the stale filter rejected terminally, or pushed already-passed rows behind the
     * cursor where a lowered threshold could not reach them.
     */
    @Test
    public void testPendingWindowKeepsTheThresholdSnapshotOfItsFirstPage() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String tail = "[\"10.0.0.5\",\"h5\",\"500\",\"50\",\"m5\"]";
            // the first window is cut off by the page budget, so it stays PENDING with the
            // filter it was opened with
            DrainingScanner scanner = new DrainingScanner(
                    truncatedPage(44L, "2026-01-02 00:00:00", "qid-thr", tail),
                    exhaustedPage(), Integer.MAX_VALUE);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> {
                persisted.add(new HashMap<>(params));
                visible.set(new HashMap<>(params));
            });

            PlanCaptureFilter windowFilter = new PlanCaptureFilter("db\\.t.*", "tmp.*",
                    1000L, 100L);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), windowFilter);
            Assertions.assertArrayEquals(new long[] {1000L, 100L}, scanner.thresholds.get(0));
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(1000L, ((Number) fields[7]).longValue(),
                    "the pending window pins the threshold it was opened with");
            Assertions.assertEquals(100L, ((Number) fields[8]).longValue());
            Assertions.assertEquals("db\\.t.*", fields[9],
                    "the table-name patterns are part of the pinned snapshot");
            Assertions.assertEquals("tmp.*", fields[10]);
            Assertions.assertEquals("1000",
                    persisted.get(persisted.size() - 1).get("minQueryTimeMs"),
                    "the pin is durable");
            Assertions.assertEquals("db\\\\.t.*",
                    persisted.get(persisted.size() - 1).get("includePattern"),
                    "the patterns are durable with the window (escapeSQL doubles the"
                            + " backslash, the reader decodes it back)");

            // a window that is still pending is NOT re-judged by the new globals: every
            // page of cycle 2 keeps the pinned values - including the patterns, so a
            // `SET GLOBAL plan_capture_include_pattern` cannot filter away rows the
            // window's earlier pages admitted
            PlanCaptureFilter raised = new PlanCaptureFilter("only_this_table", "", 7L, 7L);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), raised);
            Assertions.assertArrayEquals(new long[] {1000L, 100L},
                    scanner.thresholds.get(PlanCaptureManager.maxPagesPerCycleForTest()),
                    "the resumed window keeps its own thresholds, not the takeover's globals");
            Assertions.assertEquals("db\\.t.*", manager.getFilter().getIncludePatternText(),
                    "and its own table-name patterns");
            Assertions.assertEquals(1000L, ((Number) manager.checkpointFieldsForTest()[7]).longValue(),
                    "the pin stays on the pending window");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "the window is still pending after the second cycle");

            // a process handoff restores the pin from the checkpoint row
            Map<String, String> restored = new HashMap<>(visible.get());
            manager.resetForTest();
            manager.setCheckpointReaderForTest(() -> List.of(checkpointRow(restored)));
            manager.setCheckpointWriterForTest((sql, params) -> { });
            DrainingScanner resumed = new DrainingScanner(
                    truncatedPage(44L, "2026-01-02 00:00:00", "qid-thr", tail),
                    exhaustedPage(), 0);
            manager.setScannerForTest(resumed);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), raised);
            Assertions.assertArrayEquals(new long[] {1000L, 100L}, resumed.thresholds.get(0),
                    "the takeover continues the window with the thresholds it was opened"
                            + " with, not with its own globals");
            Assertions.assertEquals(-1L, ((Number) manager.checkpointFieldsForTest()[7]).longValue(),
                    "an exhausted window drops the pin; the next window follows the globals");

            // the NEXT window follows the refreshed filter again
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), raised);
            Assertions.assertArrayEquals(new long[] {7L, 7L}, resumed.thresholds.get(1),
                    "a new window follows the current globals");
            Assertions.assertEquals("only_this_table",
                    manager.getFilter().getIncludePatternText(),
                    "including its table-name patterns");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-30 #9: the page drain must PAUSE when the retry queue holds more than the
     * leader FE should retain. A metadata outage makes every capture fail, so a 50-page
     * drain could otherwise enqueue tens of thousands of full statements in one wakeup -
     * and the unconsumed pages must stay reachable while the replay burns the queue down.
     */
    @Test
    public void testQueueBudgetPausesTheDrainAndKeepsTheWindowReachable() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // the production budget is measured in megabytes of statements; a test-sized
            // budget is reached by the FIRST queued failure of the drain
            PlanCaptureManager.setQueuedFailureBudgetForTest(0L);
            PlanCaptureFilter accepting = new PlanCaptureFilter(null, null, 1000L, 1000L);
            CapturedQuery failing = new CapturedQuery(
                    "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = 7",
                    5000, 100000, 0, "digest-budget", "hash", "db", "internal",
                    "qid-budget");
            String tail = "[\"10.0.0.9\",\"h9\",\"900\",\"90\",\"m9\"]";
            DrainingScanner scanner = new DrainingScanner(
                    candidatePage(failing, 45L, "2026-01-02 00:00:00", "qid-drain", tail),
                    exhaustedPage(), Integer.MAX_VALUE);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), accepting);
            Assertions.assertEquals(1, scanner.calls.get(),
                    "the drain consumes the page that queues the failure and then PAUSES"
                            + " while the queue is over budget");
            Assertions.assertTrue(manager.isQueuedForTest("qid-budget"),
                    "the failed capture is queued");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "the paused window resumes promptly");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(scanner.windows.get(0)[0], ((Number) fields[1]).longValue(),
                    "the window stays pending so its remaining rows remain reachable");
            Assertions.assertEquals(scanner.windows.get(0)[1], ((Number) fields[2]).longValue());
            Assertions.assertEquals(45L, ((Number) fields[3]).longValue(),
                    "the consumed page's cursor is durable, so the resume continues after it");
            Assertions.assertEquals(tail, fields[6]);

            // once the queue is back under budget the same window drains normally
            PlanCaptureManager.setQueuedFailureBudgetForTest(Long.MAX_VALUE);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), accepting);
            Assertions.assertEquals(1 + PlanCaptureManager.maxPagesPerCycleForTest(),
                    scanner.calls.get(), "the drain resumes on the next cycle");
            Assertions.assertEquals(45L,
                    ((Number) scanner.cursors.get(1)[0]).longValue(),
                    "and continues exactly after the persisted cursor");
        } finally {
            PlanCaptureManager.setQueuedFailureBudgetForTest(null);
            manager.resetForTest();
        }
    }
}
