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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
        /** the zone each page was told to render its bounds in (pass zone). */
        private final List<String> passZones = new ArrayList<>();
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
                String cursorQueryId, String cursorTail, String firstPassZoneId) {
            calls.incrementAndGet();
            windows.add(new long[] {startTimeMs, endTimeMs});
            cursors.add(new Object[] {cursorQueryTime, cursorTime, cursorQueryId});
            tails.add(cursorTail);
            passZones.add(firstPassZoneId);
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
     * round-22 #5 + round-32 #11: a NON-EMPTY read is not proof the reservation became
     * visible - the visible row may be the OLD master's reservation (published after its
     * demotion). Confirming "some row exists" would consume OUR window and the final
     * UPSERT would replace the old master's still-unconsumed pending window. Round-32 makes
     * that row the WINDOW TO CONSUME: it is adopted (with its cursor / retry state), the
     * cycle aborts, and the earlier window is consumed next - never replaced by the derived
     * one.
     */
    @Test
    public void testReservationMustMatchOurOwnWindow() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            // the successful-but-empty read of the first cycle, then the store keeps
            // showing the OLD master's reservation (a different window, with a live cursor)
            String oldTail = "[\"10.0.0.9\",\"h9\",\"900\",\"90\",\"m9\"]";
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> reads.getAndIncrement() == 0
                    ? List.of()
                    : List.of(new ResultRow(List.of(
                            "123456", "500", "600", "7", "2026-01-01 00:00:00", "qid-old",
                            "{}", "{}", oldTail))));
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "a foreign (old master's) row must not let OUR window be consumed");
            Object[] adopted = manager.checkpointFieldsForTest();
            Assertions.assertEquals(500L, ((Number) adopted[1]).longValue(),
                    "the old master's window is adopted instead of replaced");
            Assertions.assertEquals(600L, ((Number) adopted[2]).longValue());
            Assertions.assertEquals(7L, ((Number) adopted[3]).longValue(),
                    "with its cursor and retry state");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "and it resumes promptly");

            // the store now carries the adopted state: the next cycle re-scans the EARLIER
            // window (from its own cursor) and only then advances
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get(),
                    "the same-window row confirms the reservation and the window drains");
            Assertions.assertArrayEquals(new long[] {500L, 600L}, scanner.windows.get(0),
                    "the adoption keeps the EARLIER window, not the derived one");
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
        private final List<PlanCaptureFilter> filters = new ArrayList<>();
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
                String cursorQueryId, String cursorTail, String firstPassZoneId) {
            int call = calls.incrementAndGet();
            windows.add(new long[] {startTimeMs, endTimeMs});
            cursors.add(new Object[] {cursorQueryTime, cursorTime, cursorQueryId});
            tails.add(cursorTail);
            thresholds.add(new long[] {filter.getMinQueryTimeMs(), filter.getMinScanRows()});
            filters.add(filter);
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

    /**
     * round-31 #4: the REWOUND retry window carries the FILTER SNAPSHOT of the page its
     * oldest queued failure was first seen on. A W1 scan queues 65 transient failures; the
     * checkpoint keeps the oldest W1 pre-page cursor but the durable JSON retains only 64
     * entries - and W1's exhaustion clears the pending-window filter. Persisting the
     * (now null) pending filter next to the W1 cursor left the rewound range unpinned: a
     * `SET GLOBAL plan_capture_min_query_time_ms` for the NEXT cycle then judged the
     * re-scanned rows by the tighter configuration, terminally filtering the omitted
     * oldest failure before the restored retry queue could ever retry it.
     */
    @Test
    public void testRewoundRetryWindowKeepsTheEligibilitySnapshotOfItsFirstPage()
            throws Exception {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            // a capture that cannot be planned because its catalog metadata is unavailable
            // is the retryable failure the queue exists for (see PlanCaptureTest)
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(false);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(null);

            // ONE page with 65 failing candidates that exhausts the window: all of them
            // are queued, the window's filter is dropped (clearPendingWindow) and the
            // durable JSON can only carry the NEWEST 64 entries
            List<CapturedQuery> failures = new ArrayList<>();
            for (int i = 0; i < 65; i++) {
                failures.add(new CapturedQuery(
                        "SELECT t1.a FROM ext_cat.ext_db.t" + i + " t1 JOIN ext_cat.ext_db.u"
                                + i + " t2 ON t1.a = t2.a",
                        5000, 100000, 0, "digest-r31-" + i, "hash", "ext_db", "ext_cat",
                        "qid-r31-" + i));
            }
            AuditLogScanner.ScanBatch wholeWindow = new AuditLogScanner.ScanBatch(failures,
                    true, AuditLogScanner.CURSOR_ABSENT, "", "", "");
            DrainingScanner scanner = new DrainingScanner(wholeWindow, exhaustedPage(),
                    Integer.MAX_VALUE);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            // W1 is judged by THIS snapshot: threshold 111/11, exclude pattern F1_EXCLUDED
            PlanCaptureFilter windowFilter = new PlanCaptureFilter("", "F1_EXCLUDED", 111L, 11L);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), windowFilter);
            Assertions.assertTrue(manager.isQueuedForTest("qid-r31-0"),
                    "the oldest failure is queued (its retry will be omitted from the JSON)");
            Assertions.assertTrue(manager.isQueuedForTest("qid-r31-64"),
                    "the newest failure is queued as well");
            Map<String, String> persisted = visible.get();
            Assertions.assertEquals("111", persisted.get("minQueryTimeMs"),
                    "the rewound window pins the filter its page was judged by, not -1");
            Assertions.assertEquals("11", persisted.get("minScanRows"));
            Assertions.assertEquals("F1_EXCLUDED", persisted.get("excludePattern"),
                    "including the table-name patterns of that page");

            // TAKEOVER while the globals were tightened: the restored window must be
            // re-scanned with ITS OWN snapshot, or the re-scan terminally filters the
            // omitted oldest failure (5000 < 999999) before its retry is reachable
            Map<String, String> restored = new HashMap<>(persisted);
            manager.resetForTest();
            manager.setCheckpointReaderForTest(() -> List.of(checkpointRow(restored)));
            manager.setCheckpointWriterForTest((sql, params) -> { });
            DrainingScanner resumed = new DrainingScanner(exhaustedPage(), exhaustedPage(), 0);
            manager.setScannerForTest(resumed);
            PlanCaptureFilter tightened = new PlanCaptureFilter("only_this_table", "",
                    999999L, 999999L);
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), tightened);
            Assertions.assertArrayEquals(new long[] {111L, 11L}, resumed.thresholds.get(0),
                    "the takeover re-scans the rewound window with the eligibility its rows"
                            + " were first admitted under");
            Assertions.assertEquals("F1_EXCLUDED", resumed.filters.get(0).getExcludePatternText(),
                    "the pinned table-name pattern travels with the rewound window as well");
        } finally {
            manager.resetForTest();
        }
    }

    // ==================== round-32: resume scheduling, reservation takeover, zones ====================

    /**
     * round-32 #4: an EXHAUSTED window with retries still queued keeps the SHORT resume
     * interval. replayQueuedFailures burns at most 1,000 entries per cycle, so a window
     * that exhausts while holding an outage-sized queue would otherwise sleep a full
     * capture interval (three hours by default) between every 1,000 retries - a 25,000
     * entry backlog would need days to drain.
     */
    @Test
    public void testExhaustedWindowWithQueuedRetriesRequestsPromptResume() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            // a capture that cannot be planned is the retryable failure the queue exists for
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(false);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(null);

            CapturedQuery failing = new CapturedQuery(
                    "SELECT t1.a FROM ext_cat.ext_db.t1 t1 JOIN ext_cat.ext_db.t2 t2"
                            + " ON t1.a = t2.a",
                    5000, 100000, 0, "digest-r32-4", "hash", "ext_db", "ext_cat", "qid-r32-4");
            AuditLogScanner.ScanBatch page = new AuditLogScanner.ScanBatch(List.of(failing),
                    true, AuditLogScanner.CURSOR_ABSENT, "", "", "");
            DrainingScanner scanner = new DrainingScanner(page, exhaustedPage(),
                    Integer.MAX_VALUE);
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            // an ACCEPTING window filter: the candidate must fail in PLANNING (catalog
            // unavailable), not be terminally filtered out by the thresholds
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    new PlanCaptureFilter(null, null, 1000L, 1000L));
            Assertions.assertTrue(manager.isQueuedForTest("qid-r32-4"),
                    "the failed capture stays queued for a bounded retry");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(0L, ((Number) fields[1]).longValue(),
                    "the window is exhausted and advances");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "the queued retry must resume at the pending-window cadence, not after"
                            + " a full capture interval");

            // once the queue is empty again the next exhaustion keeps the full interval
            manager.resetForTest();
            AtomicReference<Map<String, String>> visible2 = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible2.get() == null
                    ? List.of() : List.of(checkpointRow(visible2.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible2.set(new HashMap<>(params)));
            manager.setScannerForTest(new DrainingScanner(exhaustedPage(), exhaustedPage(), 0));
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertFalse(manager.isPendingWindowResumePromptForTest(),
                    "a clean exhaustion keeps the configured interval");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-32 #15: a FAILED first checkpoint read must not lose the window it would have
     * consumed. The read fails while the internal table initializes; a later cycle deriving
     * its OWN [now - interval, now) would permanently skip every eligible short row of the
     * first attempted window (no later overlap reaches behind a NEW window's start).
     */
    @Test
    public void testFailedFirstReadRetainsTheFirstAttemptedWindow() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicInteger reads = new AtomicInteger();
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> {
                if (reads.incrementAndGet() == 1) {
                    throw new RuntimeException("internal table not ready");
                }
                // becomes readable and EMPTY right afterwards; the reservation write then
                // makes its own row readable (the visibility probes need it)
                return visible.get() == null
                        ? List.of() : List.of(checkpointRow(visible.get()));
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> {
                persisted.add(new HashMap<>(params));
                visible.set(new HashMap<>(params));
            });

            long intervalMs = Math.max(1L,
                    VariableMgr.getDefaultSessionVariable().getPlanCaptureIntervalSeconds())
                    * 1000L;
            long beforeFirstCycle = System.currentTimeMillis();

            // cycle 1: read fails - nothing is scanned or written, but the retry is scheduled
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get());
            Assertions.assertEquals(0, persisted.size(),
                    "an unreadable checkpoint must never be replaced by a derived one");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "a failed read reschedules promptly instead of after three hours");

            // cycle 2: the store is readable and empty - the first attempted window is
            // scanned, not a freshly derived [now - interval, now)
            // (a short sleep keeps the two candidate starts apart on a coarse clock)
            Thread.sleep(50L);
            long beforeSecondCycle = System.currentTimeMillis();
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get());
            long scanStart = scanner.windows.get(0)[0];
            Assertions.assertTrue(scanStart < beforeSecondCycle - intervalMs,
                    "the fresh window must reach back to the FIRST attempted start, got "
                            + scanStart + " vs " + (beforeSecondCycle - intervalMs));
            Assertions.assertTrue(scanStart >= beforeFirstCycle - intervalMs,
                    "and must not reach further back than the first attempt");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-32 #11: a reservation the PREVIOUS leader wrote but that only becomes readable
     * later must be ADOPTED, not replaced. Leader A commits [09:00, 12:00) but its row is
     * still unreadable when B promotes and derives [09:10, 12:10); the single-row UPSERT
     * would replace A's record and an eligible 09:05 row would fall outside B's window and
     * every later overlap. The reconciliation read runs BEFORE the reservation write.
     */
    @Test
    public void testForeignReservationReadableBeforeTheWriteIsAdopted() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicInteger reads = new AtomicInteger();
            // call 1: the cycle-initial read is EMPTY (the foreign row is not visible yet);
            // call 2: the pre-write reconciliation sees the earlier leader's reservation
            manager.setCheckpointReaderForTest(() -> {
                if (reads.incrementAndGet() <= 1) {
                    return List.of();
                }
                return List.of(new ResultRow(List.of(
                        "0", "100", "200", "0", "", "", "{}", "{}", "",
                        "-1", "-1", "", "")));
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> persisted.add(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "the cycle must abort instead of consuming the derived window over the"
                            + " earlier leader's reservation");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(100L, ((Number) fields[1]).longValue(),
                    "the earlier leader's window is adopted");
            Assertions.assertEquals(200L, ((Number) fields[2]).longValue());
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "the adopted window resumes promptly");
            Assertions.assertFalse(persisted.isEmpty(),
                    "the adopted state is written back, so a later takeover cannot read the"
                            + " replaced window instead");
            Assertions.assertEquals("100", persisted.get(persisted.size() - 1).get("pendingStart"));

            // the next cycle consumes the EARLIER window (and reports where it stopped)
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertArrayEquals(new long[] {100L, 200L}, scanner.windows.get(0),
                    "the resumed window is the earlier leader's, not the derived one");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-32 #11 (probe half): the visibility probes of the reservation must adopt a
     * FOREIGN row that surfaces while probing - ignoring it (\"not ours\") let the cycle
     * consume the derived window although the earlier leader's pending window was now
     * readable.
     */
    @Test
    public void testForeignReservationReadableDuringTheProbesIsAdopted() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> {
                int call = reads.incrementAndGet();
                if (call <= 2) {
                    // cycle-initial read + pre-write reconciliation: still empty
                    return List.of();
                }
                // the earlier leader's row surfaces during this cycle's probes
                return List.of(new ResultRow(List.of(
                        "0", "300", "400", "0", "", "", "{}", "{}", "",
                        "-1", "-1", "", "")));
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> persisted.add(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(0, scanner.calls.get(),
                    "the cycle aborts once the foreign window is readable");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(300L, ((Number) fields[1]).longValue(),
                    "the foreign window is adopted");
            Assertions.assertEquals(400L, ((Number) fields[2]).longValue());
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest());
            Assertions.assertEquals("300", persisted.get(persisted.size() - 1).get("pendingStart"),
                    "the adopted state is pushed back over this cycle's own reservation");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-32 #3: after a global time_zone change the window is rendered in the PREVIOUS
     * zone first - the rows published before the change are stored with the old rendering
     * and are invisible to bounds rendered in the new zone (an empty page would exhaust the
     * window and advance the watermark past them forever). The following pass revisits the
     * SAME window in the current zone, and only then does the watermark advance.
     */
    @Test
    public void testZoneChangeRescansTheWindowInTheCurrentZone() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String currentZone = AuditLogScanner.auditWriteZone().getId();
            String previousZone = "UTC".equals(currentZone) ? "Asia/Tokyo" : "UTC";
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> {
                // a checkpoint row whose scan pass ran in the PREVIOUS zone, with a window
                // still pending (the zone changed while it was being consumed)
                if (reads.getAndIncrement() == 0) {
                    return List.of(new ResultRow(List.of(
                            "0", "100", "200",
                            String.valueOf(AuditLogScanner.CURSOR_ABSENT), "", "",
                            "{}", "{}", "", "111", "11", "", "", previousZone)));
                }
                return List.of();
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest((sql, params) -> persisted.add(new HashMap<>(params)));

            // pass 1: the pending window is drained in the zone it was OPENED in ...
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertEquals(previousZone, scanner.passZones.get(0),
                    "the window keeps the rendering of its cursor's zone");
            Object[] fields = manager.checkpointFieldsForTest();
            Assertions.assertEquals(100L, ((Number) fields[1]).longValue(),
                    "the zone change does NOT advance the watermark: the same window is"
                            + " re-scanned in the new zone first");
            Assertions.assertEquals(200L, ((Number) fields[2]).longValue());
            Assertions.assertEquals(AuditLogScanner.CURSOR_ABSENT, ((Number) fields[3]).longValue(),
                    "the re-scan starts from the top of the window");
            Assertions.assertTrue(manager.isPendingWindowResumePromptForTest(),
                    "the re-scan resumes promptly");
            Assertions.assertEquals(currentZone, manager.lastScanZoneForTest(),
                    "the next pass follows the current global zone");
            Assertions.assertEquals(currentZone,
                    persisted.get(persisted.size() - 1).get("scanZone"),
                    "the pass zone is persisted for a takeover");

            // pass 2: the SAME window is re-scanned in the current zone and only then drained
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            Assertions.assertEquals(2, scanner.calls.get());
            Assertions.assertEquals(currentZone, scanner.passZones.get(1),
                    "the second pass renders in the NEW zone");
            Assertions.assertArrayEquals(scanner.windows.get(0), scanner.windows.get(1),
                    "both passes cover the same window");
            Object[] drained = manager.checkpointFieldsForTest();
            Assertions.assertEquals(0L, ((Number) drained[1]).longValue(),
                    "the window is consumed by the second pass and the watermark advances");
            Assertions.assertEquals(200L, ((Number) drained[0]).longValue());
            Assertions.assertFalse(manager.isPendingWindowResumePromptForTest());
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-33 #3: the durable checkpoint must carry the scan zone of the state it
     * REWINDS to. With retries queued, persistCheckpoint rewinds the bounds / cursor to
     * the OLDEST queued entry's pre-page anchor - a page of an EARLIER window. Persisting
     * the CURRENT pass's zone beside that anchor made a takeover render the earlier
     * window's bounds in a zone its rows were never written under: the rows of the
     * window (and the retries the truncated queue could not carry) are invisible and
     * unreachable.
     */
    @Test
    public void testRewoundCheckpointPersistsTheAnchorsScanZone() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            String currentZone = AuditLogScanner.auditWriteZone().getId();
            String previousZone = "UTC".equals(currentZone) ? "Asia/Tokyo" : "UTC";
            // a restored leader state: W1's window still pending with 70 failed captures
            // queued - its pages were rendered in the PREVIOUS zone (the global time_zone
            // changed after W1 was scanned)
            Map<String, CapturedQuery> queued = new java.util.LinkedHashMap<>();
            Map<String, Integer> attempts = new java.util.LinkedHashMap<>();
            for (int i = 0; i < 70; i++) {
                queued.put("q:" + i, new CapturedQuery("select " + i, 1, 1, 1, "d", "h",
                        "db", "cat", "q:" + i, false, SqlModeHelper.MODE_DEFAULT));
                attempts.put("q:" + i, 1);
            }
            AtomicInteger reads = new AtomicInteger();
            manager.setCheckpointReaderForTest(() -> {
                if (reads.getAndIncrement() == 0) {
                    return List.of(new ResultRow(List.of(
                            "0", "100", "200",
                            String.valueOf(AuditLogScanner.CURSOR_ABSENT), "", "",
                            PlanCaptureManager.encodeFailedAttempts(attempts),
                            PlanCaptureManager.encodeRetryQueue(queued),
                            "", "-1", "-1", "", "", previousZone)));
                }
                return List.of();
            });
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            List<Map<String, String>> persisted = new ArrayList<>();
            manager.setCheckpointWriterForTest(
                    (sql, params) -> persisted.add(new HashMap<>(params)));

            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(), manager.getFilter());

            Assertions.assertEquals(1, scanner.calls.get());
            Assertions.assertEquals(previousZone, scanner.passZones.get(0),
                    "W1 is drained in the zone its cursor was rendered in");
            Assertions.assertEquals(currentZone, manager.lastScanZoneForTest(),
                    "the window is now owned by the CURRENT zone's re-scan pass");
            Map<String, String> last = persisted.get(persisted.size() - 1);
            Assertions.assertEquals("100", last.get("pendingStart"),
                    "the checkpoint rewinds to the oldest queued retry's window");
            Assertions.assertEquals(previousZone, last.get("scanZone"),
                    "the rewound bounds / cursor must keep the ANCHOR's rendering: a"
                            + " takeover rendering W1 in " + currentZone + " could not see"
                            + " its " + previousZone + "-stored rows");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-32 #12: the scan overlap grows to the LOCAL audit loader's outstanding queue
     * horizon. A fixed overlap only covers the CONFIGURED waits; the loader can still hold
     * the row of an 11:50 query while the 12:00 window advances, and only re-reading that
     * instant keeps it capturable.
     */
    @Test
    public void testScanOverlapFollowsTheAuditLoaderQueueHorizon() {
        long batchSec = 5L;
        long base = PlanCaptureManager.scanWindowOverlapMs(batchSec);
        long now = 1_000_000_000L;
        Assertions.assertEquals(base,
                PlanCaptureManager.scanWindowOverlapMs(batchSec, 0L, now),
                "nothing outstanding keeps the configured overlap");
        Assertions.assertEquals(base,
                PlanCaptureManager.scanWindowOverlapMs(batchSec, now + 5_000L, now),
                "a future horizon cannot widen the overlap");
        Assertions.assertEquals(10 * 60_000L,
                PlanCaptureManager.scanWindowOverlapMs(batchSec, now - 10 * 60_000L, now),
                "an event the loader has held for ten minutes fences the window back to it");

        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            manager.setAuditQueueHorizonForTest(() -> System.currentTimeMillis() - 10 * 60_000L);
            RecordingScanner scanner = new RecordingScanner(List.of(), true,
                    AuditLogScanner.CURSOR_ABSENT, "", "", "");
            manager.setScannerForTest(scanner);
            AtomicReference<Map<String, String>> visible = new AtomicReference<>();
            manager.setCheckpointReaderForTest(() -> visible.get() == null
                    ? List.of() : List.of(checkpointRow(visible.get())));
            manager.setCheckpointWriterForTest((sql, params) -> visible.set(new HashMap<>(params)));

            // cycle 1: a fresh window, then advance the watermark
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            long firstEnd = scanner.windows.get(0)[1];
            // cycle 2: the window must start at (or before) the outstanding horizon event, so
            // the row the loader still holds stays inside a scanned range
            manager.runCaptureCycle(VariableMgr.getDefaultSessionVariable(),
                    manager.getFilter());
            long secondStart = scanner.windows.get(1)[0];
            Assertions.assertTrue(secondStart <= System.currentTimeMillis() - 10 * 60_000L,
                    "the overlap must retain the loader's outstanding horizon, got "
                            + secondStart + " vs the first window end " + firstEnd);
        } finally {
            manager.resetForTest();
        }
    }
}
