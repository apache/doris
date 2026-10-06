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

package org.apache.doris.plugin.audit;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditEventProcessor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Frontend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Round-36: the cluster-wide audit PUBLICATION horizon.
 *
 * <ul>
 *   <li>#1: the fence is the minimum over the local pipeline AND the fresh per-FE rows
 *       reported into {@code spm_audit_horizon} - the capture runs on the leader, whose
 *       own loader cannot see a follower's backlog. A row whose reporter went silent is
 *       ignored (the events are gone with it), and an unreadable table fails the read
 *       closed instead of silently dropping the fence.</li>
 *   <li>#3: the local value covers the stages BEFORE the loader as well: a completed
 *       query sits in {@link WorkloadRuntimeStatusMgr} first, and the
 *       {@link AuditEventProcessor} can hold an event in its queue or in-flight while a
 *       plugin runs - the loader being empty proves nothing.</li>
 *   <li>round-38 #2: an OVERDUE row is only dropped when its FE is provably GONE; a
 *       live - or undecidable - reporter fails the read closed instead of silently
 *       releasing the fence, because its pipeline may still owe events (and its last
 *       confirmed value can already be stale).</li>
 * </ul>
 */
public class AuditPublicationHorizonTest {

    private WorkloadRuntimeStatusMgr mgr;
    private AuditEventProcessor processor;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    public void setUp() {
        mgr = new WorkloadRuntimeStatusMgr();
        processor = new AuditEventProcessor(null);
        mockedEnv = Mockito.mockStatic(Env.class);
        // the writer-zone registry is process-wide: every read-back / encode in this
        // class must start from a known (empty) registry
        AuditWriterZones.resetForTest();
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        AuditPublicationHorizon.horizonRowsReaderForTest = null;
        AuditPublicationHorizon.localHorizonWriterForTest = null;
        AuditPublicationHorizon.feAliveProbeForTest = null;
        AuditPublicationHorizon.reporterNamesForTest = null;
        AuditWriterZones.captureCoveredThroughForTest = null;
        AuditWriterZones.resetForTest();
    }

    private static AuditEvent event(long timestamp) {
        return new AuditEvent.AuditEventBuilder()
                .setQueryId("qid-" + timestamp)
                .setTimestamp(timestamp)
                .setStmt("select 1")
                .build();
    }

    // ==================== #3: the pre-loader stages are part of the local horizon ======

    /**
     * A completed query the runtime status manager still HOLDS (it enters the pipeline
     * before any loader sees it) and an event inside the processor's queue / in flight
     * must both fence the local horizon.
     */
    @Test
    public void testLocalHorizonIncludesHeldQueriesAndTheProcessor() {
        mockedEnv.when(Env::getCurrentAuditEventProcessor).thenReturn(processor);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(mgr);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        Assertions.assertEquals(0L, AuditPublicationHorizon.localHorizon(),
                "an empty pipeline has nothing outstanding");

        // held by the runtime status manager (released only after the audit timeout)
        mgr.submitFinishQueryToAudit(event(9_000L));
        Assertions.assertEquals(9_000L, AuditPublicationHorizon.localHorizon(),
                "a completed query still held for auditing fences progress");

        // queued in the processor (the loader has not seen it yet)
        processor.handleAuditEvent(event(7_000L));
        Assertions.assertEquals(7_000L, AuditPublicationHorizon.localHorizon(),
                "the oldest of all local stages wins");

        // dequeued and in flight while a plugin runs: still outstanding
        processor.handleAuditEvent(event(8_000L));
        Deencapsulation.setField(processor, "processingEvent", event(3_000L));
        Assertions.assertEquals(3_000L, AuditPublicationHorizon.localHorizon(),
                "an in-flight event is out of the queue but NOT published");

        Deencapsulation.setField(processor, "processingEvent", null);
        Assertions.assertEquals(7_000L, AuditPublicationHorizon.localHorizon());
    }

    // ==================== #1: the cluster-wide minimum over fresh rows ================

    /** The MINIMUM over the fresh rows of every FE fences the leader's progress. */
    @Test
    public void testClusterHorizonTakesTheMinimumOverFreshFollowerRows() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Arrays.asList(
                new Object[] {"fe-a", now - 10_000L, now}, // a follower still owing a 10s-old event
                new Object[] {"fe-b", now - 40_000L, now}, // another owing an older one
                new Object[] {"fe-c", 0L, now});            // a third with nothing outstanding
        Assertions.assertEquals(now - 40_000L, AuditPublicationHorizon.clusterHorizon(),
                "the oldest fresh row fences, regardless of which FE it is");

        // an OVERDUE row of a GONE FE is ignored: its events died with the FE, and fencing
        // forever would freeze the capture instead of protecting anything
        AuditPublicationHorizon.feAliveProbeForTest = feName -> false;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-dead", now - 10_000L, now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        Assertions.assertEquals(0L, AuditPublicationHorizon.clusterHorizon());
    }

    /** An unreadable shared table must FAIL CLOSED, never silently drop the fence. */
    @Test
    public void testClusterHorizonFailsClosedWhenTheSharedTableIsUnreadable() {
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> {
            throw new IllegalStateException("internal table read timed out");
        };
        Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "without the follower rows the fence is incomplete: the caller must not"
                        + " advance");
    }

    /** The reporter writes THIS FE's horizon; the shutdown de-registers the row. */
    @Test
    public void testLocalHorizonIsReportedThroughTheWriter() {
        List<Long> reports = new ArrayList<>();
        AuditPublicationHorizon.localHorizonWriterForTest = horizon -> {
            reports.add(horizon);
            return true;
        };
        Assertions.assertTrue(AuditPublicationHorizon.reportLocalHorizon(4_242L),
                "a confirmed write reports true");
        AuditPublicationHorizon.clearLocalReport();
        Assertions.assertEquals(Collections.singletonList(4_242L), reports,
                "de-registration must NOT re-report a zero fence (round-42 #12): the row"
                        + " is DELETED instead, and a zero report would upsert the"
                        + " shutting-down FE's row back into the shared table: " + reports);
    }

    // ==================== round-37 #5: unconfirmed writes are retried ====================

    /**
     * A report is TRUE only when the written state is readable back from the shared table
     * (round-37 #5): SQL OK can still leave a COMMITTED INSERT unpublished, and the
     * previous void return let the reporter remember the value as reported anyway - an
     * old unpublished event then had no master-visible fence until the 60s keepalive.
     */
    @Test
    public void testReportIsConfirmedOnlyWhenTheOwnRowIsReadable() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.getNodeName()).thenReturn("fe-test");
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        try (MockedStatic<StatisticsUtil> statistics = Mockito.mockStatic(StatisticsUtil.class)) {
            // the read-back shows exactly the reported value: confirmed
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.singletonList(new ResultRow(Collections.singletonList("4242"))));
            Assertions.assertTrue(AuditPublicationHorizon.reportLocalHorizon(4_242L),
                    "a readable row with the reported value confirms the fence");

            // the read-back shows a STALE value (the upsert committed but is not visible
            // yet): unconfirmed, the reporter must retry on its next tick
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.singletonList(new ResultRow(Collections.singletonList("4000"))));
            Assertions.assertFalse(AuditPublicationHorizon.reportLocalHorizon(4_242L),
                    "a read-back that does not match the report is NOT confirmed");

            // an IDLE REGISTRATION must be READABLE (round-42 #12): the row's ABSENCE
            // reads as "this FE never registered", so it can NOT confirm a zero report
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.emptyList());
            Assertions.assertFalse(AuditPublicationHorizon.reportLocalHorizon(0L),
                    "an absent row is an UNCONFIRMED idle registration");

            // a readable ZERO row is the confirming state of an idle registration
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.singletonList(
                            new ResultRow(Arrays.asList("0", "", "0"))));
            Assertions.assertTrue(AuditPublicationHorizon.reportLocalHorizon(0L),
                    "a readable zero row confirms the idle registration");

            // a stale POSITIVE row (a pending fence about to be re-reported):
            // unconfirmed
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.singletonList(new ResultRow(Collections.singletonList("99"))));
            Assertions.assertFalse(AuditPublicationHorizon.reportLocalHorizon(0L),
                    "a still-visible old row does not confirm the clearing");
        }
    }

    /**
     * An unconfirmed write must be reported as false, so the reporter does NOT remember it
     * (round-37 #5) and retries it on the next tick.
     */
    @Test
    public void testUnconfirmedWriteReportsFalseThroughTheSeam() {
        AuditPublicationHorizon.localHorizonWriterForTest = horizon -> false;
        Assertions.assertFalse(AuditPublicationHorizon.reportLocalHorizon(7_000L),
                "a failed write must not look reported");
    }

    /**
     * round-42 #3: a ZERO report is an IDLE REGISTRATION and must UPSERT the row - the
     * old path DELETED it, so a live FE with nothing outstanding was indistinguishable
     * from an FE that never registered (and round-42 #12 fails the cluster read closed
     * for those).
     */
    @Test
    public void testZeroReportUpsertsTheRegistrationInsteadOfDeletingTheRow() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.getNodeName()).thenReturn("fe-test");
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        try (MockedStatic<StatisticsUtil> statistics = Mockito.mockStatic(StatisticsUtil.class)) {
            List<String> statements = new ArrayList<>();
            statistics.when(() -> StatisticsUtil.execUpdate(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenAnswer(invocation -> {
                        statements.add(invocation.getArgument(0));
                        return null;
                    });
            statistics.when(() -> StatisticsUtil.executeQuery(
                            Mockito.anyString(), Mockito.anyMap(), Mockito.anyInt()))
                    .thenReturn(Collections.singletonList(
                            new ResultRow(Arrays.asList("0", "", "0"))));
            Assertions.assertTrue(AuditPublicationHorizon.reportLocalHorizon(0L),
                    "the idle registration is confirmed by the readable zero row");
            Assertions.assertEquals(1, statements.size(),
                    "exactly one statement: " + statements);
            Assertions.assertTrue(statements.get(0).startsWith("INSERT"),
                    "the zero report must UPSERT the row (idle registration), not DELETE"
                            + " it: " + statements.get(0));
        }
    }

    /**
     * round-42 #12: a live FE with NO row is not "nothing outstanding" - its pipeline may
     * still owe events (or a committed batch may still publish), so the cluster horizon
     * must fail CLOSED (retryable) instead of reading the absence as a zero fence. Once
     * the FE registers - a zero row IS a registration - the read succeeds again.
     */
    @Test
    public void testLiveFeWithoutARowFailsTheClusterHorizonClosed() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.reporterNamesForTest =
                () -> Collections.singleton("fe-b");
        AuditPublicationHorizon.horizonRowsReaderForTest = Collections::emptyList;
        Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "a live FE that has not registered yet must fail the cycle closed");

        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-b", 0L, now});
        Assertions.assertEquals(now, AuditPublicationHorizon.clusterHorizon(),
                "an idle registration fences its own REPORT INSTANT (round-43 #2): it"
                        + " proves only that nothing was outstanding when it was written, and"
                        + " an event captured right after it may still be unpublished");
    }

    /**
     * Round-43 #2: an idle row is NOT "no fence". It vouches for its FE's pipeline only
     * up to the instant it was written; contributing NOTHING let the capture advance past
     * that instant, and an event the FE accepted immediately after reporting idle (but
     * before its next report tick) could publish into a window the capture had already
     * consumed. The row's REPORT TIME is therefore part of the cluster minimum.
     */
    @Test
    public void testIdleRowContributesItsReportInstantToTheClusterMinimum() {
        long now = System.currentTimeMillis();
        long busyFence = now - 600_000L;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Arrays.asList(
                new Object[] {"fe-idle", 0L, now - 1_000L},
                new Object[] {"fe-busy", busyFence, now});
        Assertions.assertEquals(busyFence, AuditPublicationHorizon.clusterHorizon(),
                "the busy row's older fence still wins the minimum");

        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-idle", 0L, now - 1_000L});
        Assertions.assertEquals(now - 1_000L, AuditPublicationHorizon.clusterHorizon(),
                "with only an idle row left, the minimum is its report instant");
    }

    /**
     * Round-43 #2 (overdue half): an OVERDUE zero row of a LIVE FE is just as
     * untrustworthy as a positive one - the FE may have captured events since its last
     * report. Skipping the row BEFORE the staleness check trusted the stale zero and let
     * the capture advance past those events.
     */
    @Test
    public void testOverdueIdleRowOfALiveFeFailsClosed() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.feAliveProbeForTest = feName -> true;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-live", 0L, now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "a live FE's un-refreshed ZERO row must skip the cycle, not be trusted");
    }

    /**
     * Round-43 #3: liveness means MEMBERSHIP, not the heartbeat flag. An RPC heartbeat
     * error sets {@code isAlive()} false while the FE (and its audit loader) still run and
     * may hold a queued event: releasing its fence let the capture checkpoint past the
     * event before it published. Only absence from the membership proves the events died
     * with the FE.
     */
    @Test
    public void testHeartbeatFlagDoesNotReleaseTheFenceOfAMember() {
        long now = System.currentTimeMillis();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getNodeName()).thenReturn("fe-self");
        Frontend member = Mockito.mock(Frontend.class);
        Mockito.when(member.getNodeName()).thenReturn("fe-member");
        Mockito.when(member.isAlive()).thenReturn(false); // a failed heartbeat / RPC
        Mockito.when(env.getFrontends(Mockito.any())).thenReturn(List.of(member));
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-member", now - 20_000L,
                        now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "a member with a failed heartbeat still runs - its queued event must keep"
                        + " fencing");
        Assertions.assertTrue(failure.getMessage().contains("fe-member"),
                "the failure names the member: " + failure.getMessage());
    }

    /**
     * Round-43 #3 (membership half): the registered-row requirement covers EVERY member,
     * including one whose heartbeat flag is currently false - excluding it read the
     * not-yet-registered gap as "no obligation".
     */
    @Test
    public void testMembershipWithoutAHeartbeatStillMustRegister() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getNodeName()).thenReturn("fe-self");
        Frontend member = Mockito.mock(Frontend.class);
        Mockito.when(member.getNodeName()).thenReturn("fe-quiet");
        Mockito.when(member.isAlive()).thenReturn(false);
        Mockito.when(env.getFrontends(Mockito.any())).thenReturn(List.of(member));
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        AuditPublicationHorizon.horizonRowsReaderForTest = Collections::emptyList;
        Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "a member without a row must fail the read closed even when its heartbeat"
                        + " flag is false");
    }

    // ==================== round-37 #4: a fixed zone for update_time ====================

    /**
     * update_time is a zone-less DATETIME shared between FEs that may render their local
     * wall time in different zones: writing and reading it in the SAME explicit zone
     * (UTC) is what keeps a fresh row from looking hours old to a reader in another zone
     * (it would then be discarded as stale and drop that follower's fence).
     */
    @Test
    public void testUpdateTimeUsesAFixedZone() {
        long epochMillis = 1_780_000_000_000L;
        String rendered = AuditPublicationHorizon.renderUpdateTime(epochMillis);
        Assertions.assertEquals("2026-05-28 20:26:40", rendered,
                "the rendering is UTC, not the JVM zone (a +08:00 FE would render"
                        + " 2026-05-29 04:26:40)");
        Assertions.assertEquals(epochMillis, AuditPublicationHorizon.parseUpdateTime(rendered),
                "reading a row back yields the same instant");
    }

    // ==================== round-37 #7: internal events never fence ======================

    /**
     * Internal statements (e.g. the horizon reporter's own SQL) are never captured, so
     * they must not fence progress - otherwise the reporter's writes would keep their own
     * FE's fence (and the writes it triggers) alive forever on an idle FE.
     */
    @Test
    public void testInternalEventsDoNotFenceThePipeline() {
        mockedEnv.when(Env::getCurrentAuditEventProcessor).thenReturn(processor);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(mgr);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        AuditEvent internal = new AuditEvent.AuditEventBuilder()
                .setQueryId("internal-horizon-report")
                .setTimestamp(1_000L)
                .setStmt("INSERT INTO __internal_schema.spm_audit_horizon ...")
                .setisInternal(true)
                .build();

        mgr.submitFinishQueryToAudit(internal);
        processor.handleAuditEvent(internal);
        Deencapsulation.setField(processor, "processingEvent", internal);
        Assertions.assertEquals(0L, AuditPublicationHorizon.localHorizon(),
                "an internal event can never be captured and must not fence any stage");

        Deencapsulation.setField(processor, "processingEvent", null);
        Assertions.assertEquals(0L, AuditPublicationHorizon.localHorizon(),
                "the internal event stays out of the fence at every stage");
    }

    // ==================== round-38 #2: an overdue row of a LIVE fe fails closed =========

    /**
     * A follower's keepalive upserts can fail for minutes while the FE still holds a
     * completed event (and may even gain MORE events with older start times): the overdue
     * row must not be silently dropped, and its STALE VALUE must not be trusted either -
     * the read fails closed so the capture skips the cycle and retries instead of
     * checkpointing past the unread fence.
     */
    @Test
    public void testOverdueFenceOfALiveFeFailsClosed() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.feAliveProbeForTest = feName -> true;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-live", now - 20_000L, now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "a live FE's un-refreshed fence must skip the cycle, not release it");
        Assertions.assertTrue(failure.getMessage().contains("fe-live"),
                "the failure names the FE: " + failure.getMessage());
    }

    /** A row whose FE is provably gone is a leftover: its events died with it. */
    @Test
    public void testOverdueFenceOfAGoneFeIsDropped() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.feAliveProbeForTest = feName -> false;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-gone", now - 20_000L, now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        Assertions.assertEquals(0L, AuditPublicationHorizon.clusterHorizon(),
                "a gone FE's overdue row no longer fences: nothing of its can publish any more");
    }

    /** An undecidable liveness must be conservative: the fence fails closed. */
    @Test
    public void testOverdueFenceWithUndecidableLivenessFailsClosed() {
        long now = System.currentTimeMillis();
        // no probe: the Env-based lookup finds no membership view in the unit test, so the
        // liveness is UNKNOWN - and unknown must not be mistaken for "gone"
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-unknown", now - 20_000L, now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1});
        Assertions.assertThrows(IllegalStateException.class,
                AuditPublicationHorizon::clusterHorizon,
                "an undecidable reporter must keep the fence: dropping it could miss the"
                        + " event it still owes");
    }

    // ==================== round-40 #10: committed fences survive their FE ===============

    /**
     * A batch whose stream load reported Publish Timeout is COMMITTED but unreadable: its
     * rows can publish AFTER the FE died, so the {@code committed_fence_ms} marker keeps
     * the fence alive past the FE - dropping the row at death let the capture checkpoint
     * past rows that then appeared behind the watermark. After the SAME bound the loader
     * itself applies the batch is conclusively lost and the fence releases.
     */
    @Test
    public void testCommittedFenceOfAGoneFeSurvivesItsDeath() {
        long now = System.currentTimeMillis();
        long staleAt = now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1;
        AuditPublicationHorizon.feAliveProbeForTest = feName -> false;
        // horizon 0 (the reporter wrote "nothing outstanding" before the batch timed
        // out): only the committed marker fences
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-dead", 0L, staleAt, "", 20_000L});
        Assertions.assertEquals(20_000L, AuditPublicationHorizon.clusterHorizon(),
                "the committed batch's rows may still publish although the FE is gone");

        // the fence survives while the row is inside the survival bound even when it is
        // overdue; past the bound it is conclusively lost
        long pastBound = now - AuditPublicationHorizon.COMMITTED_FENCE_SURVIVAL_MILLIS
                - AuditPublicationHorizon.ROW_STALE_MILLIS - 1;
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-dead", 20_000L, pastBound, "", 20_000L});
        Assertions.assertEquals(0L, AuditPublicationHorizon.clusterHorizon(),
                "a bound-expired committed fence is conclusively lost, exactly like the"
                        + " loader treats its own fence");
    }

    /** A fresh row's committed marker fences even when the horizon column is zero. */
    @Test
    public void testFreshCommittedFenceContributesToTheClusterHorizon() {
        long now = System.currentTimeMillis();
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-busy", 0L, now, "", 9_000L});
        Assertions.assertEquals(9_000L, AuditPublicationHorizon.clusterHorizon(),
                "a committed-but-unreadable batch is part of the fence");
    }

    // ==================== round-40 #2: stale rows keep their zones =====================

    /**
     * The writer zones of an OVERDUE row stay part of the required scan set until
     * durable capture progress has passed the row's last refresh: a follower can publish
     * a row under -05:00 and then stop reporting (crash / stalled keepalive) while an
     * uncompleted window still contains it - dropping the zone here let a UTC leader
     * exhaust the window and checkpoint past the row stored as 05:00.
     */
    @Test
    public void testStaleRowZonesStayRequiredUntilTheCaptureHasPassedThem() {
        long now = System.currentTimeMillis();
        long staleAt = now - AuditPublicationHorizon.ROW_STALE_MILLIS - 1;
        String zones = "America/New_York=" + (now - 60_000L);
        AuditPublicationHorizon.horizonRowsReaderForTest = () -> Collections.singletonList(
                new Object[] {"fe-stopped", 0L, staleAt, zones});

        AuditWriterZones.captureCoveredThroughForTest = () -> staleAt - 1;
        Assertions.assertEquals(Set.of("America/New_York"),
                AuditPublicationHorizon.clusterWriterZones(),
                "the row's zone may still own a row of an uncompleted window");

        AuditWriterZones.captureCoveredThroughForTest = () -> staleAt + 1;
        Assertions.assertEquals(Set.of(), AuditPublicationHorizon.clusterWriterZones(),
                "durable progress past the last refresh covers every row the FE rendered");
    }
}
