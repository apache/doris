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

package org.apache.doris.nereids.spm.manager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-74: the create's ownership proof
 * ({@code BaselineManager#confirmReservationReadable}) expressed through the store instead of
 * being assembled by the caller.
 *
 * <ul>
 *   <li>the two successor reads are taken as ONE {@code ReservationView}, and the view's
 *       verdict is the only thing the proof consumes - so the proof cannot drift into a
 *       "more reliable" private read that asserts a visibility no successor has;</li>
 *   <li>{@code reservationReadsAvailable()} is a STATIC store capability, never a readback
 *       VALUE: a store that models the reads must be asked EVEN when its lagging readback
 *       answers zero (that is exactly the fresh-cluster case the proof exists for).</li>
 * </ul>
 */
public class SPMRound74ManagerTest {

    /** A durable store that counts the two reservation reads and answers configurable values. */
    private static final class CountingStore implements BaselinePersistence {
        long watermark;
        SeqReservation latest;
        boolean readsAvailable = true;
        int watermarkReads;
        int reservationReads;

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public boolean reservationReadsAvailable() {
            return readsAvailable;
        }

        @Override
        public long sequenceWatermark() {
            watermarkReads++;
            return watermark;
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            reservationReads++;
            return latest;
        }
    }

    private static BaselineManager manager(CountingStore store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    // ==================== the view's verdict ====================

    /**
     * The view shows the reservation ONLY when both halves agree: the watermark reaches the
     * id AND the identity's latest record IS that plain reservation. An older id, a
     * tombstone, or a lagging watermark all mean "a successor would not see it".
     */
    @Test
    public void testReservationViewShowsOnlyTheExactPlainReservation() {
        BaselinePersistence.SeqReservation plain =
                new BaselinePersistence.SeqReservation(7L, 1_000L, false, false);
        Assertions.assertTrue(new BaselinePersistence.ReservationView(7L, plain)
                .showsReservation(7L), "the watermark reaches the id and the record is its own");
        Assertions.assertTrue(new BaselinePersistence.ReservationView(9L, plain)
                .showsReservation(7L), "a higher watermark still covers the id");

        Assertions.assertFalse(new BaselinePersistence.ReservationView(6L, plain)
                .showsReservation(7L), "a watermark below the id has not allocated it yet");
        Assertions.assertFalse(new BaselinePersistence.ReservationView(7L, null)
                .showsReservation(7L), "no record = nothing for a successor to see");
        Assertions.assertFalse(new BaselinePersistence.ReservationView(7L,
                        new BaselinePersistence.SeqReservation(6L, 1_000L, false, false))
                .showsReservation(7L), "an OLDER id is not this CREATE's reservation");
        Assertions.assertFalse(new BaselinePersistence.ReservationView(7L,
                        new BaselinePersistence.SeqReservation(7L, 1_000L, false, true))
                .showsReservation(7L), "a tombstone of the same id resolves the identity");
    }

    /** The default view is exactly the successor's two reads, in that order, read once each. */
    @Test
    public void testDefaultViewUsesTheSuccessorReads() {
        CountingStore store = new CountingStore();
        store.watermark = 7L;
        store.latest = new BaselinePersistence.SeqReservation(7L, 1_000L, false, false);

        BaselinePersistence.ReservationView view = store.readReservationView("d", 1L);

        Assertions.assertEquals(1, store.watermarkReads,
                "the view takes the allocation watermark exactly once");
        Assertions.assertEquals(1, store.reservationReads,
                "the view takes the identity lookup exactly once");
        Assertions.assertEquals(7L, view.watermark());
        Assertions.assertEquals(store.latest, view.latest());
        Assertions.assertTrue(view.showsReservation(7L),
                "the composed view answers the proof's question");
    }

    // ==================== the capability gate ====================

    /**
     * A store that MODELS the reads is asked even when its readback lags at ZERO - the exact
     * fresh-cluster case. Inferring the capability from the VALUE skipped the proof there and
     * let a successor hand the same id out twice.
     */
    @Test
    public void testLaggingReadbackStillRunsTheProof() {
        CountingStore store = new CountingStore();
        store.watermark = 0L; // a lagging readback on a fresh cluster
        store.latest = null;
        BaselineManager manager = manager(store);

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManagerTestAccess.confirmReservationReadable(manager, 7L, "d", 1L),
                "an unproven reservation must fail the CREATE retryably, never pass silently");
        Assertions.assertTrue(failure.getMessage().contains("retry the CREATE"),
                "the failure must be retryable: " + failure.getMessage());
        Assertions.assertTrue(store.watermarkReads > 0 && store.reservationReads > 0,
                "the proof actually read BOTH halves (watermark=" + store.watermarkReads
                        + ", reservation=" + store.reservationReads + ")");
    }

    /** A store that does NOT model the reads is skipped - by CAPABILITY, with no read at all. */
    @Test
    public void testStoreWithoutTheReadsSkipsTheProof() {
        CountingStore store = new CountingStore();
        store.readsAvailable = false;
        BaselineManager manager = manager(store);

        Assertions.assertDoesNotThrow(
                () -> BaselineManagerTestAccess.confirmReservationReadable(manager, 7L, "d", 1L));

        Assertions.assertEquals(0, store.watermarkReads + store.reservationReads,
                "the gate is a static declaration: a store that does not model the reads is"
                        + " not probed at all");
    }
}
