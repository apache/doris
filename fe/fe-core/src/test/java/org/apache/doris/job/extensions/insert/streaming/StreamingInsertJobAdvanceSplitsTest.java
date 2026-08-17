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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Covers the bounded admission loop in {@link StreamingInsertJob#advanceSplitsIfNeed()}:
 * it must keep admitting within one tick yet terminate on every in-loop exit condition
 * (noMoreSplits / pending cap / no-progress round) without spinning. The deadline term is
 * wall-clock based and is covered by integration/regression rather than here.
 */
public class StreamingInsertJobAdvanceSplitsTest {

    // Assumed value of StreamingInsertJob.MAX_PENDING_SPLITS; update both together if changed.
    private static final int CAP = 512;

    private static StreamingInsertJob newJob(SourceOffsetProvider provider, long maxIntervalSec) {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Map<String, String> props = new HashMap<>();
        props.put(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY, String.valueOf(maxIntervalSec));
        Deencapsulation.setField(job, "jobProperties", new StreamingJobProperties(props));
        Deencapsulation.setField(job, "offsetProvider", provider);
        return job;
    }

    @Test
    public void testLoopsUntilNoMoreSplits() throws Exception {
        // Many small tables: one split per round, splitting completes after 5 rounds.
        FakeOffsetProvider provider = new FakeOffsetProvider(1, 5);
        newJob(provider, 3600).advanceSplitsIfNeed();
        Assert.assertEquals("must keep admitting across rounds until noMoreSplits", 5, provider.rounds);
    }

    @Test
    public void testStopsAtPendingCap() throws Exception {
        // Fast producer that never finishes: must stop once the FE backlog cap is crossed.
        FakeOffsetProvider provider = new FakeOffsetProvider(100, -1);
        newJob(provider, 3600).advanceSplitsIfNeed();
        Assert.assertTrue("must stop once pending crosses the cap", provider.pendingSplitCount() >= CAP);
        Assert.assertEquals("must not overshoot beyond one round past the cap", 6, provider.rounds);
    }

    @Test
    public void testBreaksWhenRoundProducesNothing() throws Exception {
        // A round that yields no new split (empty RPC / cursor moved) must break, not spin to deadline.
        FakeOffsetProvider provider = new FakeOffsetProvider(0, -1);
        newJob(provider, 3600).advanceSplitsIfNeed();
        Assert.assertEquals("must break after a no-progress round", 1, provider.rounds);
    }

    @Test
    public void testSkipsWhenAlreadyDone() throws Exception {
        // Entry guard: noMoreSplits up front means the loop never runs.
        FakeOffsetProvider provider = new FakeOffsetProvider(1, 0);
        newJob(provider, 3600).advanceSplitsIfNeed();
        Assert.assertEquals("entry guard must skip the loop entirely", 0, provider.rounds);
    }

    /** Minimal provider whose admission behaviour is fully controllable. */
    private static class FakeOffsetProvider implements SourceOffsetProvider {
        private int pending;
        private int rounds;
        private final int stepPerRound;     // pending increment per advanceSplits call
        private final int roundsUntilDone;  // noMoreSplits() flips true after this many rounds (-1 = never)

        FakeOffsetProvider(int stepPerRound, int roundsUntilDone) {
            this.stepPerRound = stepPerRound;
            this.roundsUntilDone = roundsUntilDone;
        }

        @Override
        public void advanceSplits() {
            rounds++;
            pending += stepPerRound;
        }

        @Override
        public int pendingSplitCount() {
            return pending;
        }

        @Override
        public boolean noMoreSplits() {
            return roundsUntilDone >= 0 && rounds >= roundsUntilDone;
        }

        @Override
        public String getSourceType() {
            return "fake";
        }

        @Override
        public Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
            return null;
        }

        @Override
        public String getShowCurrentOffset() {
            return "";
        }

        @Override
        public String getShowMaxOffset() {
            return "";
        }

        @Override
        public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset next, long id) {
            return originCommand;
        }

        @Override
        public void updateOffset(Offset offset) {
        }

        @Override
        public void fetchRemoteMeta(Map<String, String> properties) {
        }

        @Override
        public boolean hasMoreDataToConsume() {
            return true;
        }

        @Override
        public Offset deserializeOffset(String offset) {
            return null;
        }

        @Override
        public Offset deserializeOffsetProperty(String offset) {
            return null;
        }
    }
}
