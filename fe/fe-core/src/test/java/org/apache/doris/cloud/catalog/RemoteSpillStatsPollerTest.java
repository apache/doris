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

package org.apache.doris.cloud.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoteSpillStatsPollerTest {
    private int savedMaxAge;
    private int savedPollInterval;

    @BeforeEach
    public void setUp() {
        savedMaxAge = Config.cloud_spill_stats_max_age_second;
        savedPollInterval = Config.cloud_spill_stats_poll_interval_second;
        Config.cloud_spill_stats_max_age_second = 300;
        Config.cloud_spill_stats_poll_interval_second = 60;
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_spill_stats_max_age_second = savedMaxAge;
        Config.cloud_spill_stats_poll_interval_second = savedPollInterval;
    }

    @Test
    public void testRemoteSpillBytesNotFetchedYet() {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, poller::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("not been polled"), e.getMessage());
    }

    @Test
    public void testRemoteSpillBytesFresh() throws AnalysisException {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.currentTimeMillis());
        Assertions.assertEquals(12345L, poller.getRemoteSpillBytes());
        // Within the limit: still served.
        poller.setRemoteSpillStatsForTest(67L, System.currentTimeMillis() - 200_000L);
        Assertions.assertEquals(67L, poller.getRemoteSpillBytes());
    }

    @Test
    public void testRemoteSpillBytesStale() {
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.currentTimeMillis() - 301_000L);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, poller::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("stale"), e.getMessage());
        // The limit is mutable: raising it makes the same value acceptable again.
        Config.cloud_spill_stats_max_age_second = 600;
        Assertions.assertDoesNotThrow(poller::getRemoteSpillBytes);
    }

    @Test
    public void testMaxAgeCoversThreePollIntervals() {
        Assertions.assertEquals(300, RemoteSpillStatsPoller.maxAgeSecond());
        // A poll interval longer than a third of the max age cannot make every value stale.
        Config.cloud_spill_stats_poll_interval_second = 600;
        Assertions.assertEquals(1800, RemoteSpillStatsPoller.maxAgeSecond());
        RemoteSpillStatsPoller poller = new RemoteSpillStatsPoller();
        poller.setRemoteSpillStatsForTest(12345L, System.currentTimeMillis() - 700_000L);
        Assertions.assertDoesNotThrow(poller::getRemoteSpillBytes);
    }
}
