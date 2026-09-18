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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CloudTabletStatMgrTest {
    private int savedMaxAge;

    @BeforeEach
    public void setUp() {
        savedMaxAge = Config.cloud_spill_stats_max_age_second;
        Config.cloud_spill_stats_max_age_second = 300;
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_spill_stats_max_age_second = savedMaxAge;
    }

    @Test
    public void testRemoteSpillBytesNotFetchedYet() {
        CloudTabletStatMgr mgr = new CloudTabletStatMgr();
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, mgr::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("not been polled"), e.getMessage());
    }

    @Test
    public void testRemoteSpillBytesFresh() throws AnalysisException {
        CloudTabletStatMgr mgr = new CloudTabletStatMgr();
        mgr.setRemoteSpillStatsForTest(12345L, System.currentTimeMillis());
        Assertions.assertEquals(12345L, mgr.getRemoteSpillBytes());
        // Within the limit: still served.
        mgr.setRemoteSpillStatsForTest(67L, System.currentTimeMillis() - 200_000L);
        Assertions.assertEquals(67L, mgr.getRemoteSpillBytes());
    }

    @Test
    public void testRemoteSpillBytesStale() {
        CloudTabletStatMgr mgr = new CloudTabletStatMgr();
        mgr.setRemoteSpillStatsForTest(12345L, System.currentTimeMillis() - 301_000L);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, mgr::getRemoteSpillBytes);
        Assertions.assertTrue(e.getMessage().contains("stale"), e.getMessage());
        // The limit is mutable: raising it makes the same value acceptable again.
        Config.cloud_spill_stats_max_age_second = 600;
        Assertions.assertDoesNotThrow(mgr::getRemoteSpillBytes);
    }
}
