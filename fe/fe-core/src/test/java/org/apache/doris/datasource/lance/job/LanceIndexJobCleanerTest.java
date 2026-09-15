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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Retention-cleaner wiring coverage for {@link LanceIndexJobCleaner}: every clean
 * round hands the manager the keep window converted from seconds to milliseconds
 * and the per-round removal cap; a manager failure is swallowed so the daemon
 * survives to its next round; and a round that removed jobs only logs the ids.
 */
public class LanceIndexJobCleanerTest {
    @Mocked
    private Env env;
    @Mocked
    private LanceIndexJobManager lanceIndexJobManager;

    private void expectEnv() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getLanceIndexJobManager();
                minTimes = 0;
                result = lanceIndexJobManager;
            }
        };
    }

    @Test
    public void cleanRoundPassesTheKeepWindowInMillisAndThePerRoundCap() {
        expectEnv();
        new Expectations() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(anyLong, anyInt);
                minTimes = 0;
                result = Collections.emptyList();
            }
        };
        new LanceIndexJobCleaner().runAfterCatalogReady();
        new Verifications() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(Config.lance_index_job_keep_max_second * 1000L,
                        1024);
                times = 1;
            }
        };
    }

    @Test
    public void cleanRoundSwallowsAManagerFailure() {
        expectEnv();
        new Expectations() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(anyLong, anyInt);
                minTimes = 0;
                result = new RuntimeException("edit log down");
            }
        };
        // The failure is logged and the round ends normally; nothing propagates.
        new LanceIndexJobCleaner().runAfterCatalogReady();
    }

    @Test
    public void cleanRoundCompletesWhenJobsWereRemoved() {
        expectEnv();
        new Expectations() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(anyLong, anyInt);
                minTimes = 0;
                result = Arrays.asList(3L, 1L);
            }
        };
        new LanceIndexJobCleaner().runAfterCatalogReady();
        new Verifications() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(Config.lance_index_job_keep_max_second * 1000L,
                        1024);
                times = 1;
            }
        };
    }
}
