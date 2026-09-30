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
 * and the per-batch removal cap; a full batch repeats until the expired backlog
 * drains (bounded batches per round); a manager failure is swallowed so the daemon
 * survives to its next round; and the second-to-millisecond conversion saturates
 * instead of overflowing a validator-legal Long.MAX_VALUE into a negative window.
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

    @Test
    public void cleanRoundDrainsAFullBacklogInRepeatedBatches() {
        // One batch per round removes less than the dispatcher can resolve per hour,
        // so a full batch must repeat: the round keeps batching until a batch comes
        // back under the cap, draining the expired backlog instead of leaving a
        // growing residue.
        java.util.List<Long> full = new java.util.ArrayList<>();
        for (long id = 0; id < 1024; id++) {
            full.add(id);
        }
        expectEnv();
        new Expectations() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(anyLong, anyInt);
                minTimes = 0;
                result = full;
                result = Arrays.asList(5000L);
            }
        };
        new LanceIndexJobCleaner().runAfterCatalogReady();
        new Verifications() {
            {
                lanceIndexJobManager.removeResolvedJobsOlderThan(anyLong, 1024);
                times = 2;
            }
        };
    }

    @Test
    public void keepWindowConversionSaturatesInsteadOfOverflowing() {
        // The positive-long validator accepts Long.MAX_VALUE seconds; multiplying it
        // by 1000 must saturate to Long.MAX_VALUE milliseconds (retention effectively
        // forever) instead of wrapping negative and expiring fresh audit records.
        long originalKeep = Config.lance_index_job_keep_max_second;
        Config.lance_index_job_keep_max_second = Long.MAX_VALUE;
        try {
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
                    lanceIndexJobManager.removeResolvedJobsOlderThan(Long.MAX_VALUE, 1024);
                    times = 1;
                }
            };
        } finally {
            Config.lance_index_job_keep_max_second = originalKeep;
        }
    }
}
