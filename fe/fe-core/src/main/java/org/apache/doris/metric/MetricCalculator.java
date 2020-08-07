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

package org.apache.doris.metric;

import org.apache.doris.cache.CacheFactory;
import org.apache.doris.cache.CacheStats;

import java.util.List;
import java.util.TimerTask;

/*
 * MetricCalculator will collect and calculate some certain metrics at a fix rate,
 * such QPS, and save the result for users to get.
 */
public class MetricCalculator extends TimerTask {
    private long lastTs = -1;
    private long lastQueryCounter = -1;
    private long lastRequestCounter = -1;
    private long lastQueryErrCounter = -1;
    private CacheStats priorStats = null;

    @Override
    public void run() {
        update();
    }

    private void update() {
        long currentTs = System.currentTimeMillis();
        if (lastTs == -1) {
            lastTs = currentTs;
            lastQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
            lastRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
            lastQueryErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
            return;
        }

        long interval = (currentTs - lastTs) / 1000 + 1;

        // qps
        long currenQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
        double qps = (double) (currenQueryCounter - lastQueryCounter) / interval;
        MetricRepo.GAUGE_QUERY_PER_SECOND.setValue(qps < 0 ? 0.0 : qps);
        lastQueryCounter = currenQueryCounter;

        // rps
        long currentRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
        double rps = (double) (currentRequestCounter - lastRequestCounter) / interval;
        MetricRepo.GAUGE_REQUEST_PER_SECOND.setValue(rps < 0 ? 0.0 : rps);
        lastRequestCounter = currentRequestCounter;

        // err rate
        long currentErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
        double errRate = (double) (currentErrCounter - lastQueryErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ERR_RATE.setValue(errRate < 0 ? 0.0 : errRate);
        lastQueryErrCounter = currentErrCounter;

        // Cache stats
        if (priorStats == null) {
            priorStats = CacheFactory.getUniversalCache().getStats();
            MetricRepo.COUNTER_RESULT_CACHE_ERRORS.increase(priorStats.getNumErrors());
            MetricRepo.COUNTER_RESULT_CACHE_TIMEOUTS.increase(priorStats.getNumTimeouts());
            MetricRepo.COUNTER_RESULT_CACHE_EVICTIONS.increase(priorStats.getNumEvictions());
            MetricRepo.GAUGE_RESULT_CACHE_SIZE_IN_BYTES.setValue(priorStats.getSizeInBytes());
            MetricRepo.GAUGE_RESULT_CACHE_ENTRIES.setValue(priorStats.getNumEntries());
            MetricRepo.COUNTER_RESULT_CACHE_MISSES.increase(priorStats.getNumMisses());
            MetricRepo.COUNTER_RESULT_CACHE_HITS.increase(priorStats.getNumHits());
        } else {
            CacheStats currentStats = CacheFactory.getUniversalCache().getStats();
            MetricRepo.COUNTER_RESULT_CACHE_ERRORS.increase(deltaCounter(priorStats.getNumErrors(), currentStats.getNumErrors()));
            MetricRepo.COUNTER_RESULT_CACHE_TIMEOUTS.increase(deltaCounter(priorStats.getNumTimeouts(), currentStats.getNumTimeouts()));
            MetricRepo.COUNTER_RESULT_CACHE_EVICTIONS.increase(deltaCounter(priorStats.getNumEvictions(), currentStats.getNumEvictions()));
            MetricRepo.GAUGE_RESULT_CACHE_SIZE_IN_BYTES.setValue(currentStats.getSizeInBytes());
            MetricRepo.GAUGE_RESULT_CACHE_ENTRIES.setValue(currentStats.getNumEntries());
            MetricRepo.COUNTER_RESULT_CACHE_MISSES.increase(deltaCounter(priorStats.getNumMisses(), currentStats.getNumMisses()));
            MetricRepo.COUNTER_RESULT_CACHE_HITS.increase(deltaCounter(priorStats.getNumHits(), currentStats.getNumHits()));
            priorStats = currentStats;
        }

        lastTs = currentTs;

        // max tabet compaction score of all backends
        long maxCompactionScore = 0;
        List<Metric> compactionScoreMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_MAX_COMPACTION_SCORE);
        for (Metric metric : compactionScoreMetrics) {
            if (((GaugeMetric<Long>) metric).getValue() > maxCompactionScore) {
                maxCompactionScore = ((GaugeMetric<Long>) metric).getValue();
            }
        }
        MetricRepo.GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(maxCompactionScore);
    }

    /**
     * compute delta value for counter metrics
     *
     * @param prior
     * @param curent
     * @return
     */
    private static long deltaCounter(long prior, long curent) {
        return Math.max(0L, curent - prior);
    }
}
