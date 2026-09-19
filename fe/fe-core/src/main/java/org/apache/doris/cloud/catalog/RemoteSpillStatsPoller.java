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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Polls the bytes of query spill held in object storage (spill_storage_type=s3) from the alive
 * backends of all clusters, so that SHOW DATA reads them from memory. Runs on every FE on its own
 * schedule (cloud_spill_stats_poll_interval_second): the freshness of the value must not depend on
 * how long a round of the tablet stats takes.
 */
public class RemoteSpillStatsPoller extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(RemoteSpillStatsPoller.class);

    private static final int RPC_TIMEOUT_SECOND = 5;

    /** One successful poll: the value and when it was fetched. */
    private static final class RemoteSpillStats {
        private final long bytes;
        private final long fetchTimeMs;

        private RemoteSpillStats(long bytes, long fetchTimeMs) {
            this.bytes = bytes;
            this.fetchTimeMs = fetchTimeMs;
        }
    }

    // Summed over the alive BEs of all clusters. Null until the first successful poll. A BE that is
    // gone no longer contributes: its leftover objects are removed by its restart or by the
    // meta-service recycler and are not counted meanwhile.
    private volatile RemoteSpillStats remoteSpillStats = null;

    public RemoteSpillStatsPoller() {
        super("remote spill stats poller", pollIntervalMs());
    }

    private static long pollIntervalMs() {
        return Math.max(1, Config.cloud_spill_stats_poll_interval_second) * 1000L;
    }

    /**
     * A value older than this is not served: the configured max age, but at least three poll
     * intervals so that a longer interval cannot make every value stale.
     */
    @VisibleForTesting
    static long maxAgeSecond() {
        return Math.max(Config.cloud_spill_stats_max_age_second,
                3L * Math.max(1, Config.cloud_spill_stats_poll_interval_second));
    }

    @Override
    protected void runAfterCatalogReady() {
        refresh();
        // The interval is mutable.
        setInterval(pollIntervalMs());
    }

    private void refresh() {
        List<Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
        } catch (AnalysisException e) {
            LOG.warn("failed to list the backends for the remote spill stats", e);
            return;
        }
        InternalService.PGetBeResourceRequest request = InternalService.PGetBeResourceRequest.newBuilder().build();
        List<Pair<Backend, Future<InternalService.PGetBeResourceResponse>>> futures = new ArrayList<>();
        for (Backend be : backends) {
            if (!be.isAlive()) {
                continue;
            }
            futures.add(Pair.of(be, BackendServiceProxy.getInstance()
                    .getBeResourceAsync(be.getBrpcAddress(), RPC_TIMEOUT_SECOND, request)));
        }
        // Any failure keeps the previous value: a partial sum would under-report a billing input,
        // and getRemoteSpillBytes() refuses a value that stays stale for too long.
        long totalBytes = 0;
        for (Pair<Backend, Future<InternalService.PGetBeResourceResponse>> beFuture : futures) {
            if (beFuture.second == null) {
                LOG.warn("failed to send get_be_resource to backend {}", beFuture.first.getId());
                return;
            }
            try {
                InternalService.PGetBeResourceResponse response =
                        beFuture.second.get(RPC_TIMEOUT_SECOND, TimeUnit.SECONDS);
                if (!response.hasStatus() || new Status(response.getStatus()).getErrorCode() != TStatusCode.OK) {
                    LOG.warn("get_be_resource of backend {} failed: {}", beFuture.first.getId(),
                            response.hasStatus() ? response.getStatus().getErrorMsgsList() : "no status");
                    return;
                }
                totalBytes += response.getGlobalBeResourceUsage().getRemoteSpillBytes();
            } catch (Exception e) {
                LOG.warn("get_be_resource of backend {} failed", beFuture.first.getId(), e);
                return;
            }
        }
        remoteSpillStats = new RemoteSpillStats(totalBytes, System.currentTimeMillis());
    }

    @VisibleForTesting
    void setRemoteSpillStatsForTest(long bytes, long fetchTimeMs) {
        remoteSpillStats = new RemoteSpillStats(bytes, fetchTimeMs);
    }

    /**
     * Bytes of query spill currently held in object storage, as last polled from the backends.
     * This is a billing input, so a value that is missing or older than maxAgeSecond() is reported
     * as an error instead of being shown as current.
     */
    public long getRemoteSpillBytes() throws AnalysisException {
        RemoteSpillStats stats = remoteSpillStats;
        if (stats == null) {
            throw new AnalysisException("spill stats have not been polled from the backends yet");
        }
        long ageSecond = (System.currentTimeMillis() - stats.fetchTimeMs) / 1000;
        long maxAgeSecond = maxAgeSecond();
        if (ageSecond > maxAgeSecond) {
            throw new AnalysisException(String.format("spill stats polled from the backends are stale: "
                    + "last fetched %d seconds ago, limit %d seconds (the larger of "
                    + "cloud_spill_stats_max_age_second and 3 * cloud_spill_stats_poll_interval_second)",
                    ageSecond, maxAgeSecond));
        }
        return stats.bytes;
    }
}
