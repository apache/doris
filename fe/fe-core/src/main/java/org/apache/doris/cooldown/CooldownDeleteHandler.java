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

package org.apache.doris.cooldown;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class CooldownDeleteHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CooldownDeleteHandler.class);
    private static final int MAX_ACTIVE_COOLDOWN_DELETE_JOB_SIZE = 10;

    private static final int MAX_RUNABLE_COOLDOWN_DELETE_JOB_SIZE = 100;

    private static final int MAX_TABLET_PER_JOB = 100;

    private static final long timeoutMs = 1000L * Config.send_cooldown_delete_timeout_second;

    // jobId -> CooldownDeleteJob, it is used to hold CooldownDeleteJob which is used to sent delete_id to be.
    private final Map<Long, CooldownDeleteJob> runableCooldownDeleteJobs = Maps.newConcurrentMap();
    // jobId -> CooldownDeleteJob,
    public final Map<Long, CooldownDeleteJob> activeCooldownDeleteJobs = Maps.newConcurrentMap();

    public final ThreadPoolExecutor cooldownDeleteThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(
            MAX_ACTIVE_COOLDOWN_DELETE_JOB_SIZE, "cooldown-delete-pool", true);

    private void createJob(long beId, List<CooldownDelete> cooldownDeletes) {
        long jobId = Env.getCurrentEnv().getNextId();
        CooldownDeleteJob deleteJob = new CooldownDeleteJob(jobId, beId, cooldownDeletes, timeoutMs);
        runableCooldownDeleteJobs.put(jobId, deleteJob);
    }

    // deleteCooldownTabletMap: beId -> CooldownDelete
    public void handleCooldownDelete(Map<Long, List<CooldownDelete>> deleteCooldownTabletMap) {
        for (Map.Entry<Long, List<CooldownDelete>> entry : deleteCooldownTabletMap.entrySet()) {
            long beId = entry.getKey();
            List<CooldownDelete> cooldownDeletes = new LinkedList<>();
            for (CooldownDelete cooldownDelete : entry.getValue()) {
                cooldownDeletes.add(cooldownDelete);
                if (cooldownDeletes.size() >= MAX_TABLET_PER_JOB) {
                    createJob(beId, cooldownDeletes);
                    if (runableCooldownDeleteJobs.size() >= MAX_RUNABLE_COOLDOWN_DELETE_JOB_SIZE) {
                        return;
                    }
                    cooldownDeletes = new LinkedList<>();
                }
            }
            if (cooldownDeletes.size() > 0) {
                createJob(beId, cooldownDeletes);
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        clearFinishedOrCancelledCooldownDeleteJob();
        runableCooldownDeleteJobs.values().forEach(job -> {
            if (!job.isDone() && !activeCooldownDeleteJobs.containsKey(job.getJobId())
                    && activeCooldownDeleteJobs.size() < MAX_ACTIVE_COOLDOWN_DELETE_JOB_SIZE) {
                if (FeConstants.runningUnitTest) {
                    job.run();
                } else {
                    cooldownDeleteThreadPool.submit(() -> {
                        if (activeCooldownDeleteJobs.putIfAbsent(job.getJobId(), job) == null) {
                            try {
                                job.run();
                            } finally {
                                activeCooldownDeleteJobs.remove(job.getJobId());
                            }
                        }
                    });
                }
            }
        });
    }

    private void clearFinishedOrCancelledCooldownDeleteJob() {
        Iterator<Map.Entry<Long, CooldownDeleteJob>> iterator = runableCooldownDeleteJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            CooldownDeleteJob deleteJob = iterator.next().getValue();
            if (deleteJob.isDone()) {
                iterator.remove();
            }
        }
    }
}
