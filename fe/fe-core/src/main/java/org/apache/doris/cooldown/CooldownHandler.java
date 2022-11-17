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
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.thrift.TCooldownType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

public class CooldownHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CooldownHandler.class);
    private static final int MAX_ACTIVE_COOLDOWN_JOB_SIZE = 10;

    private static final int MAX_RUNABLE_COOLDOWN_JOB_SIZE = 100;

    private static final long timeoutMs = 1000L * Config.push_cooldown_conf_timeout_second;

    // tabletId -> CooldownJob, it is used to hold CooldownJob which is used to sent conf to be.
    private final Map<Long, CooldownJob> runableCooldownJobs = Maps.newConcurrentMap();
    // tabletId -> CooldownJob,
    public final Map<Long, CooldownJob> activeCooldownJobs = Maps.newConcurrentMap();

    public final ThreadPoolExecutor cooldownThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(
            MAX_ACTIVE_COOLDOWN_JOB_SIZE, "cooldown-pool", true);

    // syncCooldownTabletMap: tabletId -> TabletMeta
    public void handleCooldownConf(Map<Long, TabletMeta> syncCooldownTabletMap) {
        for (Map.Entry<Long, TabletMeta> entry : syncCooldownTabletMap.entrySet()) {
            if (runableCooldownJobs.size() >= MAX_RUNABLE_COOLDOWN_JOB_SIZE) {
                return;
            }
            Long tabletId = entry.getKey();
            TabletMeta tabletMeta = entry.getValue();
            if (runableCooldownJobs.containsKey(tabletId)) {
                continue;
            }
            List<Replica> replicas = Env.getCurrentInvertedIndex().getReplicas(tabletId);
            if (replicas.size() == 0) {
                continue;
            }
            long replicaId = -1;
            for (Replica replica : replicas) {
                if (replica.getCooldownType() == TCooldownType.UPLOAD_DATA) {
                    replicaId = replica.getId();
                }
            }
            // All replica has no UPLOAD_DATA cooldown type.
            if (replicaId == -1) {
                Random rand = new Random(System.currentTimeMillis());
                int index = rand.nextInt(replicas.size());
                replicaId = replicas.get(index).getId();
            }
            long jobId = Env.getCurrentEnv().getNextId();
            CooldownJob cooldownJob = new CooldownJob(jobId, tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletId, replicaId,
                    TCooldownType.UPLOAD_DATA, timeoutMs);
            runableCooldownJobs.put(tabletId, cooldownJob);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        clearFinishedOrCancelledCooldownJob();
        runableCooldownJobs.values().forEach(cooldownJob -> {
            if (!cooldownJob.isDone() && !activeCooldownJobs.containsKey(cooldownJob.getTabletId())
                    && activeCooldownJobs.size() < MAX_ACTIVE_COOLDOWN_JOB_SIZE) {
                if (FeConstants.runningUnitTest) {
                    cooldownJob.run();
                } else {
                    cooldownThreadPool.submit(() -> {
                        if (activeCooldownJobs.putIfAbsent(cooldownJob.getTabletId(), cooldownJob) == null) {
                            try {
                                cooldownJob.run();
                            } finally {
                                activeCooldownJobs.remove(cooldownJob.getTabletId());
                            }
                        }
                    });
                }
            }
        });
    }

    public void readField(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            CooldownJob cooldownJob = CooldownJob.read(in);
            replayCooldownJob(cooldownJob);
        }
    }

    public void replayCooldownJob(CooldownJob cooldownJob) {
        if (!cooldownJob.isDone() && !runableCooldownJobs.containsKey(cooldownJob.getTabletId())) {
            CooldownJob replayCooldownJob = new CooldownJob(cooldownJob.jobId, cooldownJob.dbId, cooldownJob.tableId,
                    cooldownJob.partitionId, cooldownJob.indexId, cooldownJob.tabletId, cooldownJob.replicaId,
                    cooldownJob.cooldownType, cooldownJob.timeoutMs);
            replayCooldownJob.replay(cooldownJob);
            runableCooldownJobs.put(cooldownJob.getTabletId(), cooldownJob);
        }
    }

    private void clearFinishedOrCancelledCooldownJob() {
        Iterator<Map.Entry<Long, CooldownJob>> iterator = runableCooldownJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            CooldownJob cooldownJob = iterator.next().getValue();
            if (cooldownJob.isDone()) {
                iterator.remove();
            }
        }
    }
}
