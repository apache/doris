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
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

public class CooldownHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CooldownHandler.class);
    private static final int MAX_ACTIVE_COOLDOWN_JOB_SIZE = 10;

    private static final int MAX_RUNABLE_COOLDOWN_JOB_SIZE = 100;

    private static final int MAX_TABLET_PER_JOB = 100;

    private static final long timeoutMs = 1000L * Config.push_cooldown_conf_timeout_second;

    // jobId -> CooldownJob, it is used to hold CooldownJob which is used to sent conf to be.
    private final Map<Long, CooldownJob> runableCooldownJobs = Maps.newConcurrentMap();
    private final Map<Long, Boolean> resetingTablet = Maps.newConcurrentMap();
    // jobId -> CooldownJob,
    public final Map<Long, CooldownJob> activeCooldownJobs = Maps.newConcurrentMap();

    public final ThreadPoolExecutor cooldownThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(
            MAX_ACTIVE_COOLDOWN_JOB_SIZE, "cooldown-pool", true);

    // syncCooldownTabletMap: tabletId -> TabletMeta
    public void handleCooldownConf(Map<Long, TabletMeta> syncCooldownTabletMap) {
        List<CooldownConf> cooldownConfList = new LinkedList<>();
        for (Map.Entry<Long, TabletMeta> entry : syncCooldownTabletMap.entrySet()) {
            if (runableCooldownJobs.size() >= MAX_RUNABLE_COOLDOWN_JOB_SIZE) {
                return;
            }
            Long tabletId = entry.getKey();
            TabletMeta tabletMeta = entry.getValue();
            if (resetingTablet.containsKey(tabletId)) {
                continue;
            }
            long cooldownReplicaId = -1;
            List<Replica> replicas = Env.getCurrentInvertedIndex().getReplicas(tabletId);
            if (replicas.size() == 0) {
                continue;
            }
            for (Replica replica : replicas) {
                if (tabletMeta.getCooldownReplicaId() == replica.getId()) {
                    cooldownReplicaId = tabletMeta.getCooldownReplicaId();
                    break;
                }
            }
            long cooldownTerm = tabletMeta.getCooldownTerm();
            if (cooldownReplicaId == -1) {
                Random rand = new Random(System.currentTimeMillis());
                int index = rand.nextInt(replicas.size());
                cooldownReplicaId = replicas.get(index).getId();
                ++cooldownTerm;
            }
            CooldownConf cooldownConf = new CooldownConf(tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletId, cooldownReplicaId, cooldownTerm);
            cooldownConfList.add(cooldownConf);
            if (cooldownConfList.size() >= MAX_TABLET_PER_JOB) {
                long jobId = Env.getCurrentEnv().getNextId();
                CooldownJob cooldownJob = new CooldownJob(jobId, cooldownConfList, timeoutMs);
                runableCooldownJobs.put(jobId, cooldownJob);
                for (CooldownConf conf : cooldownConfList) {
                    resetingTablet.put(conf.getTabletId(), true);
                }
                cooldownConfList = new LinkedList<>();
            }
        }
        if (cooldownConfList.size() > 0) {
            long jobId = Env.getCurrentEnv().getNextId();
            CooldownJob cooldownJob = new CooldownJob(jobId, cooldownConfList, timeoutMs);
            runableCooldownJobs.put(jobId, cooldownJob);
            for (CooldownConf conf : cooldownConfList) {
                resetingTablet.put(conf.getTabletId(), true);
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        clearFinishedOrCancelledCooldownJob();
        runableCooldownJobs.values().forEach(cooldownJob -> {
            if (!cooldownJob.isDone() && !activeCooldownJobs.containsKey(cooldownJob.getJobId())
                    && activeCooldownJobs.size() < MAX_ACTIVE_COOLDOWN_JOB_SIZE) {
                if (FeConstants.runningUnitTest) {
                    cooldownJob.run();
                } else {
                    cooldownThreadPool.submit(() -> {
                        if (activeCooldownJobs.putIfAbsent(cooldownJob.getJobId(), cooldownJob) == null) {
                            try {
                                cooldownJob.run();
                            } finally {
                                activeCooldownJobs.remove(cooldownJob.getJobId());
                            }
                        }
                    });
                }
            }
        });
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(runableCooldownJobs.size());
        for (CooldownJob cooldownJob : runableCooldownJobs.values()) {
            cooldownJob.write(out);
        }
    }

    public void readField(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_115) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                CooldownJob cooldownJob = CooldownJob.read(in);
                if (cooldownJob != null) {
                    replayCooldownJob(cooldownJob);
                }
            }
        }
    }

    public void replayCooldownJob(CooldownJob cooldownJob) {
        CooldownJob replayCooldownJob;
        if (!runableCooldownJobs.containsKey(cooldownJob.getJobId())) {
            replayCooldownJob = new CooldownJob(cooldownJob.jobId, cooldownJob.getCooldownConfList(),
                    cooldownJob.timeoutMs);
            runableCooldownJobs.put(cooldownJob.getJobId(), replayCooldownJob);
            for (CooldownConf cooldownConf : cooldownJob.getCooldownConfList()) {
                resetingTablet.put(cooldownConf.getTabletId(), true);
            }
        } else {
            replayCooldownJob = runableCooldownJobs.get(cooldownJob.getJobId());
        }
        replayCooldownJob.replay(cooldownJob);
        if (replayCooldownJob.isDone()) {
            runableCooldownJobs.remove(cooldownJob.getJobId());
            for (CooldownConf cooldownConf : cooldownJob.getCooldownConfList()) {
                resetingTablet.remove(cooldownConf.getTabletId());
            }
        }
    }

    private void clearFinishedOrCancelledCooldownJob() {
        Iterator<Map.Entry<Long, CooldownJob>> iterator = runableCooldownJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            CooldownJob cooldownJob = iterator.next().getValue();
            if (cooldownJob.isDone()) {
                iterator.remove();
                for (CooldownConf cooldownConf : cooldownJob.getCooldownConfList()) {
                    resetingTablet.remove(cooldownConf.getTabletId());
                }
            }
        }
    }
}
