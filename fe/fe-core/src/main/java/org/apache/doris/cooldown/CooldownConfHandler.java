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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class CooldownConfHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CooldownConfHandler.class);

    // TODO(plat1ko): better to use `Condition`?
    private static final long INTERVAL_MS = 5000; // 5s
    private static final int UPDATE_BATCH_SIZE = 512;

    private final Map<Long, CooldownConf> cooldownConfToUpdate = Maps.newConcurrentMap();

    public CooldownConfHandler() {
        super("CooldownConfHandler", INTERVAL_MS);
    }

    public void addCooldownConfToUpdate(List<CooldownConf> cooldownConfs) {
        cooldownConfs.stream().map(conf -> cooldownConfToUpdate.putIfAbsent(conf.getTabletId(), conf));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (cooldownConfToUpdate.isEmpty()) {
            return;
        }
        List<CooldownConf> cooldownConfList = cooldownConfToUpdate.values().stream().collect(Collectors.toList());
        for (int start = 0; start < cooldownConfList.size(); start += UPDATE_BATCH_SIZE) {
            updateCooldownConf(
                    cooldownConfList.subList(start, Math.min(start + UPDATE_BATCH_SIZE, cooldownConfList.size())));
        }
    }

    private void updateCooldownConf(List<CooldownConf> confToUpdate) {
        ArrayList<CooldownConf> updatedConf = new ArrayList<>();
        updatedConf.ensureCapacity(confToUpdate.size());

        for (CooldownConf conf : confToUpdate) {
            // choose cooldown replica
            List<Replica> replicas = Env.getCurrentInvertedIndex().getReplicas(conf.getTabletId());
            if (replicas.isEmpty()) {
                continue;
            }
            Random rand = new Random(System.currentTimeMillis());
            int index = rand.nextInt(replicas.size());
            conf.setCooldownReplicaId(replicas.get(index).getId());
            // find TabletMeta to get cooldown term
            TabletMeta tablet = Env.getCurrentInvertedIndex().getTabletMeta(conf.getTabletId());
            if (tablet == null) {
                continue;
            }
            conf.setCooldownTerm(tablet.getCooldownConf().second + 1);
            updatedConf.add(conf);
        }

        // write editlog
        CooldownConfList list = new CooldownConfList(updatedConf);
        Env.getCurrentEnv().getEditLog().logUpdateCooldownConf(list);

        // update TabletMeta
        // NOTE: There is no need to update cooldown conf in `Tablet`, since CHECKPOINT thread will update cooldown conf
        // in `Tablet` in image by replay editlog. Cooldown conf in `Tablet` is only used to rebuild `TabletMeta` when
        // FE starts.
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (CooldownConf conf : updatedConf) {
            TabletMeta tablet = invertedIndex.getTabletMeta(conf.getTabletId());
            if (tablet == null) {
                continue;
            }
            tablet.setCooldownConf(conf.cooldownReplicaId, conf.cooldownTerm);
            LOG.info("update cooldown conf. tabletId={} cooldownReplicaId={} cooldownTerm={}", conf.tabletId,
                    conf.cooldownReplicaId, conf.cooldownTerm);
        }

        // update finish, remove from map
        confToUpdate.stream().map(conf -> cooldownConfToUpdate.remove(conf.getTabletId()));

        // TODO(plat1ko): push CooldownConf to BE?
    }

    public static void replayUpdateCooldownConf(CooldownConfList cooldownConfList) {
        if (Env.isCheckpointThread()) {
            InternalCatalog catalog = Env.getCurrentInternalCatalog();
            for (CooldownConf conf : cooldownConfList.getCooldownConf()) {
                try {
                    Tablet tablet = ((OlapTable) catalog.getDbNullable(conf.dbId).getTable(conf.tableId)
                            .get()).getPartition(conf.partitionId).getIndex(conf.indexId).getTablet(conf.indexId);
                    tablet.setCooldownReplicaId(conf.cooldownReplicaId);
                    tablet.setCooldownTerm(conf.cooldownTerm);
                } catch (RuntimeException e) {
                    LOG.warn("failed to replayUpdateCooldownConf. tabletId={}", conf.tabletId);
                }
            }
        } else {
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (CooldownConf conf : cooldownConfList.getCooldownConf()) {
                TabletMeta tablet = invertedIndex.getTabletMeta(conf.tabletId);
                if (tablet == null) {
                    LOG.warn("failed to replayUpdateCooldownConf. tabletId={}", conf.tabletId);
                    continue;
                }
                tablet.setCooldownConf(conf.cooldownReplicaId, conf.cooldownTerm);
            }
        }
    }
}
