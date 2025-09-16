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
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
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
        cooldownConfs.forEach(conf -> cooldownConfToUpdate.put(conf.getTabletId(), conf));
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

        Map<Long, Tablet> tabletMap = new HashMap<>(); // cache tablet

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (CooldownConf conf : confToUpdate) {
            // choose cooldown replica from alive replicas
            List<Replica> replicas = invertedIndex.getReplicas(conf.getTabletId()).stream().filter(r -> r.isAlive())
                    .collect(Collectors.toList());
            if (replicas.isEmpty()) {
                continue;
            }
            Random rand = new SecureRandom();
            int index = rand.nextInt(replicas.size());
            conf.setCooldownReplicaId(replicas.get(index).getId());
            // find TabletMeta to get cooldown term
            Tablet tablet = getTablet(conf);
            if (tablet == null || tablet.getCooldownConf().second != conf.cooldownTerm) {
                // If tablet.cooldownTerm != conf.cooldownTerm, means cooldown conf of this tablet has been updated,
                // should skip this update.
                continue;
            }
            ++conf.cooldownTerm;
            updatedConf.add(conf);
            tabletMap.put(conf.tabletId, tablet);
        }

        // write editlog
        CooldownConfList list = new CooldownConfList(updatedConf);
        Env.getCurrentEnv().getEditLog().logUpdateCooldownConf(list);

        // update Tablet
        for (CooldownConf conf : updatedConf) {
            Tablet tablet = tabletMap.get(conf.tabletId);
            tablet.setCooldownConf(conf.cooldownReplicaId, conf.cooldownTerm);
            LOG.info("update cooldown conf. tabletId={} cooldownReplicaId={} cooldownTerm={}", conf.tabletId,
                    conf.cooldownReplicaId, conf.cooldownTerm);
        }

        // update finish, remove from map
        confToUpdate.forEach(conf -> cooldownConfToUpdate.remove(conf.getTabletId()));

        // TODO(plat1ko): push CooldownConf to BE?
    }

    private static Tablet getTablet(CooldownConf conf) {
        try {
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(conf.dbId)
                    .getTable(conf.tableId)
                    .get();
            table.readLock();
            try {
                return table.getPartition(conf.partitionId).getIndex(conf.indexId).getTablet(conf.tabletId);
            } finally {
                table.readUnlock();
            }
        } catch (RuntimeException e) {
            if (Env.getCurrentRecycleBin().isRecyclePartition(conf.dbId, conf.tableId, conf.partitionId)) {
                LOG.debug("failed to get tablet, it's in catalog recycle bin. tabletId={}", conf.tabletId);
            } else {
                LOG.warn("failed to get tablet. tabletId={}", conf.tabletId);
            }
            return null;
        }
    }

    public static void replayUpdateCooldownConf(CooldownConfList cooldownConfList) {
        cooldownConfList.getCooldownConf().forEach(conf -> {
            Tablet tablet = getTablet(conf);
            if (tablet != null) {
                tablet.setCooldownConf(conf.cooldownReplicaId, conf.cooldownTerm);
            }
        });
    }
}
