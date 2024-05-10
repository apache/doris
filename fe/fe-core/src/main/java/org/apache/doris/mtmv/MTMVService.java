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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.event.Event;
import org.apache.doris.event.EventException;
import org.apache.doris.event.EventListener;
import org.apache.doris.event.TableEvent;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.persist.AlterMTMV;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MTMVService implements EventListener {
    private static final Logger LOG = LogManager.getLogger(MTMVService.class);

    private Map<String, MTMVHookService> hooks = Maps.newConcurrentMap();
    private MTMVRelationManager relationManager = new MTMVRelationManager();
    private MTMVJobManager jobManager = new MTMVJobManager();

    public MTMVService() {
        registerHook("MTMVJobManager", jobManager);
        registerHook("MTMVRelationManager", relationManager);
    }

    public MTMVRelationManager getRelationManager() {
        return relationManager;
    }

    public void registerHook(String name, MTMVHookService mtmvHookService) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(mtmvHookService);
        hooks.put(name, mtmvHookService);
        LOG.info("registerHook: " + name);
    }

    public void deregisterHook(String name) {
        hooks.remove(name);
        LOG.info("deregisterHook: " + name);
    }

    public void registerMTMV(MTMV mtmv, Long dbId) {
        Objects.requireNonNull(mtmv);
        LOG.info("registerMTMV: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.registerMTMV(mtmv, dbId);
        }
    }

    public void deregisterMTMV(MTMV mtmv) {
        Objects.requireNonNull(mtmv);
        LOG.info("deregisterMTMV: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.deregisterMTMV(mtmv);
        }
    }

    public void createMTMV(MTMV mtmv) throws DdlException, AnalysisException {
        Objects.requireNonNull(mtmv);
        LOG.info("createMTMV: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.createMTMV(mtmv);
        }
    }

    public void dropMTMV(MTMV mtmv) throws DdlException {
        Objects.requireNonNull(mtmv);
        LOG.info("dropMTMV: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.dropMTMV(mtmv);
        }
    }

    public void alterMTMV(MTMV mtmv, AlterMTMV alterMTMV) throws DdlException {
        Objects.requireNonNull(mtmv);
        Objects.requireNonNull(alterMTMV);
        LOG.info("alterMTMV, mtmvName: {}, AlterMTMV: {}", mtmv.getName(), alterMTMV);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.alterMTMV(mtmv, alterMTMV);
        }
    }

    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException, JobException {
        Objects.requireNonNull(info);
        LOG.info("refreshMTMV, RefreshMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.refreshMTMV(info);
        }
    }

    public void dropTable(Table table) {
        Objects.requireNonNull(table);
        LOG.info("dropTable, tableName: {}", table.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.dropTable(table);
        }
    }

    public void alterTable(Table table) {
        Objects.requireNonNull(table);
        LOG.info("alterTable, tableName: {}", table.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.alterTable(table);
        }
    }

    public void refreshComplete(MTMV mtmv, MTMVRelation cache, MTMVTask task) {
        Objects.requireNonNull(mtmv);
        Objects.requireNonNull(task);
        LOG.info("refreshComplete: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.refreshComplete(mtmv, cache, task);
        }
    }

    public void pauseMTMV(PauseMTMVInfo info) throws DdlException, MetaNotFoundException, JobException {
        Objects.requireNonNull(info);
        LOG.info("pauseMTMV, PauseMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.pauseMTMV(info);
        }
    }

    public void resumeMTMV(ResumeMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {
        Objects.requireNonNull(info);
        LOG.info("resumeMTMV, ResumeMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.resumeMTMV(info);
        }
    }

    public void cancelMTMVTask(CancelMTMVTaskInfo info) throws MetaNotFoundException, DdlException, JobException {
        Objects.requireNonNull(info);
        LOG.info("cancelMTMVTask, CancelMTMVTaskInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.cancelMTMVTask(info);
        }
    }

    @Override
    public void processEvent(Event event) throws EventException {
        Objects.requireNonNull(event);
        if (!(event instanceof TableEvent)) {
            return;
        }
        TableEvent tableEvent = (TableEvent) event;
        LOG.info("processEvent, Event: {}", event);
        Set<BaseTableInfo> mtmvs = relationManager.getMtmvsByBaseTableOneLevel(
                new BaseTableInfo(tableEvent.getTableId(), tableEvent.getDbId(), tableEvent.getCtlId()));
        for (BaseTableInfo baseTableInfo : mtmvs) {
            try {
                // check if mtmv should trigger by event
                MTMV mtmv = MTMVUtil.getMTMV(baseTableInfo.getDbId(), baseTableInfo.getTableId());
                if (mtmv.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger().equals(RefreshTrigger.COMMIT)) {
                    jobManager.onCommit(mtmv);
                }
            } catch (Exception e) {
                throw new EventException(e);
            }
        }
    }
}
