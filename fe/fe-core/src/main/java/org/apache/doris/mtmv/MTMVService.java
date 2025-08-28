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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.event.DropPartitionEvent;
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

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        LOG.info("registerMTMV: " + mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.registerMTMV(mtmv, dbId);
        }
    }

    public void unregisterMTMV(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        LOG.info("deregisterMTMV: " + mtmv.getName());
        mtmv.writeMvLock();
        try {
            for (MTMVHookService mtmvHookService : hooks.values()) {
                mtmvHookService.unregisterMTMV(mtmv);
            }
        } finally {
            mtmv.writeMvUnlock();
        }
    }

    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException, JobException {
        Objects.requireNonNull(info, "info can not be null");
        LOG.info("refreshMTMV, RefreshMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.refreshMTMV(info);
        }
    }

    public void dropTable(Table table) {
        Objects.requireNonNull(table, "table can not be null");
        LOG.info("dropTable, tableName: {}", table.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.dropTable(table);
        }
    }

    public void alterTable(BaseTableInfo oldTableInfo, Optional<BaseTableInfo> newTableInfo, boolean isReplace) {
        Objects.requireNonNull(oldTableInfo, "oldTableInfo can not be null");
        Objects.requireNonNull(newTableInfo, "newTableInfo can not be null");
        LOG.info("alterTable, oldTableInfo: {}, newTableInfo: {}, isReplace: {}", oldTableInfo, newTableInfo,
                isReplace);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.alterTable(oldTableInfo, newTableInfo, isReplace);
        }
    }

    public void refreshComplete(MTMV mtmv, MTMVRelation cache, MTMVTask task) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        Objects.requireNonNull(task, "task can not be null");
        LOG.info("refreshComplete: " + mtmv.getName());
        mtmv.writeMvLock();
        try {
            for (MTMVHookService mtmvHookService : hooks.values()) {
                mtmvHookService.refreshComplete(mtmv, cache, task);
            }
        } finally {
            mtmv.writeMvUnlock();
        }
    }

    public void pauseMTMV(PauseMTMVInfo info) throws DdlException, MetaNotFoundException, JobException {
        Objects.requireNonNull(info, "info can not be null");
        LOG.info("pauseMTMV, PauseMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.pauseMTMV(info);
        }
    }

    public void resumeMTMV(ResumeMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {
        Objects.requireNonNull(info, "info can not be null");
        LOG.info("resumeMTMV, ResumeMTMVInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.resumeMTMV(info);
        }
    }

    public void cancelMTMVTask(CancelMTMVTaskInfo info) throws MetaNotFoundException, DdlException, JobException {
        Objects.requireNonNull(info, "info can not be null");
        LOG.info("cancelMTMVTask, CancelMTMVTaskInfo: {}", info);
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.cancelMTMVTask(info);
        }
    }

    @Override
    public void processEvent(Event event) throws EventException {
        Objects.requireNonNull(event, "event can not be null");
        if (!(event instanceof TableEvent)) {
            return;
        }
        if (event instanceof DropPartitionEvent && ((DropPartitionEvent) event).isTempPartition()) {
            return;
        }
        TableEvent tableEvent = (TableEvent) event;
        LOG.info("processEvent, Event: {}", event);
        TableIf table;
        try {
            table = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(tableEvent.getCtlName())
                    .getDbOrAnalysisException(tableEvent.getDbName())
                    .getTableOrAnalysisException(tableEvent.getTableName());
        } catch (AnalysisException e) {
            throw new EventException(e);
        }
        Set<BaseTableInfo> mtmvs = relationManager.getMtmvsByBaseTableOneLevel(
                new BaseTableInfo(table));
        for (BaseTableInfo baseTableInfo : mtmvs) {
            try {
                // check if mtmv should trigger by event
                MTMV mtmv = (MTMV) MTMVUtil.getTable(baseTableInfo);
                if (shouldRefreshOnBaseTableDataChange(mtmv, table)) {
                    jobManager.onCommit(mtmv);
                }
            } catch (Exception e) {
                throw new EventException(e);
            }
        }
    }

    private boolean shouldRefreshOnBaseTableDataChange(MTMV mtmv, TableIf table) {
        TableName tableName = null;
        try {
            tableName = new TableName(table);
        } catch (AnalysisException e) {
            LOG.warn("skip refresh mtmv: {}, because get TableName failed: {}",
                    mtmv.getName(), table.getName());
            return false;
        }
        if (MTMVPartitionUtil.isTableExcluded(mtmv.getExcludedTriggerTables(), tableName)) {
            LOG.info("skip refresh mtmv: {}, because exclude trigger table: {}",
                    mtmv.getName(), table.getName());
            return false;
        }
        return mtmv.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger().equals(RefreshTrigger.COMMIT);
    }

    public void createJob(MTMV mtmv, boolean isReplay) {
        jobManager.createJob(mtmv, isReplay);
    }

    public void dropJob(MTMV mtmv, boolean isReplay) {
        jobManager.dropJob(mtmv, isReplay);
    }

    public void alterJob(MTMV mtmv, boolean isReplay) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        LOG.info("alterMTMV, mtmvName: {}", mtmv.getName());
        jobManager.alterJob(mtmv, isReplay);
    }

    public void postCreateMTMV(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        LOG.info("postCreateMTMV, mtmvName: {}", mtmv.getName());
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.postCreateMTMV(mtmv);
        }
    }
}
