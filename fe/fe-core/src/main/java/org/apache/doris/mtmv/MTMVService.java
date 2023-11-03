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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.AlterMTMV;

import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MTMVService {
    private Map<String, MTMVHookService> hooks = Maps.newConcurrentMap();
    private MTMVCacheManager cacheManager = new MTMVCacheManager();
    private MTMVJobManager jobManager = new MTMVJobManager();

    public MTMVService() {
        registerHook("MTMVCacheManager", cacheManager);
        registerHook("MTMVJobManager", jobManager);
    }

    public MTMVCacheManager getCacheManager() {
        return cacheManager;
    }

    public void registerHook(String name, MTMVHookService mtmvHookService) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(mtmvHookService);
        hooks.put(name, mtmvHookService);
    }

    public void deregisterHook(String name) {
        hooks.remove(name);
    }

    // when create mtmv,triggered when playing back logs
    public void registerMTMV(MTMV materializedView) {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.registerMTMV(materializedView);
        }
    }

    // when drop mtmv,triggered when playing back logs
    public void deregisterMTMV(MTMV materializedView) {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.deregisterMTMV(materializedView);
        }
    }

    // when create mtmv,only trigger once
    public void createMTMV(MTMV materializedView) throws DdlException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.createMTMV(materializedView);
        }
    }

    // when drop mtmv,only trigger once
    public void dropMTMV(MTMV materializedView) throws DdlException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.dropMTMV(materializedView);
        }
    }

    // when alter mtmv,only trigger once
    public void alterMTMV(MTMV materializedView, AlterMTMV alterMTMV) throws DdlException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.alterMTMV(materializedView, alterMTMV);
        }
    }

    // when refresh mtmv,only trigger once
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.refreshMTMV(info);
        }
    }

    // when base table is dropped,only trigger once
    public void dropTable(Table table) throws UserException {
        processBaseTableChange(table, "The base table has been deleted:");
    }

    // when base table is Modified,only trigger once
    public void alterTable(Table table) throws UserException {
        processBaseTableChange(table, "The base table has been updated:");
    }

    private void processBaseTableChange(Table table, String msgPrefix) throws UserException {
        BaseTableInfo baseTableInfo = new BaseTableInfo(table);
        Set<MTMV> mtmvsByBaseTable = cacheManager.getMtmvsByBaseTable(baseTableInfo);
        if (CollectionUtils.isEmpty(mtmvsByBaseTable)) {
            return;
        }
        for (MTMV materializedView : mtmvsByBaseTable) {
            TableNameInfo tableNameInfo = new TableNameInfo(materializedView.getQualifiedDbName(),
                    materializedView.getName());
            MTMVStatus status = new MTMVStatus(MTMVState.SCHEMA_CHANGE,
                    msgPrefix + baseTableInfo);
            Env.getCurrentEnv().alterMTMVStatus(tableNameInfo, status);
        }
    }
}
