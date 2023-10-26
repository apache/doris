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

import org.apache.doris.catalog.BaseTableInfo;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.MTMVState;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.AlterMTMV;

import com.google.common.collect.Maps;

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

    public void registerMTMV(MaterializedView materializedView) {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.registerMTMV(materializedView);
        }
    }

    public void deregisterMTMV(MaterializedView materializedView) {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.deregisterMTMV(materializedView);
        }
    }

    public void createMTMV(MaterializedView materializedView) throws DdlException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.createMTMV(materializedView);
        }
    }

    public void dropMTMV(MaterializedView materializedView) {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.dropMTMV(materializedView);
        }
    }

    public void alterMTMV(MaterializedView materializedView, AlterMTMV alterMTMV) throws DdlException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.alterMTMV(materializedView, alterMTMV);
        }
    }

    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {
        for (MTMVHookService mtmvHookService : hooks.values()) {
            mtmvHookService.refreshMTMV(info);
        }
    }

    public void dropTable(Table table) throws UserException {
        processBaseTableChange(table, "The base table has been deleted:");
    }

    public void alterTable(Table table) throws UserException {
        processBaseTableChange(table, "The base table has been updated:");
    }

    private void processBaseTableChange(Table table, String msgPrefix) throws UserException {
        DatabaseIf database = table.getDatabase();
        BaseTableInfo baseTableInfo = new BaseTableInfo(database.getCatalog().getName(), database.getFullName(),
                table.getName());
        Set<MaterializedView> mtmvsByBaseTable = cacheManager.getMTMVSByBaseTable(baseTableInfo);
        for (MaterializedView materializedView : mtmvsByBaseTable) {
            TableNameInfo tableNameInfo = new TableNameInfo(materializedView.getQualifiedDbName(),
                    materializedView.getName());
            MTMVStatus status = new MTMVStatus(MTMVState.SCHEMA_CHANGE,
                    msgPrefix + baseTableInfo);
            Env.getCurrentEnv().alterMTMVStatus(tableNameInfo, status);
        }
    }
}
