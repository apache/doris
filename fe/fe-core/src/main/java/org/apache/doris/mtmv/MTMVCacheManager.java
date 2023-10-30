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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class MTMVCacheManager implements MTMVHookService {
    private Map<BaseTableInfo, Set<MTMV>> tableMTMVs = Maps.newConcurrentMap();
    private Map<MTMV, MTMVCache> mtmvCaches = Maps.newConcurrentMap();

    public Set<MTMV> getMtmvsByBaseTable(BaseTableInfo table) {
        return tableMTMVs.get(table);
    }

    public boolean isAvailableMTMV(MTMV mtmv, ConnectContext ctx) throws AnalysisException, DdlException {
        // check session variable if enable rewrite
        if (!ctx.getSessionVariable().isEnableMvRewrite()) {
            return false;
        }
        MTMVCache mtmvCache = mtmvCaches.get(mtmv);
        if (mtmvCache == null) {
            return false;
        }
        // chaek mv is normal
        if (!(mtmv.getStatus().getState() == MTMVState.NORMAL
                && mtmv.getStatus().getRefreshState() == MTMVRefreshState.SUCCESS)) {
            return false;
        }
        // check external table
        boolean containsExternalTable = containsExternalTable(mtmvCache.getBaseTables());
        if (containsExternalTable) {
            return ctx.getSessionVariable().isEnableExternalMvRewrite();
        }
        // check gracePeriod
        Long gracePeriod = mtmv.getGracePeriod();
        // do not care data is delayed
        if (gracePeriod < 0) {
            return true;
        }
        // compare with base table
        Long mtmvLastTime = getTableLastVisibleVersionTime(mtmv);
        Long maxAvailableTime = mtmvLastTime + gracePeriod;
        for (BaseTableInfo baseTableInfo : mtmvCache.getBaseTables()) {
            long tableLastVisibleVersionTime = getTableLastVisibleVersionTime(baseTableInfo);
            if (tableLastVisibleVersionTime > maxAvailableTime) {
                return false;
            }
        }
        return true;
    }

    private long getTableLastVisibleVersionTime(BaseTableInfo baseTableInfo) throws AnalysisException, DdlException {
        Table table = Env.getCurrentEnv().getInternalCatalog()
                .getDbOrAnalysisException(baseTableInfo.getDbName())
                .getTableOrDdlException(baseTableInfo.getTableName(), TableType.OLAP);
        return getTableLastVisibleVersionTime((OlapTable) table);
    }

    private long getTableLastVisibleVersionTime(OlapTable table) {
        long result = 0L;
        long visibleVersionTime;
        for (Partition partition : table.getAllPartitions()) {
            visibleVersionTime = partition.getVisibleVersionTime();
            if (visibleVersionTime > result) {
                result = visibleVersionTime;
            }
        }
        return result;
    }

    private boolean containsExternalTable(Set<BaseTableInfo> baseTableInfos) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(baseTableInfo.getCtlName())) {
                return true;
            }
        }
        return false;
    }

    private MTMVCache generateMTMVCache(MTMV materializedView) {
        // TODO: 2023/10/26 implement this method when transparently rewriting
        return new MTMVCache(Sets.newHashSet(), Sets.newHashSet());
    }

    private Set<MTMV> getOrCreateMTMVs(BaseTableInfo baseTableInfo) {
        if (tableMTMVs.containsKey(baseTableInfo)) {
            return tableMTMVs.get(baseTableInfo);
        } else {
            return tableMTMVs.put(baseTableInfo, Sets.newConcurrentHashSet());
        }
    }

    @Override
    public void createMTMV(MTMV materializedView) {

    }

    @Override
    public void dropMTMV(MTMV materializedView) {

    }

    @Override
    public void registerMTMV(MTMV materializedView) {
        MTMVCache mtmvCache = generateMTMVCache(materializedView);
        mtmvCaches.put(materializedView, mtmvCache);
        for (BaseTableInfo baseTableInfo : mtmvCache.getBaseTables()) {
            getOrCreateMTMVs(baseTableInfo).add(materializedView);
        }
        for (BaseTableInfo baseTableInfo : mtmvCache.getBaseViews()) {
            getOrCreateMTMVs(baseTableInfo).add(materializedView);
        }
    }

    @Override
    public void deregisterMTMV(MTMV materializedView) {
        if (!mtmvCaches.containsKey(materializedView)) {
            return;
        }
        MTMVCache mtmvCache = mtmvCaches.remove(materializedView);
        for (BaseTableInfo baseTableInfo : mtmvCache.getBaseTables()) {
            getOrCreateMTMVs(baseTableInfo).remove(materializedView);
        }
        for (BaseTableInfo baseTableInfo : mtmvCache.getBaseViews()) {
            getOrCreateMTMVs(baseTableInfo).remove(materializedView);
        }
    }

    @Override
    public void alterMTMV(MTMV materializedView, AlterMTMV alterMTMV) {

    }

    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {

    }
}
