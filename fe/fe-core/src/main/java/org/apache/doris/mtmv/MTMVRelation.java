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

import org.apache.doris.datasource.CatalogMgr;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class MTMVRelation {
    private static final Logger LOG = LogManager.getLogger(MTMVRelation.class);

    // if mtmv query sql is `select * from view1`;
    // and `view1` query sql is `select * from table1 join table2`
    // then baseTables will include: `table1` and `table2`
    // baseViews will include `view1`
    @SerializedName("bt")
    @Deprecated
    private Set<BaseTableInfo> baseTablesId;
    @SerializedName("bv")
    @Deprecated
    private Set<BaseTableInfo> baseViewsId;
    @SerializedName("btol")
    @Deprecated
    private Set<BaseTableInfo> baseTablesOneLevelId;

    @SerializedName("btn")
    private Set<BaseTableNameInfo> baseTables;
    @SerializedName("bvn")
    private Set<BaseTableNameInfo> baseViews;
    @SerializedName("btoln")
    private Set<BaseTableNameInfo> baseTablesOneLevel;

    public MTMVRelation(Set<BaseTableNameInfo> baseTables, Set<BaseTableNameInfo> baseTablesOneLevel,
            Set<BaseTableNameInfo> baseViews) {
        this.baseTables = baseTables;
        this.baseTablesOneLevel = baseTablesOneLevel;
        this.baseViews = baseViews;
    }

    public Set<BaseTableNameInfo> getBaseTables() {
        return baseTables;
    }

    public Set<BaseTableNameInfo> getBaseTablesOneLevel() {
        // For compatibility, previously created MTMV may not have baseTablesOneLevel
        return baseTablesOneLevel == null ? baseTables : baseTablesOneLevel;
    }

    @Deprecated
    public Set<BaseTableInfo> getBaseTablesOneLevelId() {
        // For compatibility, previously created MTMV may not have baseTablesOneLevel
        return baseTablesOneLevelId == null ? baseTablesId : baseTablesOneLevelId;
    }

    public Set<BaseTableNameInfo> getBaseViews() {
        return baseViews;
    }

    // toString() is not easy to find where to call the method
    public String toInfoString() {
        return "MTMVRelation{"
                + "baseTables=" + baseTables
                + ", baseTablesOneLevel=" + baseTablesOneLevel
                + ", baseViews=" + baseViews
                + '}';
    }

    public void compatible(CatalogMgr catalogMgr) {
        if (baseTables == null) {
            baseTables = Sets.newHashSet();
        }
        if (baseViews == null) {
            baseViews = Sets.newHashSet();
        }
        if (baseTablesOneLevel == null) {
            baseTablesOneLevel = Sets.newHashSet();
        }
        compatibleIds(catalogMgr, baseTablesId, baseTables);
        compatibleIds(catalogMgr, baseViewsId, baseViews);
        compatibleIds(catalogMgr, baseTablesOneLevelId, baseTablesOneLevel);
    }

    private void compatibleIds(CatalogMgr catalogMgr, Set<BaseTableInfo> ids, Set<BaseTableNameInfo> names) {
        if (CollectionUtils.isEmpty(ids)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : ids) {
            try {
                BaseTableNameInfo baseTableNameInfo = MTMVUtil.transferIdToName(baseTableInfo, catalogMgr);
                names.add(baseTableNameInfo);
            } catch (Throwable e) {
                LOG.warn("can not transfer tableId to tableInfo: {}, "
                        + "may be cause by catalog/db/table dropped, we need rebuild MTMV", baseTableInfo, e);
            }

        }
        ids = null;
    }
}
