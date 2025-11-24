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

import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Set;

public class MTMVRelation {
    // t1 => v1 => v2
    // t2 => mv1
    // mv1 join v2 => mv2
    //
    // data of mv2 is:
    //
    // baseTables => mv1,t1,t2
    // baseTablesOneLevelAndFromView => mv1,t1
    // baseTablesOneLevel => mv1
    // baseViews => v2,v1
    // baseViewsOneLevel => v2
    @SerializedName("bt")
    private Set<BaseTableInfo> baseTables;
    @SerializedName("bv")
    private Set<BaseTableInfo> baseViews;
    @SerializedName("btol")
    private Set<BaseTableInfo> baseTablesOneLevel;
    @SerializedName("btolafv")
    private Set<BaseTableInfo> baseTablesOneLevelAndFromView;
    @SerializedName("bvol")
    private Set<BaseTableInfo> baseViewsOneLevel;

    public MTMVRelation(Set<BaseTableInfo> baseTables, Set<BaseTableInfo> baseTablesOneLevel,
            Set<BaseTableInfo> baseTablesOneLevelAndFromView, Set<BaseTableInfo> baseViews,
            Set<BaseTableInfo> baseViewsOneLevel) {
        this.baseTables = baseTables;
        this.baseTablesOneLevel = baseTablesOneLevel;
        this.baseTablesOneLevelAndFromView = baseTablesOneLevelAndFromView;
        this.baseViews = baseViews;
        this.baseViewsOneLevel = baseViewsOneLevel;
    }

    public Set<BaseTableInfo> getBaseTables() {
        return baseTables;
    }

    public Set<BaseTableInfo> getBaseTablesOneLevel() {
        // For compatibility, previously created MTMV may not have baseTablesOneLevel
        return baseTablesOneLevel == null ? baseTables : baseTablesOneLevel;
    }

    public Set<BaseTableInfo> getBaseTablesOneLevelAndFromView() {
        // For compatibility, previously created MTMV may not have baseTablesOneLevelAndFromView
        return CollectionUtils.isEmpty(baseTablesOneLevelAndFromView) ? baseTablesOneLevel
                : baseTablesOneLevelAndFromView;
    }

    public Set<BaseTableInfo> getBaseViewsOneLevel() {
        return baseViewsOneLevel;
    }

    public Set<BaseTableInfo> getBaseViews() {
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

    public void compatible(CatalogMgr catalogMgr) throws Exception {
        compatible(catalogMgr, baseTables);
        compatible(catalogMgr, baseViews);
        compatible(catalogMgr, baseTablesOneLevel);
    }

    private void compatible(CatalogMgr catalogMgr, Set<BaseTableInfo> infos) throws Exception {
        if (CollectionUtils.isEmpty(infos)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : infos) {
            baseTableInfo.compatible(catalogMgr);
        }
    }
}
