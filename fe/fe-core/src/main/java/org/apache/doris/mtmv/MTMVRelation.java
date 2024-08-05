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

import com.google.gson.annotations.SerializedName;

import java.util.Set;

public class MTMVRelation {
    // if mtmv query sql is `select * from view1`;
    // and `view1` query sql is `select * from table1 join table2`
    // then baseTables will include: `table1` and `table2`
    // baseViews will include `view1`
    @SerializedName("bt")
    private Set<BaseTableInfo> baseTables;
    @SerializedName("bv")
    private Set<BaseTableInfo> baseViews;
    @SerializedName("btol")
    private Set<BaseTableInfo> baseTablesOneLevel;

    public MTMVRelation(Set<BaseTableInfo> baseTables, Set<BaseTableInfo> baseTablesOneLevel,
            Set<BaseTableInfo> baseViews) {
        this.baseTables = baseTables;
        this.baseTablesOneLevel = baseTablesOneLevel;
        this.baseViews = baseViews;
    }

    public Set<BaseTableInfo> getBaseTables() {
        return baseTables;
    }

    public Set<BaseTableInfo> getBaseTablesOneLevel() {
        // For compatibility, previously created MTMV may not have baseTablesOneLevel
        return baseTablesOneLevel == null ? baseTables : baseTablesOneLevel;
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
}
