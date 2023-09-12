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

package org.apache.doris.analysis;

import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.analysis.MVRefreshInfo.RefreshMethod;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.TableIf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateMultiTableMaterializedViewStmt extends CreateTableStmt {
    private final BuildMode buildMode;
    private final RefreshMethod refreshMethod;
    private final MVRefreshTriggerInfo refreshTriggerInfo;
    private final String querySql;
    private final String originSql;
    private final List<TableIf> baseTables;

    public CreateMultiTableMaterializedViewStmt(boolean ifNotExists, TableName mvName, List<Column> columns,
            BuildMode buildMode,
            RefreshMethod refreshMethod, KeysDesc keyDesc, DistributionDesc distributionDesc,
            Map<String, String> properties, String querySql, MVRefreshTriggerInfo refreshTriggerInfo,
            List<TableIf> baseTables, String originSql, String comment) {
        super(ifNotExists, false, mvName, columns, new ArrayList<Index>(), DEFAULT_ENGINE_NAME, keyDesc, null,
                distributionDesc, properties, null, comment, null, null);
        this.buildMode = buildMode;
        this.querySql = querySql;
        this.refreshTriggerInfo = refreshTriggerInfo;
        this.refreshMethod = refreshMethod;
        this.baseTables = baseTables;
        this.originSql = originSql;
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }

    public RefreshMethod getRefreshMethod() {
        return refreshMethod;
    }

    public MVRefreshTriggerInfo getRefreshTriggerInfo() {
        return refreshTriggerInfo;
    }

    public String getQuerySql() {
        return querySql;
    }

    public String getOriginSql() {
        return originSql;
    }

    public List<TableIf> getBaseTables() {
        return baseTables;
    }
}
