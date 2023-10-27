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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.nereids.trees.plans.commands.info.EnvInfo;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateMultiTableMaterializedViewStmt extends CreateTableStmt {
    private final MTMVRefreshInfo refreshInfo;
    private final String querySql;
    private final EnvInfo envInfo;
    private Map<String, String> mvProperties;

    public CreateMultiTableMaterializedViewStmt(boolean ifNotExists, TableName mvName, List<Column> columns,
            MTMVRefreshInfo refreshInfo, KeysDesc keyDesc, DistributionDesc distributionDesc,
            Map<String, String> properties, Map<String, String> mvProperties, String querySql, String comment,
            EnvInfo envInfo) {
        super(ifNotExists, false, mvName, columns, new ArrayList<Index>(), DEFAULT_ENGINE_NAME, keyDesc, null,
                distributionDesc, properties, null, comment, null, null);
        this.refreshInfo = refreshInfo;
        this.querySql = querySql;
        this.envInfo = envInfo;
        this.mvProperties = mvProperties;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public String getQuerySql() {
        return querySql;
    }

    public EnvInfo getEnvInfo() {
        return envInfo;
    }

    public Map<String, String> getMvProperties() {
        return mvProperties;
    }
}
