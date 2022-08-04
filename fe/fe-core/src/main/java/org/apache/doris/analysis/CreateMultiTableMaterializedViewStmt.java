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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import java.util.Map;

public class CreateMultiTableMaterializedViewStmt extends DdlStmt {
    private String mvName;
    private MVRefreshInfo.BuildMode buildMethod;
    private MVRefreshInfo refreshInfo;
    private PartitionDesc partition;
    private DistributionDesc distribution;
    private Map<String, String> tblProperties;
    private QueryStmt queryStmt;

    public CreateMultiTableMaterializedViewStmt(String mvName, MVRefreshInfo.BuildMode buildMethod,
            MVRefreshInfo refreshInfo, PartitionDesc partition, DistributionDesc distribution,
            Map<String, String> tblProperties, QueryStmt queryStmt) {
        this.mvName = mvName;
        this.buildMethod = buildMethod;
        this.refreshInfo = refreshInfo;
        this.partition = partition;
        this.distribution = distribution;
        this.tblProperties = tblProperties;
        this.queryStmt = queryStmt;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        refreshInfo.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ").append(mvName).append(" BUILD ON ").append(buildMethod.toString());
        if (refreshInfo != null) {
            sb.append(" ").append(refreshInfo.toString());
        }
        if (partition != null) {
            sb.append(" ").append(partition.toString());
        }
        if (distribution != null) {
            sb.append(" ").append(distribution.toString());
        }
        if (tblProperties != null && !tblProperties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(tblProperties, " = ", true, true, true));
            sb.append(")");
        }
        sb.append(" AS ").append(queryStmt.toSql());
        return sb.toString();
    }
}
