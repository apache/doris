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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class WarmUpClusterStmt extends StatementBase implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(WarmUpClusterStmt.class);
    private List<Map<TableName, String>> tableList;
    private List<Triple<String, String, String>> tables = new ArrayList<>();
    private String dstClusterName;
    private String srcClusterName;
    private boolean isWarmUpWithTable;
    private boolean isForce;

    public WarmUpClusterStmt(String dstClusterName, String srcClusterName, boolean isForce) {
        this.dstClusterName = dstClusterName;
        this.srcClusterName = srcClusterName;
        this.isForce = isForce;
        this.isWarmUpWithTable = false;
    }

    public WarmUpClusterStmt(String dstClusterName, List<Map<TableName, String>> tableList, boolean isForce) {
        this.dstClusterName = dstClusterName;
        this.tableList = tableList;
        this.isForce = isForce;
        this.isWarmUpWithTable = true;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (!Config.isCloudMode()) {
            throw new UserException("The sql is illegal in disk mode ");
        }
        super.analyze(analyzer);
        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).containClusterName(dstClusterName)) {
            throw new AnalysisException("The dstClusterName " + dstClusterName + " doesn't exist");
        }
        if (!isWarmUpWithTable
                    && !((CloudSystemInfoService) Env.getCurrentSystemInfo()).containClusterName(srcClusterName)) {
            boolean contains = false;
            try {
                contains = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().containsCluster(srcClusterName);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
            if (!contains) {
                throw new AnalysisException("The srcClusterName doesn't exist");
            }
        }
        if (!isWarmUpWithTable && Objects.equals(dstClusterName, srcClusterName)) {
            throw new AnalysisException("The dstClusterName: " + dstClusterName
                                        + " is same with srcClusterName: " + srcClusterName);
        }
        if (isWarmUpWithTable) {
            for (Map<TableName, String> tableAndPartition : tableList) {
                for (Map.Entry<TableName, String> entry : tableAndPartition.entrySet()) {
                    TableName tableName = entry.getKey();
                    String partitionName = entry.getValue();
                    tableName.analyze(analyzer);
                    String dbName = tableName.getDb();
                    if (Strings.isNullOrEmpty(dbName)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
                    }
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
                    if (db == null) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
                    }
                    OlapTable table = (OlapTable) db.getTableNullable(tableName.getTbl());
                    if (table == null) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
                    }
                    if (partitionName.length() != 0 && !table.containsPartition(partitionName)) {
                        throw new AnalysisException("The partition " + partitionName + " doesn't exist");
                    }
                    Triple<String, String, String> part =
                            new ImmutableTriple<>(dbName, tableName.getTbl(), partitionName);
                    tables.add(part);
                }
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("WARM UP CLUSTER ").append(dstClusterName).append(" WITH ");
        if (isWarmUpWithTable) {
            int i = 0;
            for (Map<TableName, String> tableAndPartition : tableList) {
                for (Map.Entry<TableName, String> entry : tableAndPartition.entrySet()) {
                    if (i++ != 0) {
                        sb.append(" AND ");
                    }
                    sb.append(" Table ").append(entry.getKey().getTbl());
                    if (entry.getValue().length() != 0) {
                        sb.append(" Partition ").append(entry.getValue());
                    }
                }
            }
        } else {
            sb.append(" CLUSTER ").append(srcClusterName);
        }
        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public List<Triple<String, String, String>> getTables() {
        return tables;
    }

    public String getDstClusterName() {
        return dstClusterName;
    }

    public String getSrcClusterName() {
        return srcClusterName;
    }

    public boolean isWarmUpWithTable() {
        return isWarmUpWithTable;
    }

    public boolean isForce() {
        return isForce;
    }
}
