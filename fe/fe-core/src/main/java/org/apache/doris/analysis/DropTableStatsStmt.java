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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manually drop statistics for tables or partitions.
 * Table or partition can be specified, if neither is specified,
 * all statistics under the current database will be deleted.
 *
 * syntax:
 *     DROP STATS [TableName [PARTITIONS(partitionNames)]];
 */
public class DropTableStatsStmt extends DdlStmt {
    private final TableName tableName;
    private final PartitionNames optPartitionNames;

    // after analyzed
    private final Map<Long, Set<String>> tblIdToPartition = Maps.newHashMap();

    public DropTableStatsStmt(TableName tableName, PartitionNames optPartitionNames) {
        this.tableName = tableName;
        this.optPartitionNames = optPartitionNames;
    }

    public Map<Long, Set<String>> getTblIdToPartition() {
        Preconditions.checkArgument(isAnalyzed(),
                "The partition name must be obtained after the parsing is complete");
        return tblIdToPartition;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (tableName != null) {
            if (Strings.isNullOrEmpty(tableName.getDb())) {
                tableName.setDb(analyzer.getDefaultDb());
            }

            tableName.analyze(analyzer);

            // check whether the deletion permission is granted
            checkAnalyzePriv(tableName.getDb(), tableName.getTbl());

            // disallow external catalog
            Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

            Database db = analyzer.getEnv().getInternalCatalog()
                    .getDbOrAnalysisException(tableName.getDb());
            long tableId = db.getTableOrAnalysisException(tableName.getTbl()).getId();

            if (optPartitionNames == null) {
                tblIdToPartition.put(tableId, null);
            } else {
                optPartitionNames.analyze(analyzer);
                List<String> pNames = optPartitionNames.getPartitionNames();
                HashSet<String> partitionNames = Sets.newHashSet(pNames);
                tblIdToPartition.put(tableId, partitionNames);
            }
        } else {
            Database db = analyzer.getEnv().getInternalCatalog()
                    .getDbOrAnalysisException(analyzer.getDefaultDb());
            for (Table table : db.getTables()) {
                checkAnalyzePriv(db.getFullName(), table.getName());
                tblIdToPartition.put(table.getId(), null);
            }
        }
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Env.getCurrentEnv().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "DROP",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + "." + tblName);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP STATS ");

        if (tableName != null) {
            sb.append(tableName.toSql());
        }

        if (optPartitionNames != null) {
            sb.append(" ");
            sb.append(optPartitionNames.toSql());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
