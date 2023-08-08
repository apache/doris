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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    private static final String ANALYZE_PARTITION_SQL_TEMPLATE = INSERT_PART_STATISTICS
            + "FROM `${dbName}`.`${tblName}` "
            + "PARTITION ${partName} ${sampleExpr}";

    // TODO Currently, NDV is computed for the full table; in fact,
    //  NDV should only be computed for the relevant partition.
    private static final String ANALYZE_COLUMN_SQL_TEMPLATE = INSERT_COL_STATISTICS
            + "     (SELECT NDV(`${colName}`) AS ndv "
            + "     FROM `${dbName}`.`${tblName}` ${sampleExpr}) t2\n";

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction(col));
        params.put("dbName", info.dbName);
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(info.tblName));
        params.put("sampleExpr", getSampleExpression());
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        try {
            tbl.readLock();
            Set<String> partNames = info.colToPartitions.get(info.colName);
            for (String partName : partNames) {
                Partition part = tbl.getPartition(partName);
                if (part == null) {
                    continue;
                }
                params.put("partId", String.valueOf(tbl.getPartition(partName).getId()));
                // Avoid error when get the default partition
                params.put("partName", "`" + partName + "`");
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                partitionAnalysisSQLs.add(stringSubstitutor.replace(ANALYZE_PARTITION_SQL_TEMPLATE));
            }
        } finally {
            tbl.readUnlock();
        }
        execSQLs(partitionAnalysisSQLs);
        params.remove("partId");
        params.put("type", col.getType().toString());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(ANALYZE_COLUMN_SQL_TEMPLATE);
        execSQL(sql);
    }

    @VisibleForTesting
    public void execSQLs(List<String> partitionAnalysisSQLs) throws Exception {
        for (String sql : partitionAnalysisSQLs) {
            execSQL(sql);
        }
    }

    @VisibleForTesting
    public void execSQL(String sql) throws Exception {
        if (killed) {
            return;
        }
        long startTime = System.currentTimeMillis();
        LOG.info("ANALYZE SQL : " + sql + " start at " + startTime);
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.execute();
            QueryState queryState = r.connectContext.getState();
            if (queryState.getStateType().equals(MysqlStateType.ERR)) {
                throw new RuntimeException(String.format("Failed to analyze %s.%s.%s, error: %s sql: %s",
                        info.catalogName, info.dbName, info.colName, sql, queryState.getErrorMessage()));
            }
        } finally {
            LOG.info("Analyze SQL: " + sql + " cost time: " + (System.currentTimeMillis() - startTime) + "ms");
        }
    }

}
