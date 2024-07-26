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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.datasource.ExternalTable;

import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.Map;

public class ExternalAnalysisTask extends BaseAnalysisTask {
    private boolean isPartitionOnly;
    protected ExternalTable table;

    // For test
    public ExternalAnalysisTask() {
    }

    public ExternalAnalysisTask(AnalysisInfo info) {
        super(info);
        isPartitionOnly = info.partitionOnly;
        table = (ExternalTable) tbl;
    }

    public void doExecute() throws Exception {
        if (killed) {
            return;
        }
        if (tableSample != null) {
            doSample();
        } else {
            doFull();
        }
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
    }

    protected void doFull() throws Exception {
        StringBuilder sb = new StringBuilder();
        Map<String, String> params = buildSqlParams();
        params.put("min", getMinFunction());
        params.put("max", getMaxFunction());
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do full collection for column {}", col.getName());
        }
        sb.append(FULL_ANALYZE_TEMPLATE);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
    }

    @Override
    protected Map<String, String> buildSqlParams() {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", "-1");
        params.put("colName", info.colName);
        params.put("colId", info.colName);
        params.put("catalogName", catalog.getName());
        params.put("dbName", db.getFullName());
        params.put("tblName", tbl.getName());
        params.put("sampleHints", getSampleHint());
        params.put("limit", "");
        params.put("scaleFactor", "1");
        params.put("index", "");
        if (col != null) {
            params.put("type", col.getType().toString());
        }
        params.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return params;
    }

    protected String getSampleHint() {
        if (tableSample == null) {
            return "";
        }
        if (tableSample.isPercent()) {
            return String.format("TABLESAMPLE(%d PERCENT)", tableSample.getSampleValue());
        } else {
            return String.format("TABLESAMPLE(%d ROWS)", tableSample.getSampleValue());
        }
    }

    protected void doSample() throws Exception {
        throw new NotImplementedException("Sample analyze for JDBC table is not supported");
    }

    // For test
    protected void setTable(ExternalTable table) {
        this.table = table;
    }
}
