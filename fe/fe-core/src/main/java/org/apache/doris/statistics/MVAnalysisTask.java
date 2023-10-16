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

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SelectListItem;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Analysis for the materialized view, only gets constructed when the AnalyzeStmt is not set which
 * columns to be analyzed.
 * TODO: Supports multi-table mv
 */
public class MVAnalysisTask extends BaseAnalysisTask {

    private static final String ANALYZE_MV_PART = INSERT_PART_STATISTICS
            + " FROM (${sql}) mv ${sampleExpr}";

    private static final String ANALYZE_MV_COL = INSERT_COL_STATISTICS
            + "     (SELECT NDV(`${colName}`) AS ndv "
            + "     FROM (${sql}) mv) t2";

    private MaterializedIndexMeta meta;

    private SelectStmt selectStmt;

    private OlapTable olapTable;

    public MVAnalysisTask(AnalysisInfo info) {
        super(info);
        init();
    }

    private void init() {
        olapTable = (OlapTable) tbl;
        meta = olapTable.getIndexMetaByIndexId(info.indexId);
        Preconditions.checkState(meta != null);
        String mvDef = meta.getDefineStmt().originStmt;
        SqlScanner input =
                new SqlScanner(new StringReader(mvDef), 0L);
        SqlParser parser = new SqlParser(input);
        CreateMaterializedViewStmt cmv = null;
        try {
            cmv = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        selectStmt = cmv.getSelectStmt();
        selectStmt.getTableRefs().get(0).getName().setDb(db.getFullName());
    }

    @Override
    public void doExecute() throws Exception {
        for (Column column : meta.getSchema()) {
            SelectStmt selectOne = (SelectStmt) selectStmt.clone();
            TableRef tableRef = selectOne.getTableRefs().get(0);
            SelectListItem selectItem = selectOne.getSelectList().getItems()
                    .stream()
                    .filter(i -> isCorrespondingToColumn(i, column))
                    .findFirst()
                    .get();
            selectItem.setAlias(column.getName());
            Map<String, String> params = new HashMap<>();
            for (String partName : tbl.getPartitionNames()) {
                PartitionNames partitionName = new PartitionNames(false,
                        Collections.singletonList(partName));
                tableRef.setPartitionNames(partitionName);
                String sql = selectOne.toSql();
                params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
                params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
                params.put("catalogId", String.valueOf(catalog.getId()));
                params.put("dbId", String.valueOf(db.getId()));
                params.put("tblId", String.valueOf(tbl.getId()));
                params.put("idxId", String.valueOf(meta.getIndexId()));
                String colName = column.getName();
                params.put("colId", colName);
                String partId = olapTable.getPartition(partName) == null ? "NULL" :
                        String.valueOf(olapTable.getPartition(partName).getId());
                params.put("partId", partId);
                params.put("dataSizeFunction", getDataSizeFunction(column));
                params.put("dbName", db.getFullName());
                params.put("colName", colName);
                params.put("tblName", tbl.getName());
                params.put("sql", sql);
                StatisticsUtil.execUpdate(ANALYZE_MV_PART, params);
            }
            params.remove("partId");
            params.remove("sampleExpr");
            params.put("type", column.getType().toString());
            StatisticsUtil.execUpdate(ANALYZE_MV_COL, params);
            Env.getCurrentEnv().getStatisticsCache()
                    .refreshColStatsSync(meta.getIndexId(), meta.getIndexId(), column.getName());
        }
    }

    //  Based on the fact that materialized view create statement's select expr only contains basic SlotRef and
    //  AggregateFunction.
    private boolean isCorrespondingToColumn(SelectListItem item, Column column) {
        Expr expr = item.getExpr();
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            return slotRef.getColumnName().equalsIgnoreCase(column.getName());
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr func = (FunctionCallExpr) expr;
            SlotRef slotRef = (SlotRef) func.getChild(0);
            return slotRef.getColumnName().equalsIgnoreCase(column.getName());
        }
        return false;
    }

    @Override
    protected void afterExecution() {
        // DO NOTHING
    }
}
