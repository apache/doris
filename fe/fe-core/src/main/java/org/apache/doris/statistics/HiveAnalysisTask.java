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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveAnalysisTask extends HMSAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HiveAnalysisTask.class);

    public static final String TOTAL_SIZE = "totalSize";
    public static final String NUM_ROWS = "numRows";
    public static final String NUM_FILES = "numFiles";
    public static final String TIMESTAMP = "transient_lastDdlTime";
    public static final String DELIMITER = "-";

    public HiveAnalysisTask(AnalysisTaskInfo info) {
        super(info);
    }

    private static final String ANALYZE_PARTITION_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '-1', '${colId}', '${partId}', "
            + "${numRows}, ${ndv}, ${nulls}, '${min}', '${max}', ${dataSize}, '${update_time}')";

    private static final String ANALYZE_TABLE_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '-1', '${colId}', NULL, "
            + "${numRows}, ${ndv}, ${nulls}, '${min}', '${max}', ${dataSize}, '${update_time}')";

    @Override
    protected void getColumnStatsByMeta() throws Exception {
        List<String> columns = new ArrayList<>();
        columns.add(col.getName());
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("colId", String.valueOf(col.getName()));

        // Get table level information.
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        // Collect table level row count, null number and timestamp.
        setParameterData(parameters, params);
        params.put("id", genColumnStatId(tbl.getId(), -1, col.getName(), null));
        List<ColumnStatisticsObj> tableStats = table.getHiveTableColumnStats(columns);
        // Collect table level ndv, nulls, min and max. tableStats contains at most 1 item;
        for (ColumnStatisticsObj tableStat : tableStats) {
            if (!tableStat.isSetStatsData()) {
                continue;
            }
            ColumnStatisticsData data = tableStat.getStatsData();
            getStatData(data, params);
        }
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(ANALYZE_TABLE_SQL_TEMPLATE);
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
            this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
            this.stmtExecutor.execute();
        }

        // Get partition level information.
        List<String> partitions = ((HMSExternalCatalog)
                catalog).getClient().listPartitionNames(db.getFullName(), table.getName());
        Map<String, List<ColumnStatisticsObj>> columnStats = table.getHivePartitionColumnStats(partitions, columns);
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        for (Map.Entry<String, List<ColumnStatisticsObj>> entry : columnStats.entrySet()) {
            String partName = entry.getKey();
            List<String> partitionValues = new ArrayList<>();
            for (String p : partName.split("/")) {
                partitionValues.add(p.split("=")[1]);
            }
            Partition partition = table.getPartition(partitionValues);
            parameters = partition.getParameters();
            // Collect row count, null number and timestamp.
            setParameterData(parameters, params);
            params.put("id", genColumnStatId(tbl.getId(), -1, col.getName(), partName));
            params.put("partId", partName);
            List<ColumnStatisticsObj> value = entry.getValue();
            Preconditions.checkState(value.size() == 1);
            ColumnStatisticsObj stat = value.get(0);
            if (!stat.isSetStatsData()) {
                continue;
            }
            // Collect ndv, nulls, min and max for different data type.
            ColumnStatisticsData data = stat.getStatsData();
            getStatData(data, params);
            stringSubstitutor = new StringSubstitutor(params);
            partitionAnalysisSQLs.add(stringSubstitutor.replace(ANALYZE_PARTITION_SQL_TEMPLATE));
        }
        // Update partition level stats for this column.
        for (String partitionSql : partitionAnalysisSQLs) {
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
                r.connectContext.getSessionVariable().disableNereidsPlannerOnce();
                this.stmtExecutor = new StmtExecutor(r.connectContext, partitionSql);
                this.stmtExecutor.execute();
            }
        }
        Env.getCurrentEnv().getStatisticsCache().refreshColStatsSync(tbl.getId(), -1, col.getName());
    }

    private void getStatData(ColumnStatisticsData data, Map<String, String> params) {
        long ndv = 0;
        long nulls = 0;
        String min = "";
        String max = "";
        // Collect ndv, nulls, min and max for different data type.
        if (data.isSetLongStats()) {
            LongColumnStatsData longStats = data.getLongStats();
            ndv = longStats.getNumDVs();
            nulls = longStats.getNumNulls();
            min = String.valueOf(longStats.getLowValue());
            max = String.valueOf(longStats.getHighValue());
        } else if (data.isSetStringStats()) {
            StringColumnStatsData stringStats = data.getStringStats();
            ndv = stringStats.getNumDVs();
            nulls = stringStats.getNumNulls();
        } else if (data.isSetDecimalStats()) {
            DecimalColumnStatsData decimalStats = data.getDecimalStats();
            ndv = decimalStats.getNumDVs();
            nulls = decimalStats.getNumNulls();
            if (decimalStats.isSetLowValue()) {
                Decimal lowValue = decimalStats.getLowValue();
                if (lowValue != null) {
                    BigDecimal lowDecimal = new BigDecimal(new BigInteger(lowValue.getUnscaled()), lowValue.getScale());
                    min = lowDecimal.toString();
                }
            }
            if (decimalStats.isSetHighValue()) {
                Decimal highValue = decimalStats.getHighValue();
                if (highValue != null) {
                    BigDecimal highDecimal = new BigDecimal(
                            new BigInteger(highValue.getUnscaled()), highValue.getScale());
                    max = highDecimal.toString();
                }
            }
        } else if (data.isSetDoubleStats()) {
            DoubleColumnStatsData doubleStats = data.getDoubleStats();
            ndv = doubleStats.getNumDVs();
            nulls = doubleStats.getNumNulls();
            min = String.valueOf(doubleStats.getLowValue());
            max = String.valueOf(doubleStats.getHighValue());
        } else if (data.isSetDateStats()) {
            DateColumnStatsData dateStats = data.getDateStats();
            ndv = dateStats.getNumDVs();
            nulls = dateStats.getNumNulls();
            if (dateStats.isSetLowValue()) {
                org.apache.hadoop.hive.metastore.api.Date lowValue = dateStats.getLowValue();
                if (lowValue != null) {
                    LocalDate lowDate = LocalDate.ofEpochDay(lowValue.getDaysSinceEpoch());
                    min = lowDate.toString();
                }
            }
            if (dateStats.isSetHighValue()) {
                org.apache.hadoop.hive.metastore.api.Date highValue = dateStats.getHighValue();
                if (highValue != null) {
                    LocalDate highDate = LocalDate.ofEpochDay(highValue.getDaysSinceEpoch());
                    max = highDate.toString();
                }
            }
        } else {
            throw new RuntimeException("Not supported data type.");
        }
        params.put("ndv", String.valueOf(ndv));
        params.put("nulls", String.valueOf(nulls));
        params.put("min", min);
        params.put("max", max);
    }

    private void setParameterData(Map<String, String> parameters, Map<String, String> params) {
        long numRows = 0;
        long timestamp = 0;
        long dataSize = 0;
        if (parameters.containsKey(NUM_ROWS)) {
            numRows = Long.parseLong(parameters.get(NUM_ROWS));
        }
        if (parameters.containsKey(TIMESTAMP)) {
            timestamp = Long.parseLong(parameters.get(TIMESTAMP));
        }
        if (parameters.containsKey(TOTAL_SIZE)) {
            dataSize = Long.parseLong(parameters.get(TOTAL_SIZE));
        }
        params.put("dataSize", String.valueOf(dataSize));
        params.put("numRows", String.valueOf(numRows));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        params.put("update_time", sdf.format(new Date(timestamp * 1000)));
    }

    private String genColumnStatId(long tableId, long indexId, String columnName, String partitionName) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableId);
        sb.append(DELIMITER);
        sb.append(indexId);
        sb.append(DELIMITER);
        sb.append(columnName);
        if (partitionName != null) {
            sb.append(DELIMITER);
            sb.append(partitionName);
        }
        return sb.toString();
    }
}
