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

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IcebergAnalysisTask extends HMSAnalysisTask {

    private long numRows = 0;
    private long dataSize = 0;
    private long numNulls = 0;

    public IcebergAnalysisTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        super(analysisTaskScheduler, info);
    }

    private static final String INSERT_TABLE_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${columnStatTbl}"
            + " values ('${id}','${catalogId}', '${dbId}', '${tblId}', '${colId}', NULL, "
            + "${numRows}, 0, ${nulls}, '0', '0', ${dataSize}, '${update_time}')";


    @Override
    protected void getColumnStatsByMeta() throws Exception {
        Table icebergTable = getIcebergTable();
        TableScan tableScan = icebergTable.newScan().includeColumnStats();
        for (FileScanTask task : tableScan.planFiles()) {
            processDataFile(task.file(), task.spec());
        }
        updateStats();
    }

    private Table getIcebergTable() {
        org.apache.iceberg.hive.HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : table.getCatalog().getCatalogProperty().getProperties().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = table.getS3Properties();
        for (Map.Entry<String, String> entry : s3Properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        hiveCatalog.setConf(conf);
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("hive.metastore.uris", table.getMetastoreUri());
        catalogProperties.put("uri", table.getMetastoreUri());
        hiveCatalog.initialize("hive", catalogProperties);
        return hiveCatalog.loadTable(TableIdentifier.of(table.getDbName(), table.getName()));
    }

    private void processDataFile(DataFile dataFile, PartitionSpec partitionSpec) {
        int colId = -1;
        for (Types.NestedField column : partitionSpec.schema().columns()) {
            if (column.name().equals(col.getName())) {
                colId = column.fieldId();
                break;
            }
        }
        if (colId == -1) {
            throw new RuntimeException(String.format("Column %s not exist.", col.getName()));
        }
        dataSize += dataFile.columnSizes().get(colId);
        numRows += dataFile.recordCount();
        numNulls += dataFile.nullValueCounts().get(colId);
    }

    private void updateStats() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("id", String.valueOf(tbl.getId()) + "-" + String.valueOf(col.getName()));
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("colId", String.valueOf(col.getName()));
        params.put("numRows", String.valueOf(numRows));
        params.put("nulls", String.valueOf(numNulls));
        params.put("dataSize", String.valueOf(dataSize));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        params.put("update_time", sdf.format(new Date()));

        // Update table level stats info of this column.
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(INSERT_TABLE_SQL_TEMPLATE);
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
            this.stmtExecutor.execute();
        }
    }
}
