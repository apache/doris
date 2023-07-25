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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.SystemInfoService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class InternalSchemaInitializer extends Thread {

    public static final int TABLE_CREATION_RETRY_INTERVAL_IN_SECONDS = 5;

    private static final Logger LOG = LogManager.getLogger(InternalSchemaInitializer.class);

    public void run() {
        if (!FeConstants.enableInternalSchemaDb) {
            return;
        }
        while (!created()) {
            try {
                FrontendNodeType feType = Env.getCurrentEnv().getFeType();
                if (feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN)) {
                    LOG.warn("FE is not ready");
                    Thread.sleep(5000);
                    continue;
                }
                Thread.currentThread()
                        .join(TABLE_CREATION_RETRY_INTERVAL_IN_SECONDS * 1000L);
                createDB();
                createTbl();
            } catch (Throwable e) {
                LOG.warn("Statistics storage initiated failed, will try again later", e);
            }
        }
        LOG.info("Internal schema is initialized");
    }

    private void createTbl() throws UserException {
        Env.getCurrentEnv().getInternalCatalog().createTable(buildAnalysisTblStmt());
        Env.getCurrentEnv().getInternalCatalog().createTable(buildStatisticsTblStmt());
        Env.getCurrentEnv().getInternalCatalog().createTable(buildHistogramTblStmt());
    }

    @VisibleForTesting
    public static void createDB() {
        CreateDbStmt createDbStmt = new CreateDbStmt(true,
                ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, FeConstants.INTERNAL_DB_NAME),
                null);
        createDbStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        try {
            Env.getCurrentEnv().createDb(createDbStmt);
        } catch (DdlException e) {
            LOG.warn("Failed to create database: {}, will try again later",
                    FeConstants.INTERNAL_DB_NAME, e);
        }
    }

    @VisibleForTesting
    public CreateTableStmt buildAnalysisTblStmt() throws UserException {
        TableName tableName = new TableName("",
                FeConstants.INTERNAL_DB_NAME, StatisticConstants.ANALYSIS_TBL_NAME);
        List<ColumnDef> columnDefs = new ArrayList<>();
        columnDefs.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        columnDefs.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        ColumnDef partId = new ColumnDef("part_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN));
        partId.setAllowNull(true);
        columnDefs.add(partId);
        columnDefs.add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("last_analyze_time_in_ms", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));
        String engineName = "olap";
        ArrayList<String> uniqueKeys = Lists.newArrayList("id", "catalog_id",
                "db_id", "tbl_id", "idx_id", "part_id");
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS, uniqueKeys);
        DistributionDesc distributionDesc = new HashDistributionDesc(
                StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT, uniqueKeys);
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("replication_num", String.valueOf(
                        Math.max(Config.statistic_internal_table_replica_num, Config.min_replication_num_per_tablet)));
            }
        };
        CreateTableStmt createTableStmt = new CreateTableStmt(true, false,
                tableName, columnDefs, engineName, keysDesc, null, distributionDesc,
                properties, null, "Doris internal statistics table, DO NOT MODIFY IT", null);
        StatisticsUtil.analyze(createTableStmt);
        return createTableStmt;
    }

    @VisibleForTesting
    public CreateTableStmt buildStatisticsTblStmt() throws UserException {
        TableName tableName = new TableName("",
                FeConstants.INTERNAL_DB_NAME, StatisticConstants.STATISTIC_TBL_NAME);
        List<ColumnDef> columnDefs = new ArrayList<>();
        columnDefs.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        columnDefs.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        ColumnDef partId = new ColumnDef("part_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN));
        partId.setAllowNull(true);
        columnDefs.add(partId);
        columnDefs.add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT), true));
        columnDefs.add(new ColumnDef("ndv", TypeDef.create(PrimitiveType.BIGINT), true));
        columnDefs.add(new ColumnDef("null_count", TypeDef.create(PrimitiveType.BIGINT), true));
        columnDefs.add(new ColumnDef("min", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH), true));
        columnDefs.add(new ColumnDef("max", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH), true));
        columnDefs.add(new ColumnDef("data_size_in_bytes", TypeDef.create(PrimitiveType.BIGINT), true));
        columnDefs.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));
        String engineName = "olap";
        ArrayList<String> uniqueKeys = Lists.newArrayList("id", "catalog_id",
                "db_id", "tbl_id", "idx_id", "col_id", "part_id");
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS, uniqueKeys);
        DistributionDesc distributionDesc = new HashDistributionDesc(
                StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT, uniqueKeys);
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("replication_num", String.valueOf(
                        Math.max(Config.statistic_internal_table_replica_num, Config.min_replication_num_per_tablet)));
            }
        };
        CreateTableStmt createTableStmt = new CreateTableStmt(true, false,
                tableName, columnDefs, engineName, keysDesc, null, distributionDesc,
                properties, null, "Doris internal statistics table, DO NOT MODIFY IT", null);
        // createTableStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        StatisticsUtil.analyze(createTableStmt);
        return createTableStmt;
    }

    @VisibleForTesting
    public CreateTableStmt buildHistogramTblStmt() throws UserException {
        TableName tableName = new TableName("",
                FeConstants.INTERNAL_DB_NAME, StatisticConstants.HISTOGRAM_TBL_NAME);
        List<ColumnDef> columnDefs = new ArrayList<>();
        columnDefs.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        columnDefs.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("sample_rate", TypeDef.create(PrimitiveType.DOUBLE)));
        columnDefs.add(new ColumnDef("buckets", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH)));
        columnDefs.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));
        String engineName = "olap";
        ArrayList<String> uniqueKeys = Lists.newArrayList("id", "catalog_id",
                "db_id", "tbl_id", "idx_id", "col_id");
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS, uniqueKeys);
        DistributionDesc distributionDesc = new HashDistributionDesc(
                StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT, uniqueKeys);
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("replication_num", String.valueOf(Math.max(Config.statistic_internal_table_replica_num,
                        Config.min_replication_num_per_tablet)));
            }
        };
        CreateTableStmt createTableStmt = new CreateTableStmt(true, false,
                tableName, columnDefs, engineName, keysDesc, null, distributionDesc,
                properties, null, "Doris internal statistics table, DO NOT MODIFY IT", null);
        StatisticsUtil.analyze(createTableStmt);
        // createTableStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        return createTableStmt;
    }

    private boolean created() {
        Optional<Database> optionalDatabase =
                Env.getCurrentEnv().getInternalCatalog()
                        .getDb(SystemInfoService.DEFAULT_CLUSTER + ":" + FeConstants.INTERNAL_DB_NAME);
        if (!optionalDatabase.isPresent()) {
            return false;
        }
        Database db = optionalDatabase.get();
        return db.getTable(StatisticConstants.ANALYSIS_TBL_NAME).isPresent()
                && db.getTable(StatisticConstants.STATISTIC_TBL_NAME).isPresent()
                && db.getTable(StatisticConstants.HISTOGRAM_TBL_NAME).isPresent();
    }

    /**
     * Compare whether the current internal table schema meets expectations,
     * delete and rebuild if it does not meet the table schema.
     * TODO remove this code after the table structure is stable
     */
    private boolean isTableChanged(TableName tableName, List<ColumnDef> columnDefs) {
        try {
            String catalogName = Env.getCurrentEnv().getInternalCatalog().getName();
            String dbName = SystemInfoService.DEFAULT_CLUSTER + ":" + tableName.getDb();
            TableIf table = StatisticsUtil.findTable(catalogName, dbName, tableName.getTbl());
            List<Column> existColumns = table.getBaseSchema(false);
            existColumns.sort(Comparator.comparing(Column::getName));
            List<Column> columns = columnDefs.stream()
                    .map(ColumnDef::toColumn)
                    .sorted(Comparator.comparing(Column::getName))
                    .collect(Collectors.toList());
            if (columns.size() != existColumns.size()) {
                return true;
            }
            for (int i = 0; i < columns.size(); i++) {
                Column c1 = columns.get(i);
                Column c2 = existColumns.get(i);
                if (!c1.getName().equals(c2.getName())
                        || c1.getDataType() != c2.getDataType()) {
                    return true;
                }
            }
            return false;
        } catch (Throwable t) {
            LOG.warn("Failed to check table schema", t);
            return false;
        }
    }

}

