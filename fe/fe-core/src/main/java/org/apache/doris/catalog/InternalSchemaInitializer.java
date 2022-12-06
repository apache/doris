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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InternalSchemaInitializer extends Thread {

    private static final Logger LOG = LogManager.getLogger(InternalSchemaInitializer.class);

    /**
     * If internal table creation failed, will retry after below seconds.
     */
    public static final int TABLE_CREATION_RETRY_INTERVAL_IN_SECONDS = 1;

    public void run() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        while (!created()) {
            FrontendNodeType feType = Env.getCurrentEnv().getFeType();
            if (feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN)) {
                LOG.warn("FE is not ready");
                continue;
            }
            try {
                Thread.currentThread()
                        .join(TABLE_CREATION_RETRY_INTERVAL_IN_SECONDS * 1000L);
                createDB();
                createTbl();
            } catch (Throwable e) {
                LOG.warn("Statistics storage initiated failed, will try again later", e);
            }
        }
        LOG.info("Internal schema initiated");
    }

    private void createTbl() throws UserException {
        Env.getCurrentEnv().getInternalCatalog().createTable(buildStatisticsTblStmt());
        Env.getCurrentEnv().getInternalCatalog().createTable(buildAnalysisJobTblStmt());
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
    public CreateTableStmt buildStatisticsTblStmt() throws UserException {
        TableName tableName = new TableName("",
                FeConstants.INTERNAL_DB_NAME, StatisticConstants.STATISTIC_TBL_NAME);
        List<ColumnDef> columnDefs = new ArrayList<>();
        columnDefs.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        columnDefs.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        columnDefs.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        ColumnDef partId = new ColumnDef("part_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN));
        partId.setAllowNull(true);
        columnDefs.add(partId);
        columnDefs.add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("ndv", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("null_count", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("min", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH)));
        columnDefs.add(new ColumnDef("max", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH)));
        columnDefs.add(new ColumnDef("data_size_in_bytes", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));
        String engineName = "olap";
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS,
                Lists.newArrayList("id"));

        DistributionDesc distributionDesc = new HashDistributionDesc(
                StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT,
                Lists.newArrayList("id"));
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("replication_num", String.valueOf(Config.statistic_internal_table_replica_num));
            }
        };
        CreateTableStmt createTableStmt = new CreateTableStmt(true, false,
                tableName, columnDefs, engineName, keysDesc, null, distributionDesc,
                properties, null, "Doris internal statistics table, don't modify it", null);
        // createTableStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        StatisticsUtil.analyze(createTableStmt);
        return createTableStmt;
    }

    @VisibleForTesting
    public CreateTableStmt buildAnalysisJobTblStmt() throws UserException {
        TableName tableName = new TableName("",
                FeConstants.INTERNAL_DB_NAME, StatisticConstants.ANALYSIS_JOB_TABLE);
        List<ColumnDef> columnDefs = new ArrayList<>();
        columnDefs.add(new ColumnDef("job_id", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("task_id", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("catalog_name", TypeDef.createVarchar(1024)));
        columnDefs.add(new ColumnDef("db_name", TypeDef.createVarchar(1024)));
        columnDefs.add(new ColumnDef("tbl_name", TypeDef.createVarchar(1024)));
        columnDefs.add(new ColumnDef("col_name", TypeDef.createVarchar(1024)));
        columnDefs.add(new ColumnDef("index_id", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("job_type", TypeDef.createVarchar(32)));
        columnDefs.add(new ColumnDef("analysis_type", TypeDef.createVarchar(32)));
        columnDefs.add(new ColumnDef("message", TypeDef.createVarchar(1024)));
        columnDefs.add(new ColumnDef("last_exec_time_in_ms", TypeDef.create(PrimitiveType.BIGINT)));
        columnDefs.add(new ColumnDef("state", TypeDef.createVarchar(32)));
        columnDefs.add(new ColumnDef("schedule_type", TypeDef.createVarchar(32)));
        String engineName = "olap";
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS,
                Lists.newArrayList("job_id"));

        DistributionDesc distributionDesc = new HashDistributionDesc(
                StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT,
                Lists.newArrayList("job_id"));
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("replication_num", String.valueOf(Config.statistic_internal_table_replica_num));
            }
        };
        CreateTableStmt createTableStmt = new CreateTableStmt(true, false,
                tableName, columnDefs, engineName, keysDesc, null, distributionDesc,
                properties, null, "Doris internal statistics table, don't modify it", null);
        // createTableStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        StatisticsUtil.analyze(createTableStmt);
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
        return db.getTable(StatisticConstants.STATISTIC_TBL_NAME).isPresent()
                && db.getTable(StatisticConstants.ANALYSIS_JOB_TABLE).isPresent();
    }

}
