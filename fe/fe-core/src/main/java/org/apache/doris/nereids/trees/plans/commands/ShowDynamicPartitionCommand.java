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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * show dynamic partition command
 */
public class ShowDynamicPartitionCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowDynamicPartitionCommand.class);
    private static final ShowResultSetMetaData SHOW_DYNAMIC_PARTITION_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Enable", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TimeUnit", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Start", ScalarType.createVarchar(20)))
                    .addColumn(new Column("End", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Prefix", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Buckets", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReplicationNum", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReplicaAllocation", ScalarType.createVarchar(128)))
                    .addColumn(new Column("StartOf", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastUpdateTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastSchedulerTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("State", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastCreatePartitionMsg", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastDropPartitionMsg", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReservedHistoryPeriods", ScalarType.createVarchar(20)))
                    .build();
    private String dbName; // if empty we will use current db;

    /**
     * constructor
     */
    public ShowDynamicPartitionCommand(String dbName) {
        super(PlanType.SHOW_DYNAMIC_PARTITION_COMMAND);
        this.dbName = dbName;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);
        if (db != null && (db instanceof Database)) {
            List<Table> tableList = db.getTables();
            for (Table tbl : tableList) {
                if (!(tbl instanceof OlapTable)) {
                    continue;
                }
                DynamicPartitionScheduler dynamicPartitionScheduler = Env.getCurrentEnv()
                        .getDynamicPartitionScheduler();
                OlapTable olapTable = (OlapTable) tbl;
                olapTable.readLock();
                try {
                    if (!olapTable.dynamicPartitionExists()) {
                        dynamicPartitionScheduler.removeRuntimeInfo(olapTable.getId());
                        continue;
                    }
                    // check tbl privs
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                    olapTable.getName(),
                                    PrivPredicate.SHOW)) {
                        continue;
                    }
                    DynamicPartitionProperty dynamicPartitionProperty
                            = olapTable.getTableProperty().getDynamicPartitionProperty();
                    String tableName = olapTable.getName();
                    ReplicaAllocation replicaAlloc = dynamicPartitionProperty.getReplicaAllocation();
                    if (replicaAlloc.isNotSet()) {
                        replicaAlloc = olapTable.getDefaultReplicaAllocation();
                    }
                    String unsortedReservedHistoryPeriods = dynamicPartitionProperty.getReservedHistoryPeriods();
                    rows.add(Lists.newArrayList(
                            tableName,
                            String.valueOf(dynamicPartitionProperty.getEnable()),
                            dynamicPartitionProperty.getTimeUnit().toUpperCase(),
                            String.valueOf(dynamicPartitionProperty.getStart()),
                            String.valueOf(dynamicPartitionProperty.getEnd()),
                            dynamicPartitionProperty.getPrefix(),
                            String.valueOf(dynamicPartitionProperty.getBuckets()),
                            String.valueOf(replicaAlloc.getTotalReplicaNum()),
                            replicaAlloc.toCreateStmt(),
                            dynamicPartitionProperty.getStartOfInfo(),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.LAST_UPDATE_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.LAST_SCHEDULER_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.DYNAMIC_PARTITION_STATE),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.CREATE_PARTITION_MSG),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.DROP_PARTITION_MSG),
                            dynamicPartitionProperty.getSortedReservedHistoryPeriods(unsortedReservedHistoryPeriods,
                                    dynamicPartitionProperty.getTimeUnit().toUpperCase())));
                } catch (DdlException e) {
                    LOG.warn("", e);
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        return new ShowResultSet(SHOW_DYNAMIC_PARTITION_META_DATA, rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDynamicPartitionCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
