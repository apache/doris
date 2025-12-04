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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSTransaction;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Insert executor for hive table
 */
public class HiveInsertExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(HiveInsertExecutor.class);

    private List<THivePartitionUpdate> partitionUpdates;

    /**
     * constructor
     */
    public HiveInsertExecutor(ConnectContext ctx, HMSExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx, boolean emptyInsert, long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    protected void beforeExec() throws UserException {
        // check params
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        Preconditions.checkArgument(insertCtx.isPresent(), "insert context must be present");
        HiveInsertCommandContext ctx = (HiveInsertCommandContext) insertCtx.get();
        TUniqueId tUniqueId = ConnectContext.get().queryId();
        Preconditions.checkArgument(tUniqueId != null, "query id shouldn't be null");
        ctx.setQueryId(DebugUtil.printId(tUniqueId));
        transaction.beginInsertTable(ctx);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        HMSTransaction transaction = (HMSTransaction) transactionManager.getTransaction(txnId);
        loadedRows = transaction.getUpdateCnt();
        transaction.finishInsertTable(((ExternalTable) table).getOrBuildNameMapping());

        // Save partition updates for cache refresh after commit
        partitionUpdates = transaction.getHivePartitionUpdates();
    }

    @Override
    protected void doAfterCommit() throws DdlException {
        HMSExternalTable hmsTable = (HMSExternalTable) table;

        // For partitioned tables, do selective partition refresh
        // For non-partitioned tables, do full table cache invalidation
        List<String> affectedPartitionNames = null;
        if (hmsTable.isPartitionedTable() && partitionUpdates != null && !partitionUpdates.isEmpty()) {
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            cache.refreshAffectedPartitions(hmsTable, partitionUpdates);

            // Collect partition names for edit log
            affectedPartitionNames = new ArrayList<>();
            for (THivePartitionUpdate update : partitionUpdates) {
                String partitionName = update.getName();
                if (partitionName != null && !partitionName.isEmpty()) {
                    affectedPartitionNames.add(partitionName);
                }
            }
        } else {
            // Non-partitioned table or no partition updates, do full table refresh
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(hmsTable);
        }

        // Write edit log to notify other FEs
        ExternalObjectLog log;
        if (affectedPartitionNames != null && !affectedPartitionNames.isEmpty()) {
            // Partition-level refresh for other FEs
            log = ExternalObjectLog.createForRefreshPartitions(
                    hmsTable.getCatalog().getId(),
                    table.getDatabase().getFullName(),
                    table.getName(),
                    affectedPartitionNames);
        } else {
            // Full table refresh for other FEs
            log = ExternalObjectLog.createForRefreshTable(
                    hmsTable.getCatalog().getId(),
                    table.getDatabase().getFullName(),
                    table.getName());
        }
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.HMS;
    }
}
