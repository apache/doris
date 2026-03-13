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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Executor for Iceberg DELETE operations.
 *
 * DELETE is implemented by generating Position Delete files
 * instead of rewriting data files.
 *
 * Flow:
 * 1. Execute query to get rows matching WHERE clause (with $row_id column)
 * 2. BE writes position delete files and returns TIcebergCommitData
 * 3. FE commits DeleteFiles to Iceberg table using RowDelta API
 */
public class IcebergDeleteExecutor extends BaseExternalTableInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(IcebergDeleteExecutor.class);
    private Optional<Expression> conflictDetectionFilter = Optional.empty();

    public IcebergDeleteExecutor(ConnectContext ctx, IcebergExternalTable table,
            String labelName, NereidsPlanner planner,
            boolean emptyInsert, long jobId) {
        // BaseExternalTableInsertExecutor requires Optional<InsertCommandContext>
        // For DELETE operations, we pass Optional.empty().
        super(ctx, table, labelName, planner, Optional.empty(), emptyInsert, jobId);
    }

    public void finalizeSinkForDelete(PlanFragment fragment, DataSink sink, PhysicalSink<?> physicalSink) {
        super.finalizeSink(fragment, sink, physicalSink);
    }

    public void setConflictDetectionFilter(Optional<Expression> filter) {
        conflictDetectionFilter = filter == null ? Optional.empty() : filter;
    }

    @Override
    protected void beforeExec() throws UserException {
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
        transaction.beginDelete((IcebergExternalTable) table);
        if (conflictDetectionFilter.isPresent()) {
            transaction.setConflictDetectionFilter(conflictDetectionFilter.get());
        } else {
            transaction.clearConflictDetectionFilter();
        }
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        IcebergExternalTable dorisTable = (IcebergExternalTable) table;
        IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);

        // Position delete files are written by BE and returned as TIcebergCommitData.
        // FE only needs to use commitDataList to update loaded rows and commit RowDelta.
        LOG.info("Processing Position Delete commit data for table: {}", dorisTable.getName());

        this.loadedRows = transaction.getUpdateCnt();

        // Finish delete and commit
        org.apache.doris.datasource.NameMapping nameMapping =
                new org.apache.doris.datasource.NameMapping(
                    dorisTable.getCatalog().getId(),
                    dorisTable.getDbName(),
                    dorisTable.getName(),
                    dorisTable.getRemoteDbName(),
                    dorisTable.getRemoteName());

        transaction.finishDelete(nameMapping);
    }

    @Override
    protected TransactionType transactionType() {
        return TransactionType.ICEBERG;
    }
}
