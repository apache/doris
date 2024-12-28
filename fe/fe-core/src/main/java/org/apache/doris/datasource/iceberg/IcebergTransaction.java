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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java
// and modified by Doris

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.UserException;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.iceberg.helper.IcebergWriterHelper;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TUpdateMode;
import org.apache.doris.transaction.Transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class IcebergTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(IcebergTransaction.class);

    private final IcebergMetadataOps ops;
    private SimpleTableInfo tableInfo;
    private Table table;


    private org.apache.iceberg.Transaction transaction;
    private final List<TIcebergCommitData> commitDataList = Lists.newArrayList();

    public IcebergTransaction(IcebergMetadataOps ops) {
        this.ops = ops;
    }

    public void updateIcebergCommitData(List<TIcebergCommitData> commitDataList) {
        synchronized (this) {
            this.commitDataList.addAll(commitDataList);
        }
    }

    public void beginInsert(SimpleTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.table = getNativeTable(tableInfo);
        this.transaction = table.newTransaction();
    }

    public void finishInsert(SimpleTableInfo tableInfo, Optional<InsertCommandContext> insertCtx) {
        if (LOG.isDebugEnabled()) {
            LOG.info("iceberg table {} insert table finished!", tableInfo);
        }
        try {
            ops.getPreExecutionAuthenticator().execute(() -> {
                //create and start the iceberg transaction
                TUpdateMode updateMode = TUpdateMode.APPEND;
                if (insertCtx.isPresent()) {
                    updateMode = ((BaseExternalTableInsertCommandContext) insertCtx.get()).isOverwrite()
                            ? TUpdateMode.OVERWRITE
                            : TUpdateMode.APPEND;
                }
                updateManifestAfterInsert(updateMode);
                return null;
            });
        } catch (Exception e) {
            LOG.warn("Failed to finish insert for iceberg table {}.", tableInfo, e);
            throw new RuntimeException(e);
        }

    }

    private void updateManifestAfterInsert(TUpdateMode updateMode) {
        PartitionSpec spec = table.spec();
        FileFormat fileFormat = IcebergUtils.getFileFormat(table);

        List<WriteResult> pendingResults;
        if (commitDataList.isEmpty()) {
            pendingResults = Collections.emptyList();
        } else {
            //convert commitDataList to writeResult
            WriteResult writeResult = IcebergWriterHelper
                    .convertToWriterResult(fileFormat, spec, commitDataList);
            pendingResults = Lists.newArrayList(writeResult);
        }

        if (updateMode == TUpdateMode.APPEND) {
            commitAppendTxn(table, pendingResults);
        } else {
            commitReplaceTxn(table, pendingResults);
        }
    }

    @Override
    public void commit() throws UserException {
        // commit the iceberg transaction
        transaction.commitTransaction();
    }

    @Override
    public void rollback() {
        //do nothing
    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TIcebergCommitData::getRowCount).sum();
    }


    private synchronized Table getNativeTable(SimpleTableInfo tableInfo) {
        Objects.requireNonNull(tableInfo);
        ExternalCatalog externalCatalog = ops.getExternalCatalog();
        return IcebergUtils.getRemoteTable(externalCatalog, tableInfo);
    }

    private void commitAppendTxn(Table table, List<WriteResult> pendingResults) {
        // commit append files.
        AppendFiles appendFiles = table.newAppend();
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files for append.");
            Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        }
        appendFiles.commit();
    }


    private void commitReplaceTxn(Table table, List<WriteResult> pendingResults) {
        if (pendingResults.isEmpty()) {
            // such as : insert overwrite table `dst_tb` select * from `empty_tb`
            // 1. if dst_tb is a partitioned table, it will return directly.
            // 2. if dst_tb is an unpartitioned table, the `dst_tb` table will be emptied.
            if (!table.spec().isPartitioned()) {
                OverwriteFiles overwriteFiles = table.newOverwrite();
                try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
                    fileScanTasks.forEach(f -> overwriteFiles.deleteFile(f.file()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                overwriteFiles.commit();
            }
            return;
        }

        // commit replace partitions
        ReplacePartitions appendPartitionOp = table.newReplacePartitions();
        for (WriteResult result : pendingResults) {
            Preconditions.checkState(result.referencedDataFiles().length == 0,
                    "Should have no referenced data files.");
            Arrays.stream(result.dataFiles()).forEach(appendPartitionOp::addFile);
        }
        appendPartitionOp.commit();
    }

}
