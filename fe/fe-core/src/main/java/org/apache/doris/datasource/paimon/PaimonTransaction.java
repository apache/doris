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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.thrift.TPaimonCommitData;
import org.apache.doris.transaction.Transaction;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Paimon transaction for direct-write insert operations.
 * BE writes ORC/Parquet files directly to table storage; FE commits via Paimon Java API
 * using file metadata (path, row count, size, partition values) collected from BEs.
 */
public class PaimonTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(PaimonTransaction.class);

    private final PaimonMetadataOps ops;
    private Table table;
    private BatchWriteBuilder writeBuilder;
    private BatchTableCommit committer;
    private final List<TPaimonCommitData> commitDataList = Lists.newArrayList();
    private PaimonInsertCommandContext insertCtx;

    public PaimonTransaction(PaimonMetadataOps ops) {
        this.ops = ops;
    }

    public void updatePaimonCommitData(List<TPaimonCommitData> commitDataList) {
        synchronized (this) {
            this.commitDataList.addAll(commitDataList);
        }
    }

    public void beginInsert(PaimonExternalTable dorisTable, Optional<InsertCommandContext> ctx)
            throws UserException {
        ctx.ifPresent(c -> this.insertCtx = (PaimonInsertCommandContext) c);
        try {
            Catalog paimonCatalog = ((PaimonExternalCatalog) dorisTable.getCatalog()).getPaimonCatalog();
            Identifier identifier = Identifier.create(dorisTable.getDbName(), dorisTable.getName());
            this.table = paimonCatalog.getTable(identifier);

            this.writeBuilder = table.newBatchWriteBuilder();
            if (insertCtx != null && insertCtx.isOverwrite()) {
                this.writeBuilder = this.writeBuilder.withOverwrite();
            }
            this.committer = writeBuilder.newCommit();
            LOG.info("Paimon insert transaction started for table: {}", dorisTable.getName());
        } catch (Exception e) {
            throw new UserException("Failed to begin insert for paimon table " + dorisTable.getName()
                    + " because: " + e.getMessage(), e);
        }
    }

    public void finishInsert() {
        LOG.debug("Paimon insert finished with {} commit data entries", commitDataList.size());
    }

    @Override
    public void commit() throws UserException {
        try {
            List<CommitMessage> commitMessages = buildCommitMessages();
            committer.commit(commitMessages);
            LOG.info("Paimon transaction committed with {} files", commitMessages.size());
        } catch (Exception e) {
            throw new UserException("Failed to commit paimon transaction: " + e.getMessage(), e);
        } finally {
            closeResources();
        }
    }

    @Override
    public void rollback() {
        LOG.info("Paimon transaction rolled back");
        closeResources();
    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TPaimonCommitData::getRowCount).sum();
    }

    /**
     * Build Paimon CommitMessage objects from file metadata collected from BE nodes.
     * Each TPaimonCommitData corresponds to one ORC/Parquet file written by a BE.
     */
    private List<CommitMessage> buildCommitMessages() throws Exception {
        if (commitDataList.isEmpty()) {
            return Collections.emptyList();
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        long schemaId = fileStoreTable.schema().id();
        List<String> partitionKeys = table.partitionKeys();
        RowType partitionType = table.rowType().project(partitionKeys);

        List<CommitMessage> messages = new ArrayList<>();
        for (TPaimonCommitData data : commitDataList) {
            // Extract just the file name from the full path
            String filePath = data.getFilePath();
            String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);

            DataFileMeta fileMeta = DataFileMeta.forAppend(
                    fileName,
                    data.getFileSize(),
                    data.getRowCount(),
                    SimpleStats.EMPTY_STATS,
                    0L,     // minSequenceNumber
                    0L,     // maxSequenceNumber
                    schemaId,
                    Collections.emptyList(),
                    null,                // embeddedIndex
                    FileSource.APPEND,
                    null,                // valueStatsCols
                    null);               // externalPath

            List<String> partitionValues = data.isSetPartitionValues()
                    ? data.getPartitionValues() : Collections.emptyList();
            BinaryRow partition = createPartitionBinaryRow(partitionValues, partitionType);

            CommitMessage message = new CommitMessageImpl(
                    partition,
                    0, // bucket-0
                    null, // totalBuckets
                    new DataIncrement(
                            Collections.singletonList(fileMeta),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    CompactIncrement.emptyIncrement());

            messages.add(message);
        }
        return messages;
    }

    private BinaryRow createPartitionBinaryRow(List<String> partitionValues, RowType partitionType) {
        if (partitionValues.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }
        GenericRow row = new GenericRow(partitionValues.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            DataType fieldType = partitionType.getTypeAt(i);
            row.setField(i, convertPartitionValue(partitionValues.get(i), fieldType));
        }
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        return serializer.toBinaryRow(row).copy();
    }

    private Object convertPartitionValue(String value, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case VARCHAR:
            case CHAR:
                return BinaryString.fromString(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DATE:
                // Paimon stores dates as int (days since epoch)
                return DateTimeUtils.parseDate(value);
            default:
                // Fallback to string representation
                return BinaryString.fromString(value);
        }
    }

    private void closeResources() {
        try {
            if (committer != null) {
                committer.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close paimon committer", e);
        }
    }
}
