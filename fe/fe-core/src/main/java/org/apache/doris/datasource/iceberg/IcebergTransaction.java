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
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.transaction.Transaction;

import com.google.common.base.VerifyException;
import com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class IcebergTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(IcebergTransaction.class);
    private final IcebergMetadataOps ops;
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

    public void beginInsert(String dbName, String tbName) {
        Table icebergTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbName));
        transaction = icebergTable.newTransaction();
    }

    public void finishInsert() {
        Table icebergTable = transaction.table();
        AppendFiles appendFiles = transaction.newAppend();

        for (CommitTaskData task : convertToCommitTaskData()) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(IcebergUtils.getFileFormat(icebergTable))
                    .withMetrics(task.getMetrics());

            if (icebergTable.spec().isPartitioned()) {
                List<String> partitionValues = task.getPartitionValues()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartitionValues(partitionValues);
            }
            appendFiles.appendFile(builder.build());
        }

        // in appendFiles.commit, it will generate metadata(manifest and snapshot)
        // after appendFiles.commit, in current transaction, you can already see the new snapshot
        appendFiles.commit();
    }

    public List<CommitTaskData> convertToCommitTaskData() {
        List<CommitTaskData> commitTaskData = new ArrayList<>();
        for (TIcebergCommitData data : this.commitDataList) {
            commitTaskData.add(new CommitTaskData(
                    data.getFilePath(),
                    data.getFileSize(),
                    new Metrics(
                            data.getRowCount(),
                            Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP,
                            Collections.EMPTY_MAP
                    ),
                    data.isSetPartitionValues() ? Optional.of(data.getPartitionValues()) : Optional.empty(),
                    convertToFileContent(data.getFileContent()),
                    data.isSetReferencedDataFiles() ? Optional.of(data.getReferencedDataFiles()) : Optional.empty()
            ));
        }
        return commitTaskData;
    }

    private FileContent convertToFileContent(TFileContent content) {
        if (content.equals(TFileContent.DATA)) {
            return FileContent.DATA;
        } else if (content.equals(TFileContent.POSITION_DELETES)) {
            return FileContent.POSITION_DELETES;
        } else {
            return FileContent.EQUALITY_DELETES;
        }
    }

    @Override
    public void commit() throws UserException {
        // Externally readable
        // Manipulate the relevant data so that others can also see the latest table, such as:
        //   1. hadoop: it will change the version number information in 'version-hint.text'
        //   2. hive: it will change the table properties, the most important thing is to revise 'metadata_location'
        //   3. and so on ...
        transaction.commitTransaction();
    }

    @Override
    public void rollback() {

    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TIcebergCommitData::getRowCount).sum();
    }

    public static class CommitTaskData {
        private final String path;
        private final long fileSizeInBytes;
        private final Metrics metrics;
        private final Optional<List<String>> partitionValues;
        private final FileContent content;
        private final Optional<List<String>> referencedDataFiles;

        public CommitTaskData(String path,
                              long fileSizeInBytes,
                              Metrics metrics,
                              Optional<List<String>> partitionValues,
                              FileContent content,
                              Optional<List<String>> referencedDataFiles) {
            this.path = path;
            this.fileSizeInBytes = fileSizeInBytes;
            this.metrics = metrics;
            this.partitionValues = partitionValues;
            this.content = content;
            this.referencedDataFiles = referencedDataFiles;
        }

        public String getPath() {
            return path;
        }

        public long getFileSizeInBytes() {
            return fileSizeInBytes;
        }

        public Metrics getMetrics() {
            return metrics;
        }

        public Optional<List<String>> getPartitionValues() {
            return partitionValues;
        }

        public FileContent getContent() {
            return content;
        }

        public Optional<List<String>> getReferencedDataFiles() {
            return referencedDataFiles;
        }
    }
}
