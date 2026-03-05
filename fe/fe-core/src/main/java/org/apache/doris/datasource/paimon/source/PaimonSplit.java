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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.SplitCreator;
import org.apache.doris.datasource.TableFormatType;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class PaimonSplit extends FileSplit {
    private static final LocationPath DUMMY_PATH = LocationPath.of("/dummyPath");
    // Paimon split - can be DataSplit or other Split types (e.g., from system tables)
    private Split paimonSplit;
    private TableFormatType tableFormatType;
    private Optional<DeletionFile> optDeletionFile = Optional.empty();
    private Optional<Long> optRowCount = Optional.empty();
    private Optional<Long> schemaId = Optional.empty();
    private Map<String, String> paimonPartitionValues = null;

    /**
     * Constructor for Paimon splits.
     * Handles both DataSplit (regular data tables) and other Split types (system tables).
     */
    public PaimonSplit(Split paimonSplit) {
        super(DUMMY_PATH, 0, 0, 0, 0, null, null);
        this.paimonSplit = paimonSplit;
        this.tableFormatType = TableFormatType.PAIMON;

        if (paimonSplit instanceof DataSplit) {
            // For DataSplit, extract file info for path and weight calculation
            DataSplit dataSplit = (DataSplit) paimonSplit;
            List<DataFileMeta> dataFileMetas = dataSplit.dataFiles();
            this.path = LocationPath.of("/" + dataFileMetas.get(0).fileName());
            this.selfSplitWeight = dataFileMetas.stream().mapToLong(DataFileMeta::fileSize).sum();
        } else {
            // For non-DataSplit (e.g., system tables), use row count as weight
            this.selfSplitWeight = paimonSplit.rowCount();
        }
    }

    private PaimonSplit(LocationPath file, long start, long length, long fileLength, long modificationTime,
            String[] hosts, List<String> partitionList) {
        super(file, start, length, fileLength, modificationTime, hosts, partitionList);
        this.tableFormatType = TableFormatType.PAIMON;
        this.selfSplitWeight = length;
    }

    @Override
    public String getConsistentHashString() {
        if (this.path == DUMMY_PATH) {
            return UUID.randomUUID().toString();
        }
        return getPathString();
    }

    /**
     * Returns the underlying Paimon split.
     * For JNI reader serialization.
     */
    public Split getSplit() {
        return paimonSplit;
    }

    /**
     * Returns the split as DataSplit if it's a DataSplit instance.
     * Returns null if this is a non-DataSplit system table split.
     */
    public DataSplit getDataSplit() {
        return paimonSplit instanceof DataSplit ? (DataSplit) paimonSplit : null;
    }

    public TableFormatType getTableFormatType() {
        return tableFormatType;
    }

    public void setTableFormatType(TableFormatType tableFormatType) {
        this.tableFormatType = tableFormatType;
    }

    public Optional<DeletionFile> getDeletionFile() {
        return optDeletionFile;
    }

    public void setDeletionFile(DeletionFile deletionFile) {
        this.selfSplitWeight += deletionFile.length();
        this.optDeletionFile = Optional.of(deletionFile);
    }

    public Optional<Long> getRowCount() {
        return optRowCount;
    }

    public void setRowCount(long rowCount) {
        this.optRowCount = Optional.of(rowCount);
    }

    public void setSchemaId(long schemaId) {
        this.schemaId = Optional.of(schemaId);
    }

    public Long getSchemaId() {
        return schemaId.orElse(null);
    }

    public void setPaimonPartitionValues(Map<String, String> paimonPartitionValues) {
        this.paimonPartitionValues = paimonPartitionValues;
    }

    public Map<String, String> getPaimonPartitionValues() {
        return paimonPartitionValues;
    }

    public static class PaimonSplitCreator implements SplitCreator {

        static final PaimonSplitCreator DEFAULT = new PaimonSplitCreator();

        @Override
        public org.apache.doris.spi.Split create(LocationPath path,
                long start,
                long length,
                long fileLength,
                long fileSplitSize,
                long modificationTime,
                String[] hosts,
                List<String> partitionValues) {
            PaimonSplit split = new PaimonSplit(path, start, length, fileLength,
                    modificationTime, hosts, partitionValues);
            split.setTargetSplitSize(fileSplitSize);
            return split;
        }
    }
}
