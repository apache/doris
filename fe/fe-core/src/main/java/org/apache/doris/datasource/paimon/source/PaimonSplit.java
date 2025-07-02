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

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PaimonSplit extends FileSplit {
    private static final LocationPath DUMMY_PATH = LocationPath.of("/dummyPath");
    private DataSplit split;
    private TableFormatType tableFormatType;
    private Optional<DeletionFile> optDeletionFile = Optional.empty();
    private Optional<Long> optRowCount = Optional.empty();
    private Optional<Long> schemaId = Optional.empty();

    public PaimonSplit(DataSplit split) {
        super(DUMMY_PATH, 0, 0, 0, 0, null, null);
        this.split = split;
        this.tableFormatType = TableFormatType.PAIMON;

        List<DataFileMeta> dataFileMetas = split.dataFiles();
        this.path = LocationPath.of("/" + dataFileMetas.get(0).fileName());
        this.selfSplitWeight = dataFileMetas.stream().mapToLong(DataFileMeta::fileSize).sum();

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

    public DataSplit getSplit() {
        return split;
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
