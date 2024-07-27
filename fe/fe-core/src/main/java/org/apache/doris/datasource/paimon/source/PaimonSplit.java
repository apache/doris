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

import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.SplitCreator;
import org.apache.doris.datasource.SplitWeight;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.qe.ConnectContext;

import org.apache.hadoop.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PaimonSplit extends FileSplit {
    private Split split;
    private TableFormatType tableFormatType;
    private Optional<DeletionFile> optDeletionFile;
    private long rowCnt;

    private static final Path DUMMY_PATH = new Path("hdfs://dummyPath");

    public PaimonSplit(Split split) {
        super(DUMMY_PATH, 0, 0, 0, null, null);
        this.split = split;
        this.tableFormatType = TableFormatType.PAIMON;
        this.optDeletionFile = Optional.empty();
        this.rowCnt = split.rowCount();

        if (split instanceof DataSplit) {
            StringBuilder sb = new StringBuilder();
            sb.append("hdfs://");
            long len = 0;
            for (DataFileMeta dataFile : ((DataSplit) split).dataFiles()) {
                sb.append(dataFile.fileName());
                len += dataFile.fileSize();
            }
            this.path = new Path(sb.toString());
            this.length = len;
        }
    }

    public PaimonSplit(Path file, long start, long length, long fileLength, String[] hosts,
            List<String> partitionList) {
        super(file, start, length, fileLength, hosts, partitionList);
        this.tableFormatType = TableFormatType.PAIMON;
        this.optDeletionFile = Optional.empty();
    }

    @Override
    public String getConsistentHashString() {
        if (this.path == DUMMY_PATH) {
            return UUID.randomUUID().toString();
        }
        return getPathString();
    }

    public Split getSplit() {
        return split;
    }

    public void setSplit(Split split) {
        this.split = split;
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
        this.optDeletionFile = Optional.of(deletionFile);
    }

    public void setRowCnt(Long rowCnt) {
        this.rowCnt = rowCnt;
    }

    @Override
    public SplitWeight getSplitWeight() {
        int way = ConnectContext.get().getSessionVariable().getSplitWeightWay();
        if (way == 1) {
            // jni的split，cnt是准的
            // native的split，
            //     1. 如果带了delete，就会少delete的rowCnt
            //     2. 如果native的文件被分割了，那这个rowCnt就多了，因为这个rowCnt是整个文件的的rowCnt，分割之后，不知道小文件的rowCnt是多少
            //     3. 如果文件没有分割，也没有delete，那rowCnt就是准的
            return SplitWeight.fromRawValueInternal(rowCnt);
        } else if (way == 2) {
            // jni的split的length是准的
            // native的split：
            //     1. 如果带了delete，就会少了delete的length
            //     2. 如果没有delete，那len就是准的
            return SplitWeight.fromRawValueInternal(getLength());
        } else {
            return SplitWeight.STANDARD_WEIGHT;
        }
    }

    public static class PaimonSplitCreator implements SplitCreator {

        static final PaimonSplitCreator DEFAULT = new PaimonSplitCreator();

        @Override
        public org.apache.doris.spi.Split create(Path path,
                long start,
                long length,
                long fileLength,
                long modificationTime,
                String[] hosts,
                List<String> partitionValues) {
            return new PaimonSplit(path, start, length, fileLength, hosts, partitionValues);
        }
    }
}
