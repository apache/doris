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

package org.apache.doris.datasource.iceberg.helper;

import org.apache.doris.datasource.statistics.CommonStatistics;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.VerifyException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergWriterHelper {

    private static final int DEFAULT_FILE_COUNT = 1;

    public static WriteResult convertToWriterResult(
            FileFormat format,
            PartitionSpec spec,
            List<TIcebergCommitData> commitDataList) {
        List<DataFile> dataFiles = new ArrayList<>();
        for (TIcebergCommitData commitData : commitDataList) {
            //get the files path
            String location = commitData.getFilePath();

            //get the commit file statistics
            long fileSize = commitData.getFileSize();
            long recordCount = commitData.getRowCount();
            CommonStatistics stat = new CommonStatistics(recordCount, DEFAULT_FILE_COUNT, fileSize);

            Optional<List<String>> partValues = Optional.empty();
            //get and check partitionValues when table is partitionedTable
            if (spec.isPartitioned()) {
                List<String> partitionValues = commitData.getPartitionValues();
                if (Objects.isNull(partitionValues) || partitionValues.isEmpty()) {
                    throw new VerifyException("No partition data for partitioned table");
                }
                partitionValues = partitionValues.stream().map(s -> s.equals("null") ? null : s)
                        .collect(Collectors.toList());
                partValues = Optional.of(partitionValues);
            }
            DataFile dataFile = genDataFile(format, location, spec, partValues, stat);
            dataFiles.add(dataFile);
        }
        return WriteResult.builder()
                .addDataFiles(dataFiles)
                .build();

    }

    public static DataFile genDataFile(
            FileFormat format,
            String location,
            PartitionSpec spec,
            Optional<List<String>> partValues,
            CommonStatistics statistics) {

        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(location)
                .withFileSizeInBytes(statistics.getTotalFileBytes())
                .withRecordCount(statistics.getRowCount())
                .withFormat(format);

        partValues.ifPresent(builder::withPartitionValues);

        return builder.build();
    }
}
