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

package org.apache.doris.datasource.hive;

import lombok.Data;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * A partition to commit for HMS.
 */
@Data
public class PartitionToCommit {
    private final String partitionName;
    private final Path stagingPath;
    private final Path targetPath;
    private final List<String> fileNames;
    private final long totalRowCount;
    private final long totalSizeInBytes;
    private CommitMode commitMode;

    public PartitionToCommit(String partitionName, Path stagingPath, Path targetPath,
                             List<String> fileNames, long totalRowCount, long totalSizeInBytes) {
        this.partitionName = partitionName;
        this.stagingPath = stagingPath;
        this.targetPath = targetPath;
        this.fileNames = fileNames;
        this.totalRowCount = totalRowCount;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public enum CommitMode {
        NEW,
        APPEND,
        OVERWRITE
    }
}
