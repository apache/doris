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

package org.apache.doris.planner.external;

import org.apache.doris.planner.Split;

import lombok.Data;
import org.apache.hadoop.fs.Path;

import java.util.List;

@Data
public class FileSplit extends Split {
    protected Path path;
    protected long start;
    // length of this split, in bytes
    protected long length;
    // length of the file this split belongs to, in bytes
    // -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    protected long fileLength;
    protected TableFormatType tableFormatType;
    // The values of partitions.
    // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
    // partitionValues would be ["part1", "part2"]
    protected List<String> partitionValues;

    public FileSplit(Path path, long start, long length, long fileLength,
                     String[] hosts, List<String> partitionValues) {
        this.path = path;
        this.start = start;
        this.length = length;
        this.fileLength = fileLength;
        this.hosts = hosts;
        this.partitionValues = partitionValues;
    }

    public String[] getHosts() {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }
}
