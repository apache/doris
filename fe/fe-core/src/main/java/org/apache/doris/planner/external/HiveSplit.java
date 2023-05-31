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

import lombok.Data;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.util.List;

@Data
public class HiveSplit extends FileSplit {
    private long fileSize;
    // The values of partitions.
    // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
    // partitionValues would be ["part1", "part2"]
    protected List<String> partitionValues;

    public HiveSplit(Path file, long start, long length, long fileSize, String[] hosts, List<String> partitionValues) {
        super(file, start, length, hosts);
        this.fileSize = fileSize;
        this.partitionValues = partitionValues;
    }

    protected TableFormatType tableFormatType;
}
