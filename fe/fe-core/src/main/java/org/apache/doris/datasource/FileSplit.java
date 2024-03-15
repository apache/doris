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

package org.apache.doris.datasource;

import org.apache.doris.spi.Split;

import lombok.Data;
import org.apache.hadoop.fs.Path;

import java.util.List;

@Data
public class FileSplit implements Split {
    public Path path;
    public long start;
    // length of this split, in bytes
    public long length;
    // length of the file this split belongs to, in bytes
    // -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    public long fileLength;
    public long modificationTime;
    public String[] hosts;
    public TableFormatType tableFormatType;
    // The values of partitions.
    // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
    // partitionValues would be ["part1", "part2"]
    public List<String> partitionValues;

    public List<String> alternativeHosts;

    public FileSplit(Path path, long start, long length, long fileLength,
            long modificationTime, String[] hosts, List<String> partitionValues) {
        this.path = path;
        this.start = start;
        this.length = length;
        this.fileLength = fileLength;
        this.modificationTime = modificationTime;
        this.hosts = hosts == null ? new String[0] : hosts;
        this.partitionValues = partitionValues;
    }

    public FileSplit(Path path, long start, long length, long fileLength,
            String[] hosts, List<String> partitionValues) {
        this(path, start, length, fileLength, 0, hosts, partitionValues);
    }

    public String[] getHosts() {
        return hosts;
    }

    @Override
    public Object getInfo() {
        return null;
    }

    @Override
    public String getPathString() {
        return path.toString();
    }

    public static class FileSplitCreator implements SplitCreator {

        public static final FileSplitCreator DEFAULT = new FileSplitCreator();

        @Override
        public Split create(Path path, long start, long length, long fileLength, long modificationTime, String[] hosts,
                List<String> partitionValues) {
            return new FileSplit(path, start, length, fileLength, modificationTime, hosts, partitionValues);
        }
    }
}
