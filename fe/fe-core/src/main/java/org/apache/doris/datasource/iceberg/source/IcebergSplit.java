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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.datasource.FileSplit;

import lombok.Data;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;

@Data
public class IcebergSplit extends FileSplit {

    private final String originalPath;

    // File path will be changed if the file is modified, so there's no need to get modification time.
    public IcebergSplit(Path file, long start, long length, long fileLength, String[] hosts,
                        Integer formatVersion, Map<String, String> config,
                        List<String> partitionList, String originalPath) {
        super(file, start, length, fileLength, hosts, partitionList);
        this.formatVersion = formatVersion;
        this.config = config;
        this.originalPath = originalPath;
    }

    private Integer formatVersion;
    private List<IcebergDeleteFileFilter> deleteFileFilters;
    private Map<String, String> config;
}


