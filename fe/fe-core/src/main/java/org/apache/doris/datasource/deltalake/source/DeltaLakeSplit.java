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

package org.apache.doris.datasource.deltalake.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.deltalake.DeletionVectorDescriptorInfo;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a single data file split for a Delta Lake table scan.
 * Extends FileSplit with Delta Lake-specific metadata: partition values
 * and deletion vector descriptor.
 */
public class DeltaLakeSplit extends FileSplit {
    private final Map<String, String> deltaPartitionValues;
    private final DeletionVectorDescriptorInfo dvInfo;

    public DeltaLakeSplit(LocationPath path, long start, long length, long fileLength,
            long modificationTime, String[] hosts,
            Map<String, String> deltaPartitionValues,
            DeletionVectorDescriptorInfo dvInfo) {
        super(path, start, length, fileLength, modificationTime, hosts, Collections.emptyList());
        this.deltaPartitionValues = deltaPartitionValues;
        this.dvInfo = dvInfo;
    }

    public Map<String, String> getDeltaPartitionValues() {
        return deltaPartitionValues;
    }

    public DeletionVectorDescriptorInfo getDvInfo() {
        return dvInfo;
    }

    public boolean hasDeletionVector() {
        return dvInfo != null;
    }
}
