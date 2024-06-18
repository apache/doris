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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.datasource.FileSplit;

import java.util.Optional;

public class MaxComputeSplit extends FileSplit {
    private final Optional<String> partitionSpec;

    public MaxComputeSplit(FileSplit rangeSplit) {
        super(rangeSplit.path, rangeSplit.start, rangeSplit.length, rangeSplit.fileLength,
                rangeSplit.hosts, rangeSplit.partitionValues);
        this.partitionSpec = Optional.empty();
    }

    public MaxComputeSplit(String partitionSpec, FileSplit rangeSplit) {
        super(rangeSplit.path, rangeSplit.start, rangeSplit.length, rangeSplit.fileLength,
                rangeSplit.hosts, rangeSplit.partitionValues);
        this.partitionSpec = Optional.of(partitionSpec);
    }

    public Optional<String> getPartitionSpec() {
        return partitionSpec;
    }
}
