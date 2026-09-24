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

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Collections;
import java.util.List;

/** A filtered HMS partition page together with its size before the metastore filter hook runs. */
public final class HmsRawPartitionFilterPage {
    private final List<Partition> partitions;
    private final int rawCount;

    public HmsRawPartitionFilterPage(List<Partition> partitions, int rawCount) {
        this.partitions = Collections.unmodifiableList(partitions);
        this.rawCount = rawCount;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public int getRawCount() {
        return rawCount;
    }
}
