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

package org.apache.doris.cloud.common.util;

import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.ImmutableMap;

public class CloudPropertyAnalyzer extends PropertyAnalyzer {

    public CloudPropertyAnalyzer() {
        forceProperties = ImmutableMap.<String, String>builder()
                .put(PropertyAnalyzer.PROPERTIES_INMEMORY, "true")
                //.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, null)
                .put(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, "")
                .put(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, "")
                .put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, "")
                .put(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM, "-1")
                .put(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION, "false")
                .put(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE, "true")
                .put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                        String.valueOf(ReplicaAllocation.DEFAULT_ALLOCATION.getTotalReplicaNum()))
                .put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        ReplicaAllocation.DEFAULT_ALLOCATION.toCreateStmt())
                //.put(DynamicPartitionProperty.PROPERTIES_STORAGE_MEDIUM, "")
                .put(DynamicPartitionProperty.REPLICATION_NUM,
                        String.valueOf(ReplicaAllocation.DEFAULT_ALLOCATION.getTotalReplicaNum()))
                .put(DynamicPartitionProperty.REPLICATION_ALLOCATION,
                        ReplicaAllocation.DEFAULT_ALLOCATION.toCreateStmt())
                .build();
    }

}
