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

package org.apache.doris.nereids.trees.plans.commands.insert;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;

/**
 * For iceberg External Table
 */
public class IcebergInsertCommandContext extends BaseExternalTableInsertCommandContext {
    private Optional<String> branchName = Optional.empty();
    // Static partition key-value pairs for INSERT OVERWRITE ... PARTITION
    // (col='val', ...)
    private Map<String, String> staticPartitionValues = Maps.newHashMap();

    public Optional<String> getBranchName() {
        return branchName;
    }

    public void setBranchName(Optional<String> branchName) {
        this.branchName = branchName;
    }

    public Map<String, String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    public void setStaticPartitionValues(Map<String, String> staticPartitionValues) {
        this.staticPartitionValues = staticPartitionValues != null
                ? Maps.newHashMap(staticPartitionValues)
                : Maps.newHashMap();
    }

    /**
     * Check if this is a static partition overwrite
     */
    public boolean isStaticPartitionOverwrite() {
        return isOverwrite() && !staticPartitionValues.isEmpty();
    }
}
