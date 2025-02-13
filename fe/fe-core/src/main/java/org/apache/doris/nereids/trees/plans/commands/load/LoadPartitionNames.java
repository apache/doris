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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * load LoadPartitionNames for nereids
 */
public class LoadPartitionNames implements LoadProperty {
    @SerializedName(value = "partitionNames")
    private final List<String> partitionNames;
    // true if these partitions are temp partitions
    @SerializedName(value = "isTemp")
    private final boolean isTemp;
    private final boolean isStar;
    private final long count;

    public LoadPartitionNames(boolean isTemp, List<String> partitionNames) {
        this.isTemp = isTemp;
        this.partitionNames = partitionNames;
        this.isStar = false;
        this.count = 0;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isTemp() {
        return isTemp;
    }

    @Override
    public void validate() throws AnalysisException {
        if (isStar && count > 0) {
            throw new AnalysisException("All partition and partition count couldn't be set at the same time.");
        }
        if (isStar || count > 0) {
            return;
        }
        if (partitionNames == null || partitionNames.isEmpty()) {
            throw new AnalysisException("No partition specified in partition lists");
        }
        // check if partition name is not empty string
        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new AnalysisException("there are empty partition name");
        }
    }
}
