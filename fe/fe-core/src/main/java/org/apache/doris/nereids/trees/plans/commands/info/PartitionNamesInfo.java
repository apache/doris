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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * PartitionNamesInfo
 */
public class PartitionNamesInfo {
    // Default partition count to collect statistic for external table.
    private static final long DEFAULT_PARTITION_COUNT = 100;
    private final List<String> partitionNames;
    // true if these partitions are temp partitions
    private final boolean isTemp;
    private final boolean isStar;
    private final long count;

    public PartitionNamesInfo(boolean isTemp, List<String> partitionNames) {
        this.partitionNames = partitionNames;
        this.isTemp = isTemp;
        this.isStar = false;
        this.count = 0;
    }

    public PartitionNamesInfo(PartitionNamesInfo other) {
        this.partitionNames = Lists.newArrayList(other.partitionNames);
        this.isTemp = other.isTemp;
        this.isStar = other.isStar;
        this.count = 0;
    }

    public PartitionNamesInfo(boolean isStar) {
        this.partitionNames = null;
        this.isTemp = false;
        this.isStar = isStar;
        this.count = 0;
    }

    public PartitionNamesInfo(long partitionCount) {
        this.partitionNames = null;
        this.isTemp = false;
        this.isStar = false;
        this.count = partitionCount;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public boolean isStar() {
        return isStar;
    }

    public long getCount() {
        return count;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
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

    /**
     * toSql
     */
    public String toSql() {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (isTemp) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITIONS (");
        sb.append(Joiner.on(", ").join(partitionNames));
        sb.append(")");
        return sb.toString();
    }

    public PartitionNames translateToLegacyPartitionNames() {
        return new PartitionNames(isTemp, partitionNames, isStar, count);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
